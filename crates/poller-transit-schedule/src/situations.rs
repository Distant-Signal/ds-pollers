//! The situations loop (transit spec §6.1, §10.1), every 5 minutes: the
//! BODS SIRI-SX bulk archive (a 302 to a signed, expiring download URL,
//! fetched with `If-None-Match`), filtered and normalised by
//! [`crate::adapter::sirisx`], sent as one `transit-situations/1` entry on
//! its own latest-snapshot producer (C3: it must not supersede an unsent
//! timetable).
//!
//! The ingest-writer has no handler for the schema until batch 4, and its
//! `transit-<network>` stream stays `off` until then: an applied stream
//! would dead-letter these entries.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use common::transit::{SituationScope, TransitSituations, schema};
use ds_runtime::metrics::metric_name;
use ingest_stream::snapshot::Snapshot;
use ingest_stream::{Envelope, SchemaId};

use crate::Context;
use crate::adapter::diagnostics::{
    Class, Failure, PREVIEW_BYTES, ResponseInfo, Seen, attempted, has_zip_magic, report,
    retry_pause, safe_url_str, unreadable_body,
};
use crate::adapter::{FeedError, Retry, sirisx};

/// The archive's download cap (290 KB on 2026-10-09).
pub(crate) const MAX_ARCHIVE_BYTES: u64 = 50 * 1024 * 1024;

/// `transit_situations_current{network, scope}` (gauge).
pub(crate) const SITUATIONS_CURRENT: &str = "transit_situations_current";
/// `transit_situation_unmapped_line_refs_total{network, ref}`.
pub(crate) const UNMAPPED_LINE_REFS: &str = "transit_situation_unmapped_line_refs_total";
/// `transit_situations_skipped_total{network, owner}`: situations in the
/// file skipped for an unreadable time (`owner` is `network` for the
/// network's own, else `other`).
pub(crate) const SITUATIONS_SKIPPED: &str = "transit_situations_skipped_total";

/// One attempt: the archive and its `ETag`, or `None` when it is
/// unchanged (HTTP 304). The body is checked to be a readable zip, so a
/// 200 with an error page or a cut archive fails here with what the
/// response looked like ([`diagnostics::report`]).
#[expect(clippy::too_many_lines, reason = "one request, checked at each step")]
async fn fetch_once(ctx: &Context) -> Result<Option<(Vec<u8>, Option<String>)>, Box<Failure>> {
    let target = safe_url_str(&ctx.config.situations_url);
    let mut seen = Seen::default();
    let fail = |seen: &Seen, class, retryable, err| {
        Box::new(Failure {
            err,
            class,
            retryable,
            seen: seen.clone(),
        })
    };
    // reqwest's own text names the URL, query included, so it is left out.
    let transport = |seen: &Seen, err: reqwest::Error| {
        let class = if seen.info.is_some() {
            Class::TruncatedZip
        } else {
            Class::Error
        };
        fail(
            seen,
            class,
            true,
            FeedError::Io(format!("SIRI-SX download: {}", err.without_url())),
        )
    };
    let mut request = ctx.client.get(&ctx.config.situations_url);
    let etag = ctx.situations_etag.lock().ok().and_then(|e| e.clone());
    if let Some(etag) = &etag {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let mut response = request.send().await.map_err(|err| transport(&seen, err))?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    let info = ResponseInfo::from_response(&response);
    seen.info = Some(info.clone());
    let status = response.status();
    if !status.is_success() {
        let _ = poller_http::check_status(&response, "transit-situations");
        let after = info
            .retry_after
            .map_or_else(String::new, |d| format!(", retry-after {}s", d.as_secs()));
        return Err(fail(
            &seen,
            Class::HttpStatus,
            status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
            FeedError::Io(format!(
                "SIRI-SX download: HTTP {status} from {target}{after}"
            )),
        ));
    }
    seen.declared = response.content_length();
    let oversize = |seen: &Seen| {
        fail(
            seen,
            Class::Error,
            false,
            FeedError::Oversize("the SIRI-SX archive".to_owned()),
        )
    };
    if seen.declared.is_some_and(|len| len > MAX_ARCHIVE_BYTES) {
        return Err(oversize(&seen));
    }
    let new_etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut body = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(err) => return Err(transport(&seen, err)),
        };
        if seen.head.len() < PREVIEW_BYTES {
            let take = (PREVIEW_BYTES - seen.head.len()).min(chunk.len());
            seen.head.extend_from_slice(&chunk[..take]);
        }
        body.extend_from_slice(&chunk);
        seen.received = body.len() as u64;
        if seen.received > MAX_ARCHIVE_BYTES {
            return Err(oversize(&seen));
        }
    }
    if let Some(declared) = seen.declared
        && declared != seen.received
    {
        return Err(fail(
            &seen,
            Class::TruncatedZip,
            true,
            FeedError::Io(format!(
                "SIRI-SX download: received {} of {declared} bytes",
                seen.received
            )),
        ));
    }
    let readable = if has_zip_magic(&seen.head) {
        zip::ZipArchive::new(std::io::Cursor::new(&body))
            .map(|_| ())
            .map_err(|err| err.to_string())
    } else {
        Err("no zip signature".to_owned())
    };
    match readable {
        Ok(()) => Ok(Some((body, new_etag))),
        Err(zip_error) => Err(unreadable_body(seen, &zip_error, "SIRI-SX: ")),
    }
}

/// The archive, or `None` when it is unchanged (HTTP 304), with the
/// retries of [`crate::adapter::download_zip_to_file`] (a truncated body
/// again, a page that is not a zip once, a 429 or 5xx with its short
/// `Retry-After`), then the failure.
async fn download(ctx: &Context) -> Result<Option<Vec<u8>>, FeedError> {
    let retry = Retry::new(&ctx.network.id, "sirisx");
    let started = std::time::Instant::now();
    let mut attempt = 1;
    loop {
        let failure = match fetch_once(ctx).await {
            Ok(Some((body, new_etag))) => {
                attempted(retry.network, retry.source, "ok");
                if let Ok(mut slot) = ctx.situations_etag.lock() {
                    *slot = new_etag;
                }
                return Ok(Some(body));
            }
            Ok(None) => return Ok(None),
            Err(failure) => failure,
        };
        report(retry.network, retry.source, attempt, &failure);
        let Some(pause) = retry_pause(
            &failure,
            attempt,
            retry.attempts,
            retry.backoff,
            started.elapsed(),
            retry.window,
        ) else {
            return Err(failure.err);
        };
        tokio::time::sleep(pause).await;
        attempt += 1;
    }
}

/// Counts (and, for the network's own, logs) the situations skipped for an
/// unreadable time.
fn record_skipped(network: &str, found: &sirisx::Situations) {
    let ours = found.skipped_ours.len();
    for (owner, n) in [("network", ours), ("other", found.skipped - ours)] {
        if n > 0 {
            metrics::counter!(metric_name(SITUATIONS_SKIPPED), "network" => network.to_owned(), "owner" => owner)
                .increment(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }
    if ours > 0 {
        tracing::warn!(
            %network,
            situations = ?found.skipped_ours,
            "skipped the network's situations with an unreadable time"
        );
    }
}

/// One situations cycle.
pub(crate) async fn poll_once(ctx: &Context) -> Result<()> {
    let produced_at = Utc::now();
    let network = ctx.network.id.clone();
    let body = match download(ctx).await {
        Ok(Some(body)) => body,
        Ok(None) => {
            tracing::debug!(%network, "SIRI-SX unchanged (304)");
            return Ok(());
        }
        Err(err) => {
            metrics::counter!(
                metric_name(crate::timetable::FEED_FETCH),
                "network" => network.clone(), "source" => "sirisx", "outcome" => "error"
            )
            .increment(1);
            return Err(err.into());
        }
    };
    let config = ctx.network.clone();
    let parsed = tokio::task::spawn_blocking(move || {
        sirisx::parse_archive(std::io::Cursor::new(body), &config)
    })
    .await
    .context("the SIRI-SX parse task failed")?;
    let found = match parsed {
        Ok(found) => found,
        Err(err) => {
            metrics::counter!(
                metric_name(crate::timetable::FEED_FETCH),
                "network" => network.clone(), "source" => "sirisx", "outcome" => "error"
            )
            .increment(1);
            // Drop the ETag, so the next cycle fetches the archive again.
            if let Ok(mut slot) = ctx.situations_etag.lock() {
                *slot = None;
            }
            return Err(err.into());
        }
    };
    metrics::counter!(
        metric_name(crate::timetable::FEED_FETCH),
        "network" => network.clone(), "source" => "sirisx", "outcome" => "ok"
    )
    .increment(1);
    for (line_ref, n) in &found.unmapped_line_refs {
        metrics::counter!(metric_name(UNMAPPED_LINE_REFS), "network" => network.clone(), "ref" => line_ref.clone())
            .increment(*n);
    }
    record_skipped(&network, &found);
    let mut scopes: BTreeMap<&'static str, usize> =
        [("line", 0), ("stop", 0), ("network", 0)].into();
    for situation in &found.situations {
        let scope = match situation.scope {
            SituationScope::Line => "line",
            SituationScope::Stop => "stop",
            SituationScope::Network => "network",
        };
        *scopes.entry(scope).or_default() += 1;
    }
    for (scope, n) in scopes {
        #[expect(clippy::cast_precision_loss, reason = "a small count")]
        metrics::gauge!(metric_name(SITUATIONS_CURRENT), "network" => network.clone(), "scope" => scope)
            .set(n as f64);
    }
    tracing::info!(
        %network,
        seen = found.seen,
        kept = found.situations.len(),
        skipped = found.skipped,
        unmapped_line_refs = found.unmapped_line_refs.len(),
        "parsed SIRI-SX"
    );

    let body = TransitSituations {
        network: ctx.network.network_id(),
        situations: found.situations,
    };
    let key = format!(
        "{}:{}:1/1",
        schema::SITUATIONS,
        ingest_stream::envelope::format_produced_at(produced_at)
    );
    let entry = Envelope::new(
        SchemaId::new(schema::SITUATIONS, 1)?,
        ctx.situations.producer_id(),
        key,
        produced_at,
        &body,
    )?
    .encode();
    let entry = match entry {
        Ok(entry) => entry,
        Err(err) => {
            ingest_stream::metrics::record_oversize(&ctx.stream);
            bail!("the situations snapshot does not fit one entry: {err}");
        }
    };
    let mut snapshot = Snapshot::new();
    snapshot.push(entry, body.situations.len());
    let _receipt = ctx.situations.submit(snapshot).await?;
    Ok(())
}
