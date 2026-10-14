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
use common::metrics::metric_name;
use common::transit::{SituationScope, TransitSituations, schema};
use ingest_stream::snapshot::Snapshot;
use ingest_stream::{Envelope, SchemaId};

use crate::Context;
use crate::adapter::{FeedError, sirisx};

/// The archive's download cap (290 KB on 2026-10-09).
pub(crate) const MAX_ARCHIVE_BYTES: u64 = 50 * 1024 * 1024;

/// `transit_situations_current{network, scope}` (gauge).
pub(crate) const SITUATIONS_CURRENT: &str = "transit_situations_current";
/// `transit_situation_unmapped_line_refs_total{network, ref}`.
pub(crate) const UNMAPPED_LINE_REFS: &str = "transit_situation_unmapped_line_refs_total";

/// The archive, or `None` when it is unchanged (HTTP 304).
async fn download(ctx: &Context) -> Result<Option<Vec<u8>>, FeedError> {
    let io = |err: &dyn std::fmt::Display| FeedError::Io(format!("SIRI-SX download: {err}"));
    let mut request = ctx.client.get(&ctx.config.situations_url);
    let etag = ctx.situations_etag.lock().ok().and_then(|e| e.clone());
    if let Some(etag) = &etag {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let mut response = request.send().await.map_err(|err| io(&err))?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    if let Err(err) = response.error_for_status_ref() {
        return Err(io(&err));
    }
    if response
        .content_length()
        .is_some_and(|len| len > MAX_ARCHIVE_BYTES)
    {
        return Err(FeedError::Oversize("the SIRI-SX archive".to_owned()));
    }
    let new_etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|err| io(&err))? {
        body.extend_from_slice(&chunk);
        if body.len() as u64 > MAX_ARCHIVE_BYTES {
            return Err(FeedError::Oversize("the SIRI-SX archive".to_owned()));
        }
    }
    if let Ok(mut slot) = ctx.situations_etag.lock() {
        *slot = new_etag;
    }
    Ok(Some(body))
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
