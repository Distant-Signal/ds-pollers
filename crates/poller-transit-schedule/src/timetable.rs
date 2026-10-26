//! The timetable loop (transit spec §6.1-6.3), one cycle a day:
//!
//! 1. download the network's GTFS or `TransXChange` zip to the work dir
//!    and parse it on the blocking pool ([`fetch_model`]);
//! 2. apply the `NaPTAN` names (a failure is counted, never fatal);
//! 3. the feed guard against the last commit on the stream;
//! 4. build the item (parts, then the commit) with the rail links;
//! 5. skip it if the last commit has the same content and is under
//!    [`SKIP_UNCHANGED_FOR`] old (a restart) and no refresh request is
//!    pending ([`crate::refresh`]); else submit it.

use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::bail;
use chrono::{DateTime, Utc};
use common::metrics::metric_name;
use common::transit::{TimetableCommit, schema};

use crate::Context;
use crate::adapter::gtfs::{self, GtfsOptions};
use crate::adapter::txc::{self, TxcOptions};
use crate::adapter::{FeedError, download_to_file, ftp};
use crate::model::TransitModel;
use crate::network::AdapterKind;
use crate::{feed_guard, item, naptan, rail_links};

/// `transit_feed_refused_total{network, reason}`.
pub(crate) const FEED_REFUSED: &str = "transit_feed_refused_total";
/// `transit_feed_fetch_total{network, source, outcome}`.
pub(crate) const FEED_FETCH: &str = "transit_feed_fetch_total";
/// `transit_feed_bytes{network, source}` (gauge, the last download).
pub(crate) const FEED_BYTES: &str = "transit_feed_bytes";
/// `transit_feed_rows{network, kind}` (gauge, the last parse).
pub(crate) const FEED_ROWS: &str = "transit_feed_rows";
/// `transit_enrichment_failed_total{network, source}`.
pub(crate) const ENRICHMENT_FAILED: &str = "transit_enrichment_failed_total";

/// An unchanged feed is not re-sent within this long of its last commit
/// (a pod restart). After it, the daily re-send lets the writer move the
/// timetable's freshness.
pub(crate) const SKIP_UNCHANGED_FOR: Duration = Duration::from_secs(20 * 3600);

/// The last commit sent, and when.
#[derive(Debug, Clone)]
pub(crate) struct Baseline {
    pub commit: TimetableCommit,
    pub produced_at: DateTime<Utc>,
}

fn count(ctx: &Context, name: &str, labels: [(&'static str, &'static str); 1]) {
    let [(key, value)] = labels;
    metrics::counter!(metric_name(name), "network" => ctx.network.id.clone(), key => value)
        .increment(1);
}

fn fetched(ctx: &Context, source: &'static str, outcome: &'static str) {
    metrics::counter!(
        metric_name(FEED_FETCH),
        "network" => ctx.network.id.clone(),
        "source" => source,
        "outcome" => outcome
    )
    .increment(1);
}

/// Downloads the network's timetable zip to `path`: GTFS over HTTPS, or
/// `TransXChange` over HTTPS or (TNDS) FTP with the login. The bytes.
async fn download(ctx: &Context, path: &std::path::Path) -> Result<u64, FeedError> {
    let config = &ctx.config;
    match ctx.network.adapter {
        AdapterKind::Gtfs => {
            download_to_file(&ctx.client, &config.gtfs_url, path, config.gtfs_max_bytes).await
        }
        AdapterKind::Txc if config.txc_over_ftp() => {
            let login = ftp::Login {
                user: config.txc_username.clone(),
                password: config.txc_password.clone(),
            };
            ftp::download_to_file(
                &config.txc_url,
                &login,
                path,
                config.txc_max_bytes,
                crate::REQUEST_TIMEOUT,
            )
            .await
        }
        AdapterKind::Txc => {
            download_to_file(&ctx.client, &config.txc_url, path, config.txc_max_bytes).await
        }
    }
}

/// Downloads and parses the network's timetable with its adapter.
pub(crate) async fn fetch_model(ctx: &Context) -> Result<TransitModel, FeedError> {
    let path = ctx.config.work_dir.join("feed.zip");
    let adapter = ctx.network.adapter;
    let bytes = download(ctx, &path).await?;
    #[expect(
        clippy::cast_precision_loss,
        reason = "a byte count far below 2^53"
    )]
    metrics::gauge!(metric_name(FEED_BYTES), "network" => ctx.network.id.clone(), "source" => adapter.as_str())
        .set(bytes as f64);
    let today = common::transit::service_date(Utc::now());
    let horizon_days = ctx.config.horizon_days;
    let network = ctx.network.clone();
    let parse_path = path.clone();
    let started = std::time::Instant::now();
    let parsed = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&parse_path)
            .map_err(|err| FeedError::Io(format!("opening {}: {err}", parse_path.display())))?;
        let reader = std::io::BufReader::new(file);
        match adapter {
            AdapterKind::Gtfs => gtfs::parse(
                reader,
                &network,
                &GtfsOptions {
                    today,
                    horizon_days,
                    max_entry_bytes: gtfs::MAX_ENTRY_BYTES,
                },
            ),
            AdapterKind::Txc => txc::parse(
                reader,
                &network,
                &TxcOptions {
                    today,
                    horizon_days,
                    max_entry_bytes: txc::MAX_ENTRY_BYTES,
                },
            ),
        }
    })
    .await
    .map_err(|err| FeedError::Io(format!("the parse task failed: {err}")))?;
    if let Err(err) = tokio::fs::remove_file(&path).await {
        tracing::warn!(error = %err, path = %path.display(), "could not delete the downloaded feed");
    }
    let model = parsed?;
    tracing::info!(
        network = %ctx.network.id,
        bytes,
        seconds = started.elapsed().as_secs_f64(),
        lines = model.lines.len(),
        stops = model.stops.len(),
        patterns = model.patterns.len(),
        trips = model.trips.len(),
        service_days = model.service_day_count(),
        "parsed the timetable"
    );
    Ok(model)
}

/// Applies the area's `NaPTAN` names; a failure leaves the GTFS names.
async fn enrich(ctx: &Context, model: &mut TransitModel) {
    let url = ctx.config.naptan_url_for(&ctx.network.naptan_area);
    let nodes = match naptan::fetch(&ctx.client, &url).await {
        Ok(text) => naptan::parse(&text, &ctx.network.atco_prefix),
        Err(err) => Err(err),
    };
    match nodes {
        Ok(nodes) => {
            fetched(ctx, "naptan", "ok");
            let changed = naptan::apply(model, &nodes, &ctx.network);
            tracing::info!(network = %ctx.network.id, changed, "applied NaPTAN names");
        }
        Err(err) => {
            fetched(ctx, "naptan", "error");
            count(ctx, ENRICHMENT_FAILED, [("source", "naptan")]);
            tracing::warn!(network = %ctx.network.id, error = %format!("{err:#}"), "NaPTAN unavailable; keeping the GTFS stop names");
        }
    }
}

/// The last commit: the newest on the stream (so it survives a restart),
/// else the one this process sent.
pub(crate) async fn baseline(ctx: &Context) -> Option<Baseline> {
    match ingest_stream::snapshot::newest_of_schema(
        &ctx.redis,
        &ctx.stream,
        schema::TIMETABLE_COMMIT,
    )
    .await
    {
        Ok(Some(envelope)) => match envelope.payload_as::<TimetableCommit>() {
            Ok(commit) => {
                return Some(Baseline {
                    commit,
                    produced_at: envelope.produced_at,
                });
            }
            Err(err) => {
                tracing::warn!(error = %err, "the stream's last timetable commit does not decode");
            }
        },
        Ok(None) => {}
        Err(err) => {
            tracing::warn!(error = %err, "could not read the last timetable commit from the stream");
        }
    }
    ctx.last_sent.lock().ok().and_then(|last| last.clone())
}

/// The timetable loop's cursor: the newest timetable commit's time, not
/// the newest entry's (situations share the stream every 5 minutes). An
/// unanswered refresh request ([`crate::refresh`]) reads as "never
/// fetched", so the cycle runs at once.
pub(crate) async fn cursor(ctx: &Context) -> anyhow::Result<Option<DateTime<Utc>>> {
    if ctx.refresh_pending.load(Ordering::Relaxed) {
        return Ok(None);
    }
    Ok(
        ingest_stream::snapshot::newest_of_schema(
            &ctx.redis,
            &ctx.stream,
            schema::TIMETABLE_COMMIT,
        )
        .await?
        .map(|envelope| envelope.produced_at),
    )
}

/// One timetable cycle.
pub(crate) async fn poll_once(ctx: &Context) -> anyhow::Result<()> {
    let produced_at = Utc::now();
    let mut model = match fetch_model(ctx).await {
        Ok(model) => {
            fetched(ctx, ctx.network.adapter.as_str(), "ok");
            model
        }
        Err(err) => {
            fetched(ctx, ctx.network.adapter.as_str(), "error");
            count(ctx, FEED_REFUSED, [("reason", err.reason())]);
            return Err(err.into());
        }
    };
    enrich(ctx, &mut model).await;
    let links = rail_links::rail_links(&ctx.network.id)?;
    let counts = feed_guard::counts(&model);
    let last = baseline(ctx).await;
    let today = common::transit::service_date(produced_at);
    if let Err(refusal) = feed_guard::check(
        &model,
        counts,
        last.as_ref().map(|b| b.commit.counts),
        today,
        ctx.config.feed_max_drop_fraction,
    ) {
        count(ctx, FEED_REFUSED, [("reason", refusal.reason())]);
        bail!(
            "network {}: the timetable was refused ({refusal}); the previous version stays active",
            ctx.network.id
        );
    }
    for (kind, rows) in [
        ("lines", counts.lines),
        ("stops", counts.stops),
        ("patterns", counts.patterns),
        ("trips", counts.trips),
        ("service_days", counts.service_days),
    ] {
        metrics::gauge!(metric_name(FEED_ROWS), "network" => ctx.network.id.clone(), "kind" => kind)
            .set(f64::from(rows));
    }

    // An unanswered refresh request (crate::refresh): publish even an
    // unchanged feed, and say which request this commit answers.
    let forced = ctx.refresh_pending.load(Ordering::Relaxed);
    let built = item::build(
        &ctx.network,
        &model,
        &links,
        &ctx.stream,
        ctx.timetable.producer_id(),
        produced_at,
        item::SourceTag {
            url_kind: ctx.network.adapter.as_str(),
            refresh: forced.then_some(ctx.config.timetable_refresh.as_str()),
        },
    )?;
    if let Some(last) = last.as_ref().filter(|_| !forced) {
        let age = (produced_at - last.produced_at)
            .to_std()
            .unwrap_or_default();
        if last.commit.content_sha256 == built.commit.content_sha256 && age < SKIP_UNCHANGED_FOR {
            tracing::info!(
                network = %ctx.network.id,
                feed_version = %built.commit.feed_version,
                "the timetable is unchanged since the last commit; not re-sending it yet"
            );
            return Ok(());
        }
    }
    let parts = built.snapshot.parts().len();
    let receipt = ctx.timetable.submit(built.snapshot).await?;
    tracing::info!(
        network = %ctx.network.id,
        feed_version = %built.commit.feed_version,
        parts,
        refresh = ?built.commit.source.refresh,
        "timetable queued for the stream"
    );
    if forced {
        ctx.refresh_pending.store(false, Ordering::Relaxed);
    }
    if let Ok(mut slot) = ctx.last_sent.lock() {
        *slot = Some(Baseline {
            commit: built.commit,
            produced_at,
        });
    }
    let network = ctx.network.id.clone();
    tokio::spawn(async move {
        match receipt.written().await {
            Ok(ids) => {
                tracing::info!(%network, entries = ids.len(), "timetable written to the stream");
            }
            Err(err) => {
                tracing::warn!(%network, error = %err, "timetable not written (a newer one superseded it, or shutdown)");
            }
        }
    });
    Ok(())
}
