//! `poller-tfl`: polls `TfL`'s Unified API for line status across the modes
//! this app displays (tube, DLR, Overground, Elizabeth line, tram) and
//! publishes it to the `ds:ingest:tfl` stream as `tfl-line-status/1`
//! (ingest plan 3c.2), which the ingest-writer applies; see
//! `ingest_stream::snapshot`.
//!
//! Unlike the four RDM pollers, what this one carries is already finished
//! line status — `TfL` publishes status directly, so nothing downstream has
//! to infer it from incidents or departure boards, and the aggregator is
//! not involved. `schema.rs` does the whole TfL→domain mapping (severity
//! codes above all) so nothing downstream sees `TfL`'s JSON.
//!
//! With `TFL_STOPS_ENABLED` it also runs a daily stops job ([`stops`]):
//! every Tube, DLR and tram stop, published as `tfl-stops/1` on
//! `ds:ingest:tfl-stops` for the untracked-leg stop lookup
//! (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md).
//!
//! With `TFL_ROUTES_ENABLED` it also runs a daily route-sequences job
//! ([`routes`], DS-151): each of those lines' routes in stop order,
//! published as `tfl-line-routes/1` on the same stream, which the
//! planner's Tube access legs measure rides along.
//!
//! There is no historical endpoint on `TfL`'s side. Everything this app can
//! ever show for "the Victoria line last Tuesday" is what this poller
//! wrote into `line_status_history` at the time.

mod config;
mod routes;
mod schema;
mod stops;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use config::Config;
use ingest_stream::snapshot::SnapshotStream;
use reqwest::{Client, StatusCode};

/// `TfL`'s subscription-key header. Not in `common::ingest` alongside
/// `RDM_AUTH_HEADER_NAME`: that constant is there because four pollers and
/// the api crate all have to agree on it, whereas this one has exactly one
/// consumer.
const TFL_AUTH_HEADER_NAME: &str = "Ocp-Apim-Subscription-Key";

/// Per-request timeout, matching the other pollers: a peer that accepts the
/// connection and never answers would otherwise hang the poll loop forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Attempts per poll cycle before giving up and waiting for the next tick.
/// `TfL`'s registered free tier is documented at roughly 500 requests per
/// minute, but community reports say the enforcement is inconsistent — so
/// this poller does not assume a budget, it just backs off when told to.
const MAX_ATTEMPTS: u32 = 3;

/// Worth retrying inside the cycle: rate limiting and transient upstream
/// faults. A 4xx that is not 429 means this poller is wrong (bad key, bad
/// mode name) and retrying it just burns quota.
fn should_retry(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// 2s, 4s. Both delays plus two requests fit comfortably inside the 300s
/// poll interval, so a retrying cycle can never overlap the next one.
fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(2u64.pow(attempt))
}

/// Fails startup if `key` is empty (after trimming whitespace). Guards
/// against orchestrators that set `TFL_APP_KEY` to an empty string rather
/// than leaving it unset — `clap`'s `env` attribute only enforces
/// "present", not "non-empty", so that case would otherwise sail through
/// `Config::parse()` and start polling `TfL` anonymously.
fn require_non_empty_key(key: &str) -> anyhow::Result<()> {
    if key.trim().is_empty() {
        anyhow::bail!(
            "TFL_APP_KEY must be set (see api-portal.tfl.gov.uk) — refusing to poll TfL anonymously"
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-tfl");

    let config = Config::parse();
    let progress = health_http::spawn_liveness(&config.health);
    // `clap` treats a present-but-empty env var as a supplied value, so an
    // orchestrator (e.g. `docker-compose.yml`'s `TFL_APP_KEY: ${TFL_APP_KEY}`)
    // that leaves the shell variable unset still gets `Config::parse()` to
    // succeed with `tfl_app_key = ""` rather than failing — silently sending
    // every request unauthenticated instead of refusing to start. Catch that
    // here, before the client is built.
    require_non_empty_key(&config.tfl_app_key)?;
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let stream = stream_sink(&config)?;

    let stops = if config.tfl_stops_enabled {
        Some(daily_sink(
            &config,
            common::tfl_stops::SCHEMA,
            "TFL_STOPS_ENABLED",
        )?)
    } else {
        tracing::info!("the stops job is off (TFL_STOPS_ENABLED)");
        None
    };
    let routes = if config.tfl_routes_enabled {
        Some(daily_sink(
            &config,
            common::tfl_routes::SCHEMA,
            "TFL_ROUTES_ENABLED",
        )?)
    } else {
        tracing::info!("the route-sequences job is off (TFL_ROUTES_ENABLED)");
        None
    };

    let cycle = || poll_once(&client, &config, &stream);

    // The startup cursor is the stream's newest entry (spec §11.3).
    let status_loop = common::poller_loop::run_poll_loop_with_cursor(
        "tfl",
        || async { Ok(stream.last_produced_at().await?) },
        poll_interval,
        config.metrics.metrics_enabled,
        config.metrics_port,
        &progress,
        cycle,
    );
    let stops_loop = async {
        let Some(stops) = &stops else {
            return std::future::pending().await;
        };
        common::poller_loop::run_poll_loop_with_cursor(
            "tfl-stops",
            || async { Ok(stops.stream.last_produced_at().await?) },
            Duration::from_secs(config.tfl_stops_interval_secs),
            // The status loop installs the exporter.
            false,
            config.metrics_port,
            &progress,
            || stops_once(&client, &config, stops),
        )
        .await
    };
    let routes_loop = async {
        let Some(routes) = &routes else {
            return std::future::pending().await;
        };
        common::poller_loop::run_poll_loop_with_cursor(
            "tfl-routes",
            || async { Ok(routes.stream.last_produced_at().await?) },
            Duration::from_secs(config.tfl_stops_interval_secs),
            false,
            config.metrics_port,
            &progress,
            || routes_once(&client, &config, routes),
        )
        .await
    };
    tokio::select! {
        result = status_loop => result,
        result = stops_loop => result,
        result = routes_loop => result,
    }
}

/// A daily job's producer on `ds:ingest:tfl-stops` (the stops job's
/// `tfl-stops/1` or the routes job's `tfl-line-routes/1`), and the Redis
/// client its feed guard reads the last snapshot back with.
struct DailySink {
    stream: SnapshotStream,
    redis: redis::Client,
}

/// A `ds:ingest:tfl-stops` producer of `schema` (version 1): one part per
/// snapshot (a few hundred stops, or about 80 routes, well under the 512
/// KiB part cap after gzip), so the writer replaces its table from one
/// entry. Each job has its own producer, so neither supersedes the other's
/// unsent snapshot.
fn daily_sink(config: &Config, schema: &str, why: &str) -> anyhow::Result<DailySink> {
    let redis = config.redis.client(why).map_err(anyhow::Error::msg)?;
    tracing::info!("producing TfL {schema} snapshots to ds:ingest:tfl-stops");
    Ok(DailySink {
        stream: SnapshotStream::spawn(
            redis.clone(),
            ingest_stream::streams::TFL_STOPS,
            ingest_stream::SchemaId::new(schema, 1)?,
            "poller-tfl",
            usize::MAX,
        ),
        redis,
    })
}

/// The gap between two `TfL` calls of one stops run: about ten calls a
/// day, far under `TfL`'s 500 a minute, so this is courtesy, not budget.
const STOPS_CALL_GAP: Duration = Duration::from_millis(250);

/// One stops run (spec §1): every page of Tube, DLR and tram stop points,
/// then their hubs, through the feed guard, published as one snapshot.
async fn stops_once(client: &Client, config: &Config, sink: &DailySink) -> anyhow::Result<()> {
    let fetched_at = Utc::now();
    let base = config.tfl_base_url.trim_end_matches('/');
    let modes = common::tfl_stops::MODES.join(",");
    let mut points = Vec::new();
    for page in 1..=stops::MAX_PAGES {
        if page > 1 {
            tokio::time::sleep(STOPS_CALL_GAP).await;
        }
        let url = format!("{base}/StopPoint/Mode/{modes}?page={page}");
        let body = fetch_json(client, &url, config, "stops").await?;
        let parsed = stops::parse_page(&body)?;
        let full = parsed.raw_count >= stops::PAGE_SIZE;
        points.extend(parsed.stop_points);
        if !full {
            break;
        }
        if page == stops::MAX_PAGES {
            anyhow::bail!(
                "TfL still had stop points after {} pages; refusing a partial snapshot",
                stops::MAX_PAGES
            );
        }
    }
    let mut found = stops::collect_stops(points);

    let mut hubs = std::collections::BTreeMap::new();
    for chunk in stops::hub_codes(&found).chunks(stops::HUB_BATCH) {
        tokio::time::sleep(STOPS_CALL_GAP).await;
        let url = format!("{base}/StopPoint/{}", chunk.join(","));
        let body = fetch_json(client, &url, config, "stop-hubs").await?;
        hubs.extend(stops::hub_rail(stops::parse_page(&body)?.stop_points));
    }
    stops::apply_hubs(&mut found, &hubs);
    let rows: Vec<common::tfl_stops::TflStop> = found.into_values().collect();

    let baseline = last_published_count(sink, common::tfl_stops::SCHEMA).await?;
    if let Err(refusal) = stops::check(rows.len(), baseline, config.tfl_stops_max_drop_fraction) {
        metrics::counter!(
            common::metrics::metric_name("tfl_stops_refused_total"),
            "reason" => refusal.reason()
        )
        .increment(1);
        anyhow::bail!("refusing the TfL stops snapshot: {refusal}");
    }

    let with_rail = rows
        .iter()
        .filter(|stop| !stop.rail_tiplocs.is_empty())
        .count();
    tracing::info!(
        stops = rows.len(),
        hubs = hubs.len(),
        with_rail,
        "parsed TfL stops"
    );
    sink.stream.publish(&rows, fetched_at).await?;
    #[expect(clippy::cast_precision_loss, reason = "a few hundred stops")]
    metrics::gauge!(common::metrics::metric_name("tfl_stops_published")).set(rows.len() as f64);
    #[expect(clippy::cast_precision_loss, reason = "a Unix timestamp in seconds")]
    metrics::gauge!(common::metrics::metric_name(
        "tfl_stops_last_success_timestamp_seconds"
    ))
    .set(fetched_at.timestamp() as f64);
    Ok(())
}

/// One route-sequences run (DS-151): the Tube, DLR and tram lines, then
/// each line's routes in both directions, through the feed guard (on the
/// route count), published as one snapshot. A line with no route at all
/// fails the run: the snapshot replaces every line's routes.
async fn routes_once(client: &Client, config: &Config, sink: &DailySink) -> anyhow::Result<()> {
    let fetched_at = Utc::now();
    let base = config.tfl_base_url.trim_end_matches('/');
    let modes = common::tfl_stops::MODES.join(",");
    let body = fetch_json(
        client,
        &format!("{base}/Line/Mode/{modes}"),
        config,
        "lines",
    )
    .await?;
    let lines = routes::parse_lines(&body)?;
    let mut found = Vec::new();
    let mut invalid = 0;
    for line in &lines {
        let mut of_line = 0;
        for direction in common::tfl_routes::DIRECTIONS {
            tokio::time::sleep(STOPS_CALL_GAP).await;
            // `parse_lines` kept only lower-case slugs: safe in a path.
            let url = format!("{base}/Line/{line}/Route/Sequence/{direction}");
            let body = fetch_json(client, &url, config, "route-sequence").await?;
            let parsed = routes::parse_sequence(line, direction, &body)?;
            invalid += parsed.invalid;
            of_line += parsed.routes.len();
            found.extend(parsed.routes);
        }
        if of_line == 0 {
            anyhow::bail!(
                "TfL returned no routes for the {line} line; refusing a partial snapshot"
            );
        }
    }
    let rows = routes::dedup(found);

    let baseline = last_published_count(sink, common::tfl_routes::SCHEMA).await?;
    if let Err(refusal) = stops::check(rows.len(), baseline, config.tfl_stops_max_drop_fraction) {
        metrics::counter!(
            common::metrics::metric_name("tfl_routes_refused_total"),
            "reason" => refusal.reason()
        )
        .increment(1);
        anyhow::bail!("refusing the TfL route sequences: {refusal}");
    }
    tracing::info!(
        lines = lines.len(),
        routes = rows.len(),
        invalid,
        "parsed TfL route sequences"
    );
    sink.stream.publish(&rows, fetched_at).await?;
    #[expect(clippy::cast_precision_loss, reason = "about a hundred routes")]
    metrics::gauge!(common::metrics::metric_name("tfl_routes_published")).set(rows.len() as f64);
    #[expect(clippy::cast_precision_loss, reason = "a Unix timestamp in seconds")]
    metrics::gauge!(common::metrics::metric_name(
        "tfl_routes_last_success_timestamp_seconds"
    ))
    .set(fetched_at.timestamp() as f64);
    Ok(())
}

/// The row count of the last `schema` snapshot on the stream (the feed
/// guard's baseline), `None` when there is none.
async fn last_published_count(sink: &DailySink, schema: &str) -> anyhow::Result<Option<usize>> {
    let Some(envelope) =
        ingest_stream::snapshot::newest_of_schema(&sink.redis, sink.stream.stream(), schema)
            .await?
    else {
        return Ok(None);
    };
    let rows: Vec<serde_json::Value> = envelope.payload_as()?;
    Ok(Some(rows.len()))
}

/// The `ds:ingest:tfl` producer. One part per snapshot (about 20 lines),
/// so the writer's prune sees every line.
fn stream_sink(config: &Config) -> anyhow::Result<SnapshotStream> {
    let client = config
        .redis
        .client("the ingest stream")
        .map_err(anyhow::Error::msg)?;
    tracing::info!("producing TfL snapshots to ds:ingest:tfl");
    Ok(SnapshotStream::spawn(
        client,
        ingest_stream::streams::TFL,
        ingest_stream::SchemaId::new("tfl-line-status", 1)?,
        "poller-tfl",
        usize::MAX,
    ))
}

async fn poll_once(
    client: &Client,
    config: &Config,
    stream: &SnapshotStream,
) -> anyhow::Result<()> {
    // The snapshot's `produced_at` on the stream (decision D13): when it
    // was fetched, not when it is sent.
    let fetched_at = Utc::now();
    let body = fetch_status_json(client, config).await?;
    let reports = schema::parse_line_status(&body, fetched_at)?;

    // Never publish an empty snapshot. The writer prunes TfL rows that are
    // missing from the snapshot it applies, so an empty one would read as
    // "TfL has no lines any more" and blank the whole section. The writer
    // guards this too; this is the half that knows it is a fault.
    if reports.is_empty() {
        anyhow::bail!(
            "TfL returned no lines for modes {}; refusing to publish an empty snapshot",
            config.tfl_modes
        );
    }

    tracing::info!(count = reports.len(), "parsed line statuses from TfL");

    stream.publish(&reports, fetched_at).await?;
    Ok(())
}

async fn fetch_status_json(client: &Client, config: &Config) -> anyhow::Result<String> {
    let url = format!(
        "{}/Line/Mode/{}/Status",
        config.tfl_base_url.trim_end_matches('/'),
        config.tfl_modes
    );
    fetch_json(client, &url, config, "line-status").await
}

/// One authenticated GET against `TfL`, with this poller's shared status
/// checking, in-cycle backoff, and outcome metric. Every `TfL` call goes
/// through here: a 429 or a 5xx has a body too, and handing that body to a
/// parser produces a confusing serde error in place of the real cause.
///
/// `what` names the call in errors, logs, and the `distant_signal_tfl_fetch_total`
/// metric's `what` label (e.g. `"line-status"`).
async fn fetch_json(
    client: &Client,
    url: &str,
    config: &Config,
    what: &str,
) -> anyhow::Result<String> {
    let mut attempt = 0;
    // Tracks the status code of the most recent retryable failure, if any
    // -- this is what distinguishes a same-cycle "succeeded on the first
    // try" outcome from "succeeded after backing off from a 429/5xx",
    // which the plain success/failure result of the call alone can't tell
    // apart. See docs/superpowers/specs/2026-08-29-metrics-design.md's v1
    // scope item 3.
    let mut retried_status: Option<StatusCode> = None;
    loop {
        let response = client
            .get(url)
            .header(TFL_AUTH_HEADER_NAME, &config.tfl_app_key)
            .send()
            .await?;
        let status = response.status();

        if status.is_success() {
            let outcome = match retried_status {
                None => "success",
                Some(s) if s == StatusCode::TOO_MANY_REQUESTS => "retried_429",
                Some(_) => "retried_5xx",
            };
            metrics::counter!(
                common::metrics::metric_name("tfl_fetch_total"),
                "what" => what.to_string(),
                "outcome" => outcome
            )
            .increment(1);
            return Ok(response.text().await?);
        }

        attempt += 1;
        if attempt >= MAX_ATTEMPTS || !should_retry(status) {
            metrics::counter!(
                common::metrics::metric_name("tfl_fetch_total"),
                "what" => what.to_string(),
                "outcome" => "exhausted"
            )
            .increment(1);
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("TfL {what} fetch failed: {status} {body}");
        }

        retried_status = Some(status);
        let delay = retry_delay(attempt);
        tracing::warn!(%status, attempt, delay_secs = delay.as_secs(), "TfL {what} fetch failed; retrying");
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiting_and_upstream_faults_are_retried() {
        assert!(should_retry(StatusCode::TOO_MANY_REQUESTS));
        assert!(should_retry(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(should_retry(StatusCode::BAD_GATEWAY));
    }

    #[test]
    fn our_own_mistakes_are_not_retried() {
        // A bad subscription key or a mode TfL doesn't know is not going to
        // fix itself two seconds later; retrying just spends quota.
        assert!(!should_retry(StatusCode::UNAUTHORIZED));
        assert!(!should_retry(StatusCode::FORBIDDEN));
        assert!(!should_retry(StatusCode::NOT_FOUND));
    }

    #[test]
    fn backoff_fits_inside_one_poll_interval() {
        assert_eq!(retry_delay(1), Duration::from_secs(2));
        assert_eq!(retry_delay(2), Duration::from_secs(4));
        let total: u64 = (1..MAX_ATTEMPTS)
            .map(|attempt| retry_delay(attempt).as_secs())
            .sum();
        assert!(
            total < 300,
            "total backoff {total}s must not overrun the 300s poll interval"
        );
    }

    #[test]
    fn an_empty_key_is_rejected() {
        assert!(require_non_empty_key("").is_err());
        // Whitespace-only is what a shell-expanded-but-blank env var can
        // look like too (e.g. `TFL_APP_KEY=" "`); treat it the same as empty.
        assert!(require_non_empty_key("   ").is_err());
    }

    #[test]
    fn a_real_key_is_accepted() {
        assert!(require_non_empty_key("abc123").is_ok());
    }
}
