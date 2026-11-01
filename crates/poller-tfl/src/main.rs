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
//! ([`routes`], DS-151): each of those lines' routes in stop order, with
//! (DS-206) the timetabled running time between each two stops, published
//! as `tfl-line-routes/1` on the same stream, which the planner's Tube
//! access legs time rides along.
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

/// Once mode: how long a daily job waits for its snapshot to reach the
/// stream before failing (exit 1, so the Job retries).
const ONCE_FLUSH_GRACE: Duration = Duration::from_secs(120);

/// How a fetch retries a 429 or 5xx. `TfL`'s registered free tier is
/// documented at roughly 500 requests per minute, but the enforcement is
/// inconsistent (the 2026-11-01 routes run was told "Try again in 17
/// seconds" after about 100 requests in 30 s), so this poller does not
/// assume a budget: it backs off when told to, for at least as long as
/// `TfL` asks (`Retry-After`, or the 429 body's "Try again in N seconds"),
/// jittered. The status poll and the daily jobs share one key's quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetryPolicy {
    /// Attempts in all, the first included.
    max_attempts: u32,
    /// Backoff when `TfL` names no wait: 2 s doubling, capped.
    backoff: common::backoff::Backoff,
}

/// The five-minute line-status poll: three attempts, so a retrying cycle
/// stays well inside its 300 s interval even when told to wait.
const STATUS_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    backoff: common::backoff::Backoff::new(Duration::from_secs(2), Duration::from_secs(60)),
};

/// The daily stops and routes jobs: no deadline worth keeping, and a fetch
/// they give up on loses data (a route timetable's running times), so they
/// retry longer.
const DAILY_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 6,
    backoff: common::backoff::Backoff::new(Duration::from_secs(2), Duration::from_secs(60)),
};

/// The policy for a fetch, by its `what` label.
fn retry_policy(what: &str) -> RetryPolicy {
    if what == "line-status" {
        STATUS_RETRY
    } else {
        DAILY_RETRY
    }
}

/// How long `TfL` asked to wait: the `Retry-After` header in seconds, else
/// the "Try again in N seconds." of its 429 body (API Management's).
fn requested_wait(retry_after: Option<&str>, body: &str) -> Option<Duration> {
    if let Some(secs) = retry_after.and_then(|v| v.trim().parse::<u64>().ok()) {
        return Some(Duration::from_secs(secs));
    }
    let rest = body.split("Try again in ").nth(1)?;
    let secs: u64 = rest.split_whitespace().next()?.parse().ok()?;
    rest.split_whitespace()
        .nth(1)
        .is_some_and(|unit| unit.starts_with("second"))
        .then(|| Duration::from_secs(secs))
}

/// A `TfL` request that ended with an HTTP error status (after any
/// retries). The message is the one the poller always logged.
#[derive(Debug)]
struct FetchFailed {
    what: String,
    status: StatusCode,
    body: String,
}

impl std::fmt::Display for FetchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TfL {} fetch failed: {} {}",
            self.what, self.status, self.body
        )
    }
}

impl std::error::Error for FetchFailed {}

/// Whether `err` is `TfL` saying the resource does not exist (404).
fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<FetchFailed>()
        .is_some_and(|f| f.status == StatusCode::NOT_FOUND)
}

/// Worth retrying inside the cycle: rate limiting and transient upstream
/// faults. A 4xx that is not 429 means this poller is wrong (bad key, bad
/// mode name) and retrying it just burns quota.
fn should_retry(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
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
    // See `release_after`.
    common::heap::pin_mmap_threshold();

    let config = Config::parse();
    let once = common::poller_loop::RunOnce::from_env()?.is_some();
    config.check_jobs(once)?;
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

    let stops = if config.tfl_stops_enabled && config.jobs.daily() {
        Some(daily_sink(
            &config,
            common::tfl_stops::SCHEMA,
            "TFL_STOPS_ENABLED",
        )?)
    } else {
        tracing::info!("the stops job is off (TFL_STOPS_ENABLED)");
        None
    };
    let routes = if config.tfl_routes_enabled && config.jobs.daily() {
        Some(daily_sink(
            &config,
            common::tfl_routes::SCHEMA,
            "TFL_ROUTES_ENABLED",
        )?)
    } else {
        tracing::info!("the route-sequences job is off (TFL_ROUTES_ENABLED)");
        None
    };

    if once {
        return daily_once(&client, &config, &progress, stops, routes).await;
    }

    let cycle = || poll_once(&client, &config, &stream);

    // The startup cursor is the stream's newest entry (spec §11.3).
    let status_loop = async {
        if !config.jobs.status() {
            tracing::info!("the status poll is off (TFL_JOBS)");
            return std::future::pending().await;
        }
        common::poller_loop::run_poll_loop_with_cursor(
            "tfl",
            || async { Ok(stream.last_produced_at().await?) },
            poll_interval,
            config.metrics.metrics_enabled,
            config.metrics_port,
            &progress,
            cycle,
        )
        .await
    };
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
            || release_after(stops_once(&client, &config, stops)),
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
            || release_after(routes_once(&client, &config, routes)),
        )
        .await
    };
    tokio::select! {
        result = status_loop => result,
        result = stops_loop => result,
        result = routes_loop => result,
    }
}

/// Once mode (`RUN_ONCE` with `TFL_JOBS=daily`, a scheduled Job;
/// docs/scheduled-jobs.md): each enabled daily job once, each skipping if
/// its own snapshot is fresher than `RUN_ONCE_SKIP_IF_FRESHER_THAN_SECS`,
/// each waiting for its snapshot to reach the stream. Both run even if one
/// fails. A transient failure of either exits 1 (the Job retries, and the
/// one that succeeded then skips); a refusal alone exits 3.
async fn daily_once(
    client: &Client,
    config: &Config,
    progress: &common::progress::Progress,
    stops: Option<DailySink>,
    routes: Option<DailySink>,
) -> anyhow::Result<()> {
    let interval = Duration::from_secs(config.tfl_stops_interval_secs);
    let mut results = Vec::new();
    if let Some(stops) = stops {
        let result = common::poller_loop::run_poll_loop_with_cursor(
            "tfl-stops",
            || async { Ok(stops.stream.last_produced_at().await?) },
            interval,
            false,
            config.metrics_port,
            progress,
            || release_after(stops_once(client, config, &stops)),
        )
        .await;
        results.push(
            common::poller_loop::finish_once(result, stops.stream.shutdown(ONCE_FLUSH_GRACE)).await,
        );
    }
    if let Some(routes) = routes {
        let result = common::poller_loop::run_poll_loop_with_cursor(
            "tfl-routes",
            || async { Ok(routes.stream.last_produced_at().await?) },
            interval,
            false,
            config.metrics_port,
            progress,
            || release_after(routes_once(client, config, &routes)),
        )
        .await;
        results.push(
            common::poller_loop::finish_once(result, routes.stream.shutdown(ONCE_FLUSH_GRACE))
                .await,
        );
    }
    worst_of(results)
}

/// The error a once-mode run exits with: a transient one (exit 1, retried)
/// before a refusal (exit 3, not retried), so a retry still re-runs the
/// job that can succeed.
fn worst_of(results: Vec<anyhow::Result<()>>) -> anyhow::Result<()> {
    let mut rejected = None;
    for result in results {
        if let Err(err) = result {
            if common::logging::exit_status_of(&err) == 1 {
                return Err(err);
            }
            rejected.get_or_insert(err);
        }
    }
    rejected.map_or(Ok(()), Err)
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
/// Runs a daily job (stops or routes), then hands its freed heap back to
/// the kernel. Each builds hundreds of stops or every line's route
/// timetables and frees them a moment later, but glibc kept the peak
/// resident for the rest of the day: 88 MiB against the pod's 32Mi request
/// on 2026-11-01, hours after the 19:00 routes run. With the mmap threshold
/// pinned at startup the big buffers are their own mappings, and the trim
/// returns the rest (`common::heap`, as poller-transit-schedule does).
async fn release_after(job: impl Future<Output = anyhow::Result<()>>) -> anyhow::Result<()> {
    let result = job.await;
    common::heap::release_freed_memory();
    tracing::debug!(
        rss_bytes = common::heap::resident_set_bytes(),
        "released the daily job's freed heap"
    );
    result
}

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
        // The data itself refused: the next interval, or exit 3 in a Job.
        return Err(anyhow::Error::new(common::ingest::DataRejected(format!(
            "refusing the TfL stops snapshot: {refusal}"
        ))));
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
        // The data itself refused: the next interval, or exit 3 in a Job.
        return Err(anyhow::Error::new(common::ingest::DataRejected(format!(
            "refusing the TfL route sequences: {refusal}"
        ))));
    }
    let rows = with_running_times(client, config, rows).await;
    let timed = rows.iter().filter(|r| r.step_seconds.is_some()).count();
    tracing::info!(
        lines = lines.len(),
        routes = rows.len(),
        timed,
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

/// The route timetables `TfL` does not publish (404), logged once each.
static UNPUBLISHED_TIMETABLES: std::sync::LazyLock<common::log_once::LogOnceSet> =
    std::sync::LazyLock::new(common::log_once::LogOnceSet::new);

/// `routes` with their running times (DS-206; [`routes::StepSamples`]):
/// one `GET /Line/{id}/Timetable/{first}/to/{last}` per route (about 80 a
/// day, at [`STOPS_CALL_GAP`]), every timetable of a line timing every
/// route of it. A timetable that fails to fetch or parse is logged and
/// counted (`tfl_route_timetables_failed_total`) and leaves only its own
/// samples out: a step no timetable times has no time, and the api falls
/// back to its distance estimate there.
///
/// A route `TfL` publishes no timetable for (a 404: the trams from
/// anywhere but Wimbledon) also takes, for each step no timetable times,
/// the same two stops' running time the other way
/// ([`routes::StepSamples::step_seconds_mirrored`]), counted in
/// `tfl_route_steps_mirrored_total{line}` and logged.
async fn with_running_times(
    client: &Client,
    config: &Config,
    mut rows: Vec<common::tfl_routes::TflLineRoute>,
) -> Vec<common::tfl_routes::TflLineRoute> {
    let base = config.tfl_base_url.trim_end_matches('/');
    let mut samples = routes::StepSamples::default();
    let mut asked = std::collections::BTreeSet::new();
    // `(line, first stop, last stop)` of the routes `TfL` has no timetable
    // for.
    let mut unpublished = std::collections::BTreeSet::new();
    for route in &rows {
        let (Some(first), Some(last)) = (route.stop_ids.first(), route.stop_ids.last()) else {
            continue;
        };
        if !asked.insert((&route.line_id, first, last)) {
            continue;
        }
        tokio::time::sleep(STOPS_CALL_GAP).await;
        // Line ids are slugs and stop ids `NaPTAN` ids (both checked by
        // `routes::parse_sequence`): safe in a path.
        let url = format!("{base}/Line/{}/Timetable/{first}/to/{last}", route.line_id);
        let added = match fetch_json(client, &url, config, "route-timetable").await {
            Ok(body) => samples.add_timetable(&route.line_id, &body),
            Err(err) if is_not_found(&err) => {
                // TfL publishes a timetable only from a line's timetabled
                // origins (trams: from Wimbledon alone), so a 404 is a
                // route it has none for, every day: not a failure. Logged
                // once a process.
                metrics::counter!(common::metrics::metric_name(
                    "tfl_route_timetables_unpublished_total"
                ))
                .increment(1);
                unpublished.insert((route.line_id.clone(), first.clone(), last.clone()));
                if UNPUBLISHED_TIMETABLES.should_log(&url) {
                    tracing::info!(
                        line = %route.line_id,
                        %first,
                        %last,
                        "TfL publishes no timetable for this route; its steps keep any times other routes give them"
                    );
                }
                continue;
            }
            Err(err) => Err(err),
        };
        if let Err(err) = added {
            metrics::counter!(common::metrics::metric_name(
                "tfl_route_timetables_failed_total"
            ))
            .increment(1);
            tracing::warn!(
                line = %route.line_id,
                %first,
                %last,
                error = %err,
                "no running times from this TfL timetable"
            );
        }
    }
    for route in &mut rows {
        let key = (
            route.line_id.clone(),
            route.stop_ids.first().cloned().unwrap_or_default(),
            route.stop_ids.last().cloned().unwrap_or_default(),
        );
        if !unpublished.contains(&key) {
            route.step_seconds = samples.step_seconds(route);
            continue;
        }
        let (steps, mirrored) = samples.step_seconds_mirrored(route);
        route.step_seconds = steps;
        if mirrored > 0 {
            metrics::counter!(
                common::metrics::metric_name("tfl_route_steps_mirrored_total"),
                "line" => route.line_id.clone()
            )
            .increment(u64::try_from(mirrored).unwrap_or(u64::MAX));
            tracing::info!(
                line = %route.line_id,
                route = %route.name,
                mirrored,
                steps = route.stop_ids.len().saturating_sub(1),
                "timed steps of a route TfL publishes no timetable for from the reverse direction"
            );
        }
    }
    rows
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
    let policy = retry_policy(what);
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
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = response.text().await.unwrap_or_default();
        if attempt >= policy.max_attempts || !should_retry(status) {
            // A 404 is `TfL` not publishing the resource (a tram timetable
            // from anywhere but its timetabled origin), not a fault.
            let outcome = if status == StatusCode::NOT_FOUND {
                "not_found"
            } else {
                "exhausted"
            };
            metrics::counter!(
                common::metrics::metric_name("tfl_fetch_total"),
                "what" => what.to_string(),
                "outcome" => outcome
            )
            .increment(1);
            return Err(FetchFailed {
                what: what.to_owned(),
                status,
                body,
            }
            .into());
        }

        retried_status = Some(status);
        metrics::counter!(
            common::metrics::metric_name("tfl_fetch_retries_total"),
            "what" => what.to_string(),
            "status" => if status == StatusCode::TOO_MANY_REQUESTS { "429" } else { "5xx" }
        )
        .increment(1);
        let asked = requested_wait(retry_after.as_deref(), &body);
        // At least what TfL asked for (jittered up by at most a quarter,
        // capped at 5 minutes), else the jittered exponential backoff.
        let delay = policy.backoff.delay_honouring(attempt - 1, asked);
        tracing::warn!(
            %status,
            attempt,
            delay_secs = delay.as_secs(),
            asked_secs = asked.map(|d| d.as_secs()),
            "TfL {what} fetch failed; retrying"
        );
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
    fn the_status_poll_backoff_fits_inside_one_poll_interval() {
        // Worst case: every retry told to wait the backoff's cap, jittered
        // up by a quarter.
        let worst: Duration = (0..STATUS_RETRY.max_attempts - 1)
            .map(|_| STATUS_RETRY.backoff.max.mul_f64(1.25))
            .sum();
        assert!(
            worst < Duration::from_secs(300),
            "total backoff {worst:?} must not overrun the 300s poll interval"
        );
        assert_eq!(retry_policy("line-status"), STATUS_RETRY);
        for daily in [
            "stops",
            "stop-hubs",
            "lines",
            "route-sequence",
            "route-timetable",
        ] {
            assert_eq!(retry_policy(daily), DAILY_RETRY, "{daily}");
        }
        const { assert!(DAILY_RETRY.max_attempts > STATUS_RETRY.max_attempts) };
    }

    #[test]
    fn tfl_s_requested_wait_comes_from_the_header_or_the_429_body() {
        let body = r#"{ "statusCode": 429, "message": "Rate limit is exceeded. Try again in 17 seconds." }"#;
        assert_eq!(requested_wait(None, body), Some(Duration::from_secs(17)));
        assert_eq!(
            requested_wait(Some("5"), body),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            requested_wait(Some("soon"), body),
            Some(Duration::from_secs(17))
        );
        assert_eq!(requested_wait(None, "Internal Server Error"), None);
        assert_eq!(requested_wait(None, "Try again in 3 minutes."), None);
    }

    #[test]
    fn a_retry_waits_at_least_what_tfl_asked_for() {
        for attempt in 0..5 {
            let delay = DAILY_RETRY
                .backoff
                .delay_honouring(attempt, Some(Duration::from_secs(17)));
            assert!(
                delay >= Duration::from_secs(17) && delay <= Duration::from_secs(60).mul_f64(1.25),
                "attempt {attempt}: {delay:?}"
            );
        }
    }

    #[test]
    fn a_once_run_exits_transient_before_rejected() {
        let rejected =
            || Err(anyhow::anyhow!("refused").context(common::logging::ExitStatus::REJECTED));
        let transient = || Err(anyhow::anyhow!("TfL timed out"));
        assert!(worst_of(vec![Ok(()), Ok(())]).is_ok());
        let err = worst_of(vec![rejected(), transient()]).unwrap_err();
        assert_eq!(common::logging::exit_status_of(&err), 1);
        let err = worst_of(vec![Ok(()), rejected()]).unwrap_err();
        assert_eq!(common::logging::exit_status_of(&err), 3);
    }

    #[test]
    fn a_404_is_told_apart_from_other_failures() {
        let not_found = anyhow::Error::from(FetchFailed {
            what: "route-timetable".into(),
            status: StatusCode::NOT_FOUND,
            body: String::new(),
        });
        assert!(is_not_found(&not_found));
        assert_eq!(
            not_found.to_string(),
            "TfL route-timetable fetch failed: 404 Not Found "
        );
        let limited = anyhow::Error::from(FetchFailed {
            what: "route-timetable".into(),
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::new(),
        });
        assert!(!is_not_found(&limited));
        assert!(!is_not_found(&anyhow::anyhow!("timed out")));
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
