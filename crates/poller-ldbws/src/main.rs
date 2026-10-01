//! `poller-ldbws`: samples live departure-board data for every station any
//! line's inference logic depends on, and forwards parsed `StationSample`s
//! to the `api` crate's `/private/station-samples` ingestion endpoint.
//!
//! See `docs/superpowers/specs/2026-07-06-ldbws-sampler-poller-design.md`
//! for the full design and `docs/superpowers/plans/2026-07-06-ldbws-sampler-poller.md`
//! for the RDM facts this is built against (a documentation-discovery pass
//! against a fetched Swagger spec for RDM's Live Departure Board REST
//! product, `GetDepBoardWithDetails`). The exact RDM product-slug segment
//! of the base URL is a documented gap carried into `config.rs`, where it
//! is env-configurable rather than guessed.
//!
//! Request volume (LEG-18): the current cadence and station set were
//! accepted by the repo owner under the Rail Data Marketplace terms on
//! 2026-09-27, so the defaults are unchanged. `config.rs` has three
//! operator knobs, all off by default, for cutting the volume without a
//! code change: an hourly request budget (`budget.rs`), a pinned-lines-only
//! filter and a station cap (both applied by `api`).
//!
//! Unlike the other three pollers, this one calls a second `api` endpoint
//! first (`GET /private/sample-stations`) to learn which CRS codes to
//! sample, then makes one LDBWS call *per station* each cycle — there is
//! no bulk/multi-station LDBWS operation.

mod budget;
mod config;
mod platform_history;
mod rotation;
mod schema;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use budget::{BudgetLimit, RequestBudget};
use chrono::Utc;
use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use common::{StationDeparture, StationSample};
use config::Config;
use platform_history::PlatformHistory;
use reqwest::{Client, StatusCode};
use rotation::Rotation;

/// Per-request timeout — see the other three pollers' identical rationale.
/// 30s is comfortably short relative to the 60s default poll interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of `numRows` values `fetch_departures` will try for a single
/// station before giving up on it for the cycle: the operator-configured
/// value plus up to three halved fallbacks (10 -> 5 -> 2 -> 1 for the
/// default `num_rows`). This is a cap on top of `numrows_step_down`'s own
/// natural floor at 1, not a replacement for it — it bounds how long one
/// troublesome station can hold up the rest of the per-station loop even
/// for an operator-configured `num_rows` much larger than 10, where the
/// halving sequence alone would otherwise take many more attempts to reach
/// 1.
const MAX_NUMROWS_ATTEMPTS: u32 = 4;

/// Delay between successive numRows-fallback attempts for the *same*
/// station. Deliberately much shorter than `poller-tfl`'s 2s/4s backoff
/// (`crates/poller-tfl/src/main.rs`'s `retry_delay`): that poller retries
/// once per cycle against a single call, whereas this poller calls
/// `GetDepBoardWithDetails` once *per station* — up to ~280 of them, see
/// `lines/*.toml`'s `sample_stations` — inside the same cycle, so a heavy
/// per-attempt delay compounds badly if several busy stations need it in
/// the same cycle. Still non-zero: RDM is a real, rate-limited external API
/// and a run of fallback attempts must not hammer it back-to-back.
const NUMROWS_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Upper bound on how long the whole per-station sampling loop in
/// `poll_once` may run in a single cycle, regardless of how many stations
/// there are or how slow individual upstream responses are.
///
/// Signal Box Audit, poll-area Low finding -- "per-station pollers have no
/// per-cycle time budget": before this, the loop over every sample station
/// (`lines/*.toml`'s deduplicated `sample_stations`, easily 100+ entries)
/// had no cap of its own -- `fetch_departures`'s per-request
/// `REQUEST_TIMEOUT` (30s) plus up to `MAX_NUMROWS_ATTEMPTS` retries with
/// `NUMROWS_RETRY_DELAY` waits bounds *one* station, but nothing bounded
/// the sum across all of them, so enough individually-slow (not even
/// hanging) stations in one cycle could still let that cycle run for many
/// multiples of `poll_interval_secs`, degrading every subsequent cycle
/// gracelessly rather than boundedly. 45s is comfortably under this
/// crate's own 60s conservative `poll_interval_secs` default (see
/// `config.rs`) so a budget-exceeded cycle still yields back well before
/// the next tick would otherwise be starved entirely -- deliberately NOT
/// derived from `poll_interval_secs` itself, an operator-configured value
/// with no guaranteed relationship to how long sampling every station
/// should take.
const CYCLE_TIME_BUDGET: Duration = Duration::from_secs(45);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    common::logging::init("poller-ldbws");

    let config = Config::parse();
    let progress = health_http::spawn_liveness(&config.health);
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let internal_oauth = config.internal_oauth.token_cache();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    // `Rc<RefCell<_>>`, mirroring `poller-tfl`'s own `dlr_state` exactly
    // (see that crate's `main.rs` for the full reasoning) -- `run_poll_loop`'s
    // `cycle: FnMut() -> Fut` can't let a per-call `Fut` borrow the
    // closure's captured environment past that one call, so each cycle
    // clones the `Rc` into its own `async move` block instead of capturing
    // `platform_history` by reference.
    let platform_history = Rc::new(RefCell::new(PlatformHistory::new()));
    // Same shape, for the station rotation (SVC-04).
    let rotation = Rc::new(RefCell::new(Rotation::new(std::time::Instant::now())));
    // And for the optional hourly request budget (LEG-18), whose rolling
    // window has to outlive a single cycle.
    let budget = Rc::new(RefCell::new(RequestBudget::new(
        config.hourly_request_budget,
        config.poll_interval_secs,
    )));
    // Registered at 0 so the counter exists (and `rate()` works) before
    // the first skip; stays 0 forever with no budget set.
    for limit in [BudgetLimit::Cycle, BudgetLimit::Hour] {
        metrics::counter!(
            common::metrics::metric_name("ldbws_budget_skipped_polls_total"),
            "limit" => limit.as_str()
        )
        .increment(0);
    }
    if let Some(per_cycle) = budget.borrow().per_cycle_limit() {
        tracing::info!(
            hourly_request_budget = config.hourly_request_budget,
            per_cycle,
            "LDBWS hourly request budget enabled"
        );
    }

    common::poller_loop::run_poll_loop(
        "ldbws",
        &client,
        &config.api_ingest_url,
        &internal_oauth,
        poll_interval,
        config.metrics.metrics_enabled,
        config.metrics_port,
        &progress,
        || {
            let platform_history = Rc::clone(&platform_history);
            let rotation = Rc::clone(&rotation);
            let budget = Rc::clone(&budget);
            let client = &client;
            let config = &config;
            let internal_oauth = &internal_oauth;
            async move {
                let mut history = std::mem::take(&mut *platform_history.borrow_mut());
                let mut rotation_state = rotation.replace(Rotation::new(std::time::Instant::now()));
                // The placeholder carries the same limits, so even a cycle
                // that never put its state back could not lift the budget.
                let mut budget_state = budget.replace(RequestBudget::new(
                    config.hourly_request_budget,
                    config.poll_interval_secs,
                ));
                let result = poll_once(
                    client,
                    config,
                    &mut history,
                    &mut rotation_state,
                    &mut budget_state,
                    internal_oauth,
                )
                .await;
                *platform_history.borrow_mut() = history;
                *rotation.borrow_mut() = rotation_state;
                *budget.borrow_mut() = budget_state;
                result
            }
        },
    )
    .await
}

async fn poll_once(
    client: &Client,
    config: &Config,
    platform_history: &mut PlatformHistory,
    rotation: &mut Rotation,
    request_budget: &mut RequestBudget,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<()> {
    let stations =
        well_formed_stations(fetch_sample_stations(client, config, internal_oauth).await?);
    tracing::info!(count = stations.len(), "fetched station list to sample");

    // SVC-04: start where the previous cycle stopped, so the budget no
    // longer always truncates the same (late-alphabet) tail. Same number of
    // requests per cycle -- see `rotation`'s module docs.
    let unix_secs = u64::try_from(Utc::now().timestamp()).unwrap_or_default();
    let ordered = rotation.order(&stations, unix_secs, config.poll_interval_secs);
    for crs in rotation.prune_invalid(&ordered) {
        tracing::info!(crs = %crs, "station LDBWS rejected as an invalid CRS is no longer in the sample list");
        set_invalid_crs_gauge(&crs, false);
    }
    // Stations LDBWS rejected as invalid are left out until their hourly
    // re-probe is due -- see `rotation`'s module docs.
    let polled = rotation.pollable(&ordered, std::time::Instant::now());
    request_budget.start_cycle();
    let CycleSampling {
        samples,
        completed,
        skipped_for_budget,
        invalid_crs,
        cut_short: _,
    } = sample_stations_within_budget(
        client,
        config,
        platform_history,
        request_budget,
        &polled,
        CYCLE_TIME_BUDGET,
    )
    .await;
    if let Some((skipped, limit)) = skipped_for_budget {
        record_budget_skip(config, polled.len(), skipped, limit);
    }
    let now = std::time::Instant::now();
    for crs in rotation.finish_cycle(
        &polled,
        completed,
        samples.iter().map(|sample| sample.crs.as_str()),
        now,
    ) {
        tracing::info!(crs = %crs, "station LDBWS had rejected as an invalid CRS sampled successfully again");
        set_invalid_crs_gauge(&crs, false);
    }
    for (crs, body) in &invalid_crs {
        record_invalid_crs(rotation, crs, body, now);
    }
    let stalest = rotation.stalest_age(&ordered, now);
    record_cycle_metrics(ordered.len(), completed, samples.len(), stalest);
    record_rotation_progress(rotation, polled.len(), completed, stalest, now);

    if samples.is_empty() {
        tracing::warn!("no station samples collected this cycle; nothing to post");
        return Ok(());
    }

    ingest::post_batch_retrying(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &samples,
        "station samples",
        common::poller_loop::post_retry_budget(Duration::from_secs(config.poll_interval_secs)),
    )
    .await
}

/// SVC-04's per-cycle gauges: how many stations there are, how many the
/// cycle got through (attempted to completion) and sampled successfully,
/// and how long ago the least recently sampled station was sampled -- the
/// number to alert on if the rotation ever stops reaching part of the list.
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
)]
fn record_cycle_metrics(total: usize, completed: usize, sampled: usize, stalest: Duration) {
    metrics::gauge!(common::metrics::metric_name("ldbws_stations_total")).set(total as f64);
    metrics::gauge!(common::metrics::metric_name(
        "ldbws_stations_attempted_per_cycle"
    ))
    .set(completed as f64);
    metrics::gauge!(common::metrics::metric_name(
        "ldbws_stations_sampled_per_cycle"
    ))
    .set(sampled as f64);
    metrics::gauge!(common::metrics::metric_name(
        "ldbws_stalest_station_age_seconds"
    ))
    .set(stalest.as_secs_f64());
}

/// The aggregator's [`common::STATION_SAMPLE_MAX_AGE_MINUTES`], the age at
/// which it stops using a station's sample.
const AGGREGATOR_MAX_SAMPLE_AGE: Duration =
    Duration::from_secs(60 * common::STATION_SAMPLE_MAX_AGE_MINUTES as u64);

/// Logs and exports the rotation's progress. A cycle cut short by the time
/// budget is normal (the next cycle carries on from there; see `rotation`),
/// so it is only logged at debug. Instead there is one info line per full
/// pass over the list, plus `ldbws_full_rotation_seconds` and
/// `ldbws_full_rotation_cycles`. Warnings are kept for the case that
/// matters: a pass slower than the aggregator's sample-age limit, or a
/// station not sampled for longer than that limit, so the aggregator is
/// dropping it.
fn record_rotation_progress(
    rotation: &mut Rotation,
    total: usize,
    completed: usize,
    stalest: Duration,
    now: std::time::Instant,
) {
    if let Some(full) = rotation.record_progress(total, completed, now) {
        metrics::gauge!(common::metrics::metric_name("ldbws_full_rotation_seconds"))
            .set(full.duration.as_secs_f64());
        metrics::gauge!(common::metrics::metric_name("ldbws_full_rotation_cycles"))
            .set(f64::from(full.cycles));
        if full.duration > AGGREGATOR_MAX_SAMPLE_AGE {
            tracing::warn!(
                stations_total = total,
                cycles = full.cycles,
                rotation_secs = full.duration.as_secs(),
                max_sample_age_secs = AGGREGATOR_MAX_SAMPLE_AGE.as_secs(),
                "a full LDBWS station rotation took longer than the aggregator's sample-age \
                 limit, so stations go stale between samples; see \
                 ldbws_stations_attempted_per_cycle"
            );
        } else {
            tracing::info!(
                stations_total = total,
                cycles = full.cycles,
                rotation_secs = full.duration.as_secs(),
                "completed a full LDBWS station rotation"
            );
        }
    }
    match rotation.note_stale(stalest > AGGREGATOR_MAX_SAMPLE_AGE) {
        Some(true) => tracing::warn!(
            stalest_age_secs = stalest.as_secs(),
            max_sample_age_secs = AGGREGATOR_MAX_SAMPLE_AGE.as_secs(),
            "an LDBWS station has gone unsampled for longer than the aggregator's sample-age \
             limit; the aggregator now ignores it (ldbws_stalest_station_age_seconds)"
        ),
        Some(false) => tracing::info!(
            stalest_age_secs = stalest.as_secs(),
            "every LDBWS station is within the aggregator's sample-age limit again"
        ),
        None => {}
    }
}

/// `ldbws_invalid_crs_station{crs}`: 1 while LDBWS rejects `crs` as an
/// invalid CRS code, 0 once it recovers or leaves the list. The `crs` label
/// is the one exception to this crate's no-per-station-labels rule (see
/// `fetch_departures`): only codes LDBWS has actually rejected ever get a
/// series, i.e. catalogue typos, a handful at most, not the ~560 sampled
/// stations.
fn set_invalid_crs_gauge(crs: &str, invalid: bool) {
    metrics::gauge!(
        common::metrics::metric_name("ldbws_invalid_crs_station"),
        "crs" => crs.to_string()
    )
    .set(if invalid { 1.0 } else { 0.0 });
}

/// Records a station LDBWS rejected as an invalid CRS: logged (at error)
/// and flagged in `ldbws_invalid_crs_station` the first time only, so a
/// permanent catalogue typo is one log line, not one per cycle. The
/// rotation then leaves it out of the stalest-age gauge and out of the
/// cycle until its hourly re-probe.
fn record_invalid_crs(rotation: &mut Rotation, crs: &str, body: &str, now: std::time::Instant) {
    if rotation.mark_invalid(crs, now) {
        tracing::error!(
            crs = %crs,
            response = %body,
            reprobe_secs = rotation::INVALID_CRS_REPROBE.as_secs(),
            "LDBWS rejects this station as an invalid CRS code; excluding it from sampling and \
             from ldbws_stalest_station_age_seconds (re-probed hourly). Fix the lines/*.toml \
             file that lists it (cargo run -p line-catalogue-validator)"
        );
        set_invalid_crs_gauge(crs, true);
    } else {
        tracing::debug!(crs = %crs, "re-probed station is still an invalid CRS code");
    }
}

/// LEG-18: counts the station polls the hourly request budget skipped this
/// cycle (`ldbws_budget_skipped_polls_total`, labelled by which limit) and
/// logs one warning. Never called with no budget set.
fn record_budget_skip(config: &Config, total: usize, skipped: usize, limit: BudgetLimit) {
    metrics::counter!(
        common::metrics::metric_name("ldbws_budget_skipped_polls_total"),
        "limit" => limit.as_str()
    )
    .increment(skipped as u64);
    tracing::warn!(
        stations_total = total,
        stations_skipped = skipped,
        limit = limit.as_str(),
        hourly_request_budget = config.hourly_request_budget,
        "LDBWS hourly request budget reached; skipping the rest of this cycle's \
         stations (the next cycle starts with them)"
    );
}

/// What one budgeted sampling pass produced.
#[derive(Debug)]
struct CycleSampling {
    samples: Vec<StationSample>,
    /// Stations attempted to completion, successfully or not, before the
    /// budget ran out -- the first `completed` of the list passed in.
    completed: usize,
    /// Stations not polled this cycle because the hourly request budget
    /// (LEG-18, off by default) refused them, and which limit did. Always
    /// `None` with no budget set.
    skipped_for_budget: Option<(usize, BudgetLimit)>,
    /// Stations LDBWS answered "Invalid crs code supplied" for, with the
    /// response body -- a permanent error, handled apart from the
    /// transient failures that are just logged and retried next time.
    invalid_crs: Vec<(String, String)>,
    /// The time budget ran out before every station was attempted. The
    /// normal case in production: the next cycle carries on from here.
    cut_short: bool,
}

/// Samples every station in `stations`, but never for longer than
/// `budget` in total: if the per-station loop (see `sample_all_stations`)
/// hasn't finished within `budget`, it's aborted in place and whatever
/// samples were already collected are returned as-is, with `cut_short`
/// set. This is [`sample_stations_until`] with a `budget`-long sleep as its
/// cut-off.
///
/// `request_budget` is the separate, optional hourly request budget: when
/// it refuses a station, the rest of the list is skipped for this cycle
/// and reported in `skipped_for_budget` (see `record_budget_skip`).
async fn sample_stations_within_budget(
    client: &Client,
    config: &Config,
    platform_history: &mut PlatformHistory,
    request_budget: &mut RequestBudget,
    stations: &[String],
    budget: Duration,
) -> CycleSampling {
    sample_stations_until(
        client,
        config,
        platform_history,
        request_budget,
        stations,
        tokio::time::sleep(budget),
    )
    .await
}

/// Samples `stations` in order until every one has been attempted or
/// `cutoff` resolves, whichever comes first. Production passes a
/// `CYCLE_TIME_BUDGET` sleep (through [`sample_stations_within_budget`]).
/// Tests pass a cut-off they fire themselves, so how many stations a cut
/// cycle completed does not depend on wall-clock timing or machine load.
///
/// `cutoff` is polled first (`biased`): once it is ready the loop stops
/// there, even if a station's response arrived at the same moment.
async fn sample_stations_until(
    client: &Client,
    config: &Config,
    platform_history: &mut PlatformHistory,
    request_budget: &mut RequestBudget,
    stations: &[String],
    cutoff: impl Future<Output = ()>,
) -> CycleSampling {
    let mut sampling = CycleSampling {
        samples: Vec::with_capacity(stations.len()),
        completed: 0,
        skipped_for_budget: None,
        invalid_crs: Vec::new(),
        cut_short: false,
    };
    sampling.cut_short = {
        let work = sample_all_stations(
            client,
            config,
            platform_history,
            request_budget,
            stations,
            &mut sampling,
        );
        tokio::select! {
            biased;
            () = cutoff => true,
            () = work => false,
        }
    };

    if sampling.cut_short {
        // Debug, not warn: this is every production cycle (~255 of 560
        // stations fit the budget) and the rotation is designed around it.
        // `record_rotation_progress` reports per full pass instead.
        tracing::debug!(
            stations_total = stations.len(),
            stations_completed = sampling.completed,
            stations_sampled = sampling.samples.len(),
            "per-cycle station-sampling time budget reached; the next cycle starts where \
             this one stopped"
        );
    }

    sampling
}

/// The per-station loop itself, extracted so `sample_stations_until` can
/// race it against its cut-off -- when the cut-off fires, this
/// future (and its local state) is dropped mid-iteration, but every sample
/// already pushed into the caller-owned `samples` accumulator before that
/// point survives, since it's a `&mut` borrow of state the caller owns,
/// not state local to this future. The same goes for `skipped_for_budget`,
/// set just before the loop stops early for the request budget, and for
/// `invalid_crs`.
async fn sample_all_stations(
    client: &Client,
    config: &Config,
    platform_history: &mut PlatformHistory,
    request_budget: &mut RequestBudget,
    stations: &[String],
    out: &mut CycleSampling,
) {
    for (index, crs) in stations.iter().enumerate() {
        if let Err(limit) = request_budget.check(std::time::Instant::now()) {
            out.skipped_for_budget = Some((stations.len() - index, limit));
            return;
        }
        match fetch_departures(client, config, request_budget, crs).await {
            Ok(mut departures) => {
                platform_history.apply(crs, &mut departures);
                out.samples.push(StationSample {
                    crs: crs.clone(),
                    polled_at: Utc::now(),
                    departures,
                });
            }
            Err(err) => match err.downcast::<InvalidCrs>() {
                // Logged once per station by `record_invalid_crs`, not
                // here every cycle.
                Ok(invalid) => out.invalid_crs.push((crs.clone(), invalid.body)),
                Err(err) => {
                    tracing::error!(crs = %crs, error = ?err, "failed to sample station; skipping");
                }
            },
        }
        out.completed += 1;
    }
}

/// Calls the `api` crate's own `/private/sample-stations` endpoint — not an
/// RDM endpoint — to get the deduplicated CRS list computed from the
/// loaded line catalogue. Sent with an internal-oauth bearer token, not
/// the RDM API key.
async fn fetch_sample_stations(
    client: &Client,
    config: &Config,
    tokens: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<Vec<String>> {
    let url = sample_stations_url(config)?;
    ingest::get_json(client, &url, tokens).await
}

/// Keeps only stations that are exactly three ASCII letters (uppercased,
/// first occurrence kept), warning about and skipping anything else.
///
/// Defence in depth for M3 (2026-09-26 review): each station is spliced
/// into a `GetDepBoardWithDetails/{crs}` URL path sent with the org's RDM
/// key. `api` validates custom-line stations at write time and normalises
/// the list it serves, but this poller should not depend on that: a row
/// written before the validation existed, or any future bug upstream,
/// must not be able to put `/`, `?` or other URL-structuring characters
/// into that path.
fn well_formed_stations(stations: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::with_capacity(stations.len());
    stations
        .into_iter()
        .filter_map(|crs| {
            if crs.len() == 3 && crs.bytes().all(|b| b.is_ascii_alphabetic()) {
                Some(crs.to_ascii_uppercase())
            } else {
                tracing::warn!(crs = ?crs, "skipping sample station that is not a 3-letter CRS code");
                None
            }
        })
        .filter(|crs| seen.insert(crs.clone()))
        .collect()
}

/// `api_sample_stations_url` plus the LEG-18 station-set knobs as query
/// parameters. With neither knob set it is returned unchanged, so `api`
/// sees exactly the request it always has.
fn sample_stations_url(config: &Config) -> anyhow::Result<String> {
    if !config.sample_pinned_lines_only && config.sample_max_stations == 0 {
        return Ok(config.api_sample_stations_url.clone());
    }
    let mut url = reqwest::Url::parse(&config.api_sample_stations_url)?;
    {
        let mut query = url.query_pairs_mut();
        if config.sample_pinned_lines_only {
            query.append_pair("pinned_lines_only", "true");
        }
        if config.sample_max_stations > 0 {
            query.append_pair("max_stations", &config.sample_max_stations.to_string());
        }
    }
    Ok(url.into())
}

/// The two ways a single `GetDepBoardWithDetails` attempt can fail:
/// `Status` (a non-2xx response, with its body already drained) is the one
/// `fetch_departures`'s retry loop inspects and can act on; `Other` covers
/// everything else (connection errors, timeouts, body-read failures) and is
/// always propagated immediately -- a smaller `numRows` has no bearing on
/// either.
enum FetchError {
    Status(StatusCode, String),
    Other(anyhow::Error),
}

impl From<reqwest::Error> for FetchError {
    fn from(err: reqwest::Error) -> Self {
        FetchError::Other(err.into())
    }
}

/// LDBWS's answer for a CRS code it does not know, e.g. a catalogue typo.
/// Seen in production (2026-09-28, "ANV") as
/// `400 Bad Request {"Message":"Invalid crs code supplied"}`, returned on
/// every request, so it is a permanent per-station error rather than a
/// failure worth retrying.
#[derive(Debug)]
struct InvalidCrs {
    crs: String,
    body: String,
}

impl std::fmt::Display for InvalidCrs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LDBWS rejected {} as an invalid CRS code: {}",
            self.crs, self.body
        )
    }
}

impl std::error::Error for InvalidCrs {}

/// Whether a failed response is LDBWS's invalid-CRS answer. Matches the
/// status and the message text (case-insensitively, anywhere in the body,
/// so a change of JSON wrapping does not break it); any other 400 stays an
/// ordinary failure.
fn is_invalid_crs_response(status: StatusCode, body: &str) -> bool {
    status == StatusCode::BAD_REQUEST && body.to_ascii_lowercase().contains("invalid crs code")
}

/// Steps a `numRows` value down for a retry after a 500 from RDM, per the
/// repo owner's own empirical finding that a smaller `numRows` succeeds
/// where the default fails for a busy terminus (see this module's docs).
/// Halves down to (and stops at) 1 rather than jumping straight to some
/// fixed small number, since exactly how much headroom a given station
/// needs under load is the unknown this is probing for -- and per
/// docs/superpowers/specs/2026-08-31-sample-data-availability-design.md's
/// Correction 1, the aggregator only needs `min_sample_size` (default 3)
/// *relevant* departures pooled across a line's whole `sample_stations`
/// list to report anything at all, so even a heavily-reduced `numRows` at
/// one busy station is far from useless. Returns `None` once `current` is
/// already at the floor (1) -- nothing smaller left to try.
fn numrows_step_down(current: u32) -> Option<u32> {
    if current <= 1 {
        None
    } else {
        Some((current / 2).max(1))
    }
}

/// Worth retrying with a smaller `numRows`: a 5xx is exactly the failure
/// mode reported (`GetDepBoardWithDetails` failing at `numRows=10` for a
/// busy terminus like PAD but succeeding at a smaller value) -- consistent
/// with some internal RDM limit (response size or generation time) that
/// scales with `numRows` x station busyness. A 4xx is a different class of
/// problem entirely (bad API key, bad CRS, an RDM auth change) that a
/// smaller `numRows` will never fix -- retrying it would just mask a real
/// misconfiguration behind repeated, pointless requests. 429 is
/// deliberately excluded too: it's a quota/rate problem, not a
/// payload-size-or-generation-time one, and there is no evidence from the
/// reported symptom that a smaller `numRows` buys anything against it.
fn should_retry_with_smaller_rows(status: StatusCode) -> bool {
    status.is_server_error()
}

/// One `GetDepBoardWithDetails` call for a single station at a specific
/// `num_rows`, with no retry logic of its own -- `fetch_departures` owns
/// the retry loop so it can vary `num_rows` between attempts.
async fn fetch_departures_once(
    client: &Client,
    config: &Config,
    crs: &str,
    num_rows: u32,
) -> Result<String, FetchError> {
    let url = format!(
        "{}/GetDepBoardWithDetails/{crs}?numRows={num_rows}",
        config.ldbws_base_url
    );

    let response = client
        .get(&url)
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(FetchError::Status(status, body));
    }

    Ok(response.text().await?)
}

/// Samples a single station, retrying a 500 with progressively smaller
/// `numRows` values (see `numrows_step_down`) before giving up for this
/// cycle -- the evidence-backed fix for the reported "failed to sample
/// station; skipping crs=PAD" symptom at busy termini during rush hour. A
/// non-5xx failure (bad key, bad CRS, a network error) is never retried,
/// so a genuinely different failure class can't be masked or hammered by
/// this loop. See `docs/superpowers/specs/2026-07-06-ldbws-sampler-poller-design.md`.
///
/// Every attempt, fallback retries included, uses up one request from
/// `request_budget`; a retry the budget refuses ends the station's attempts
/// for this cycle with an error instead.
async fn fetch_departures(
    client: &Client,
    config: &Config,
    request_budget: &mut RequestBudget,
    crs: &str,
) -> anyhow::Result<Vec<StationDeparture>> {
    let mut num_rows = config.num_rows;
    let mut attempt = 1;
    let mut fell_back = false;

    loop {
        if let Err(limit) = request_budget.try_acquire(std::time::Instant::now()) {
            anyhow::bail!(
                "LDBWS fetch for {crs} stopped before attempt {attempt} (numRows={num_rows}): \
                 the hourly request budget's {} limit was reached",
                limit.as_str()
            );
        }
        match fetch_departures_once(client, config, crs, num_rows).await {
            Ok(body) => {
                if fell_back {
                    tracing::warn!(
                        crs = %crs,
                        num_rows,
                        attempt,
                        "sampled station after falling back to a smaller numRows"
                    );
                    // No `crs` label here -- deliberately, per
                    // docs/superpowers/specs/2026-08-29-metrics-design.md's
                    // own "Per-line / per-station cardinality metrics" non-
                    // goal, which names "ldbws sample results by station" as
                    // explicitly deferred: this poller samples one station
                    // per line across a catalogue of 50-100+ lines, and
                    // labeling a metric by CRS is exactly the unbounded-
                    // cardinality trap that doc already rejected for v1.
                    // `crs` still appears on the structured log line above
                    // -- logs don't carry the same per-series cardinality
                    // cost a Prometheus label does.
                    metrics::counter!(
                        common::metrics::metric_name("ldbws_numrows_fallback_total"),
                        "outcome" => "recovered"
                    )
                    .increment(1);
                }
                return schema::parse_departures(&body);
            }
            Err(FetchError::Other(err)) => return Err(err),
            Err(FetchError::Status(status, body)) if is_invalid_crs_response(status, &body) => {
                return Err(InvalidCrs {
                    crs: crs.to_string(),
                    body,
                }
                .into());
            }
            Err(FetchError::Status(status, body)) => {
                let next_num_rows =
                    if attempt < MAX_NUMROWS_ATTEMPTS && should_retry_with_smaller_rows(status) {
                        numrows_step_down(num_rows)
                    } else {
                        None
                    };

                let Some(next_num_rows) = next_num_rows else {
                    if fell_back {
                        metrics::counter!(
                            common::metrics::metric_name("ldbws_numrows_fallback_total"),
                            "outcome" => "exhausted"
                        )
                        .increment(1);
                    }
                    anyhow::bail!(
                        "LDBWS fetch failed for {crs} after {attempt} attempt(s), last numRows={num_rows}: {status} {body}"
                    );
                };

                tracing::warn!(
                    crs = %crs,
                    %status,
                    from_num_rows = num_rows,
                    to_num_rows = next_num_rows,
                    "GetDepBoardWithDetails failed; retrying with a smaller numRows"
                );
                fell_back = true;
                num_rows = next_num_rows;
                attempt += 1;
                tokio::time::sleep(NUMROWS_RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod well_formed_station_tests {
    use super::well_formed_stations;

    #[test]
    fn keeps_three_letter_codes_and_skips_everything_else() {
        let input = [
            "WOK", "wok", " CLJ", "WA/", "WA", "WATX", "W?T", "\u{c4}BC", "", "clj", "PAD",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(well_formed_stations(input), vec!["WOK", "CLJ", "PAD"]);
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[test]
    fn numrows_halves_down_to_and_stops_at_one() {
        assert_eq!(numrows_step_down(10), Some(5));
        assert_eq!(numrows_step_down(5), Some(2));
        assert_eq!(numrows_step_down(2), Some(1));
        assert_eq!(numrows_step_down(1), None);
        // Odd, non-default configured values still land on the floor.
        assert_eq!(numrows_step_down(3), Some(1));
    }

    #[test]
    fn only_server_errors_trigger_a_numrows_retry() {
        assert!(should_retry_with_smaller_rows(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(should_retry_with_smaller_rows(StatusCode::BAD_GATEWAY));
        assert!(should_retry_with_smaller_rows(
            StatusCode::SERVICE_UNAVAILABLE
        ));
    }

    #[test]
    fn a_different_class_of_failure_is_never_retried() {
        // A bad API key, a bad CRS, or upstream rate-limiting are not going
        // to be fixed by asking for fewer rows -- retrying them would just
        // mask a real problem (or, for 429, burn quota for nothing).
        assert!(!should_retry_with_smaller_rows(StatusCode::UNAUTHORIZED));
        assert!(!should_retry_with_smaller_rows(StatusCode::FORBIDDEN));
        assert!(!should_retry_with_smaller_rows(StatusCode::NOT_FOUND));
        assert!(!should_retry_with_smaller_rows(
            StatusCode::TOO_MANY_REQUESTS
        ));
    }

    /// Fills every `Config` field `fetch_departures` doesn't touch with
    /// inert placeholders -- only `ldbws_base_url`, `rdm_api_key`, and
    /// `num_rows` matter to the code under test here.
    fn test_config(base_url: String, num_rows: u32) -> Config {
        Config {
            ldbws_base_url: base_url,
            rdm_api_key: "test-api-key".to_string(),
            num_rows,
            api_sample_stations_url: "http://api:8080/private/sample-stations".to_string(),
            api_ingest_url: "http://api:8080/private/station-samples".to_string(),
            internal_oauth: common::oauth_client::InternalOAuthArgs {
                internal_oauth_token_url: "http://auth.invalid/token".to_string(),
                internal_oauth_client_id: "distant-signal-internal".to_string(),
                internal_oauth_scope: "groups".to_string(),
                internal_oauth_username: "svc-poller-ldbws".to_string(),
                internal_oauth_password: "app-password".to_string(),
            },
            poll_interval_secs: 60,
            hourly_request_budget: 0,
            sample_pinned_lines_only: false,
            sample_max_stations: 0,
            metrics_port: 9091,
            metrics: common::service_args::MetricsArgs {
                metrics_enabled: false,
            },
            health: common::service_args::HealthArgs {
                health_bind_url: "127.0.0.1:0".to_string(),
                progress_stall_secs: 1800,
            },
        }
    }

    const ONE_SERVICE_BODY: &str = r#"{"trainServices":[{"serviceID":"svc-1","operatorCode":"GW","destination":[{"crs":"RDG"}],"std":"10:00","etd":"10:05","isCancelled":false}]}"#;

    #[tokio::test]
    async fn a_200_on_the_first_try_is_not_retried_or_delayed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let start = std::time::Instant::now();
        let departures = fetch_departures(&client, &config, &mut RequestBudget::unlimited(), "PAD")
            .await
            .expect("a 200 on the first try must succeed");
        let elapsed = start.elapsed();

        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].service_id, "svc-1");
        // No retry means no `NUMROWS_RETRY_DELAY` sleep was ever hit.
        assert!(
            elapsed < NUMROWS_RETRY_DELAY,
            "an unretried fetch took {elapsed:?}, as long as a real retry delay"
        );
        // wiremock's `.expect(1)` (asserted on Drop) is the real assertion
        // that only one request was made.
    }

    #[tokio::test]
    async fn a_500_at_the_default_numrows_falls_back_to_a_smaller_value_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let departures = fetch_departures(&client, &config, &mut RequestBudget::unlimited(), "PAD")
            .await
            .expect("falling back to numRows=5 must recover");

        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].service_id, "svc-1");
        // The two `.expect`/`.up_to_n_times` mock assertions above (checked
        // on `Drop`) confirm exactly one request was made at numRows=10 and
        // exactly one at numRows=5 -- the fallback, not a coincidence of a
        // looser matcher.
    }

    #[tokio::test]
    async fn giving_up_after_the_smallest_numrows_still_500s_returns_an_error_not_a_hang() {
        let server = MockServer::start().await;
        // num_rows=3 steps down to 1 (numrows_step_down(3) == Some(1)) and
        // then stops -- exactly two attempts total, both failing, so this
        // confirms the loop terminates cleanly instead of retrying forever.
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "3"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "1"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 3);
        let client = Client::new();

        let result =
            fetch_departures(&client, &config, &mut RequestBudget::unlimited(), "PAD").await;

        assert!(
            result.is_err(),
            "every numRows value 500ing must surface as an error, not silently succeed"
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("PAD"));
        assert!(message.contains("500"));
        // The two `.expect(1)` mocks above (checked on `Drop`) confirm the
        // loop stopped at exactly two attempts (numRows=3 then 1) rather
        // than looping indefinitely or re-trying numRows=1 again.
    }

    #[tokio::test]
    async fn a_401_is_never_retried_with_a_smaller_numrows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad api key"))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let result =
            fetch_departures(&client, &config, &mut RequestBudget::unlimited(), "PAD").await;

        assert!(result.is_err());
        let requests = server
            .received_requests()
            .await
            .expect("request recording is on by default");
        assert_eq!(
            requests.len(),
            1,
            "a 401 must not trigger any numRows fallback retry"
        );
    }

    #[tokio::test]
    async fn a_cycle_time_budget_bounds_total_sampling_time_across_slow_stations() {
        // Three stations, each individually well within a single request's
        // own timeout, but slow enough that all three together would take
        // far longer than the tiny budget this test gives the whole loop.
        // Without the budget, this would take >= 3 * 150ms; with it, the
        // loop must give up once the 200ms budget elapses.
        let server = MockServer::start().await;
        for crs in ["AAA", "BBB", "CCC"] {
            Mock::given(method("GET"))
                .and(path(format!("/GetDepBoardWithDetails/{crs}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(ONE_SERVICE_BODY)
                        .set_delay(Duration::from_millis(150)),
                )
                .mount(&server)
                .await;
        }
        let config = test_config(server.uri(), 10);
        let client = Client::new();
        let stations = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
        let mut history = PlatformHistory::new();

        let start = std::time::Instant::now();
        let CycleSampling { samples, .. } = sample_stations_within_budget(
            &client,
            &config,
            &mut history,
            &mut RequestBudget::unlimited(),
            &stations,
            Duration::from_millis(200),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(150 * 3),
            "the budget should have cut the loop short well before all three \
             150ms-delayed stations finished, took {elapsed:?}"
        );
        assert!(
            samples.len() < stations.len(),
            "a budget-cut cycle must not have sampled every station: {samples:?}"
        );
    }

    #[tokio::test]
    async fn a_generous_budget_does_not_truncate_a_normal_cycle() {
        let server = MockServer::start().await;
        for crs in ["AAA", "BBB"] {
            Mock::given(method("GET"))
                .and(path(format!("/GetDepBoardWithDetails/{crs}")))
                .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
                .mount(&server)
                .await;
        }
        let config = test_config(server.uri(), 10);
        let client = Client::new();
        let stations = vec!["AAA".to_string(), "BBB".to_string()];
        let mut history = PlatformHistory::new();

        let CycleSampling { samples, .. } = sample_stations_within_budget(
            &client,
            &config,
            &mut history,
            &mut RequestBudget::unlimited(),
            &stations,
            CYCLE_TIME_BUDGET,
        )
        .await;

        assert_eq!(
            samples.len(),
            2,
            "a fast cycle well within budget must sample every station"
        );
    }

    /// Answers every board request, and fires `cutoff` when a cycle's
    /// `per_cycle + 1`th request arrives: the budget "runs out" while that
    /// station is in flight, exactly `per_cycle` stations into every cycle.
    struct CutAfter {
        per_cycle: usize,
        received: std::sync::atomic::AtomicUsize,
        cutoff: std::sync::Arc<tokio::sync::Notify>,
    }

    impl wiremock::Respond for CutAfter {
        fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
            let index = self
                .received
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if index % (self.per_cycle + 1) == self.per_cycle {
                // Stored as a permit before this response is even sent, so
                // the loop's biased select sees the cut-off first.
                self.cutoff.notify_one();
            }
            ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY)
        }
    }

    /// SVC-04 end to end through the real budgeted loop: a budget that fits
    /// two of five stations per cycle still reaches all five within
    /// ceil(5 / 2) = 3 cycles, each cycle attempting the same number of
    /// stations it did before rotation.
    ///
    /// The budget is a cut-off the mock server fires on each cycle's third
    /// request, not a wall-clock timeout: the old 500 ms budget over 200 ms
    /// responses completed only one station in a cycle on a loaded machine.
    #[tokio::test]
    async fn rotation_reaches_every_station_across_budget_cut_cycles() {
        let server = MockServer::start().await;
        let cutoff = std::sync::Arc::new(tokio::sync::Notify::new());
        Mock::given(method("GET"))
            .and(wiremock::matchers::path_regex(
                "^/GetDepBoardWithDetails/[A-Z]{3}$",
            ))
            .respond_with(CutAfter {
                per_cycle: 2,
                received: std::sync::atomic::AtomicUsize::new(0),
                cutoff: std::sync::Arc::clone(&cutoff),
            })
            .mount(&server)
            .await;
        let names = ["AAA", "BBB", "CCC", "DDD", "EEE"];
        let config = test_config(server.uri(), 10);
        let client = Client::new();
        let stations: Vec<String> = names.iter().map(ToString::to_string).collect();
        let mut history = PlatformHistory::new();
        let mut rotation = Rotation::new(std::time::Instant::now());

        let mut seen = std::collections::HashSet::new();
        let mut per_cycle = Vec::new();
        for cycle in 0..3u64 {
            let ordered = rotation.order(&stations, cycle * 60, 60);
            let CycleSampling {
                samples,
                completed,
                cut_short,
                ..
            } = sample_stations_until(
                &client,
                &config,
                &mut history,
                &mut RequestBudget::unlimited(),
                &ordered,
                cutoff.notified(),
            )
            .await;
            assert!(cut_short, "cycle {cycle} ran out of budget");
            rotation.finish_cycle(
                &ordered,
                completed,
                samples.iter().map(|s| s.crs.as_str()),
                std::time::Instant::now(),
            );
            per_cycle.push(completed);
            seen.extend(samples.into_iter().map(|s| s.crs));
        }

        assert_eq!(per_cycle, vec![2, 2, 2], "same per-cycle count every cycle");
        assert_eq!(
            seen.len(),
            5,
            "every station sampled within 3 cycles: {seen:?}"
        );
    }

    async fn server_answering(names: &[&str]) -> MockServer {
        let server = MockServer::start().await;
        for crs in names {
            Mock::given(method("GET"))
                .and(path(format!("/GetDepBoardWithDetails/{crs}")))
                .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
                .mount(&server)
                .await;
        }
        server
    }

    /// LEG-18: with the knob at its default (0), the budget built from the
    /// config never skips anything, however long the list.
    #[tokio::test]
    async fn the_default_request_budget_skips_nothing() {
        let names: Vec<String> = (0..200).map(|i| format!("S{i:02}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let server = server_answering(&name_refs).await;
        let config = test_config(server.uri(), 10);
        let mut budget =
            RequestBudget::new(config.hourly_request_budget, config.poll_interval_secs);
        budget.start_cycle();

        let sampling = sample_stations_within_budget(
            &Client::new(),
            &config,
            &mut PlatformHistory::new(),
            &mut budget,
            &names,
            CYCLE_TIME_BUDGET,
        )
        .await;

        assert_eq!(sampling.samples.len(), names.len());
        assert_eq!(sampling.completed, names.len());
        assert_eq!(sampling.skipped_for_budget, None);
        assert_eq!(
            server.received_requests().await.expect("recording").len(),
            names.len()
        );
    }

    /// A budget of 180 an hour at 60 s cycles allows 3 requests a cycle:
    /// the rest of a 5-station list is skipped (not failed), no request is
    /// made for them, and the rotation starts the next cycle with them.
    #[tokio::test]
    async fn a_request_budget_skips_the_rest_of_the_cycle_and_the_rotation_resumes_there() {
        let names = ["AAA", "BBB", "CCC", "DDD", "EEE"];
        let server = server_answering(&names).await;
        let mut config = test_config(server.uri(), 10);
        config.hourly_request_budget = 180;
        let client = Client::new();
        let stations: Vec<String> = names.iter().map(ToString::to_string).collect();
        let mut history = PlatformHistory::new();
        let mut rotation = Rotation::new(std::time::Instant::now());
        let mut budget =
            RequestBudget::new(config.hourly_request_budget, config.poll_interval_secs);

        budget.start_cycle();
        let ordered = rotation.order(&stations, 0, 60);
        assert_eq!(ordered[0], "AAA", "clock offset 0 starts at the top");
        let first = sample_stations_within_budget(
            &client,
            &config,
            &mut history,
            &mut budget,
            &ordered,
            CYCLE_TIME_BUDGET,
        )
        .await;
        assert_eq!(first.completed, 3);
        assert_eq!(first.samples.len(), 3);
        assert_eq!(first.skipped_for_budget, Some((2, BudgetLimit::Cycle)));
        assert_eq!(
            server.received_requests().await.expect("recording").len(),
            3
        );
        rotation.finish_cycle(
            &ordered,
            first.completed,
            first.samples.iter().map(|s| s.crs.as_str()),
            std::time::Instant::now(),
        );

        budget.start_cycle();
        let ordered = rotation.order(&stations, 60, 60);
        assert_eq!(&ordered[..2], ["DDD", "EEE"], "skipped stations go first");
        let second = sample_stations_within_budget(
            &client,
            &config,
            &mut history,
            &mut budget,
            &ordered,
            CYCLE_TIME_BUDGET,
        )
        .await;
        assert_eq!(second.completed, 3);
        assert_eq!(second.skipped_for_budget, Some((2, BudgetLimit::Cycle)));
    }

    /// A numRows fallback retry is a request too: with one request left,
    /// a 500 is not retried and the station fails for the cycle.
    #[tokio::test]
    async fn a_request_budget_also_bounds_numrows_fallback_retries() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let mut budget = RequestBudget::new(60, 60); // 1 per cycle
        budget.start_cycle();

        let result = fetch_departures(&Client::new(), &config, &mut budget, "PAD").await;

        let message = result
            .expect_err("the only allowed attempt 500s")
            .to_string();
        assert!(message.contains("budget"), "{message}");
        assert_eq!(
            server.received_requests().await.expect("recording").len(),
            1
        );
    }

    /// LDBWS's real answer for an unknown code (prod logs, 2026-09-28).
    const INVALID_CRS_BODY: &str = r#"{"Message":"Invalid crs code supplied"}"#;

    #[test]
    fn only_a_400_saying_invalid_crs_is_an_invalid_crs() {
        assert!(is_invalid_crs_response(
            StatusCode::BAD_REQUEST,
            INVALID_CRS_BODY
        ));
        assert!(is_invalid_crs_response(
            StatusCode::BAD_REQUEST,
            "invalid CRS code supplied"
        ));
        assert!(!is_invalid_crs_response(
            StatusCode::BAD_REQUEST,
            r#"{"Message":"numRows out of range"}"#
        ));
        // A 5xx is transient whatever its body says (LBG's intermittent
        // 500s stay on the numRows-fallback / staleness path).
        assert!(!is_invalid_crs_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            INVALID_CRS_BODY
        ));
    }

    #[tokio::test]
    async fn an_invalid_crs_400_is_a_typed_error_and_is_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/ANV"))
            .respond_with(ResponseTemplate::new(400).set_body_string(INVALID_CRS_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);

        let err = fetch_departures(
            &Client::new(),
            &config,
            &mut RequestBudget::unlimited(),
            "ANV",
        )
        .await
        .expect_err("a 400 is a failure");

        let invalid = err
            .downcast_ref::<InvalidCrs>()
            .expect("an invalid-CRS 400 must surface as InvalidCrs");
        assert_eq!(invalid.crs, "ANV");
        assert_eq!(invalid.body, INVALID_CRS_BODY);
    }

    #[tokio::test]
    async fn a_transient_500_is_not_an_invalid_crs() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/LBG"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 2);

        let sampling = sample_stations_within_budget(
            &Client::new(),
            &config,
            &mut PlatformHistory::new(),
            &mut RequestBudget::unlimited(),
            &["LBG".to_string()],
            CYCLE_TIME_BUDGET,
        )
        .await;

        assert!(sampling.samples.is_empty());
        assert!(sampling.invalid_crs.is_empty());
        assert_eq!(sampling.completed, 1);
    }

    /// The prod incident end to end through the real sampling loop: a
    /// station LDBWS always rejects is requested once (not every cycle),
    /// flagged once in `ldbws_invalid_crs_station{crs}`, and left out of
    /// the stalest age, which therefore tracks the valid stations only --
    /// no longer the process's uptime.
    #[tokio::test]
    async fn a_permanently_invalid_station_is_flagged_once_and_not_stale() {
        let server = server_answering(&["AAA", "BBB"]).await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/ANV"))
            .respond_with(ResponseTemplate::new(400).set_body_string(INVALID_CRS_BODY))
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();
        let stations: Vec<String> = ["AAA", "ANV", "BBB"].map(String::from).to_vec();
        let mut history = PlatformHistory::new();
        let started = std::time::Instant::now();
        let mut rotation = Rotation::new(started);
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let mut last_now = started;
        for cycle in 0..3u64 {
            let ordered = rotation.order(&stations, cycle * 60, 60);
            // Pretend each cycle is a poll interval later than the last.
            let now = started + Duration::from_secs(60 * (cycle + 1));
            let polled = rotation.pollable(&ordered, now);
            let sampling = sample_stations_within_budget(
                &client,
                &config,
                &mut history,
                &mut RequestBudget::unlimited(),
                &polled,
                CYCLE_TIME_BUDGET,
            )
            .await;
            rotation.finish_cycle(
                &polled,
                sampling.completed,
                sampling.samples.iter().map(|s| s.crs.as_str()),
                now,
            );
            metrics::with_local_recorder(&recorder, || {
                for (crs, body) in &sampling.invalid_crs {
                    record_invalid_crs(&mut rotation, crs, body, now);
                }
            });
            last_now = now;
        }

        let anv_requests = server
            .received_requests()
            .await
            .expect("recording")
            .iter()
            .filter(|r| r.url.path().ends_with("/ANV"))
            .count();
        assert_eq!(anv_requests, 1, "not re-requested within the re-probe hour");
        assert_eq!(
            rotation.stalest_age(&stations, last_now),
            Duration::ZERO,
            "AAA and BBB were sampled this cycle; ANV no longer counts"
        );
        let rendered = handle.render();
        assert!(
            rendered.contains(r#"distant_signal_ldbws_invalid_crs_station{crs="ANV"} 1"#),
            "{rendered}"
        );
        assert!(!rendered.contains(r#"crs="AAA""#), "{rendered}");

        // Fixing the catalogue drops ANV from the list; its gauge goes to 0.
        let fixed: Vec<String> = ["AAA", "BBB"].map(String::from).to_vec();
        metrics::with_local_recorder(&recorder, || {
            for crs in rotation.prune_invalid(&fixed) {
                set_invalid_crs_gauge(&crs, false);
            }
        });
        assert!(
            handle
                .render()
                .contains(r#"distant_signal_ldbws_invalid_crs_station{crs="ANV"} 0"#)
        );
    }

    /// A full pass is exported once it completes; cycles in between leave
    /// the gauges at the previous pass.
    #[test]
    fn a_full_rotation_is_exported_when_it_completes() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let started = std::time::Instant::now();
        let mut rotation = Rotation::new(started);
        metrics::with_local_recorder(&recorder, || {
            for cycle in 1..=2u64 {
                let now = started + Duration::from_secs(60 * cycle);
                record_rotation_progress(&mut rotation, 560, 255, Duration::ZERO, now);
            }
        });
        assert!(!handle.render().contains("full_rotation"));
        metrics::with_local_recorder(&recorder, || {
            let now = started + Duration::from_secs(180);
            record_rotation_progress(&mut rotation, 560, 255, Duration::ZERO, now);
        });
        let rendered = handle.render();
        assert!(
            rendered.contains("distant_signal_ldbws_full_rotation_seconds 180"),
            "{rendered}"
        );
        assert!(
            rendered.contains("distant_signal_ldbws_full_rotation_cycles 3"),
            "{rendered}"
        );
    }

    #[test]
    fn the_sample_stations_url_is_unchanged_unless_a_knob_is_set() {
        let mut config = test_config("http://ldbws.invalid".to_string(), 10);
        assert_eq!(
            sample_stations_url(&config).expect("valid url"),
            "http://api:8080/private/sample-stations"
        );

        config.sample_max_stations = 150;
        assert_eq!(
            sample_stations_url(&config).expect("valid url"),
            "http://api:8080/private/sample-stations?max_stations=150"
        );

        config.sample_pinned_lines_only = true;
        assert_eq!(
            sample_stations_url(&config).expect("valid url"),
            "http://api:8080/private/sample-stations?pinned_lines_only=true&max_stations=150"
        );
    }
}
