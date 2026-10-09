//! `poller-tfl`: polls `TfL`'s Unified API for line status across the modes
//! this app displays (tube, DLR, Overground, Elizabeth line, tram) and
//! forwards it to the `api` crate's `/private/tfl-line-status` endpoint, or
//! (`INGEST_SINK=http+shadow|stream`, ingest plan 3c.2) to the
//! `ds:ingest:tfl` stream as `tfl-line-status/1`, whose body is the same
//! JSON; see `ingest_stream::snapshot`.
//!
//! Unlike the four RDM pollers, what this one carries is already finished
//! line status — `TfL` publishes status directly, so nothing downstream has
//! to infer it from incidents or departure boards, and the aggregator is
//! not involved. `schema.rs` does the whole TfL→domain mapping (severity
//! codes above all) so the `api` crate never sees `TfL`'s JSON.
//!
//! There is no historical endpoint on `TfL`'s side. Everything this app can
//! ever show for "the Victoria line last Tuesday" is what this poller
//! wrote into `line_status_history` at the time.

mod config;
mod schema;

use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Parser;
use common::ingest;
use config::Config;
use ingest_stream::snapshot::{SinkMode, SnapshotStream};
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
    common::metrics::ingest_sink_info(&config.ingest_sink.to_string());
    let progress = health_http::spawn_liveness(&config.health);
    // `clap` treats a present-but-empty env var as a supplied value, so an
    // orchestrator (e.g. `docker-compose.yml`'s `TFL_APP_KEY: ${TFL_APP_KEY}`)
    // that leaves the shell variable unset still gets `Config::parse()` to
    // succeed with `tfl_app_key = ""` rather than failing — silently sending
    // every request unauthenticated instead of refusing to start. Catch that
    // here, before the client is built.
    require_non_empty_key(&config.tfl_app_key)?;
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let internal_oauth = config.internal_oauth.token_cache();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let stream = stream_sink(&config)?;
    let stream = stream.as_ref();

    let cycle = || poll_once(&client, &config, &internal_oauth, stream);

    match (config.ingest_sink, stream) {
        (SinkMode::Stream, Some(stream)) => {
            // The startup cursor is the stream's newest entry (spec §11.3).
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
        }
        _ => {
            common::poller_loop::run_poll_loop(
                "tfl",
                &client,
                &config.api_ingest_url,
                &internal_oauth,
                poll_interval,
                config.metrics.metrics_enabled,
                config.metrics_port,
                &progress,
                cycle,
            )
            .await
        }
    }
}

/// The `ds:ingest:tfl` producer under `INGEST_SINK=http+shadow` or
/// `stream`; `None` under `http`. One part per snapshot (about 20 lines),
/// so the writer's prune sees every line.
fn stream_sink(config: &Config) -> anyhow::Result<Option<SnapshotStream>> {
    if !config.ingest_sink.produces() {
        return Ok(None);
    }
    let why = format!("INGEST_SINK={}", config.ingest_sink);
    let client = config.redis.client(&why).map_err(anyhow::Error::msg)?;
    tracing::info!(sink = %config.ingest_sink, "producing TfL snapshots to ds:ingest:tfl");
    Ok(Some(SnapshotStream::spawn(
        client,
        ingest_stream::streams::TFL,
        ingest_stream::SchemaId::new("tfl-line-status", 1)?,
        "poller-tfl",
        usize::MAX,
    )))
}

/// Sends one snapshot where `INGEST_SINK` says (see the module docs): the
/// api's `POST` under `http` and `http+shadow`, then the stream copy under
/// `http+shadow` and `stream`. Under `http+shadow` the copy is of a
/// snapshot the api accepted (a failed `POST` fails the cycle before it,
/// as poller-ldbws does), so the compare step counts the same snapshots on
/// both sides, and the copy never fails the cycle.
async fn deliver(
    client: &Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    stream: Option<&SnapshotStream>,
    reports: &[common::LineStatusReport],
    fetched_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    if config.ingest_sink.posts_http() {
        ingest::post_batch_retrying(
            client,
            &config.api_ingest_url,
            internal_oauth,
            reports,
            "TfL line statuses",
            common::poller_loop::post_retry_budget(Duration::from_secs(config.poll_interval_secs)),
        )
        .await?;
        if let Some(stream) = stream {
            stream.record_http(reports.len());
        }
    }
    if let Some(stream) = stream {
        match stream.publish(reports, fetched_at).await {
            Ok(()) => {}
            Err(err) if config.ingest_sink == SinkMode::HttpShadow => {
                tracing::warn!(error = %err, "shadow copy to ds:ingest:tfl not queued; the api POST already landed");
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

async fn poll_once(
    client: &Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    stream: Option<&SnapshotStream>,
) -> anyhow::Result<()> {
    // The snapshot's `produced_at` on the stream (decision D13): when it
    // was fetched, not when it is sent.
    let fetched_at = Utc::now();
    let body = fetch_status_json(client, config).await?;
    let reports = schema::parse_line_status(&body, fetched_at)?;

    // Never post an empty batch. The ingest endpoint prunes TfL rows that
    // are missing from the batch it receives, so an empty one would read as
    // "TfL has no lines any more" and blank the whole section. The api side
    // guards this too; this is the half that knows it is a fault.
    if reports.is_empty() {
        anyhow::bail!(
            "TfL returned no lines for modes {}; refusing to post an empty batch",
            config.tfl_modes
        );
    }

    tracing::info!(count = reports.len(), "parsed line statuses from TfL");

    deliver(client, config, internal_oauth, stream, &reports, fetched_at).await
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
