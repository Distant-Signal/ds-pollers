//! `poller-tfl`: polls TfL's Unified API for line status across the modes
//! this app displays (tube, DLR, Overground, Elizabeth line, tram) and
//! forwards it to the `api` crate's `/private/tfl-line-status` endpoint.
//!
//! Unlike the four RDM pollers, what this one carries is already finished
//! line status — TfL publishes status directly, so nothing downstream has
//! to infer it from incidents or departure boards, and the aggregator is
//! not involved. `schema.rs` does the whole TfL→domain mapping (severity
//! codes above all) so the `api` crate never sees TfL's JSON.
//!
//! There is no historical endpoint on TfL's side. Everything this app can
//! ever show for "the Victoria line last Tuesday" is what this poller
//! wrote into `line_status_history` at the time.

mod config;
mod schema;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use common::ingest;
use config::Config;
use reqwest::{Client, StatusCode};

/// TfL's subscription-key header. Not in `common::ingest` alongside
/// `RDM_AUTH_HEADER_NAME`: that constant is there because four pollers and
/// the api crate all have to agree on it, whereas this one has exactly one
/// consumer.
const TFL_AUTH_HEADER_NAME: &str = "Ocp-Apim-Subscription-Key";

/// Per-request timeout, matching the other pollers: a peer that accepts the
/// connection and never answers would otherwise hang the poll loop forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Attempts per poll cycle before giving up and waiting for the next tick.
/// TfL's registered free tier is documented at roughly 500 requests per
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
/// `Config::parse()` and start polling TfL anonymously.
fn require_non_empty_key(key: &str) -> anyhow::Result<()> {
    if key.trim().is_empty() {
        anyhow::bail!(
            "TFL_APP_KEY must be set (see api-portal.tfl.gov.uk) — refusing to poll TfL anonymously"
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    // `clap` treats a present-but-empty env var as a supplied value, so an
    // orchestrator (e.g. `docker-compose.yml`'s `TFL_APP_KEY: ${TFL_APP_KEY}`)
    // that leaves the shell variable unset still gets `Config::parse()` to
    // succeed with `tfl_app_key = ""` rather than failing — silently sending
    // every request unauthenticated instead of refusing to start. Catch that
    // here, before the client is built.
    require_non_empty_key(&config.tfl_app_key)?;
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let delay =
        ingest::time_until_next_poll(&client, &config.api_ingest_url, &config.internal_token, poll_interval).await;
    if !delay.is_zero() {
        tracing::info!(delay_secs = delay.as_secs(), "data still fresh from a prior run; delaying first poll");
    }
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + delay, poll_interval);

    loop {
        interval.tick().await;

        if let Err(err) = poll_once(&client, &config).await {
            tracing::error!(error = ?err, "poll cycle failed; will retry next interval");
        }
    }
}

async fn poll_once(client: &Client, config: &Config) -> anyhow::Result<()> {
    let body = fetch_status_json(client, config).await?;
    let reports = schema::parse_line_status(&body, Utc::now())?;

    // Never post an empty batch. The ingest endpoint prunes TfL rows that
    // are missing from the batch it receives, so an empty one would read as
    // "TfL has no lines any more" and blank the whole section. The api side
    // guards this too; this is the half that knows it is a fault.
    if reports.is_empty() {
        anyhow::bail!("TfL returned no lines for modes {}; refusing to post an empty batch", config.tfl_modes);
    }

    tracing::info!(count = reports.len(), "parsed line statuses from TfL");

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        &config.internal_token,
        &reports,
        "TfL line statuses",
    )
    .await
}

async fn fetch_status_json(client: &Client, config: &Config) -> anyhow::Result<String> {
    let url = format!(
        "{}/Line/Mode/{}/Status",
        config.tfl_base_url.trim_end_matches('/'),
        config.tfl_modes
    );

    let mut attempt = 0;
    loop {
        let response = client
            .get(&url)
            .header(TFL_AUTH_HEADER_NAME, &config.tfl_app_key)
            .send()
            .await?;
        let status = response.status();

        if status.is_success() {
            return Ok(response.text().await?);
        }

        attempt += 1;
        if attempt >= MAX_ATTEMPTS || !should_retry(status) {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("TfL line-status fetch failed: {status} {body}");
        }

        let delay = retry_delay(attempt);
        tracing::warn!(%status, attempt, delay_secs = delay.as_secs(), "TfL line-status fetch failed; retrying");
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
        let total: u64 = (1..MAX_ATTEMPTS).map(|attempt| retry_delay(attempt).as_secs()).sum();
        assert!(total < 300, "total backoff {total}s must not overrun the 300s poll interval");
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
