//! `poller-stations`: polls the RDM Stations JSON feed on an interval and
//! forwards parsed `StationReference`s to the `api` crate's
//! `/private/stations` ingestion endpoint.
//!
//! See `.superpowers/sdd/task-4-brief.md` for the RDM facts this is built
//! against (RSPS5050 P-03-00 Rev A, §6) — this is the best-documented of
//! the three RDM products: the `/stations` endpoint path and the 24-hour
//! poll frequency are both confirmed. The one open gap is the exact JSON
//! field casing (see `schema.rs` module docs for how that's handled).

mod config;
mod schema;

use std::time::Duration;

use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use config::Config;
use reqwest::Client;

/// Per-request timeout for both the RDM fetch and the ingestion POST. A
/// peer that accepts the TCP connection but never responds would otherwise
/// hang `poll_once` forever, silently ending the "log and keep the loop
/// alive" resilience the poll loop relies on. 30s is comfortably short
/// relative to the 24-hour recommended poll interval for this feed.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let internal_oauth = config.internal_oauth.token_cache();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);

    common::poller_loop::run_poll_loop(
        "stations",
        &client,
        &config.api_ingest_url,
        &internal_oauth,
        poll_interval,
        config.metrics.metrics_enabled,
        config.metrics_port,
        || poll_once(&client, &config, &internal_oauth),
    )
    .await
}

async fn poll_once(
    client: &Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<()> {
    let body = fetch_stations_json(client, config).await?;
    let stations = schema::parse_stations(&body)?;

    tracing::info!(count = stations.len(), "parsed stations from RDM feed");

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &stations,
        "stations",
    )
    .await
}

async fn fetch_stations_json(client: &Client, config: &Config) -> anyhow::Result<String> {
    // Header not stated specifically for the Stations product in
    // RSPS5050 P-03-00 Rev A §6 ("An API Key will be required to access
    // the JSON feed via RDM" — no header name given); this is the same
    // working assumption used for `poller-incidents` (Task 3).
    let response = client
        .get(format!("{}/stations", config.rdm_stations_base_url))
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?
        .error_for_status()?;

    let body = response.text().await?;

    // GAP: the JSON field casing for this feed is unconfirmed (see
    // `schema.rs` module docs). Logging the raw body here, before parsing,
    // is the mechanism for resolving that gap on a real run: enable
    // `RUST_LOG=poller_stations=debug`, inspect the logged body against a
    // known station (e.g. `EUS`), and adjust `schema::RdmStation`'s
    // `rename_all`/per-field `rename` attributes if reality differs.
    tracing::debug!(body = %body, "raw stations response body");

    Ok(body)
}
