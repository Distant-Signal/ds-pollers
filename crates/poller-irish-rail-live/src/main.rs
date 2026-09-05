//! `poller-irish-rail-live`: polls `api.irishrail.ie`'s legacy realtime XML
//! service for every station it lists, and forwards raw per-station
//! departure-board samples to `api`'s
//! `/private/island-of-ireland-station-samples` endpoint. Tier B of
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md; see
//! docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md Task B4.
//! Deliberately raw ingestion only -- no severity inference, no
//! aggregator involvement (Judgment Call #3 there).
//!
//! Modeled on `crates/poller-ldbws/src/main.rs`'s per-station polling loop
//! shape (the closest existing precedent for "make one API call per
//! station each cycle") and `crates/poller-incidents/src/schema.rs`'s
//! `quick-xml` parsing pattern -- see `schema.rs`'s own module docs for
//! this crate's real, live-fetch-confirmed departure-board schema.
//! Deliberately does NOT depend on Tier A's `island_of_ireland_stations`
//! catalogue (this plan's Judgment Call #1): it discovers its own station
//! codes from `api.irishrail.ie`'s own `getAllStationsXML` each cycle.

mod config;
mod schema;

use std::time::Duration;

use clap::Parser;
use common::ingest;
use config::Config;
use reqwest::Client;

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
        "irish-rail-live",
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
    let station_codes = if config.station_codes_override.is_empty() {
        fetch_all_station_codes(client, config).await?
    } else {
        config.station_codes_override.clone()
    };
    tracing::info!(
        count = station_codes.len(),
        "fetched station code list to sample"
    );

    let mut samples = Vec::with_capacity(station_codes.len());
    for code in &station_codes {
        match fetch_station_departures(client, config, code).await {
            Ok(departures) => samples.push(schema::to_sample(code, departures)),
            Err(err) => {
                tracing::error!(station_code = %code, error = ?err, "failed to sample station; skipping");
            }
        }
    }

    if samples.is_empty() {
        tracing::warn!("no station samples collected this cycle; nothing to post");
        return Ok(());
    }

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &samples,
        "island-of-ireland station samples",
    )
    .await
}

async fn fetch_all_station_codes(client: &Client, config: &Config) -> anyhow::Result<Vec<String>> {
    let url = format!("{}/getAllStationsXML", config.irish_rail_base_url);
    let body = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    tracing::debug!(body = %body, "raw getAllStationsXML response body");
    schema::parse_all_stations(&body)
}

async fn fetch_station_departures(
    client: &Client,
    config: &Config,
    station_code: &str,
) -> anyhow::Result<Vec<common::island_of_ireland::IslandOfIrelandDeparture>> {
    let url = format!(
        "{}/getStationDataByCodeXML?StationCode={station_code}",
        config.irish_rail_base_url
    );
    let body = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    tracing::debug!(station_code = %station_code, body = %body, "raw getStationDataByCodeXML response body");
    schema::parse_station_departures(&body)
}
