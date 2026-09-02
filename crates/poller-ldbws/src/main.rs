//! `poller-ldbws`: samples live departure-board data for every station any
//! line's inference logic depends on, and forwards parsed `StationSample`s
//! to the `api` crate's `/private/station-samples` ingestion endpoint.
//!
//! See `docs/superpowers/specs/2026-07-06-ldbws-sampler-poller-design.md`
//! for the full design and `docs/superpowers/plans/2026-07-06-ldbws-sampler-poller.md`
//! for the RDM facts this is built against (a documentation-discovery pass
//! against a fetched Swagger spec for RDM's Live Departure Board REST
//! product, `GetDepBoardWithDetails`). Two documented gaps carried into
//! `config.rs`: the exact RDM product-slug segment of the base URL, and
//! this feed's real rate limit — both are env-configurable rather than
//! guessed.
//!
//! Unlike the other three pollers, this one calls a second `api` endpoint
//! first (`GET /private/sample-stations`) to learn which CRS codes to
//! sample, then makes one LDBWS call *per station* each cycle — there is
//! no bulk/multi-station LDBWS operation.

mod config;
mod schema;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use common::{StationDeparture, StationSample};
use config::Config;
use reqwest::Client;

/// Per-request timeout — see the other three pollers' identical rationale.
/// 30s is comfortably short relative to the 60s default poll interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let internal_oauth =
        common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
            token_url: config.internal_oauth_token_url.clone(),
            client_id: config.internal_oauth_client_id.clone(),
            scope: config.internal_oauth_scope.clone(),
            username: config.internal_oauth_username.clone(),
            password: config.internal_oauth_password.clone(),
        });

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let delay = ingest::time_until_next_poll(
        &client,
        &config.api_ingest_url,
        &internal_oauth,
        poll_interval,
    )
    .await;
    if !delay.is_zero() {
        tracing::info!(
            delay_secs = delay.as_secs(),
            "data still fresh from a prior run; delaying first poll"
        );
    }
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + delay, poll_interval);

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = poll_once(&client, &config, &internal_oauth).await;
        metrics::histogram!(
            common::metrics::metric_name("poller_cycle_duration_seconds"),
            "poller" => "ldbws"
        )
        .record(cycle_start.elapsed().as_secs_f64());
        metrics::counter!(
            common::metrics::metric_name("poller_cycle_total"),
            "poller" => "ldbws",
            "result" => if result.is_ok() { "success" } else { "failure" }
        )
        .increment(1);

        if let Err(err) = result {
            tracing::error!(error = ?err, "poll cycle failed; will retry next interval");
        }
    }
}

async fn poll_once(
    client: &Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<()> {
    let stations = fetch_sample_stations(client, config, internal_oauth).await?;
    tracing::info!(count = stations.len(), "fetched station list to sample");

    let mut samples = Vec::with_capacity(stations.len());

    for crs in &stations {
        match fetch_departures(client, config, crs).await {
            Ok(departures) => samples.push(StationSample {
                crs: crs.clone(),
                polled_at: Utc::now(),
                departures,
            }),
            Err(err) => {
                tracing::error!(crs = %crs, error = ?err, "failed to sample station; skipping");
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
        "station samples",
    )
    .await
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
    let token = tokens.get_token(client).await?;
    let response = client
        .get(&config.api_sample_stations_url)
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?;

    Ok(response.json::<Vec<String>>().await?)
}

/// One `GetDepBoardWithDetails` call for a single station.
async fn fetch_departures(
    client: &Client,
    config: &Config,
    crs: &str,
) -> anyhow::Result<Vec<StationDeparture>> {
    let url = format!(
        "{}/GetDepBoardWithDetails/{crs}?numRows={}",
        config.ldbws_base_url, config.num_rows
    );

    let response = client
        .get(&url)
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?
        .error_for_status()?;

    let body = response.text().await?;
    schema::parse_departures(&body)
}
