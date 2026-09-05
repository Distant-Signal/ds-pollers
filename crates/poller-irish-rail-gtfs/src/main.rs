//! `poller-irish-rail-gtfs`: downloads Transport for Ireland's public GTFS
//! zip for Iarnród Éireann on an interval, parses it via `gtfs-structures`,
//! and forwards the derived station/line catalogue to `api`'s
//! `/private/island-of-ireland-{stations,lines}` ingestion endpoints. Tier
//! A of docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md;
//! see docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md Task A4.

mod config;
mod mapping;

use std::time::Duration;

use clap::Parser;
use common::ingest;
use config::Config;
use gtfs_structures::Gtfs;
use reqwest::Client;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

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
    // Freshness is checked against the stations endpoint only -- both
    // ingest together every cycle (see poll_once), so one check suffices,
    // matching poller-ldbws's own single freshness check even though it
    // also posts to a second api endpoint conceptually (sample-stations is
    // a GET, not a parallel POST target, but the precedent for "one
    // freshness check per poller, not one per ingest target" holds).
    let delay = ingest::time_until_next_poll(
        &client,
        &config.api_stations_ingest_url,
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
            "poller" => "irish-rail-gtfs"
        )
        .record(cycle_start.elapsed().as_secs_f64());
        metrics::counter!(
            common::metrics::metric_name("poller_cycle_total"),
            "poller" => "irish-rail-gtfs",
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
    let bytes = client
        .get(&config.gtfs_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let gtfs = Gtfs::from_reader(std::io::Cursor::new(bytes.to_vec()))
        .map_err(|err| anyhow::anyhow!("failed to parse GTFS feed: {err}"))?;

    let stations = mapping::map_stations(&gtfs);
    let lines = mapping::map_lines(&gtfs);
    tracing::info!(
        stations = stations.len(),
        lines = lines.len(),
        "parsed Iarnrod Eireann GTFS feed"
    );

    ingest::post_batch(
        client,
        &config.api_stations_ingest_url,
        internal_oauth,
        &stations,
        "island-of-ireland stations",
    )
    .await?;
    ingest::post_batch(
        client,
        &config.api_lines_ingest_url,
        internal_oauth,
        &lines,
        "island-of-ireland lines",
    )
    .await?;
    Ok(())
}
