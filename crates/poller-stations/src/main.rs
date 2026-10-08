//! `poller-stations`: polls the RDM Stations JSON feed on an interval and
//! forwards parsed `StationReference`s to the `api` crate's
//! `/private/stations` ingestion endpoint, or (`INGEST_SINK=db`, ingest
//! architecture plan 2b) writes them to Postgres directly; see `sink.rs`.
//!
//! Built against RSPS5050 P-03-00 Rev A, §6 — this is the best-documented of
//! the three RDM products: the `/stations` endpoint path and the 24-hour
//! poll frequency are both confirmed, and production runs this poller
//! against the live feed. The spec leaves the JSON field casing open; see
//! `schema.rs` module docs for how that's handled.

#[cfg(test)]
mod alloc_meter;
mod config;
mod schema;
mod sink;

use std::time::Duration;

use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use config::{Config, IngestSink};
use reqwest::Client;
use sink::{DbSink, HttpSink, StationsSink};

/// `pg_stat_activity.application_name` under `INGEST_SINK=db`.
const APPLICATION_NAME: &str = "distant-signal-poller-stations";
/// Spec §6.6: pool 1, role limit 2. The writes are one transaction per
/// day, after the cursor read; nothing runs concurrently.
const DEFAULT_MAX_CONNECTIONS: u32 = 1;

/// Per-request timeout for both the RDM fetch and the ingestion POST. A
/// peer that accepts the TCP connection but never responds would otherwise
/// hang `poll_once` forever, silently ending the "log and keep the loop
/// alive" resilience the poll loop relies on. 30s is comfortably short
/// relative to the 24-hour recommended poll interval for this feed.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-stations");

    let config = Config::parse();
    common::metrics::ingest_sink_info(&common::metrics::value_enum_name(&config.ingest_sink));
    config.validate()?;
    // Installed here rather than by the poll loop, so the schema gate's
    // `db_schema_ready` (under `INGEST_SINK=db`) is exported while it waits.
    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let progress = health_http::spawn_liveness(&config.health);
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let retry_budget = common::poller_loop::post_retry_budget(poll_interval);

    match config.ingest_sink {
        IngestSink::Http => {
            let sink = HttpSink {
                client: client.clone(),
                url: config.api_ingest_url.clone(),
                tokens: config.internal_oauth.token_cache(),
                retry_budget,
            };
            run_with(&client, &config, &progress, &sink).await
        }
        IngestSink::Db => {
            let pool = connect_database(&config, &progress).await?;
            tracing::info!("INGEST_SINK=db: writing stations to Postgres directly");
            let sink = DbSink {
                pool,
                retry_budget,
                backoff: ingest::POST_RETRY_BACKOFF,
            };
            run_with(&client, &config, &progress, &sink).await
        }
    }
}

/// `INGEST_SINK=db`: waits for Postgres (INF-5), connects the pool (with
/// the `db_pool_*` metrics) and passes the schema gate as the `stations`
/// role (spec §12.2) before the first poll.
async fn connect_database(
    config: &Config,
    progress: &common::progress::Progress,
) -> anyhow::Result<sqlx::PgPool> {
    let url = config
        .database_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("INGEST_SINK=db needs DATABASE_URL"))?;
    common::startup::retry_until_ready(
        "Postgres",
        common::startup::CONNECT_BACKOFF,
        Some(progress),
        || async {
            use sqlx::Connection;
            sqlx::PgConnection::connect(url.expose())
                .await?
                .close()
                .await
        },
    )
    .await;
    ds_store::pool::register_metrics();
    let pool = ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
        .connect(url.expose())
        .await?;
    ds_store::schema::wait_for_schema(&pool, ds_store::schema::DbRole::Stations, Some(progress))
        .await?;
    Ok(pool)
}

async fn run_with<S: StationsSink>(
    client: &Client,
    config: &Config,
    progress: &common::progress::Progress,
    sink: &S,
) -> anyhow::Result<()> {
    common::poller_loop::run_poll_loop_with_source(
        "stations",
        sink.cursor(),
        Duration::from_secs(config.poll_interval_secs),
        // Installed in `run`.
        false,
        config.metrics_port,
        progress,
        || poll_once(client, config, sink),
    )
    .await
}

async fn poll_once<S: StationsSink>(
    client: &Client,
    config: &Config,
    sink: &S,
) -> anyhow::Result<()> {
    // `stations` borrows its passthrough JSON from `body` (see
    // `schema::parse_stations` on memory), so `body` stays alive for the
    // write.
    let body = fetch_stations_json(client, config).await?;
    let stations = schema::parse_stations(&body)?;

    tracing::info!(count = stations.len(), "parsed stations from RDM feed");

    sink.publish(&stations).await
}

/// Returns the raw body as bytes, not `.text()`: `.text()` copies the whole
/// ~37MB body a second time while validating it as UTF-8, and
/// `serde_json::from_slice` validates it anyway.
async fn fetch_stations_json(
    client: &Client,
    config: &Config,
) -> anyhow::Result<impl std::ops::Deref<Target = [u8]>> {
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

    let body = response.bytes().await?;

    // GAP: the JSON field casing for this feed is unconfirmed (see
    // `schema.rs` module docs). Logging the raw body here, before parsing,
    // is the mechanism for resolving that gap on a real run: enable
    // `RUST_LOG=poller_stations=debug`, inspect the logged body against a
    // known station (e.g. `EUS`), and adjust `schema::RdmStation`'s
    // `rename_all`/per-field `rename` attributes if reality differs.
    tracing::debug!(body = %String::from_utf8_lossy(&body), "raw stations response body");

    Ok(body)
}
