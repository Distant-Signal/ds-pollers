//! `poller-incidents`: polls the RDM Knowledgebase Incidents feed on an
//! interval and forwards parsed `IncidentMessage`s to the `api` crate's
//! `/private/incidents` ingestion endpoint, or, with `INGEST_SINK=db`
//! (ingest architecture plan 2c.2, off by default), writes them to Postgres
//! itself (`sink::DbSink`).
//!
//! Built against RSPS5050 P-03-00 Rev A, §10. That spec publishes no
//! endpoint path, so `RDM_INCIDENTS_BASE_URL` is the full feed URL from the
//! operator's RDM subscription; requests authenticate with the `x-apikey`
//! header. Production runs this poller against the live feed.

mod config;
mod schema;
mod sink;

use std::time::Duration;

use clap::Parser;
use common::ingest::RDM_AUTH_HEADER_NAME;
use config::{Config, IngestSink};
use reqwest::Client;
use sink::IncidentSink;

/// Per-request timeout for both the RDM fetch and the ingestion POST.
/// Without this, a peer that accepts the TCP connection but never responds
/// (unlike the connection-refused case) would hang `poll_once` forever —
/// the process wouldn't panic, but it would also never poll again, which
/// defeats the "log and keep the loop alive" resilience goal. 30s is
/// comfortably short relative to the 300s recommended poll interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

/// `pg_stat_activity.application_name` of the DB sink's pool.
const APPLICATION_NAME: &str = "distant-signal-poller-incidents";
/// The DB sink's pool: one snapshot at a time, so one connection writes and
/// a second covers the publish-order reads; spec §6.6 gives the role 3.
const DEFAULT_MAX_CONNECTIONS: u32 = 2;

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-incidents");

    let config = Config::parse();
    config.validate()?;
    let progress = health_http::spawn_liveness(&config.health);
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let internal_oauth = config.internal_oauth.token_cache();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let budget = common::poller_loop::post_retry_budget(poll_interval);

    match config.ingest_sink {
        IngestSink::Http => {
            let sink = sink::HttpSink {
                client: &client,
                url: &config.api_ingest_url,
                internal_oauth: &internal_oauth,
                budget,
            };
            common::poller_loop::run_poll_loop(
                "incidents",
                &client,
                &config.api_ingest_url,
                &internal_oauth,
                poll_interval,
                config.metrics.metrics_enabled,
                config.metrics_port,
                &progress,
                || poll_once(&client, &config, &sink),
            )
            .await
        }
        IngestSink::Db => {
            if config.metrics.metrics_enabled {
                common::metrics::install(config.metrics_port)?;
            }
            let sink = db_sink(&config, &progress, budget).await?;
            tracing::info!(
                row_heartbeat = config.incidents_row_heartbeat,
                "writing incident snapshots to Postgres (INGEST_SINK=db)"
            );
            common::poller_loop::run_poll_loop_from_cursor(
                "incidents",
                poll_interval,
                &progress,
                || ds_store::freshness::last_incidents_fetch(&sink.pool),
                || poll_once(&client, &config, &sink),
            )
            .await
        }
    }
}

/// The DB sink's dependencies, in startup order: Postgres (waited for, as
/// every DB service does), the schema gate as the `incidents` role, the line
/// catalogue for the matcher, and the Redis client (lazy: it connects per
/// publish, so a Redis outage never blocks startup or ingest).
async fn db_sink(
    config: &Config,
    progress: &common::progress::Progress,
    budget: Duration,
) -> anyhow::Result<sink::DbSink<sink::RedisPublisher>> {
    let database_url = config
        .database_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("INGEST_SINK=db needs DATABASE_URL"))?;
    let redis_url = config
        .redis_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("INGEST_SINK=db needs REDIS_URL"))?;

    // Before any DB work: a broken catalogue is a configuration error.
    let lines = common::config::parse_lines(&config.lines_dir)?;
    anyhow::ensure!(
        !lines.is_empty(),
        "no line definitions in {}: refusing to write incidents with an empty catalogue \
         (every affected_lines would be cleared)",
        config.lines_dir
    );
    let matcher = common::matcher::LineMatcher::new(&lines);

    common::startup::retry_until_ready(
        "Postgres",
        common::startup::CONNECT_BACKOFF,
        Some(progress),
        || async {
            use sqlx::Connection;
            sqlx::PgConnection::connect(database_url.expose())
                .await?
                .close()
                .await
        },
    )
    .await;
    let pool = ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
        .connect(database_url.expose())
        .await?;
    ds_store::schema::wait_for_schema(&pool, ds_store::schema::DbRole::Incidents, Some(progress))
        .await?;
    // The inference's metrics (`api_`-prefixed: the api registered them
    // first, and the chart's alert sums them across pods).
    ds_store::incidents::removal::register_metrics();

    let redis_url = common::redis_auth::redis_url_with_credentials(
        redis_url.expose(),
        config.redis_username.as_deref(),
        config.redis_password.as_ref(),
    )?;
    let redis = redis::Client::open(redis_url.expose())?;

    Ok(sink::DbSink {
        pool,
        matcher,
        heartbeat: ds_store::incidents::RowHeartbeat::from_flag(config.incidents_row_heartbeat),
        publisher: sink::RedisPublisher(redis),
        budget,
    })
}

async fn poll_once(
    client: &Client,
    config: &Config,
    sink: &impl IncidentSink,
) -> anyhow::Result<()> {
    let body = fetch_incidents_xml(client, config).await?;
    let parsed = schema::parse_incidents(&body)?;

    tracing::info!(
        count = parsed.incidents.len(),
        skipped = parsed.skipped,
        document_closed = parsed.document_closed,
        complete = parsed.complete(),
        "parsed incidents from RDM feed"
    );
    metrics::counter!(common::metrics::metric_name(
        "poller_incidents_skipped_elements_total"
    ))
    .increment(parsed.skipped);

    // The whole feed, with whether it IS the whole feed: "no longer
    // listed" is inferred only from complete snapshots (see
    // `common::IncidentSnapshot`).
    let snapshot = parsed.into_snapshot();
    sink.publish(&snapshot)
        .await
        .map_err(sink::SinkError::into_anyhow)
}

async fn fetch_incidents_xml(client: &Client, config: &Config) -> anyhow::Result<String> {
    // Header per RSPS5050 P-03-00 Rev A §10 — corroborated for the RDM
    // platform generally via a different product's confirmed example, not
    // proven specifically for this product.
    let response = client
        .get(&config.rdm_incidents_base_url)
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?
        .error_for_status()?;

    Ok(response.text().await?)
}
