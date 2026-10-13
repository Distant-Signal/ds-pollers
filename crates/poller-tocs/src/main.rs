//! `poller-tocs`: polls the RDM Train Operating Company List feed on an
//! interval and publishes the parsed `TocReference`s to the
//! `ds:ingest:reference` stream as `tocs/1` (ingest plan 3c.2, decision
//! D8), which the ingest-writer applies; see `ingest_stream::snapshot`.
//!
//! Built against RSPS5050 P-03-00 Rev A, §3. That spec publishes no
//! endpoint path, so `RDM_TOCS_BASE_URL` is the full feed URL from the
//! operator's RDM subscription; requests authenticate with the `x-apikey`
//! header. Production runs this poller against the live feed.

mod config;
mod schema;

use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Parser;
use common::ingest::RDM_AUTH_HEADER_NAME;
use config::Config;
use ingest_stream::snapshot::SnapshotStream;
use reqwest::Client;

/// Per-request timeout for the RDM fetch.
/// Without this, a peer that accepts the TCP connection but never responds
/// (unlike the connection-refused case) would hang `poll_once` forever —
/// the process wouldn't panic, but it would also never poll again, which
/// defeats the "log and keep the loop alive" resilience goal. 30s is
/// comfortably short relative to the 86400s recommended poll interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-tocs");

    let config = Config::parse();
    common::metrics::ingest_sink_info(&config.ingest_sink);
    let progress = health_http::spawn_liveness(&config.health);
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let stream = stream_sink(&config)?;
    let cycle = || poll_once(&client, &config, &stream);

    // The startup cursor is the stream's newest entry (spec §11.3).
    common::poller_loop::run_poll_loop_with_cursor(
        "tocs",
        || async { Ok(stream.last_produced_at().await?) },
        poll_interval,
        config.metrics.metrics_enabled,
        config.metrics_port,
        &progress,
        cycle,
    )
    .await
}

/// The `ds:ingest:reference` producer. One part per snapshot (about 40
/// rows).
fn stream_sink(config: &Config) -> anyhow::Result<SnapshotStream> {
    let client = config
        .redis
        .client("INGEST_SINK=stream")
        .map_err(anyhow::Error::msg)?;
    tracing::info!("producing TOC snapshots to ds:ingest:reference");
    Ok(SnapshotStream::spawn(
        client,
        ingest_stream::streams::REFERENCE,
        ingest_stream::SchemaId::new("tocs", 1)?,
        "poller-tocs",
        usize::MAX,
    ))
}

async fn poll_once(
    client: &Client,
    config: &Config,
    stream: &SnapshotStream,
) -> anyhow::Result<()> {
    // The snapshot's `produced_at` on the stream (decision D13).
    let fetched_at: DateTime<Utc> = Utc::now();
    let body = fetch_tocs_xml(client, config).await?;
    let tocs = schema::parse_tocs(&body)?;

    tracing::info!(count = tocs.len(), "parsed TOCs from RDM feed");

    stream.publish(&tocs, fetched_at).await?;
    Ok(())
}

async fn fetch_tocs_xml(client: &Client, config: &Config) -> anyhow::Result<String> {
    // Header per RSPS5050 P-03-00 Rev A §3 — corroborated for the RDM
    // platform generally via a different product's confirmed example, not
    // proven specifically for this product.
    let response = client
        .get(&config.rdm_tocs_base_url)
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?
        .error_for_status()?;

    Ok(response.text().await?)
}
