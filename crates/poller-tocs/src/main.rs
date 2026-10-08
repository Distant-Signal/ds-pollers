//! `poller-tocs`: polls the RDM Train Operating Company List feed on an
//! interval and forwards parsed `TocReference`s to the `api` crate's
//! `/private/tocs` ingestion endpoint, or (`INGEST_SINK=http+shadow|stream`,
//! ingest plan 3c.2, decision D8) to the `ds:ingest:reference` stream as
//! `tocs/1`, whose body is the same JSON; see
//! `ingest_stream::snapshot`.
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
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use config::Config;
use ingest_stream::snapshot::{SinkMode, SnapshotStream};
use reqwest::Client;

/// Per-request timeout for both the RDM fetch and the ingestion POST.
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
    common::metrics::ingest_sink_info(&config.ingest_sink.to_string());
    let progress = health_http::spawn_liveness(&config.health);
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
        _ => {
            common::poller_loop::run_poll_loop(
                "tocs",
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

/// The `ds:ingest:reference` producer under `INGEST_SINK=http+shadow` or
/// `stream`; `None` under `http`. One part per snapshot (about 40 rows).
fn stream_sink(config: &Config) -> anyhow::Result<Option<SnapshotStream>> {
    if !config.ingest_sink.produces() {
        return Ok(None);
    }
    let why = format!("INGEST_SINK={}", config.ingest_sink);
    let client = config.redis.client(&why).map_err(anyhow::Error::msg)?;
    tracing::info!(sink = %config.ingest_sink, "producing TOC snapshots to ds:ingest:reference");
    Ok(Some(SnapshotStream::spawn(
        client,
        ingest_stream::streams::REFERENCE,
        ingest_stream::SchemaId::new("tocs", 1)?,
        "poller-tocs",
        usize::MAX,
    )))
}

async fn poll_once(
    client: &Client,
    config: &Config,
    internal_oauth: &common::oauth_client::OAuthTokenCache,
    stream: Option<&SnapshotStream>,
) -> anyhow::Result<()> {
    // The snapshot's `produced_at` on the stream (decision D13).
    let fetched_at: DateTime<Utc> = Utc::now();
    let body = fetch_tocs_xml(client, config).await?;
    let tocs = schema::parse_tocs(&body)?;

    tracing::info!(count = tocs.len(), "parsed TOCs from RDM feed");

    // The api's POST first, then the stream copy: under `http+shadow` the
    // copy is of a snapshot the api accepted (as poller-ldbws), so the
    // compare step counts the same snapshots on both sides, and the copy
    // never fails the cycle.
    if config.ingest_sink.posts_http() {
        ingest::post_batch_retrying(
            client,
            &config.api_ingest_url,
            internal_oauth,
            &tocs,
            "TOCs",
            common::poller_loop::post_retry_budget(Duration::from_secs(config.poll_interval_secs)),
        )
        .await?;
        if let Some(stream) = stream {
            stream.record_http(tocs.len());
        }
    }
    if let Some(stream) = stream {
        match stream.publish(&tocs, fetched_at).await {
            Ok(()) => {}
            Err(err) if config.ingest_sink == SinkMode::HttpShadow => {
                tracing::warn!(error = %err, "shadow copy to ds:ingest:reference not queued; the api POST already landed");
            }
            Err(err) => return Err(err.into()),
        }
    }
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
