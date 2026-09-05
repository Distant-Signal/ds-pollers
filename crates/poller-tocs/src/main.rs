//! `poller-tocs`: polls the RDM Train Operating Company List feed on an
//! interval and forwards parsed `TocReference`s to the `api` crate's
//! `/private/tocs` ingestion endpoint.
//!
//! See `.superpowers/sdd/task-5-brief.md` for the RDM facts this is built
//! against (RSPS5050 P-03-00 Rev A, §3) and the documented gaps: no
//! confirmed endpoint path for this product, and an auth header name
//! corroborated only via a different product's example.

mod config;
mod schema;

use std::time::Duration;

use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use config::Config;
use reqwest::Client;

/// Per-request timeout for both the RDM fetch and the ingestion POST.
/// Without this, a peer that accepts the TCP connection but never responds
/// (unlike the connection-refused case) would hang `poll_once` forever —
/// the process wouldn't panic, but it would also never poll again, which
/// defeats the "log and keep the loop alive" resilience goal. 30s is
/// comfortably short relative to the 86400s recommended poll interval.
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
        "tocs",
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
    let body = fetch_tocs_xml(client, config).await?;
    let tocs = schema::parse_tocs(&body)?;

    tracing::info!(count = tocs.len(), "parsed TOCs from RDM feed");

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &tocs,
        "TOCs",
    )
    .await
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
