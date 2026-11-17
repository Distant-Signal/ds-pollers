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

/// The client for the credentialed upstream calls. It never follows a
/// redirect: it sends the RDM key in a custom header, which reqwest would carry
/// to another host (poller-http's module docs).
fn build_client() -> reqwest::Result<Client> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(poller_http::no_redirects())
        .build()
}

/// The most of the TOC list body read (decoded). The real feed is about 40
/// rows of XML (a few KB); 8 MiB is orders of magnitude above that and
/// still harmless to hold.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Once mode (`RUN_ONCE`, a scheduled Job; `common::poller_loop`): how long
/// the run waits for its snapshot to reach the stream before failing (exit
/// 1, so the Job retries). Inside the chart's `activeDeadlineSeconds`.
const ONCE_FLUSH_GRACE: Duration = Duration::from_secs(120);

/// What `--print-config-defaults` prints (`ds_runtime::service_args`).
fn config_defaults() -> ds_runtime::service_args::ConfigDefaults {
    poller_lifecycle::config_defaults(
        ds_runtime::service_args::ConfigDefaults::for_config::<Config>("poller-tocs"),
    )
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    ds_runtime::service_args::exit_if_print_config_defaults(config_defaults);
    ds_runtime::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    ds_runtime::logging::init("poller-tocs");

    let config = Config::parse();
    let (lifecycle, progress) = poller_lifecycle::Lifecycle::spawn(&config.health)?;
    let once = common::poller_loop::RunOnce::from_env()?.is_some();
    let client = build_client()?;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let stream = stream_sink(&config)?;
    // The producer is lazy and `publish` only queues a snapshot, never
    // waiting for Redis: a Redis outage does not fail a cycle
    // (`poller_cycle_total` stays `success`). The newest snapshot stays
    // queued and is retried until Redis is back.
    lifecycle.sink_connected();
    let cycle = || lifecycle.track(poll_once(&client, &config, &stream));

    // The startup cursor is the stream's newest entry (spec §11.3).
    let poll_loop = common::poller_loop::run_poll_loop_with_cursor(
        "tocs",
        || async { Ok(stream.last_produced_at().await?) },
        poll_interval,
        config.metrics.metrics_enabled,
        config.metrics_port,
        &progress,
        cycle,
    );
    // The loop returns in once mode, on a fatal error or at SIGTERM: exit
    // only once the snapshot is in the stream (a run fails without it).
    let result = lifecycle.run(Box::pin(poll_loop), once).await;
    let grace = if once {
        ONCE_FLUSH_GRACE
    } else {
        poller_lifecycle::FLUSH_GRACE
    };
    common::poller_loop::finish_once(result, stream.shutdown(grace)).await
}

/// The `ds:ingest:reference` producer. One part per snapshot (about 40
/// rows).
fn stream_sink(config: &Config) -> anyhow::Result<SnapshotStream> {
    let client = config
        .redis
        .client("the ingest stream")
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
    fetch_capped(
        client,
        &config.rdm_tocs_base_url,
        config.rdm_api_key.expose(),
        MAX_BODY_BYTES,
    )
    .await
}

async fn fetch_capped(
    client: &Client,
    url: &str,
    api_key: &str,
    cap: usize,
) -> anyhow::Result<String> {
    // Header per RSPS5050 P-03-00 Rev A §3 — corroborated for the RDM
    // platform generally via a different product's confirmed example, not
    // proven specifically for this product.
    let response = client
        .get(url)
        .header(RDM_AUTH_HEADER_NAME, api_key)
        .send()
        .await?;
    let response = poller_http::ensure_success(response, "tocs")?;

    Ok(poller_http::read_capped_text(response, cap, "tocs").await?)
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn an_oversized_body_fails_the_cycle() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 4096]))
            .mount(&server)
            .await;
        let client = Client::new();
        let ok = fetch_capped(&client, &server.uri(), "k", 4096).await;
        assert_eq!(ok.unwrap().len(), 4096);
        let err = fetch_capped(&client, &server.uri(), "k", 4095)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeded"), "{err}");
    }

    /// A redirect is not followed, so the host it names gets nothing, and
    /// the key header goes nowhere but the configured host.
    #[tokio::test]
    async fn a_redirect_is_not_followed_and_the_target_gets_nothing() {
        let target = MockServer::start().await;
        let origin = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/stolen", target.uri())),
            )
            .expect(1)
            .mount(&origin)
            .await;
        let client = build_client().unwrap();
        let err = fetch_capped(&client, &origin.uri(), "k", 4096)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("redirect"), "{err}");
        assert!(target.received_requests().await.unwrap().is_empty());
    }
}
