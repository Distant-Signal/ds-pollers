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

/// Firm cap on the GTFS zip's total size, applied both to the server's
/// declared `Content-Length` (rejected outright, before a single byte of
/// body is read) and to the running count of bytes actually streamed in
/// (in case a response lies about, omits, or simply exceeds its own
/// `Content-Length`).
///
/// Signal Box Audit, poll-area Low finding -- "unbounded in-memory GTFS
/// zip download": `poll_once` used to `.bytes().await` the whole response
/// straight into memory with no size check at all, so a large or hostile
/// response (compromised upstream, DNS hijack, a misconfigured mirror)
/// could OOM-kill this poller. The real feed is ~9 MB as of this writing
/// (confirmed live: `curl -I` against `Config::gtfs_url`'s default reports
/// `content-length: 9145817`). 200 MiB is roughly 20x that -- generous
/// enough to absorb years of feed growth, but bounded enough that an
/// unbounded/hostile response can't exhaust this process's memory.
const MAX_GTFS_ZIP_BYTES: u64 = 200 * 1024 * 1024;

/// Downloads `url`'s body with `MAX_GTFS_ZIP_BYTES` enforced. Thin wrapper
/// around `download_capped` so production code always uses the real cap
/// while tests can exercise the same logic against a small one (streaming
/// 200 MiB just to prove the cap trips would make the test suite slow and
/// memory-hungry for no extra coverage).
async fn download_gtfs_zip(client: &Client, url: &str) -> anyhow::Result<Vec<u8>> {
    download_capped(client, url, MAX_GTFS_ZIP_BYTES).await
}

/// Downloads `url`'s body with `max_bytes` enforced both ways: a
/// `Content-Length` over the cap is rejected before any body bytes are
/// read at all, and the actual streamed byte count is checked as it grows
/// in case `Content-Length` is absent, wrong, or understated. Streams via
/// `Response::chunk` (no extra reqwest feature needed -- unlike
/// `bytes_stream`, which requires the `stream` feature this crate doesn't
/// otherwise enable) rather than `Response::bytes`, which buffers the
/// entire body internally before this code ever gets to check its size.
async fn download_capped(client: &Client, url: &str, max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let mut response = client.get(url).send().await?.error_for_status()?;

    if let Some(declared_len) = response.content_length()
        && declared_len > max_bytes
    {
        anyhow::bail!(
            "GTFS feed declared Content-Length {declared_len} bytes, exceeding the \
             {max_bytes}-byte cap; refusing to download"
        );
    }

    let mut bytes: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 > max_bytes {
            anyhow::bail!(
                "GTFS feed body exceeded the {max_bytes}-byte cap while streaming; \
                 aborting download"
            );
        }
    }
    Ok(bytes)
}

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
    let bytes = download_gtfs_zip(client, &config.gtfs_url).await?;

    let gtfs = Gtfs::from_reader(std::io::Cursor::new(bytes))
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

#[cfg(test)]
mod download_capped_tests {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn a_body_within_the_cap_downloads_fully() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![7u8; 16]))
            .mount(&server)
            .await;

        let client = Client::new();
        let bytes = download_capped(&client, &server.uri(), 1024)
            .await
            .expect("a small body under the cap should download fine");
        assert_eq!(bytes.len(), 16);
    }

    #[tokio::test]
    async fn a_content_length_over_the_cap_is_rejected_before_downloading() {
        let server = MockServer::start().await;
        // wiremock sets Content-Length from the body it's given, so a
        // large declared length is simulated with a large (but still
        // test-cheap) body -- what matters is that the cap check on
        // `content_length()` runs and rejects it.
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1u8; 2048]))
            .mount(&server)
            .await;

        let client = Client::new();
        let err = download_capped(&client, &server.uri(), 1024)
            .await
            .expect_err("a declared Content-Length over the cap must be rejected");
        assert!(
            err.to_string().contains("Content-Length"),
            "error should explain the Content-Length rejection: {err}"
        );
    }

    #[tokio::test]
    async fn a_streamed_body_exceeding_the_cap_is_aborted_mid_stream() {
        // Simulates a response that lies about (or omits a trustworthy)
        // Content-Length: wiremock's chunked encoding means
        // `Response::content_length()` reports `None` here, so the only
        // thing that can catch an oversized body is the running
        // byte-count check inside the streaming loop itself.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![9u8; 4096])
                    .append_header("transfer-encoding", "chunked"),
            )
            .mount(&server)
            .await;

        let client = Client::new();
        let err = download_capped(&client, &server.uri(), 1024)
            .await
            .expect_err("a body exceeding the cap while streaming must be aborted");
        assert!(
            err.to_string().contains("exceeded"),
            "error should explain the mid-stream size rejection: {err}"
        );
    }
}
