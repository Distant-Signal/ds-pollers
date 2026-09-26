//! `poller-irish-rail-gtfs`: downloads Transport for Ireland's public GTFS
//! zip for Iarnród Éireann on an interval, parses it via `gtfs-structures`,
//! and forwards the derived station/line catalogue to `api`'s
//! `/private/island-of-ireland-{stations,lines}` ingestion endpoints. Tier
//! A of docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md;
//! see docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md Task A4.

mod config;
mod mapping;

use std::io::Read;
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

/// Cumulative decompressed-bytes budget for [`reject_gtfs_zip_bomb`]'s
/// bounded pre-scan of the downloaded GTFS zip -- see that function's own
/// doc comment for how this is actually enforced. Comfortably above the
/// real feed's actual decompressed size (the ~9 MB compressed feed as of
/// this writing decompresses to, at most, a few tens of MB of CSV) while
/// still bounding a hostile upstream's blast radius to a fixed, moderate
/// amount of real memory and CPU time -- nowhere near the potentially
/// unbounded ratio a crafted zip bomb could otherwise claim.
const MAX_GTFS_INFLATED_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// **L15 (2026-09-26 review): `Gtfs::from_reader` inflates every zip entry
/// with no bound of its own.** [`MAX_GTFS_ZIP_BYTES`]/[`download_capped`]
/// above only bound the COMPRESSED download size, not what a hostile
/// upstream's crafted zip could decompress to -- confirmed directly against
/// `gtfs-structures` 0.50.0's `GtfsReader::read_from_reader`, which opens
/// its own `zip::ZipArchive` and hands each entry's `Read` impl straight to
/// a `csv::Reader` with no size cap anywhere in between, and against the
/// `zip` crate (8.6.0) itself, which exposes no per-entry or total-inflated
/// size limit option (its `read::Config` only covers locating the archive's
/// start offset). This is a hostile-upstream-only risk: Transport for
/// Ireland's real feed is trusted in the ordinary case, and
/// `MAX_GTFS_ZIP_BYTES` already covers that legitimate case fully -- see
/// this function's own callers.
///
/// An entry's declared "uncompressed size" field in its local/central
/// directory header is attacker-controlled metadata written into the zip
/// itself, not a fact -- a hand-crafted entry can declare a small
/// uncompressed size while actually inflating to far more, exactly the same
/// reasoning `ticket_extraction::reject_pdf_compression_bombs` already
/// documents for why a PDF stream's own declared `/Length` can't be trusted
/// either (`crates/api/src/data/ticket_extraction.rs`). So instead of
/// trusting any declared size, this function borrows that exact mitigation
/// shape: it opens its OWN `zip::ZipArchive` over the already-downloaded
/// bytes, and for every entry, streams its decompressed content through a
/// small, FIXED-SIZE, REUSED buffer -- never a growing `Vec` -- so this
/// function's own memory use stays tiny regardless of how large an entry
/// claims (or turns out) to decompress to. It aborts a single entry's
/// decompression, and immediately rejects the whole archive, the instant
/// the RUNNING TOTAL across every entry seen so far exceeds
/// `MAX_GTFS_INFLATED_BYTES` -- without ever letting any entry fully
/// materialize.
///
/// This duplicates the decompression work `Gtfs::from_reader` goes on to do
/// if this check passes: `Gtfs::from_reader` offers no injectable read
/// wrapper or size-bound option of its own to avoid that (its `ZipArchive`
/// is entirely internal to `read_from_reader`, never exposed to the
/// caller), so there is no way to check as-you-go during the real parse
/// itself. Paying for the decompression twice is a non-issue here: the real
/// feed is small, and this whole poller runs once per poll cycle (minutes
/// apart), not once per inbound request.
///
/// Thin wrapper around [`reject_gtfs_zip_bomb_with_budget`], mirroring
/// [`download_gtfs_zip`]/[`download_capped`]'s own split just below: real
/// callers always use [`MAX_GTFS_INFLATED_BYTES`], while tests exercise the
/// identical logic against a tiny budget (actually decompressing 2 GiB just
/// to prove the cap trips would make the test suite slow and memory-hungry
/// for no extra coverage).
fn reject_gtfs_zip_bomb(bytes: &[u8]) -> anyhow::Result<()> {
    reject_gtfs_zip_bomb_with_budget(bytes, MAX_GTFS_INFLATED_BYTES)
}

fn reject_gtfs_zip_bomb_with_budget(bytes: &[u8], max_inflated_bytes: u64) -> anyhow::Result<()> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|err| anyhow::anyhow!("failed to open GTFS zip for its size pre-check: {err}"))?;

    let mut total_inflated: u64 = 0;
    let mut buf = [0u8; 64 * 1024];
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| anyhow::anyhow!("failed to open GTFS zip entry {i}: {err}"))?;
        loop {
            let n = entry
                .read(&mut buf)
                .map_err(|err| anyhow::anyhow!("failed to inflate GTFS zip entry {i}: {err}"))?;
            if n == 0 {
                break;
            }
            total_inflated += n as u64;
            anyhow::ensure!(
                total_inflated <= max_inflated_bytes,
                "GTFS zip contains an entry that decompresses to an implausible size \
                 (cumulative total exceeds the {max_inflated_bytes}-byte budget); \
                 refusing to parse it"
            );
        }
    }
    Ok(())
}

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

/// Builds the poll-cycle `tokio::time::Interval`, first ticking at `start`
/// and thereafter every `poll_interval` -- with `MissedTickBehavior::Delay`
/// rather than the default `Burst`.
///
/// `Burst` fires every missed tick back-to-back with zero gap once a cycle
/// overruns `poll_interval` (a slow GTFS zip download, a stuck ingest POST)
/// -- exactly when Transport for Ireland's feed endpoint is already slow,
/// it would pile up a burst of immediate follow-up cycles instead of
/// settling back into its normal cadence. `Delay` instead waits a fresh
/// `poll_interval` from whenever the overrun tick actually completes, so a
/// slow cycle degrades to a slower cadence, never a thundering-herd burst.
/// Same fix, same rationale, as `common::poller_loop`'s own
/// `poll_interval_with_delay_on_overrun` (this crate doesn't share that
/// scaffolding -- see its own doc comment -- so it needs the identical fix
/// applied locally). Split into its own function so the configuration is
/// directly assertable in a unit test via `Interval::missed_tick_behavior()`,
/// since the missed-tick BEHAVIOR itself (skipping ticks under a real
/// overrun) isn't practically observable without a slow, flaky, real-time
/// test.
fn poll_interval_with_delay_on_overrun(
    start: tokio::time::Instant,
    poll_interval: Duration,
) -> tokio::time::Interval {
    let mut interval = tokio::time::interval_at(start, poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
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
    let mut interval =
        poll_interval_with_delay_on_overrun(tokio::time::Instant::now() + delay, poll_interval);

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

    // Both steps are synchronous, CPU-bound work over a potentially large
    // amount of decompressed data -- `reject_gtfs_zip_bomb` by design (see
    // its own doc comment), and `Gtfs::from_reader` in the ordinary case --
    // so both are moved onto the blocking pool together. Without this, a
    // pathological feed would stall this whole process's tokio runtime for
    // however long the (bounded, but still real) decompression work takes,
    // including this poller's own `/metrics` endpoint
    // (`common::metrics::install` runs its HTTP listener on this same
    // runtime).
    let gtfs = tokio::task::spawn_blocking(move || {
        reject_gtfs_zip_bomb(&bytes)?;
        Gtfs::from_reader(std::io::Cursor::new(bytes))
            .map_err(|err| anyhow::anyhow!("failed to parse GTFS feed: {err}"))
    })
    .await
    .map_err(|join_err| {
        anyhow::anyhow!("GTFS parse task panicked or was cancelled: {join_err}")
    })??;

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

#[cfg(test)]
mod poll_interval_tests {
    use super::*;

    /// Regression for the "L1 -- MissedTickBehavior::Burst still default"
    /// finding: this poller's own interval must opt into `Delay`, not
    /// leave `Burst` as the default, so an overrun cycle doesn't fire a
    /// burst of back-to-back catch-up ticks against Transport for
    /// Ireland's feed endpoint.
    #[tokio::test]
    async fn poll_interval_defaults_to_delay_not_burst_on_a_missed_tick() {
        let interval = poll_interval_with_delay_on_overrun(
            tokio::time::Instant::now(),
            Duration::from_secs(60),
        );
        assert_eq!(
            interval.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay
        );
    }
}

#[cfg(test)]
mod reject_gtfs_zip_bomb_tests {
    use std::io::Write;

    use super::*;

    /// Builds a single-entry zip, `name` -> `contents`, via a real
    /// `zip::ZipWriter` -- the exact code path `reject_gtfs_zip_bomb` (and
    /// `Gtfs::from_reader`) reads back, not a hand-rolled byte layout.
    fn build_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options: zip::write::FileOptions<()> = zip::write::FileOptions::default();
            for (name, contents) in entries {
                zip.start_file(*name, options).unwrap();
                zip.write_all(contents).unwrap();
            }
            zip.finish().unwrap();
        }
        buf
    }

    #[test]
    fn an_ordinary_small_feed_is_accepted() {
        let zip_bytes = build_zip(&[
            ("agency.txt", b"agency_id,agency_name\nIR,Iarnrod Eireann\n"),
            ("stops.txt", b"stop_id,stop_name\nSTOP_A,Zesttown\n"),
        ]);
        assert!(reject_gtfs_zip_bomb(&zip_bytes).is_ok());
    }

    #[test]
    fn a_non_zip_input_is_rejected_before_any_inflate_is_attempted() {
        let err = reject_gtfs_zip_bomb(b"this is not a zip file at all")
            .expect_err("a non-zip input must be rejected");
        assert!(
            err.to_string().contains("open GTFS zip"),
            "error should explain the archive-open failure: {err}"
        );
    }

    /// The real failure shape L15 exists to prevent: a small,
    /// highly-compressed entry that decompresses to far more than a
    /// deliberately tiny test budget -- proving the pre-check catches an
    /// entry whose declared/actual decompressed size exceeds what's
    /// plausible, without this test needing to build anything close to the
    /// real (2 GiB) production budget.
    #[test]
    fn a_highly_compressed_bomb_entry_is_rejected() {
        let bomb_plaintext = vec![0u8; 64 * 1024];
        let zip_bytes = build_zip(&[("stop_times.txt", &bomb_plaintext)]);

        let err = reject_gtfs_zip_bomb_with_budget(&zip_bytes, 1024)
            .expect_err("an entry that decompresses past the budget must be rejected");
        assert!(
            err.to_string().contains("implausible size"),
            "error should explain the size-budget rejection: {err}"
        );
    }

    /// The budget is cumulative across every entry in the archive, not
    /// reset per-entry -- a GTFS zip legitimately has several files, and a
    /// bound that only ever looked at one entry at a time would miss a
    /// bomb spread across several individually-small-looking entries.
    #[test]
    fn the_budget_is_cumulative_across_multiple_entries_not_reset_per_entry() {
        let each = vec![0u8; 4096];
        let zip_bytes = build_zip(&[("a.txt", &each), ("b.txt", &each)]);

        // Each entry alone (4096 bytes) is under a 6000-byte budget, but
        // their combined total (8192) is not.
        let err = reject_gtfs_zip_bomb_with_budget(&zip_bytes, 6000)
            .expect_err("two entries whose combined total exceeds the budget must be rejected");
        assert!(
            err.to_string().contains("implausible size"),
            "error should explain the size-budget rejection: {err}"
        );
    }

    #[test]
    fn two_entries_individually_and_cumulatively_within_budget_are_accepted() {
        let each = vec![0u8; 4096];
        let zip_bytes = build_zip(&[("a.txt", &each), ("b.txt", &each)]);

        assert!(reject_gtfs_zip_bomb_with_budget(&zip_bytes, 16 * 1024).is_ok());
    }
}
