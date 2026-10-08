//! `poller-irish-rail-gtfs`: downloads Transport for Ireland's public GTFS
//! zip for Iarnród Éireann on an interval, parses it via `gtfs-structures`,
//! and forwards the derived station/line catalogue to the
//! `ds:ingest:ioi-gtfs` stream as `ioi-stations/1` and
//! `ioi-lines/1` (ingest plan 3c.2, decision D8), which the ingest-writer
//! applies. The api's `/private/island-of-ireland-*` routes stay until
//! phase 5, but this poller no longer calls them. Tier A of
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md; see
//! docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md Task A4.

mod config;
mod feed_guard;
mod mapping;

use std::io::Read;
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use config::Config;
use gtfs_structures::Gtfs;
use ingest_stream::SchemaId;
use ingest_stream::snapshot::SnapshotStream;
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
/// doc comment for how this is actually enforced.
///
/// **Sized against the pod, not just against the feed (L15 follow-up,
/// 2026-09-27).** This was 2 GiB, which bounded nothing useful: the
/// pre-scan itself streams (constant memory), but anything that PASSES it
/// is then parsed wholesale into memory by `Gtfs::from_reader`, inside a
/// container whose chart memory limit is 768Mi
/// (`pollerIrishRailGtfs.resources.limits.memory`). A crafted zip inflating
/// to ~1.5 GiB of CSV sailed through the old budget and `OOMKilled` the pod
/// on the parse instead. The real feed measured 2026-09-27 is 9.0 MB
/// compressed / 27.2 MB inflated (`shapes.txt` alone is 24.8 MB), so 256
/// MiB is still ~9x headroom for feed growth while keeping inflated CSV
/// plus its parsed form plus the (<= [`MAX_GTFS_ZIP_BYTES`]) zip bytes
/// themselves inside that limit. Raise the pod limit first if this ever
/// needs to grow.
const MAX_GTFS_INFLATED_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

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
/// identical logic against a tiny budget (actually decompressing 256 MiB just
/// to prove the cap trips would make the test suite slow and memory-hungry
/// for no extra coverage).
fn reject_gtfs_zip_bomb(bytes: &[u8]) -> anyhow::Result<()> {
    reject_gtfs_zip_bomb_with_budget(bytes, MAX_GTFS_INFLATED_BYTES)
}

#[expect(
    clippy::large_stack_arrays,
    reason = "a 64 KiB read buffer is fine on a thread stack"
)]
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

/// The `User-Agent` every request to Irish Rail / NTA carries (LEG-21):
/// honest and identifying (crate name, version, project URL) rather than
/// reqwest's default of sending none at all. See `common::user_agent`.
const USER_AGENT: &str = common::user_agent!();

/// Builds the HTTP client used for the GTFS download and ingest POSTs.
/// Split out of `main` so the test below can assert, over real HTTP, that
/// the client actually sends [`USER_AGENT`].
fn build_client() -> reqwest::Result<Client> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-irish-rail-gtfs");

    let config = Config::parse();
    common::metrics::ingest_sink_info(&config.ingest_sink);
    let progress = health_http::spawn_liveness(&config.health);
    let client = build_client()?;
    let redis = config
        .redis
        .client("INGEST_SINK=stream")
        .map_err(anyhow::Error::msg)?;
    let sinks = Sinks {
        stations: SnapshotStream::spawn(
            redis.clone(),
            ingest_stream::streams::IOI_GTFS,
            SchemaId::new("ioi-stations", 1)?,
            "poller-irish-rail-gtfs",
            500,
        ),
        lines: SnapshotStream::spawn(
            redis,
            ingest_stream::streams::IOI_GTFS,
            SchemaId::new("ioi-lines", 1)?,
            "poller-irish-rail-gtfs",
            500,
        ),
    };

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    // The startup cursor is the newest `ioi-stations/1` entry: both
    // snapshots are produced together every cycle (see poll_once).
    common::poller_loop::run_poll_loop_with_cursor(
        "irish-rail-gtfs",
        || async { Ok(sinks.stations.last_produced_at().await?) },
        poll_interval,
        config.metrics_enabled,
        config.metrics_port,
        &progress,
        || poll_once(&client, &config, &sinks),
    )
    .await
}

/// The two schemas this poller produces, one latest-snapshot producer each
/// on `ds:ingest:ioi-gtfs`.
struct Sinks {
    stations: SnapshotStream,
    lines: SnapshotStream,
}

async fn poll_once(client: &Client, config: &Config, sinks: &Sinks) -> anyhow::Result<()> {
    // The snapshots' `produced_at` (decision D13): the fetch time.
    let fetched_at = Utc::now();
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

    let mapping::MappedFeed {
        stations,
        lines,
        issues,
    } = mapping::map_feed(&gtfs);
    issues.record();
    tracing::info!(
        stations = stations.len(),
        lines = lines.len(),
        "parsed Iarnrod Eireann GTFS feed"
    );
    let counts = feed_guard::FeedCounts {
        stations: stations.len(),
        lines: lines.len(),
    };
    feed_guard::check_against_last_published(counts, config.feed_max_drop_fraction)?;

    sinks.stations.publish(&stations, fetched_at).await?;
    sinks.lines.publish(&lines, fetched_at).await?;
    feed_guard::record_published(counts);
    Ok(())
}

#[cfg(test)]
mod download_capped_tests {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// LEG-21: the production client must identify itself. wiremock's
    /// exact-value `header` matcher plus `.expect(1)` fails this test if
    /// `build_client` ever stops sending [`USER_AGENT`].
    #[tokio::test]
    async fn client_sends_an_identifying_user_agent() {
        assert!(USER_AGENT.starts_with(concat!("distant-signal-", env!("CARGO_PKG_NAME"), "/")));
        assert!(USER_AGENT.contains(common::user_agent::PROJECT_URL));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(wiremock::matchers::header("user-agent", USER_AGENT))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let response = build_client()
            .unwrap()
            .get(server.uri())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

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
            let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
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
    /// real (256 MiB) production budget.
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

    /// L15 follow-up pin: the production budget must stay small enough
    /// that a zip which passes the pre-scan can still be parsed inside the
    /// pod's 768Mi chart memory limit (see [`MAX_GTFS_INFLATED_BYTES`]'s
    /// doc comment) -- the original 2 GiB value could not -- while staying
    /// comfortably above the real feed's measured 27.2 MB inflated size.
    #[test]
    fn the_production_inflated_budget_fits_the_pod_and_the_real_feed() {
        const POD_MEMORY_LIMIT_BYTES: u64 = 768 * 1024 * 1024;
        const REAL_FEED_INFLATED_BYTES: u64 = 27_193_634;
        // Compile-time checks (a plain `assert!` on constants trips
        // `clippy::assertions_on_constants`).
        const {
            assert!(
                MAX_GTFS_INFLATED_BYTES <= POD_MEMORY_LIMIT_BYTES / 3,
                "inflated budget leaves no room to parse it in the pod"
            );
            assert!(MAX_GTFS_INFLATED_BYTES >= 4 * REAL_FEED_INFLATED_BYTES);
        }
    }

    #[test]
    fn two_entries_individually_and_cumulatively_within_budget_are_accepted() {
        let each = vec![0u8; 4096];
        let zip_bytes = build_zip(&[("a.txt", &each), ("b.txt", &each)]);

        assert!(reject_gtfs_zip_bomb_with_budget(&zip_bytes, 16 * 1024).is_ok());
    }
}
