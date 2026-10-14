//! `poller-transit-schedule`: one transit network's producer (transit
//! networks spec §6; Manchester Metrolink first). It reads the network's
//! public timetable and disruption feeds and XADDs them to its own stream,
//! `ds:ingest:transit-<network>`, as its own Redis ACL user; the
//! ingest-writer applies the stream. It has no database or api credentials.
//!
//! Two loops, each with its own latest-snapshot producer on the stream
//! (spec C3: under `LatestSnapshot` one producer's newer item replaces an
//! unsent older one, so a situations snapshot must not supersede an unsent
//! timetable):
//!
//! - **timetable** (daily): the source adapter (GTFS), `NaPTAN` names and the
//!   rail links, as `transit-timetable/1` parts plus one
//!   `transit-timetable-commit/1`, one item (plan tasks 2.2, 2.3);
//! - **situations** (every 5 minutes): SIRI-SX as `transit-situations/1`
//!   (plan task 2.4; the writer applies it from batch 4).
//!
//! Off by default: the chart deploys it only for an enabled network
//! (`pollerTransitSchedule.networks.<network>.enabled`).

mod adapter;
mod config;
mod feed_guard;
mod item;
mod model;
mod naptan;
mod network;
mod rail_links;
mod situations;
mod timetable;

use std::time::Duration;

use clap::Parser;
use config::Config;
use ingest_stream::snapshot::SnapshotProducer;
use network::NetworkConfig;

/// The envelope's `producer` component.
const COMPONENT: &str = "poller-transit-schedule";

/// How long shutdown waits for an unsent snapshot.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// One whole download (the North West GTFS was 159 MB on 2026-10-14).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// The `User-Agent` of every request: honest and identifying (LEG-21).
const USER_AGENT: &str = common::user_agent!();

/// What every cycle needs.
pub(crate) struct Context {
    pub config: Config,
    pub network: NetworkConfig,
    pub client: reqwest::Client,
    /// `ds:ingest:transit-<network>`.
    pub stream: String,
    /// The network's Redis user, for reading its stream back (`XREVRANGE`).
    pub redis: redis::Client,
    /// The timetable's producer (one item: every part, then the commit).
    pub timetable: SnapshotProducer,
    /// The last timetable this process sent: the feed guard's baseline
    /// when the stream cannot be read.
    pub last_sent: std::sync::Mutex<Option<timetable::Baseline>>,
    /// The situations' producer, separate so a situations snapshot never
    /// supersedes an unsent timetable (spec C3).
    pub situations: SnapshotProducer,
    /// The SIRI-SX archive's last `ETag` (`If-None-Match`).
    pub situations_etag: std::sync::Mutex<Option<String>>,
}

/// How often readiness re-checks the producers.
const READINESS_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    common::logging::init(COMPONENT);

    let config = Config::parse();
    let network = NetworkConfig::load(&config.network)?;
    let stream = ingest_stream::streams::transit(&network.id);
    tracing::info!(network = %network.network_id(), %stream, ?config, "starting");
    let redis = config
        .redis
        .client("the transit stream")
        .map_err(anyhow::Error::msg)?;
    // `/healthz` (readiness) answers 503 "stream_unavailable" while either
    // producer's last XADD failed (spec §6.1); `/livez` is loop progress.
    let (ready, progress) = health_http::spawn_with_progress(
        config.health.health_bind_url.clone(),
        "ok",
        "stream_unavailable",
        config.health.stall_after(),
    );
    ready.store(true, std::sync::atomic::Ordering::Relaxed);
    let timetable = SnapshotProducer::new(redis.clone(), &stream, COMPONENT);
    timetable.start();
    let situations = SnapshotProducer::new(redis.clone(), &stream, COMPONENT);
    situations.start();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()?;
    let ctx = Context {
        config,
        network,
        client,
        stream,
        redis,
        timetable,
        last_sent: std::sync::Mutex::new(None),
        situations,
        situations_etag: std::sync::Mutex::new(None),
    };
    let readiness = async {
        let mut tick = tokio::time::interval(READINESS_INTERVAL);
        loop {
            tick.tick().await;
            ready.store(
                ctx.timetable.is_available() && ctx.situations.is_available(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    };

    let timetable_loop = common::poller_loop::run_poll_loop_with_cursor(
        "transit-timetable",
        // The newest timetable commit, not the newest entry: situations
        // share the stream every 5 minutes.
        || async {
            Ok(ingest_stream::snapshot::newest_of_schema(
                &ctx.redis,
                &ctx.stream,
                common::transit::schema::TIMETABLE_COMMIT,
            )
            .await?
            .map(|envelope| envelope.produced_at))
        },
        ctx.config.timetable_interval(),
        false,
        ctx.config.metrics_port,
        &progress,
        || timetable::poll_once(&ctx),
    );
    let situations_loop = async {
        if !ctx.config.situations_enabled {
            tracing::info!("the situations loop is off (TRANSIT_SITUATIONS_ENABLED)");
            return std::future::pending().await;
        }
        common::poller_loop::run_poll_loop_with_cursor(
            "transit-situations",
            // Poll at once at startup: the archive changes every minute.
            || async { Ok(None) },
            ctx.config.situations_interval(),
            false,
            ctx.config.metrics_port,
            &progress,
            || situations::poll_once(&ctx),
        )
        .await
    };
    let result = tokio::select! {
        result = timetable_loop => result,
        result = situations_loop => result,
        () = readiness => Ok(()),
        () = shutdown_signal() => {
            tracing::info!("shutting down");
            Ok(())
        }
    };
    if !ctx.timetable.shutdown(SHUTDOWN_GRACE).await {
        tracing::warn!(
            "an unsent timetable was dropped at shutdown; the next start sends a fresh one"
        );
    }
    let _ = ctx.situations.shutdown(SHUTDOWN_GRACE).await;
    result
}

/// SIGTERM (Kubernetes) or Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(term) => term,
                Err(err) => {
                    tracing::warn!(error = %err, "no SIGTERM handler; Ctrl-C only");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
