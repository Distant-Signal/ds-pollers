//! `poller-nir-stations`: downloads `OpenDataNI`'s two Translink CSVs
//! ("Northern Ireland Railways Stations"/"...Halts") on an interval,
//! parses/filters/dedups them, and forwards the derived
//! `NorthernIreland`-tagged station catalogue -- plus a small hand-curated
//! line catalogue -- to the `ds:ingest:island-of-ireland` stream as
//! `ioi-stations/1` and `ioi-lines/1` (shared with
//! `poller-irish-rail-gtfs`; ingest plan 3c.2, decision D8), which the
//! ingest-writer applies. The api's `/private/island-of-ireland-*` routes
//! stay until phase 5, but this poller no longer calls them. Tier A of
//! docs/superpowers/specs/2026-09-05-nir-tier-a-implementation-design.md;
//! see docs/superpowers/plans/2026-09-05-nir-tier-a-implementation-plan.md
//! Task 2.

mod config;
mod mapping;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use config::Config;
use ingest_stream::SchemaId;
use ingest_stream::snapshot::SnapshotStream;
use reqwest::Client;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    common::logging::init("poller-nir-stations");

    let config = Config::parse();
    common::metrics::ingest_sink_info(&config.ingest_sink);
    let progress = health_http::spawn_liveness(&config.health);
    // `.user_agent(...)` is NOT optional -- see config::USER_AGENT's own
    // doc comment and this plan's Global Constraints. Every request this
    // client makes to admin.opendatani.gov.uk 403s without it.
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(config::USER_AGENT)
        .build()?;
    let redis = config
        .redis
        .client("INGEST_SINK=stream")
        .map_err(anyhow::Error::msg)?;
    let sinks = Sinks {
        stations: SnapshotStream::spawn(
            redis.clone(),
            ingest_stream::streams::ISLAND_OF_IRELAND,
            SchemaId::new("ioi-stations", 1)?,
            "poller-nir-stations",
            500,
        ),
        lines: SnapshotStream::spawn(
            redis,
            ingest_stream::streams::ISLAND_OF_IRELAND,
            SchemaId::new("ioi-lines", 1)?,
            "poller-nir-stations",
            500,
        ),
    };

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    // The startup cursor is the newest `ioi-stations/1` entry: both
    // snapshots are produced together every cycle (see poll_once). The
    // stream is shared with poller-irish-rail-gtfs, whose stations count
    // too, which only delays a restart's first poll.
    common::poller_loop::run_poll_loop_with_cursor(
        "nir-stations",
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
/// on `ds:ingest:island-of-ireland`.
struct Sinks {
    stations: SnapshotStream,
    lines: SnapshotStream,
}

async fn poll_once(client: &Client, config: &Config, sinks: &Sinks) -> anyhow::Result<()> {
    // The snapshots' `produced_at` (decision D13): the fetch time.
    let fetched_at = Utc::now();
    let stations_csv = client
        .get(&config.stations_csv_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let halts_csv = client
        .get(&config.halts_csv_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let stations = mapping::map_stations(&stations_csv, &halts_csv)?;
    let lines = mapping::map_lines();
    tracing::info!(
        stations = stations.len(),
        lines = lines.len(),
        "parsed NIR station/line catalogue"
    );

    sinks.stations.publish(&stations, fetched_at).await?;
    sinks.lines.publish(&lines, fetched_at).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// Real HTTP-level assertion that the client actually sends the
    /// required `User-Agent` -- this is the one thing that silently breaks
    /// the whole poller in production if regressed (Global Constraints).
    /// `wiremock`'s exact-value `header(...)` matcher only matches a
    /// request carrying exactly this header/value pair; `.expect(1)`
    /// fails the test on drop if that never happened -- so a
    /// `Client::builder()` call that dropped `.user_agent(...)` would make
    /// this test fail with a connection/mock-mismatch error, not silently
    /// pass.
    #[tokio::test]
    async fn client_sends_the_required_user_agent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/stations.csv"))
            .and(header("user-agent", config::USER_AGENT))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("OID_,NAME,TYPE,EASTING,NORTHING,Comment,Lat,Long\n"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(config::USER_AGENT)
            .build()
            .unwrap();
        let response = client
            .get(format!("{}/stations.csv", server.uri()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
}
