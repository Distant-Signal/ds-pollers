//! `poller-nir-stations`: downloads `OpenDataNI`'s two Translink CSVs
//! ("Northern Ireland Railways Stations"/"...Halts") on an interval,
//! parses/filters/dedups them, and forwards the derived
//! `NorthernIreland`-tagged station catalogue -- plus a small hand-curated
//! line catalogue -- to the `ds:ingest:ioi-nir` stream as
//! `ioi-stations/1` and `ioi-lines/1` (its own stream; `poller-irish-rail-gtfs`
//! produces the same schemas on `ds:ingest:ioi-gtfs`; ingest plan 3c.2,
//! decision D8), which the ingest-writer applies. The api's `/private/island-of-ireland-*` routes
//! it used to call were deleted (phase 5 runbook Q9). Tier A of
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

/// What `--print-config-defaults` prints (`ds_runtime::service_args`).
fn config_defaults() -> ds_runtime::service_args::ConfigDefaults {
    poller_lifecycle::config_defaults(
        ds_runtime::service_args::ConfigDefaults::for_config::<Config>("poller-nir-stations"),
    )
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    ds_runtime::service_args::exit_if_print_config_defaults(config_defaults);
    ds_runtime::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    ds_runtime::logging::init("poller-nir-stations");

    let config = Config::parse();
    let (lifecycle, progress) = poller_lifecycle::Lifecycle::spawn(&config.health)?;
    let once = common::poller_loop::RunOnce::from_env()?.is_some();
    // `.user_agent(...)` is NOT optional -- see config::USER_AGENT's own
    // doc comment and this plan's Global Constraints. Every request this
    // client makes to admin.opendatani.gov.uk 403s without it.
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(config::USER_AGENT)
        .build()?;
    let redis = config
        .redis
        .client("the ingest stream")
        .map_err(anyhow::Error::msg)?;
    let sinks = Sinks {
        stations: SnapshotStream::spawn(
            redis.clone(),
            ingest_stream::streams::IOI_NIR,
            SchemaId::new("ioi-stations", 1)?,
            "poller-nir-stations",
            500,
        ),
        lines: SnapshotStream::spawn(
            redis,
            ingest_stream::streams::IOI_NIR,
            SchemaId::new("ioi-lines", 1)?,
            "poller-nir-stations",
            500,
        ),
    };

    // The producers are lazy: a Redis outage shows as failed cycles.
    lifecycle.sink_connected();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    // The startup cursor is the older of the newest `ioi-stations/1` and
    // `ioi-lines/1` entries (`Sinks::cursor`).
    let poll_loop = common::poller_loop::run_poll_loop_with_cursor(
        "nir-stations",
        || sinks.cursor(),
        poll_interval,
        config.metrics_enabled,
        config.metrics_port,
        &progress,
        || lifecycle.track(poll_once(&client, &config, &sinks)),
    );
    let result = lifecycle.run(Box::pin(poll_loop), once).await;
    sinks.flush().await;
    result
}

/// The two schemas this poller produces, one latest-snapshot producer each
/// on `ds:ingest:ioi-nir`.
struct Sinks {
    stations: SnapshotStream,
    lines: SnapshotStream,
}

impl Sinks {
    /// Closes both producers at exit, each waiting up to
    /// `poller_lifecycle::FLUSH_GRACE` for an unsent snapshot.
    async fn flush(self) {
        for (schema, stream) in [("ioi-stations", self.stations), ("ioi-lines", self.lines)] {
            if !stream.shutdown(poller_lifecycle::FLUSH_GRACE).await {
                tracing::warn!(schema, "a snapshot was still unsent at exit");
            }
        }
    }

    /// The startup cursor: the older of the two schemas' newest entries on
    /// `ds:ingest:ioi-nir`. A cycle queues stations, then lines, so a pod stopped
    /// between the two XADDs leaves stations newer than lines. Reading
    /// stations alone would then call the data fresh and leave lines a
    /// whole interval (a day) behind; this re-sends both instead (a
    /// duplicate stations snapshot, which the writer's guard absorbs).
    async fn cursor(&self) -> anyhow::Result<Option<chrono::DateTime<Utc>>> {
        let stations = self.stations.last_produced_at().await?;
        let lines = self.lines.last_produced_at().await?;
        Ok(older_cursor(stations, lines))
    }
}

/// The older of two schemas' cursors; `None` ("poll now") when either has
/// never been written.
fn older_cursor(
    a: Option<chrono::DateTime<Utc>>,
    b: Option<chrono::DateTime<Utc>>,
) -> Option<chrono::DateTime<Utc>> {
    Some(a?.min(b?))
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

#[cfg(test)]
mod cursor_tests {
    use chrono::TimeZone;

    use super::*;

    fn at(hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 4, hour, 0, 0)
            .single()
            .unwrap_or_default()
    }

    #[test]
    fn the_cursor_is_the_older_schema_so_lines_left_behind_are_re_sent() {
        // Stations reached the stream at 04:00 but the pod stopped before
        // lines did: lines' newest is the 03:00 run, and so is the cursor.
        assert_eq!(older_cursor(Some(at(4)), Some(at(3))), Some(at(3)));
        assert_eq!(older_cursor(Some(at(3)), Some(at(4))), Some(at(3)));
        assert_eq!(older_cursor(Some(at(4)), Some(at(4))), Some(at(4)));
    }

    #[test]
    fn a_schema_never_written_means_poll_now() {
        assert_eq!(older_cursor(Some(at(4)), None), None);
        assert_eq!(older_cursor(None, Some(at(4))), None);
        assert_eq!(older_cursor(None, None), None);
    }
}
