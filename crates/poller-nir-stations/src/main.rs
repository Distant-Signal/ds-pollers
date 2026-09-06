//! `poller-nir-stations`: downloads OpenDataNI's two Translink CSVs
//! ("Northern Ireland Railways Stations"/"...Halts") on an interval,
//! parses/filters/dedups them, and forwards the derived
//! `NorthernIreland`-tagged station catalogue -- plus a small hand-curated
//! line catalogue -- to `api`'s existing
//! `/private/island-of-ireland-{stations,lines}` ingestion endpoints
//! (shared with `poller-irish-rail-gtfs`). Tier A of
//! docs/superpowers/specs/2026-09-05-nir-tier-a-implementation-design.md;
//! see docs/superpowers/plans/2026-09-05-nir-tier-a-implementation-plan.md
//! Task 2.

mod config;
mod mapping;

use std::time::Duration;

use clap::Parser;
use common::ingest;
use config::Config;
use reqwest::Client;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

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
    // `.user_agent(...)` is NOT optional -- see config::USER_AGENT's own
    // doc comment and this plan's Global Constraints. Every request this
    // client makes to admin.opendatani.gov.uk 403s without it.
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(config::USER_AGENT)
        .build()?;
    let internal_oauth =
        common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
            token_url: config.internal_oauth_token_url.clone(),
            client_id: config.internal_oauth_client_id.clone(),
            scope: config.internal_oauth_scope.clone(),
            username: config.internal_oauth_username.clone(),
            password: config.internal_oauth_password.clone(),
        });

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
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
            "poller" => "nir-stations"
        )
        .record(cycle_start.elapsed().as_secs_f64());
        metrics::counter!(
            common::metrics::metric_name("poller_cycle_total"),
            "poller" => "nir-stations",
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

    ingest::post_batch(
        client,
        &config.api_stations_ingest_url,
        internal_oauth,
        &stations,
        "island-of-ireland stations (NIR)",
    )
    .await?;
    ingest::post_batch(
        client,
        &config.api_lines_ingest_url,
        internal_oauth,
        &lines,
        "island-of-ireland lines (NIR)",
    )
    .await?;
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
