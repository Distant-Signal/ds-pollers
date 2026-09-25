//! `poller-irish-rail-live`: polls `api.irishrail.ie`'s legacy realtime XML
//! service for every station it lists, and forwards raw per-station
//! departure-board samples to `api`'s
//! `/private/island-of-ireland-station-samples` endpoint. Tier B of
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md; see
//! docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md Task B4.
//! Deliberately raw ingestion only -- no severity inference, no
//! aggregator involvement (Judgment Call #3 there).
//!
//! Modeled on `crates/poller-ldbws/src/main.rs`'s per-station polling loop
//! shape (the closest existing precedent for "make one API call per
//! station each cycle") and `crates/poller-incidents/src/schema.rs`'s
//! `quick-xml` parsing pattern -- see `schema.rs`'s own module docs for
//! this crate's real, live-fetch-confirmed departure-board schema.
//! Deliberately does NOT depend on Tier A's `island_of_ireland_stations`
//! catalogue (this plan's Judgment Call #1): it discovers its own station
//! codes from `api.irishrail.ie`'s own `getAllStationsXML` each cycle.

mod config;
mod schema;

use std::time::Duration;

use clap::Parser;
use common::ingest;
use config::Config;
use reqwest::Client;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on how long the whole per-station sampling loop in
/// `poll_once` may run in a single cycle, regardless of how many stations
/// `getAllStationsXML` returns or how slow individual upstream responses
/// are.
///
/// Signal Box Audit, poll-area Low finding -- "per-station pollers have no
/// per-cycle time budget": before this, the loop over every station this
/// crate discovers (171 per this crate's own live confirmation -- see
/// `config.rs`'s module docs) had no cap of its own -- `REQUEST_TIMEOUT`
/// (30s) bounds *one* station's call, but nothing bounded the sum across
/// all of them, so enough individually-slow (not even hanging) stations in
/// one cycle could let that cycle run for many multiples of
/// `poll_interval_secs`, degrading every subsequent cycle gracelessly
/// rather than boundedly -- a real risk against `api.irishrail.ie`'s own
/// unconfirmed capacity (see `config.rs`'s `poll_interval_secs` doc
/// comment). 240s leaves a comfortable margin under this crate's own 300s
/// conservative `poll_interval_secs` default so a budget-exceeded cycle
/// still yields back well before the next tick would otherwise be starved
/// entirely -- deliberately NOT derived from `poll_interval_secs` itself,
/// an operator-configured value with no guaranteed relationship to how
/// long sampling every station should take.
const CYCLE_TIME_BUDGET: Duration = Duration::from_secs(240);

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
        "irish-rail-live",
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
    let station_codes = if config.station_codes_override.is_empty() {
        fetch_all_station_codes(client, config).await?
    } else {
        config.station_codes_override.clone()
    };
    tracing::info!(
        count = station_codes.len(),
        "fetched station code list to sample"
    );

    let samples =
        sample_stations_within_budget(client, config, &station_codes, CYCLE_TIME_BUDGET).await;

    if samples.is_empty() {
        tracing::warn!("no station samples collected this cycle; nothing to post");
        return Ok(());
    }

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &samples,
        "island-of-ireland station samples",
    )
    .await
}

/// Samples every code in `station_codes`, but never for longer than
/// `budget` in total: if the per-station loop (see `sample_all_stations`)
/// hasn't finished within `budget`, it's aborted in place and whatever
/// samples were already collected are returned as-is, with a warning
/// logged. `budget` is a parameter (rather than reading `CYCLE_TIME_BUDGET`
/// directly) purely so tests can exercise the timeout path with a budget
/// measured in milliseconds instead of `CYCLE_TIME_BUDGET`'s real 240s.
async fn sample_stations_within_budget(
    client: &Client,
    config: &Config,
    station_codes: &[String],
    budget: Duration,
) -> Vec<common::island_of_ireland::IslandOfIrelandStationSample> {
    let mut samples = Vec::with_capacity(station_codes.len());
    let outcome = tokio::time::timeout(
        budget,
        sample_all_stations(client, config, station_codes, &mut samples),
    )
    .await;

    if outcome.is_err() {
        tracing::warn!(
            stations_total = station_codes.len(),
            stations_sampled = samples.len(),
            budget_secs = budget.as_secs_f64(),
            "per-cycle station-sampling time budget exceeded; moving on with what was \
             collected so far rather than blocking this and every subsequent cycle"
        );
    }

    samples
}

/// The per-station loop itself, extracted so `sample_stations_within_budget`
/// can wrap it in `tokio::time::timeout` -- when the timeout fires, this
/// future (and its local state) is dropped mid-iteration, but every sample
/// already pushed into the caller-owned `samples` accumulator before that
/// point survives, since it's a `&mut` borrow of state the caller owns,
/// not state local to this future.
async fn sample_all_stations(
    client: &Client,
    config: &Config,
    station_codes: &[String],
    samples: &mut Vec<common::island_of_ireland::IslandOfIrelandStationSample>,
) {
    for code in station_codes {
        match fetch_station_departures(client, config, code).await {
            Ok(departures) => samples.push(schema::to_sample(code, departures)),
            Err(err) => {
                tracing::error!(station_code = %code, error = ?err, "failed to sample station; skipping");
            }
        }
    }
}

async fn fetch_all_station_codes(client: &Client, config: &Config) -> anyhow::Result<Vec<String>> {
    let url = format!("{}/getAllStationsXML", config.irish_rail_base_url);
    let body = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    tracing::debug!(body = %body, "raw getAllStationsXML response body");
    schema::parse_all_stations(&body)
}

async fn fetch_station_departures(
    client: &Client,
    config: &Config,
    station_code: &str,
) -> anyhow::Result<Vec<common::island_of_ireland::IslandOfIrelandDeparture>> {
    // `station_code` comes from `getAllStationsXML` (a different upstream
    // response than the one being fetched here), not from this process's
    // own config, so it's treated the same defensive way `Traincode` is
    // treated elsewhere in this crate: trimmed, and -- unlike `Traincode`,
    // which never leaves this process -- also URL-encoded before it is
    // spliced into a query string, since it also becomes this station's
    // sample DB primary key. `reqwest::Url::query_pairs_mut` does real
    // percent-encoding (not just naive string interpolation), so a
    // surprising future upstream value (whitespace, `&`, `%`, `#`, ...)
    // can't reshape the query string or smuggle a different `StationCode`
    // in.
    let mut url = reqwest::Url::parse(&format!(
        "{}/getStationDataByCodeXML",
        config.irish_rail_base_url
    ))?;
    url.query_pairs_mut()
        .append_pair("StationCode", station_code.trim());
    let body = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    tracing::debug!(station_code = %station_code, body = %body, "raw getStationDataByCodeXML response body");
    schema::parse_station_departures(&body)
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// Fills every `Config` field `fetch_station_departures` doesn't touch
    /// with inert placeholders -- only `irish_rail_base_url` matters to the
    /// code under test here.
    fn test_config(base_url: String) -> Config {
        Config {
            irish_rail_base_url: base_url,
            api_ingest_url: "http://api:8080/private/island-of-ireland-station-samples".to_string(),
            internal_oauth: common::oauth_client::InternalOAuthArgs {
                internal_oauth_token_url: "http://auth.invalid/token".to_string(),
                internal_oauth_client_id: "distant-signal-internal".to_string(),
                internal_oauth_scope: "groups".to_string(),
                internal_oauth_username: "svc-poller-irish-rail-live".to_string(),
                internal_oauth_password: "app-password".to_string(),
            },
            poll_interval_secs: 300,
            station_codes_override: vec![],
            metrics_port: 9091,
            metrics: common::service_args::MetricsArgs {
                metrics_enabled: false,
            },
        }
    }

    const EMPTY_STATION_BODY: &str = r#"<?xml version="1.0" encoding="utf-8"?>
        <ArrayOfObjStationData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://api.irishrail.ie/realtime/" />"#;

    #[tokio::test]
    async fn station_code_is_trimmed_and_url_encoded_not_spliced_raw() {
        let server = MockServer::start().await;
        // A station code carrying leading/trailing whitespace (as
        // `Traincode` is known to in the real feed -- see schema.rs) and an
        // embedded space that MUST be percent-encoded to survive as a
        // single query value rather than corrupting the query string.
        // wiremock's `query_param` matcher compares against the DECODED
        // value, so a match here proves the value reached the wire
        // correctly percent-encoded ("BF%20STC") and that the surrounding
        // whitespace was trimmed before that -- not spliced in raw via
        // `format!`.
        Mock::given(method("GET"))
            .and(path("/getStationDataByCodeXML"))
            .and(query_param("StationCode", "BF STC"))
            .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_STATION_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri());
        let client = Client::new();

        let result = fetch_station_departures(&client, &config, "  BF STC  ").await;

        assert!(
            result.is_ok(),
            "a trimmed, percent-encoded station code must still reach the mocked endpoint \
             matching on the decoded value: {:?}",
            result.err()
        );
        // wiremock's `.expect(1)` (asserted on `Drop`) is the real
        // assertion that the mock matched at all -- an untrimmed or
        // raw-spliced value would produce a different (non-matching) query
        // string and this test would fail at that assertion.
    }

    #[tokio::test]
    async fn a_station_code_with_special_characters_is_percent_encoded() {
        let server = MockServer::start().await;
        // `&` and `=` would corrupt an unencoded query string outright
        // (splitting it into extra bogus params); confirming these survive
        // as a single decoded value is a stronger proof of real encoding
        // than a plain space alone.
        Mock::given(method("GET"))
            .and(path("/getStationDataByCodeXML"))
            .and(query_param("StationCode", "A&B=C"))
            .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_STATION_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri());
        let client = Client::new();

        let result = fetch_station_departures(&client, &config, "A&B=C").await;

        assert!(
            result.is_ok(),
            "a station code containing '&'/'=' must still be delivered as one decoded value: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn a_cycle_time_budget_bounds_total_sampling_time_across_slow_stations() {
        // Three stations, each individually well within a single request's
        // own timeout, but slow enough that all three together would take
        // far longer than the tiny budget this test gives the whole loop.
        let server = MockServer::start().await;
        for code in ["AAA", "BBB", "CCC"] {
            Mock::given(method("GET"))
                .and(path("/getStationDataByCodeXML"))
                .and(query_param("StationCode", code))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(EMPTY_STATION_BODY)
                        .set_delay(Duration::from_millis(150)),
                )
                .mount(&server)
                .await;
        }
        let config = test_config(server.uri());
        let client = Client::new();
        let station_codes = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];

        let start = std::time::Instant::now();
        let samples = sample_stations_within_budget(
            &client,
            &config,
            &station_codes,
            Duration::from_millis(200),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(150 * 3),
            "the budget should have cut the loop short well before all three \
             150ms-delayed stations finished, took {elapsed:?}"
        );
        assert!(
            samples.len() < station_codes.len(),
            "a budget-cut cycle must not have sampled every station: {samples:?}"
        );
    }

    #[tokio::test]
    async fn a_generous_budget_does_not_truncate_a_normal_cycle() {
        let server = MockServer::start().await;
        for code in ["AAA", "BBB"] {
            Mock::given(method("GET"))
                .and(path("/getStationDataByCodeXML"))
                .and(query_param("StationCode", code))
                .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_STATION_BODY))
                .mount(&server)
                .await;
        }
        let config = test_config(server.uri());
        let client = Client::new();
        let station_codes = vec!["AAA".to_string(), "BBB".to_string()];

        let samples =
            sample_stations_within_budget(&client, &config, &station_codes, CYCLE_TIME_BUDGET)
                .await;

        assert_eq!(
            samples.len(),
            2,
            "a fast cycle well within budget must sample every station"
        );
    }
}
