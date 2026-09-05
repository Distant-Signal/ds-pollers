//! `poller-ldbws`: samples live departure-board data for every station any
//! line's inference logic depends on, and forwards parsed `StationSample`s
//! to the `api` crate's `/private/station-samples` ingestion endpoint.
//!
//! See `docs/superpowers/specs/2026-07-06-ldbws-sampler-poller-design.md`
//! for the full design and `docs/superpowers/plans/2026-07-06-ldbws-sampler-poller.md`
//! for the RDM facts this is built against (a documentation-discovery pass
//! against a fetched Swagger spec for RDM's Live Departure Board REST
//! product, `GetDepBoardWithDetails`). Two documented gaps carried into
//! `config.rs`: the exact RDM product-slug segment of the base URL, and
//! this feed's real rate limit — both are env-configurable rather than
//! guessed.
//!
//! Unlike the other three pollers, this one calls a second `api` endpoint
//! first (`GET /private/sample-stations`) to learn which CRS codes to
//! sample, then makes one LDBWS call *per station* each cycle — there is
//! no bulk/multi-station LDBWS operation.

mod config;
mod schema;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use common::ingest::{self, RDM_AUTH_HEADER_NAME};
use common::{StationDeparture, StationSample};
use config::Config;
use reqwest::{Client, StatusCode};

/// Per-request timeout — see the other three pollers' identical rationale.
/// 30s is comfortably short relative to the 60s default poll interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of `numRows` values `fetch_departures` will try for a single
/// station before giving up on it for the cycle: the operator-configured
/// value plus up to three halved fallbacks (10 -> 5 -> 2 -> 1 for the
/// default `num_rows`). This is a cap on top of `numrows_step_down`'s own
/// natural floor at 1, not a replacement for it — it bounds how long one
/// troublesome station can hold up the rest of the per-station loop even
/// for an operator-configured `num_rows` much larger than 10, where the
/// halving sequence alone would otherwise take many more attempts to reach
/// 1.
const MAX_NUMROWS_ATTEMPTS: u32 = 4;

/// Delay between successive numRows-fallback attempts for the *same*
/// station. Deliberately much shorter than `poller-tfl`'s 2s/4s backoff
/// (`crates/poller-tfl/src/main.rs`'s `retry_delay`): that poller retries
/// once per cycle against a single call, whereas this poller calls
/// `GetDepBoardWithDetails` once *per station* — up to ~280 of them, see
/// `lines/*.toml`'s `sample_stations` — inside the same cycle, so a heavy
/// per-attempt delay compounds badly if several busy stations need it in
/// the same cycle. Still non-zero: RDM is a real, rate-limited external API
/// and a run of fallback attempts must not hammer it back-to-back.
const NUMROWS_RETRY_DELAY: Duration = Duration::from_millis(500);

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
        "ldbws",
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
    let stations = fetch_sample_stations(client, config, internal_oauth).await?;
    tracing::info!(count = stations.len(), "fetched station list to sample");

    let mut samples = Vec::with_capacity(stations.len());

    for crs in &stations {
        match fetch_departures(client, config, crs).await {
            Ok(departures) => samples.push(StationSample {
                crs: crs.clone(),
                polled_at: Utc::now(),
                departures,
            }),
            Err(err) => {
                tracing::error!(crs = %crs, error = ?err, "failed to sample station; skipping");
            }
        }
    }

    if samples.is_empty() {
        tracing::warn!("no station samples collected this cycle; nothing to post");
        return Ok(());
    }

    ingest::post_batch(
        client,
        &config.api_ingest_url,
        internal_oauth,
        &samples,
        "station samples",
    )
    .await
}

/// Calls the `api` crate's own `/private/sample-stations` endpoint — not an
/// RDM endpoint — to get the deduplicated CRS list computed from the
/// loaded line catalogue. Sent with an internal-oauth bearer token, not
/// the RDM API key.
async fn fetch_sample_stations(
    client: &Client,
    config: &Config,
    tokens: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<Vec<String>> {
    common::ingest::get_json(client, &config.api_sample_stations_url, tokens).await
}

/// The two ways a single `GetDepBoardWithDetails` attempt can fail:
/// `Status` (a non-2xx response, with its body already drained) is the one
/// `fetch_departures`'s retry loop inspects and can act on; `Other` covers
/// everything else (connection errors, timeouts, body-read failures) and is
/// always propagated immediately -- a smaller `numRows` has no bearing on
/// either.
enum FetchError {
    Status(StatusCode, String),
    Other(anyhow::Error),
}

impl From<reqwest::Error> for FetchError {
    fn from(err: reqwest::Error) -> Self {
        FetchError::Other(err.into())
    }
}

/// Steps a `numRows` value down for a retry after a 500 from RDM, per the
/// repo owner's own empirical finding that a smaller `numRows` succeeds
/// where the default fails for a busy terminus (see this module's docs).
/// Halves down to (and stops at) 1 rather than jumping straight to some
/// fixed small number, since exactly how much headroom a given station
/// needs under load is the unknown this is probing for -- and per
/// docs/superpowers/specs/2026-08-31-sample-data-availability-design.md's
/// Correction 1, the aggregator only needs `min_sample_size` (default 3)
/// *relevant* departures pooled across a line's whole `sample_stations`
/// list to report anything at all, so even a heavily-reduced `numRows` at
/// one busy station is far from useless. Returns `None` once `current` is
/// already at the floor (1) -- nothing smaller left to try.
fn numrows_step_down(current: u32) -> Option<u32> {
    if current <= 1 {
        None
    } else {
        Some((current / 2).max(1))
    }
}

/// Worth retrying with a smaller `numRows`: a 5xx is exactly the failure
/// mode reported (`GetDepBoardWithDetails` failing at `numRows=10` for a
/// busy terminus like PAD but succeeding at a smaller value) -- consistent
/// with some internal RDM limit (response size or generation time) that
/// scales with `numRows` x station busyness. A 4xx is a different class of
/// problem entirely (bad API key, bad CRS, an RDM auth change) that a
/// smaller `numRows` will never fix -- retrying it would just mask a real
/// misconfiguration behind repeated, pointless requests. 429 is
/// deliberately excluded too: it's a quota/rate problem, not a
/// payload-size-or-generation-time one, and there is no evidence from the
/// reported symptom that a smaller `numRows` buys anything against it.
fn should_retry_with_smaller_rows(status: StatusCode) -> bool {
    status.is_server_error()
}

/// One `GetDepBoardWithDetails` call for a single station at a specific
/// `num_rows`, with no retry logic of its own -- `fetch_departures` owns
/// the retry loop so it can vary `num_rows` between attempts.
async fn fetch_departures_once(
    client: &Client,
    config: &Config,
    crs: &str,
    num_rows: u32,
) -> Result<String, FetchError> {
    let url = format!(
        "{}/GetDepBoardWithDetails/{crs}?numRows={num_rows}",
        config.ldbws_base_url
    );

    let response = client
        .get(&url)
        .header(RDM_AUTH_HEADER_NAME, &config.rdm_api_key)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(FetchError::Status(status, body));
    }

    Ok(response.text().await?)
}

/// Samples a single station, retrying a 500 with progressively smaller
/// `numRows` values (see `numrows_step_down`) before giving up for this
/// cycle -- the evidence-backed fix for the reported "failed to sample
/// station; skipping crs=PAD" symptom at busy termini during rush hour. A
/// non-5xx failure (bad key, bad CRS, a network error) is never retried,
/// so a genuinely different failure class can't be masked or hammered by
/// this loop. See `docs/superpowers/specs/2026-07-06-ldbws-sampler-poller-design.md`.
async fn fetch_departures(
    client: &Client,
    config: &Config,
    crs: &str,
) -> anyhow::Result<Vec<StationDeparture>> {
    let mut num_rows = config.num_rows;
    let mut attempt = 1;
    let mut fell_back = false;

    loop {
        match fetch_departures_once(client, config, crs, num_rows).await {
            Ok(body) => {
                if fell_back {
                    tracing::warn!(
                        crs = %crs,
                        num_rows,
                        attempt,
                        "sampled station after falling back to a smaller numRows"
                    );
                    // No `crs` label here -- deliberately, per
                    // docs/superpowers/specs/2026-08-29-metrics-design.md's
                    // own "Per-line / per-station cardinality metrics" non-
                    // goal, which names "ldbws sample results by station" as
                    // explicitly deferred: this poller samples one station
                    // per line across a catalogue of 50-100+ lines, and
                    // labeling a metric by CRS is exactly the unbounded-
                    // cardinality trap that doc already rejected for v1.
                    // `crs` still appears on the structured log line above
                    // -- logs don't carry the same per-series cardinality
                    // cost a Prometheus label does.
                    metrics::counter!(
                        common::metrics::metric_name("ldbws_numrows_fallback_total"),
                        "outcome" => "recovered"
                    )
                    .increment(1);
                }
                return schema::parse_departures(&body);
            }
            Err(FetchError::Other(err)) => return Err(err),
            Err(FetchError::Status(status, body)) => {
                let next_num_rows =
                    if attempt < MAX_NUMROWS_ATTEMPTS && should_retry_with_smaller_rows(status) {
                        numrows_step_down(num_rows)
                    } else {
                        None
                    };

                let Some(next_num_rows) = next_num_rows else {
                    if fell_back {
                        metrics::counter!(
                            common::metrics::metric_name("ldbws_numrows_fallback_total"),
                            "outcome" => "exhausted"
                        )
                        .increment(1);
                    }
                    anyhow::bail!(
                        "LDBWS fetch failed for {crs} after {attempt} attempt(s), last numRows={num_rows}: {status} {body}"
                    );
                };

                tracing::warn!(
                    crs = %crs,
                    %status,
                    from_num_rows = num_rows,
                    to_num_rows = next_num_rows,
                    "GetDepBoardWithDetails failed; retrying with a smaller numRows"
                );
                fell_back = true;
                num_rows = next_num_rows;
                attempt += 1;
                tokio::time::sleep(NUMROWS_RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[test]
    fn numrows_halves_down_to_and_stops_at_one() {
        assert_eq!(numrows_step_down(10), Some(5));
        assert_eq!(numrows_step_down(5), Some(2));
        assert_eq!(numrows_step_down(2), Some(1));
        assert_eq!(numrows_step_down(1), None);
        // Odd, non-default configured values still land on the floor.
        assert_eq!(numrows_step_down(3), Some(1));
    }

    #[test]
    fn only_server_errors_trigger_a_numrows_retry() {
        assert!(should_retry_with_smaller_rows(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(should_retry_with_smaller_rows(StatusCode::BAD_GATEWAY));
        assert!(should_retry_with_smaller_rows(
            StatusCode::SERVICE_UNAVAILABLE
        ));
    }

    #[test]
    fn a_different_class_of_failure_is_never_retried() {
        // A bad API key, a bad CRS, or upstream rate-limiting are not going
        // to be fixed by asking for fewer rows -- retrying them would just
        // mask a real problem (or, for 429, burn quota for nothing).
        assert!(!should_retry_with_smaller_rows(StatusCode::UNAUTHORIZED));
        assert!(!should_retry_with_smaller_rows(StatusCode::FORBIDDEN));
        assert!(!should_retry_with_smaller_rows(StatusCode::NOT_FOUND));
        assert!(!should_retry_with_smaller_rows(
            StatusCode::TOO_MANY_REQUESTS
        ));
    }

    /// Fills every `Config` field `fetch_departures` doesn't touch with
    /// inert placeholders -- only `ldbws_base_url`, `rdm_api_key`, and
    /// `num_rows` matter to the code under test here.
    fn test_config(base_url: String, num_rows: u32) -> Config {
        Config {
            ldbws_base_url: base_url,
            rdm_api_key: "test-api-key".to_string(),
            num_rows,
            api_sample_stations_url: "http://api:8080/private/sample-stations".to_string(),
            api_ingest_url: "http://api:8080/private/station-samples".to_string(),
            internal_oauth: common::oauth_client::InternalOAuthArgs {
                internal_oauth_token_url: "http://auth.invalid/token".to_string(),
                internal_oauth_client_id: "distant-signal-internal".to_string(),
                internal_oauth_scope: "groups".to_string(),
                internal_oauth_username: "svc-poller-ldbws".to_string(),
                internal_oauth_password: "app-password".to_string(),
            },
            poll_interval_secs: 60,
            metrics_port: 9091,
            metrics: common::service_args::MetricsArgs {
                metrics_enabled: false,
            },
        }
    }

    const ONE_SERVICE_BODY: &str = r#"{"trainServices":[{"serviceID":"svc-1","operatorCode":"GW","destination":[{"crs":"RDG"}],"std":"10:00","etd":"10:05","isCancelled":false}]}"#;

    #[tokio::test]
    async fn a_200_on_the_first_try_is_not_retried_or_delayed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let start = std::time::Instant::now();
        let departures = fetch_departures(&client, &config, "PAD")
            .await
            .expect("a 200 on the first try must succeed");
        let elapsed = start.elapsed();

        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].service_id, "svc-1");
        // No retry means no `NUMROWS_RETRY_DELAY` sleep was ever hit.
        assert!(
            elapsed < NUMROWS_RETRY_DELAY,
            "an unretried fetch took {elapsed:?}, as long as a real retry delay"
        );
        // wiremock's `.expect(1)` (asserted on Drop) is the real assertion
        // that only one request was made.
    }

    #[tokio::test]
    async fn a_500_at_the_default_numrows_falls_back_to_a_smaller_value_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ONE_SERVICE_BODY))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let departures = fetch_departures(&client, &config, "PAD")
            .await
            .expect("falling back to numRows=5 must recover");

        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].service_id, "svc-1");
        // The two `.expect`/`.up_to_n_times` mock assertions above (checked
        // on `Drop`) confirm exactly one request was made at numRows=10 and
        // exactly one at numRows=5 -- the fallback, not a coincidence of a
        // looser matcher.
    }

    #[tokio::test]
    async fn giving_up_after_the_smallest_numrows_still_500s_returns_an_error_not_a_hang() {
        let server = MockServer::start().await;
        // num_rows=3 steps down to 1 (numrows_step_down(3) == Some(1)) and
        // then stops -- exactly two attempts total, both failing, so this
        // confirms the loop terminates cleanly instead of retrying forever.
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "3"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "1"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 3);
        let client = Client::new();

        let result = fetch_departures(&client, &config, "PAD").await;

        assert!(
            result.is_err(),
            "every numRows value 500ing must surface as an error, not silently succeed"
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("PAD"));
        assert!(message.contains("500"));
        // The two `.expect(1)` mocks above (checked on `Drop`) confirm the
        // loop stopped at exactly two attempts (numRows=3 then 1) rather
        // than looping indefinitely or re-trying numRows=1 again.
    }

    #[tokio::test]
    async fn a_401_is_never_retried_with_a_smaller_numrows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/GetDepBoardWithDetails/PAD"))
            .and(query_param("numRows", "10"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad api key"))
            .expect(1)
            .mount(&server)
            .await;
        let config = test_config(server.uri(), 10);
        let client = Client::new();

        let result = fetch_departures(&client, &config, "PAD").await;

        assert!(result.is_err());
        let requests = server
            .received_requests()
            .await
            .expect("request recording is on by default");
        assert_eq!(
            requests.len(),
            1,
            "a 401 must not trigger any numRows fallback retry"
        );
    }
}
