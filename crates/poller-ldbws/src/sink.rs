//! Where a cycle's station samples go (ingest architecture plan 3a.7, spec
//! §13.1), chosen by `INGEST_SINK`:
//!
//! | Sink | Each cycle | Startup cursor |
//! |---|---|---|
//! | `http` (default) | `POST /private/station-samples`, retried within the cycle's budget | `GET` on the same route |
//! | `http+shadow` | the same POST (authoritative: its result is the cycle's), then, if it succeeded, the same samples `XADD`ed to `ds:ingest:station-samples`, best effort | `GET` |
//! | `stream` | XADD only | the stream's newest `produced_at` (`XREVRANGE`, spec §11.3) |
//!
//! **The stream copy** is one snapshot per cycle, in parts of 100 stations
//! (`station-samples/1`, each part's body today's POST body), with
//! `produced_at` the cycle's newest `polled_at` (what the HTTP cursor, the
//! route's `MAX(polled_at)`, reports). It is encoded once, so a retry
//! re-sends the same keys and `produced_at` (spec §7.8). Rows are counted in
//! `ingest_stream_sink_rows_total{sink}` for the rollout's compare step.
//!
//! **While Redis is down** (`stream`): the producer keeps only the latest
//! unsent snapshot ([`ingest_stream::ProducePolicy::LatestSnapshot`]), and
//! the cycle waits for it to be written up to the same budget as a POST's
//! retries, then fails (transient) with the snapshot still held. That
//! composes with the held samples (`pending.rs`, batch 49): a failed
//! delivery holds the rotation (`Rotation::hold`), so the next cycle samples
//! the same stations and its snapshot supersedes this one losslessly; the
//! producer's buffer plays `PendingSamples`' role, which `stream` mode
//! bypasses (`main.rs`'s `poll_once`). Under `http` and `http+shadow`,
//! `deliver_pending` sends the whole held set through [`SampleSink::deliver`].
//!
//! The readiness side of spec §7.5 (`stream_unavailable`) is the cycle
//! failure here: a poller's `/healthz` is its loop's progress.

use std::borrow::Borrow;
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::StationSample;
use common::ingest::{self, DataRejected, LastFetchedResponse};
use common::oauth_client::OAuthTokenCache;
use ingest_stream::SchemaId;
use ingest_stream::envelope::EnvelopeError;
use ingest_stream::snapshot::{ROWS_PER_PART, SinkMode, Snapshot, SnapshotProducer};
use serde::Serialize;

use crate::config::Config;

/// The schema of a station-samples part.
fn schema() -> SchemaId {
    #[expect(clippy::expect_used, reason = "a constant, valid schema id")]
    SchemaId::new("station-samples", 1).expect("valid schema id")
}

/// The configured sink: the HTTP route, the stream, or both.
pub(crate) struct SampleSink {
    mode: SinkMode,
    stream: Option<SnapshotProducer>,
}

impl SampleSink {
    /// From `INGEST_SINK` and its Redis settings.
    pub(crate) fn from_config(config: &Config) -> anyhow::Result<Self> {
        let stream = config.ingest.redis_client()?.map(|client| {
            SnapshotProducer::new(
                client,
                ingest_stream::streams::STATION_SAMPLES,
                "poller-ldbws",
                vec![schema()],
            )
        });
        Ok(Self {
            mode: config.ingest.ingest_sink,
            stream,
        })
    }

    pub(crate) fn mode(&self) -> SinkMode {
        self.mode
    }

    /// The startup cursor (spec §11.3): under `stream`, the newest
    /// `produced_at` on `ds:ingest:station-samples` (`None`, poll now, for
    /// an empty stream); otherwise the api's `GET` on the ingest route, as
    /// before.
    pub(crate) async fn last_fetched(
        &self,
        client: &reqwest::Client,
        config: &Config,
        tokens: &OAuthTokenCache,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        match &self.stream {
            Some(stream) if self.mode == SinkMode::Stream => Ok(stream.cursor().await?),
            _ => {
                let body: LastFetchedResponse =
                    ingest::get_json(client, &config.api_ingest_url, tokens).await?;
                Ok(body.fetched_at)
            }
        }
    }

    /// Delivers one cycle's samples (owned, or borrowed from
    /// `PendingSamples::batch`). `budget` bounds the POST's retries and,
    /// under `stream`, the wait for the XADD.
    pub(crate) async fn deliver<S>(
        &self,
        client: &reqwest::Client,
        config: &Config,
        tokens: &OAuthTokenCache,
        samples: &[S],
        budget: Duration,
    ) -> anyhow::Result<()>
    where
        S: Borrow<StationSample> + Serialize,
    {
        if self.mode.posts_http() {
            ingest::post_batch_retrying(
                client,
                &config.api_ingest_url,
                tokens,
                samples,
                "station samples",
                budget,
            )
            .await?;
            ingest_stream::metrics::sink_rows(
                ingest_stream::streams::STATION_SAMPLES,
                schema().name(),
                "http",
                samples.len(),
            );
        }
        let Some(stream) = self.stream.as_ref().filter(|_| self.mode.produces()) else {
            return Ok(());
        };
        let snapshot = match encode(stream, samples) {
            Ok(snapshot) => snapshot,
            Err(err) if self.mode == SinkMode::HttpShadow => {
                tracing::warn!(error = %err, "could not encode the shadow copy of the station samples");
                return Ok(());
            }
            // The same samples would fail the same way: not a transient
            // failure.
            Err(err) => return Err(DataRejected(format!("station samples snapshot: {err}")).into()),
        };
        let receipt = stream.submit(snapshot).await?;
        if self.mode == SinkMode::HttpShadow {
            // Best effort: the POST is authoritative. The receipt still
            // counts the rows once written.
            return Ok(());
        }
        match tokio::time::timeout(budget, receipt.written()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(anyhow::anyhow!(
                "station samples snapshot not written to {}: {err}",
                stream.stream()
            )),
            Err(_) => Err(anyhow::anyhow!(
                "{} unavailable for {}s; holding the latest station samples snapshot",
                stream.stream(),
                budget.as_secs()
            )),
        }
    }
}

/// One cycle's samples as a `station-samples/1` snapshot.
fn encode<S: Borrow<StationSample> + Serialize>(
    stream: &SnapshotProducer,
    samples: &[S],
) -> Result<Snapshot, EnvelopeError> {
    let produced_at = samples
        .iter()
        .map(|sample| sample.borrow().polled_at)
        .max()
        .unwrap_or_else(Utc::now);
    let mut snapshot = Snapshot::new();
    snapshot.add(
        stream.stream(),
        &schema(),
        stream.producer_id(),
        produced_at,
        samples,
        ROWS_PER_PART,
    )?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use ingest_stream::Envelope;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn sample(crs: &str, polled_at: DateTime<Utc>) -> StationSample {
        StationSample {
            crs: crs.to_owned(),
            polled_at,
            departures: Vec::new(),
        }
    }

    /// A sink on `stream` (a test's own key, leaked to `'static` as the
    /// real stream name is).
    fn stream_sink(mode: SinkMode, redis_url: &str, stream: &str) -> SampleSink {
        SampleSink {
            mode,
            stream: Some(SnapshotProducer::new(
                redis::Client::open(redis_url).unwrap(),
                Box::leak(stream.to_owned().into_boxed_str()),
                "poller-ldbws",
                vec![schema()],
            )),
        }
    }

    /// A config whose api (POSTs and the token endpoint) is `api`.
    fn config(api: &str) -> Config {
        use clap::Parser;
        Config::try_parse_from([
            "poller-ldbws",
            "--ldbws-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "key",
            "--internal-oauth-token-url",
            &format!("{api}/token/"),
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "svc-password",
            "--api-ingest-url",
            &format!("{api}/private/station-samples"),
        ])
        .unwrap()
    }

    #[test]
    fn a_cycle_is_one_snapshot_in_parts_of_100_stations_at_its_newest_polled_at() {
        let sink = stream_sink(SinkMode::Stream, "redis://127.0.0.1:1", "unused");
        let t0: DateTime<Utc> = "2026-10-08T12:00:00Z".parse().unwrap();
        let samples: Vec<StationSample> = (0..250)
            .map(|i| sample(&format!("Z{i:02}"), t0 + TimeDelta::seconds(i)))
            .collect();
        let snapshot = encode(sink.stream.as_ref().unwrap(), &samples).unwrap();
        assert_eq!(snapshot.parts().len(), 3);
        assert_eq!(snapshot.rows(), [("station-samples".to_owned(), 250)]);
        let parts: Vec<Envelope> = snapshot
            .parts()
            .iter()
            .map(|part| Envelope::decode(&part.fields).unwrap())
            .collect();
        let newest = t0 + TimeDelta::seconds(249);
        for (i, part) in parts.iter().enumerate() {
            assert_eq!(part.schema.to_string(), "station-samples/1");
            assert_eq!(part.produced_at, newest);
            assert_eq!(
                part.key,
                format!("station-samples:2026-10-08T12:04:09.000Z:{}/3", i + 1)
            );
        }
        // Each part's body is today's POST body for its chunk.
        let first: Vec<StationSample> = parts[0].payload_as().unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first[0].crs, "Z00");
    }

    /// `http` POSTs and never touches Redis (there is none).
    #[tokio::test]
    async fn http_posts_only() {
        let api = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "t", "expires_in": 300
            })))
            .mount(&api)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/station-samples"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"upserted": 1})),
            )
            .expect(1)
            .mount(&api)
            .await;
        let mut config = config(&api.uri());
        config.ingest.ingest_sink = SinkMode::Http;
        let sink = SampleSink::from_config(&config).unwrap();
        assert!(sink.stream.is_none());
        let tokens = config.internal_oauth.token_cache();
        sink.deliver(
            &reqwest::Client::new(),
            &config,
            &tokens,
            &[sample("ZQA", Utc::now())],
            Duration::ZERO,
        )
        .await
        .unwrap();
    }

    /// `stream` with Redis unreachable: the cycle waits up to its budget,
    /// then fails transiently (the snapshot stays held), without a POST.
    #[tokio::test]
    async fn stream_with_redis_down_fails_the_cycle_transiently_without_posting() {
        let api = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&api)
            .await;
        let mut config = config(&api.uri());
        config.ingest.ingest_sink = SinkMode::Stream;
        config.ingest.redis_url = Some("redis://127.0.0.1:1".to_owned());
        let sink = SampleSink::from_config(&config).unwrap();
        let tokens = config.internal_oauth.token_cache();
        let err = sink
            .deliver(
                &reqwest::Client::new(),
                &config,
                &tokens,
                &[sample("ZQA", Utc::now())],
                Duration::from_millis(300),
            )
            .await
            .unwrap_err();
        assert_eq!(
            ingest::classify_failure(&err),
            ingest::FailureClass::Transient,
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("holding the latest"), "{err:#}");
        assert_eq!(sink.stream.as_ref().unwrap().producer().buffered(), 1);
    }

    /// Against a live Redis (`REDIS_URL`, default local valkey): `stream`
    /// XADDs the cycle's parts, and the cursor reads its `produced_at`;
    /// `http+shadow` POSTs and XADDs the same samples.
    #[tokio::test]
    #[ignore = "requires Redis; run with --ignored (REDIS_URL, default redis://127.0.0.1:6379)"]
    async fn stream_and_shadow_xadd_the_cycle_and_the_cursor_reads_it() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let stream = format!(
            "test-{}:ds:ingest:station-samples",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let api = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "t", "expires_in": 300
            })))
            .mount(&api)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/station-samples"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"upserted": 2})),
            )
            .expect(1)
            .mount(&api)
            .await;

        let config = config(&api.uri());
        let tokens = config.internal_oauth.token_cache();
        let http = reqwest::Client::new();
        let t1: DateTime<Utc> = "2026-10-08T12:00:00.250Z".parse().unwrap();

        let sink = stream_sink(SinkMode::Stream, &redis_url, &stream);
        assert_eq!(
            sink.last_fetched(&http, &config, &tokens).await.unwrap(),
            None,
            "empty stream: poll now"
        );
        sink.deliver(
            &http,
            &config,
            &tokens,
            &[sample("ZQA", t1), sample("ZQB", t1)],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(
            sink.last_fetched(&http, &config, &tokens).await.unwrap(),
            Some(t1)
        );

        // http+shadow: the POST (expected once above), then the copy.
        let shadow = stream_sink(SinkMode::HttpShadow, &redis_url, &stream);
        let t2 = t1 + TimeDelta::minutes(1);
        shadow
            .deliver(
                &http,
                &config,
                &tokens,
                &[sample("ZQA", t2)],
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        // The copy is best effort: wait for it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while shadow.stream.as_ref().unwrap().cursor().await.unwrap() != Some(t2) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "shadow copy not written"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(entries.ids.len(), 2);
        let _: () = redis::cmd("DEL")
            .arg(&stream)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
