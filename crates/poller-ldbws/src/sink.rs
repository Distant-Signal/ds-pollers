//! Where a cycle's station samples go (ingest architecture plan 3a.7, spec
//! §13.1): `ds:ingest:station-samples`, which the ingest-writer applies.
//! Each cycle XADDs one snapshot; the startup cursor is the stream's newest
//! `produced_at` (`XREVRANGE`, spec §11.3). Release B (ingest phase 5)
//! removed the `http` and `http+shadow` sinks (`POST /private/station-samples`).
//!
//! **The snapshot** is one per cycle, in parts of 100 stations
//! (`station-samples/1`, each part's body the retired POST body), with
//! `produced_at` the cycle's newest `polled_at`. It is encoded once, so a
//! retry re-sends the same keys and `produced_at` (spec §7.8).
//!
//! **While Redis is down**: the producer keeps only the latest unsent
//! snapshot ([`ingest_stream::ProducePolicy::LatestSnapshot`]), and the
//! cycle waits for it to be written up to the poll's retry budget, then
//! fails (transient) with the snapshot still held. A failed delivery holds
//! the rotation (`Rotation::hold`), so the next cycle samples the same
//! stations and its snapshot supersedes this one losslessly.
//!
//! The readiness side of spec §7.5 (`stream_unavailable`) is the cycle
//! failure here: a poller's `/healthz` is its loop's progress.

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::ingest::DataRejected;
use common::{StationSample, TrackedStopBoard};
use ingest_stream::SchemaId;
use ingest_stream::envelope::EnvelopeError;
use ingest_stream::snapshot::{ROWS_PER_PART, Snapshot, SnapshotProducer};

use crate::config::Config;

/// The schema of a station-samples part.
fn schema() -> SchemaId {
    #[expect(clippy::expect_used, reason = "a constant, valid schema id")]
    SchemaId::new("station-samples", 1).expect("valid schema id")
}

/// The `ds:ingest:station-samples` producer.
pub(crate) struct SampleSink {
    stream: SnapshotProducer,
}

impl SampleSink {
    /// From the Redis settings (`REDIS_URL` required).
    pub(crate) fn from_config(config: &Config) -> anyhow::Result<Self> {
        let client = config.ingest.redis_client()?;
        Ok(Self {
            stream: SnapshotProducer::new(
                client,
                ingest_stream::streams::STATION_SAMPLES,
                "poller-ldbws",
            ),
        })
    }

    /// The startup cursor (spec §11.3): the newest `produced_at` on
    /// `ds:ingest:station-samples` (`None`, poll now, for an empty stream).
    pub(crate) async fn last_fetched(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        Ok(self.stream.cursor().await?)
    }

    /// Delivers one cycle's samples and tracked-stop boards, waiting up to
    /// `budget` for the XADD.
    pub(crate) async fn deliver(
        &self,
        samples: &[StationSample],
        tracked: &[TrackedStopBoard],
        budget: Duration,
    ) -> anyhow::Result<()> {
        let snapshot = match encode(&self.stream, samples, tracked) {
            Ok(snapshot) => snapshot,
            // The same samples would fail the same way: not a transient
            // failure.
            Err(err) => return Err(DataRejected(format!("station samples snapshot: {err}")).into()),
        };
        let receipt = self.stream.submit(snapshot).await?;
        match tokio::time::timeout(budget, receipt.written()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(anyhow::anyhow!(
                "station samples snapshot not written to {}: {err}",
                self.stream.stream()
            )),
            Err(_) => Err(anyhow::anyhow!(
                "{} unavailable for {}s; holding the latest station samples snapshot",
                self.stream.stream(),
                budget.as_secs()
            )),
        }
    }
}

/// The schema of a tracked-stop boards part (DS-127).
fn tracked_schema() -> SchemaId {
    #[expect(clippy::expect_used, reason = "a constant, valid schema id")]
    SchemaId::new(common::TRACKED_STOP_BOARDS_SCHEMA, 1).expect("valid schema id")
}

/// One cycle's samples as a `station-samples/1` snapshot, plus its
/// tracked-stop boards as `tracked-stop-boards/1` parts.
fn encode(
    stream: &SnapshotProducer,
    samples: &[StationSample],
    tracked: &[TrackedStopBoard],
) -> Result<Snapshot, EnvelopeError> {
    let produced_at = samples
        .iter()
        .map(|sample| sample.polled_at)
        .chain(tracked.iter().map(|board| board.polled_at))
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
    snapshot.add(
        stream.stream(),
        &tracked_schema(),
        stream.producer_id(),
        produced_at,
        tracked,
        ROWS_PER_PART,
    )?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use common::ingest;
    use ingest_stream::Envelope;

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
    fn stream_sink(redis_url: &str, stream: &str) -> SampleSink {
        SampleSink {
            stream: SnapshotProducer::new(
                redis::Client::open(redis_url).unwrap(),
                Box::leak(stream.to_owned().into_boxed_str()),
                "poller-ldbws",
            ),
        }
    }

    #[test]
    fn tracked_stop_boards_ride_the_same_snapshot_as_their_own_schema() {
        let sink = stream_sink("redis://127.0.0.1:1", "unused");
        let t0: DateTime<Utc> = "2026-10-08T12:00:00Z".parse().unwrap();
        let tracked = [TrackedStopBoard {
            train_uid: "W12345".to_owned(),
            service_date: "2026-10-08".parse().unwrap(),
            crs: "YRK".to_owned(),
            polled_at: t0 + TimeDelta::seconds(90),
            departures: Vec::new(),
        }];
        let snapshot = encode(&sink.stream, &[sample("ZQA", t0)], &tracked).unwrap();
        assert_eq!(
            snapshot.rows(),
            [
                ("station-samples".to_owned(), 1),
                ("tracked-stop-boards".to_owned(), 1)
            ]
        );
        let parts: Vec<Envelope> = snapshot
            .parts()
            .iter()
            .map(|part| Envelope::decode(&part.fields).unwrap())
            .collect();
        // The cycle's newest poll is the snapshot's time, either lane's.
        assert!(
            parts
                .iter()
                .all(|part| part.produced_at == t0 + TimeDelta::seconds(90))
        );
    }

    #[test]
    fn a_cycle_is_one_snapshot_in_parts_of_100_stations_at_its_newest_polled_at() {
        let sink = stream_sink("redis://127.0.0.1:1", "unused");
        let t0: DateTime<Utc> = "2026-10-08T12:00:00Z".parse().unwrap();
        let samples: Vec<StationSample> = (0..250)
            .map(|i| sample(&format!("Z{i:02}"), t0 + TimeDelta::seconds(i)))
            .collect();
        let snapshot = encode(&sink.stream, &samples, &[]).unwrap();
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
        // Each part's body is the retired POST body for its chunk.
        let first: Vec<StationSample> = parts[0].payload_as().unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first[0].crs, "Z00");
    }

    /// Redis unreachable: the cycle waits up to its budget, then fails
    /// transiently (the snapshot stays held).
    #[tokio::test]
    async fn redis_down_fails_the_cycle_transiently_and_holds_the_snapshot() {
        let sink = stream_sink("redis://127.0.0.1:1", "ds:ingest:station-samples");
        let err = sink
            .deliver(
                &[sample("ZQA", Utc::now())],
                &[],
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
        assert_eq!(sink.stream.producer().buffered(), 1);
    }

    /// Against a live Redis (`REDIS_URL`, default local valkey): the sink
    /// XADDs the cycle's parts, and the cursor reads its `produced_at`.
    #[tokio::test]
    #[ignore = "requires Redis; run with --ignored (REDIS_URL, default redis://127.0.0.1:6379)"]
    async fn the_sink_xadds_the_cycle_and_the_cursor_reads_it() {
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
        let t1: DateTime<Utc> = "2026-10-08T12:00:00.250Z".parse().unwrap();

        let sink = stream_sink(&redis_url, &stream);
        assert_eq!(
            sink.last_fetched().await.unwrap(),
            None,
            "empty stream: poll now"
        );
        sink.deliver(
            &[sample("ZQA", t1), sample("ZQB", t1)],
            &[],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(sink.last_fetched().await.unwrap(), Some(t1));

        let t2 = t1 + TimeDelta::minutes(1);
        sink.deliver(&[sample("ZQA", t2)], &[], Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(sink.last_fetched().await.unwrap(), Some(t2));
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
