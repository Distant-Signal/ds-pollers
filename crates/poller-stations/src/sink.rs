//! Where a parsed stations feed goes (ingest architecture plan 2b.1,
//! spec §9.3): [`DbSink`] writes it with
//! `ds_store::reference::upsert_stations`, retried within the poll's
//! budget, as the `stations` role, and reads the startup cursor (plan 4.6)
//! from `ds_store::freshness::last_stations_fetch`. Release B (ingest phase
//! 5) removed the `http` sink (`POST /private/stations`).
//!
//! The ingest-writer's 10-minute `CORPUS_CROSSWALK` loop rebuilds the
//! CORPUS crosswalk after a refresh (plan 2b.2, spec §9.3), so the
//! `stations` role needs no crosswalk grants.

use std::collections::BTreeMap;
use std::time::Duration;

use common::backoff::Backoff;
use common::ingest;
use ds_store::reference::StationRow;
use serde_json::value::RawValue;
use sqlx::PgPool;

use crate::schema::StationRecord;

/// The sink: straight into Postgres, as the `stations` role.
pub(crate) struct DbSink {
    pub pool: PgPool,
    /// How long one `publish` may retry a transient failure
    /// (`common::poller_loop::post_retry_budget`), without re-fetching the
    /// 38 MB feed.
    pub retry_budget: Duration,
    pub backoff: Backoff,
}

impl DbSink {
    /// When the stations feed last landed (`None` if never), as the poll
    /// loop reads it: the poll waits out the rest of the interval after a
    /// restart (plan 4.6's `CursorSource`).
    pub(crate) fn cursor(&self) -> ingest::CursorSource<'_> {
        ingest::CursorSource::db(|| ds_store::freshness::last_stations_fetch(&self.pool))
    }

    /// Writes one whole parsed feed.
    pub(crate) async fn publish(&self, stations: &[StationRecord<'_>]) -> anyhow::Result<()> {
        let started = tokio::time::Instant::now();
        let mut attempt: u32 = 0;
        loop {
            match ds_store::reference::upsert_stations(&self.pool, stations).await {
                Ok(upserted) => {
                    tracing::info!(upserted, "wrote stations to the database");
                    return Ok(());
                }
                Err(err) => {
                    let delay = self.backoff.delay(attempt);
                    // A data error (SQLSTATE class 22 or 23) fails the same
                    // way again; anything else (a dropped connection, a pool
                    // or statement timeout, a deadlock) is retried.
                    let rejected = ds_store::backlog::classify_anyhow_data_error(&err).is_some();
                    if rejected || started.elapsed() + delay > self.retry_budget {
                        return Err(err.context("writing stations to the database"));
                    }
                    tracing::warn!(
                        error = ?err,
                        attempt = attempt + 1,
                        retry_in_secs = delay.as_secs(),
                        "writing stations failed; retrying without re-fetching the feed"
                    );
                    tokio::time::sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

impl<'a> StationRow for StationRecord<'a> {
    type Accessibility = BTreeMap<String, &'a RawValue>;

    fn crs(&self) -> &str {
        &self.crs
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn latitude(&self) -> Option<f64> {
        self.latitude
    }
    fn longitude(&self) -> Option<f64> {
        self.longitude
    }
    fn station_operator(&self) -> Option<&str> {
        self.station_operator.as_deref()
    }
    fn accessibility(&self) -> &Self::Accessibility {
        &self.accessibility
    }
}

/// Database-gated (`DATABASE_URL`, a migrated database; run with
/// `--ignored --test-threads=1`). The rows they write use CRS codes no
/// other test uses. They also run as the narrow `stations` role (CI's
/// per-service step, plan 2b.3), which has no `DELETE`: a test overwrites
/// its rows with `scramble` instead of deleting them first,
/// and its final `delete` is best effort.
#[cfg(test)]
mod db_tests {
    use common::ingest::LastFetched as _;

    use super::*;
    use crate::schema::parse_stations;

    async fn pool() -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to postgres")
    }

    fn db_sink(pool: PgPool) -> DbSink {
        DbSink {
            pool,
            retry_budget: Duration::ZERO,
            backoff: ingest::POST_RETRY_BACKOFF,
        }
    }

    type Row = (
        String,
        String,
        Option<f64>,
        Option<f64>,
        Option<String>,
        String,
    );

    async fn rows(pool: &PgPool, codes: &[&str]) -> Vec<Row> {
        sqlx::query_as(
            "SELECT crs::text, name, latitude, longitude, station_operator, accessibility::text \
               FROM stations WHERE crs = ANY($1) ORDER BY crs",
        )
        .bind(codes)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Sets every column a write sets to something no feed sends, so the
    /// next write has to rewrite each of them (the upsert skips a row that
    /// is already identical).
    async fn scramble(pool: &PgPool, codes: &[&str]) {
        sqlx::query(
            "UPDATE stations SET name = 'scrambled', latitude = -1, longitude = -1, \
                    station_operator = 'scrambled', accessibility = '{\"scrambled\": true}' \
              WHERE crs = ANY($1)",
        )
        .bind(codes)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Removes the rows if the role may (the superuser and the app role
    /// may; the narrow `stations` role may not, see the module docs).
    async fn delete(pool: &PgPool, codes: &[&str]) {
        if let Err(err) = sqlx::query("DELETE FROM stations WHERE crs = ANY($1)")
            .bind(codes)
            .execute(pool)
            .await
        {
            eprintln!("leaving the test stations in place: {err}");
        }
    }

    /// The live shape (see `schema.rs`): nested passthrough objects and
    /// arrays, a null location, a lower-case code the upsert normalises,
    /// numbers and booleans in the passthrough.
    const FEED: &str = r#"{"stations": [
        {"crsCode": "ZQS", "name": "Sink Test One",
         "location": {"latitude": 51.5, "longitude": -0.12},
         "stationOperator": {"operatorCode": "NR", "name": "Network Rail"},
         "slug": "sink-test-one",
         "stationFacilities": {"items": [{"id": 1, "available": true, "note": "a \"quoted\" note"}]},
         "minimumConnectionTime": 5},
        {"crsCode": "zqt", "name": "Sink Test Two", "location": null, "stationOperator": null,
         "changeHistory": {"changedBy": "AAP2", "lastChangedDate": "2026-06-23T21:37:34.000Z"}},
        {"crsCode": "ZQU", "name": "Sink Test Three"}
    ]}"#;
    const CODES: [&str; 3] = ["ZQS", "ZQT", "ZQU"];

    /// Plan 2b.1: the feed through the sink leaves the same rows as the
    /// retired api route did with the same body (decoded as
    /// `Vec<StationReference>` and passed to the same `upsert_stations`).
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_db_sink_writes_the_rows_the_api_route_did_for_a_fixture_feed() {
        let pool = pool().await;
        scramble(&pool, &CODES).await;
        let stations = parse_stations(FEED.as_bytes()).unwrap();

        let wire = serde_json::to_vec(&stations).unwrap();
        let body: Vec<common::StationReference> = serde_json::from_slice(&wire).unwrap();
        ds_store::reference::upsert_stations(&pool, &body)
            .await
            .unwrap();
        let via_http = rows(&pool, &CODES).await;
        assert_eq!(via_http.len(), 3, "{via_http:?}");
        assert!(via_http.iter().all(|row| row.1 != "scrambled"));

        scramble(&pool, &CODES).await;
        let db = db_sink(pool.clone());
        db.publish(&stations).await.unwrap();
        let via_db = rows(&pool, &CODES).await;
        assert_eq!(via_db, via_http);
        assert!(via_db[0].5.contains("sink-test-one"), "{:?}", via_db[0]);
        assert!(
            db.cursor().last_fetched().await.unwrap().is_some(),
            "the db sink records ingest_freshness('stations') and reads it back"
        );

        delete(&pool, &CODES).await;
    }

    /// `n` (at most 2,704) distinct CRS codes, `[JQXZ][A-Z][A-Z]`.
    fn codes(n: usize) -> Vec<String> {
        let letter = |i: usize| char::from(b'A' + u8::try_from(i % 26).unwrap());
        (0..n)
            .map(|i| {
                let first = ['J', 'Q', 'X', 'Z'][i / 676];
                format!("{first}{}{}", letter(i / 26), letter(i))
            })
            .collect()
    }

    /// The production feed's shape: 2,600 stations of about 14 KB of
    /// facilities JSON each (spec §4: 2,614 stations, 38.5 MB).
    fn production_sized_feed(codes: &[String]) -> Vec<u8> {
        let station = |crs: &String| {
            serde_json::json!({
                "crsCode": crs,
                "name": format!("Memory Test {crs}"),
                "location": { "latitude": 51.5, "longitude": -0.1 },
                "stationOperator": { "operatorCode": "NR" },
                "slug": format!("memory-test-{crs}"),
                "stationFacilities": {
                    "items": (0..350)
                        .map(|n| serde_json::json!({ "id": n, "available": n % 2 == 0, "note": "x" }))
                        .collect::<Vec<_>>()
                },
            })
        };
        serde_json::to_vec(&serde_json::json!({
            "stations": codes.iter().map(station).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    /// Plan 2b.1, the 2026-09-27 `OOMKilled` (see
    /// `schema::tests::parsing_and_serializing_a_large_feed_stays_near_the_body_size`):
    /// writing a production-sized feed through `DbSink` allocates about
    /// what the parsed vector itself does, far less than the feed. The
    /// write encodes `UPSERT_STATIONS_CHUNK` rows at a time straight from
    /// the borrowed `&RawValue`s (the retired HTTP path serialized the
    /// whole feed into one POST body). Measured (2026-10-07, debug build): a
    /// 36 MB feed, 1.9 MB to parse, 6.7 MB to write; 29 MB to write with
    /// 500 rows per statement.
    ///
    /// A current-thread runtime, so every allocation of the write (the
    /// bind encoding and the connection's buffers) is on this thread, where
    /// `alloc_meter` counts.
    #[test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    fn the_db_sink_writes_a_production_sized_feed_in_about_the_parsed_vector_s_memory() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = runtime.block_on(pool());
        // Open the connection before measuring.
        runtime
            .block_on(sqlx::query("SELECT 1").execute(&pool))
            .unwrap();
        let codes = codes(2_600);
        let code_refs: Vec<&str> = codes.iter().map(String::as_str).collect();
        let feed = production_sized_feed(&codes);
        assert!(feed.len() > 35_000_000, "feed is {} bytes", feed.len());

        let (stations, parsed) = crate::alloc_meter::peak_during(|| parse_stations(&feed).unwrap());
        assert_eq!(stations.len(), 2_600);
        let sink = db_sink(pool.clone());
        let ((), written) =
            crate::alloc_meter::peak_during(|| runtime.block_on(sink.publish(&stations)).unwrap());
        eprintln!(
            "feed {} bytes; parse peaked at {parsed} bytes; the db write at {written} bytes",
            feed.len()
        );

        assert_eq!(runtime.block_on(rows(&pool, &code_refs)).len(), 2_600);
        assert!(
            written < parsed + feed.len() / 4,
            "the db write peaked at {written} bytes above baseline: more than the parsed vector \
             ({parsed} bytes) plus a quarter of the {}-byte feed",
            feed.len()
        );
        runtime.block_on(delete(&pool, &code_refs));
    }
}
