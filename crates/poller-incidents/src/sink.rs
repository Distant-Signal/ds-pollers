//! Where a parsed snapshot goes (ingest architecture spec §9.1, plan 2c.2):
//! [`IncidentSink`], with [`HttpSink`] (today's POST to the api, unchanged)
//! and [`DbSink`] (Postgres through `ds_store::incidents`, then the
//! `incident-text-changed` XADD from here). `INGEST_SINK` picks one at
//! startup.
//!
//! Both write a snapshot the same way, in the same order (spec §9.4):
//! `apply_snapshot` (committed chunks), the XADD of the text changes (best
//! effort), then the removal inference. Both retry a transient failure of
//! the whole snapshot within `common::poller_loop::post_retry_budget`, and
//! return a data rejection at once.

use std::time::Duration;

use common::IncidentSnapshot;
use common::ingest::{self, DataRejected};
use common::matcher::LineMatcher;
use ds_store::incidents::{self, IncidentSnapshotOutcome, RowHeartbeat};
use sqlx::PgPool;

/// One error vocabulary for both sinks (spec §9.1), so the retry decision
/// is the same whichever is active. (`Busy`, the schedule publish's
/// advisory-lock 409, has no incidents counterpart.)
#[derive(Debug)]
pub(crate) enum SinkError {
    /// A statement timeout (SQLSTATE 57014): retried.
    Timeout(anyhow::Error),
    /// The data itself was refused (an HTTP 400/413/422, or a class 22 or
    /// 23 SQLSTATE): the same snapshot fails the same way, so it is not
    /// retried; the cycle waits for the next poll.
    Rejected(anyhow::Error),
    /// Anything else (unreachable, a 5xx, a dropped connection): retried.
    Transient(anyhow::Error),
}

impl SinkError {
    /// The DB sink's classification of an error from `ds_store`.
    pub(crate) fn from_db(err: anyhow::Error) -> Self {
        let code = err
            .chain()
            .find_map(|cause| match cause.downcast_ref::<sqlx::Error>() {
                Some(sqlx::Error::Database(db)) => db.code().map(|code| code.into_owned()),
                _ => None,
            });
        match code.as_deref() {
            Some("57014") => Self::Timeout(err),
            Some(code) if code.starts_with("22") || code.starts_with("23") => {
                let message = err.to_string();
                Self::Rejected(err.context(DataRejected(message)))
            }
            _ => Self::Transient(err),
        }
    }

    /// The HTTP sink's: `common::ingest::classify_failure`, as today.
    pub(crate) fn from_http(err: anyhow::Error) -> Self {
        match ingest::classify_failure(&err) {
            ingest::FailureClass::Rejected => Self::Rejected(err),
            ingest::FailureClass::Transient => Self::Transient(err),
        }
    }

    /// The error for the poll loop, whose own `classify_failure` still sees
    /// a rejection as one (the HTTP status, or [`DataRejected`]).
    pub(crate) fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Timeout(err) | Self::Rejected(err) | Self::Transient(err) => err,
        }
    }

    fn is_retryable(&self) -> bool {
        !matches!(self, Self::Rejected(_))
    }

    fn inner(&self) -> &anyhow::Error {
        match self {
            Self::Timeout(err) | Self::Rejected(err) | Self::Transient(err) => err,
        }
    }
}

/// Where one parsed snapshot is written.
pub(crate) trait IncidentSink {
    async fn publish(&self, snapshot: &IncidentSnapshot) -> Result<(), SinkError>;
}

/// Today's path: `POST /private/incidents`, retried within `budget` by
/// `common::ingest::post_counted_retrying` exactly as before.
pub(crate) struct HttpSink<'a> {
    pub client: &'a reqwest::Client,
    pub url: &'a str,
    pub internal_oauth: &'a common::oauth_client::OAuthTokenCache,
    pub budget: Duration,
}

impl IncidentSink for HttpSink<'_> {
    async fn publish(&self, snapshot: &IncidentSnapshot) -> Result<(), SinkError> {
        // An api older than this body rejects it with a 4xx, which fails
        // just this cycle -- deploy the api first.
        ingest::post_counted_retrying(
            self.client,
            self.url,
            self.internal_oauth,
            snapshot,
            snapshot.incidents.len(),
            "incidents",
            self.budget,
        )
        .await
        .map_err(SinkError::from_http)
    }
}

/// Publishes the text-changed ids. A trait so the DB sink's tests can see
/// when it runs; production uses [`RedisPublisher`].
pub(crate) trait TextChangedPublisher {
    /// Best effort: logs and returns on any failure, never fails the write.
    async fn publish(&self, incident_ids: Vec<String>);
}

/// `common::incident_text_changed::publish` as the `poller-incidents` Redis
/// user: one bounded connect per snapshot that has text changes, so a Redis
/// outage costs one `CONNECT_TIMEOUT` and the snapshot is still written.
pub(crate) struct RedisPublisher(pub redis::Client);

impl TextChangedPublisher for RedisPublisher {
    async fn publish(&self, incident_ids: Vec<String>) {
        common::incident_text_changed::publish(&self.0, incident_ids).await;
    }
}

/// Straight into Postgres (plan 2c.2), as the api's handler does.
pub(crate) struct DbSink<P> {
    pub pool: PgPool,
    pub matcher: LineMatcher,
    pub heartbeat: RowHeartbeat,
    pub publisher: P,
    pub budget: Duration,
}

impl<P: TextChangedPublisher> DbSink<P> {
    /// One attempt: apply, publish, infer (`ds_store::incidents`' module
    /// docs). A retry after a failed inference publishes nothing twice:
    /// the first attempt's text changes are already stored.
    pub(crate) async fn write_once(
        &self,
        snapshot: &IncidentSnapshot,
    ) -> Result<IncidentSnapshotOutcome, SinkError> {
        if snapshot.skipped > 0 {
            tracing::warn!(
                skipped = snapshot.skipped,
                "skipped malformed <PtIncident> elements this poll"
            );
        }
        let applied = incidents::apply_snapshot(
            &self.pool,
            &self.matcher,
            &snapshot.incidents,
            self.heartbeat,
        )
        .await
        .map_err(SinkError::from_db)?;
        // After every chunk has committed, before the inference (whose
        // failure must not drop these).
        if !applied.text_changed_ids.is_empty() {
            self.publisher.publish(applied.text_changed_ids).await;
        }
        let present_ids: Vec<&str> = snapshot
            .incidents
            .iter()
            .map(|incident| incident.incident_id.as_str())
            .collect();
        let inference = incidents::removal::infer_removals(
            &self.pool,
            &present_ids,
            snapshot.complete,
            self.heartbeat,
        )
        .await
        .map_err(SinkError::from_db)?;
        Ok(IncidentSnapshotOutcome {
            upserted: applied.upserted,
            inference,
        })
    }
}

impl<P: TextChangedPublisher> IncidentSink for DbSink<P> {
    /// [`DbSink::write_once`], retrying the whole snapshot on a retryable
    /// failure with `common::ingest::POST_RETRY_BACKOFF` for up to
    /// `budget`, like the HTTP sink's POST. Safe (spec §9.4): the chunk
    /// upserts are idempotent, inference's guard skips a retry within 120 s
    /// of a committed baseline, and "listed resets the counters" is
    /// idempotent.
    async fn publish(&self, snapshot: &IncidentSnapshot) -> Result<(), SinkError> {
        let started = tokio::time::Instant::now();
        let mut failures: u32 = 0;
        loop {
            let err = match self.write_once(snapshot).await {
                Ok(outcome) => {
                    tracing::info!(
                        upserted = outcome.upserted,
                        inference = outcome.inference.label(),
                        "wrote incidents snapshot to Postgres"
                    );
                    return Ok(());
                }
                Err(err) => err,
            };
            if !err.is_retryable() {
                return Err(err);
            }
            let delay = ingest::POST_RETRY_BACKOFF.delay(failures);
            if started.elapsed() + delay > self.budget {
                return Err(err);
            }
            tracing::warn!(
                error = ?err.inner(),
                attempt = failures + 1,
                retry_in_secs = delay.as_secs(),
                "writing the incidents snapshot failed; retrying without re-fetching upstream"
            );
            tokio::time::sleep(delay).await;
            failures = failures.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_db_error_without_a_sqlstate_is_transient() {
        let err = SinkError::from_db(anyhow::Error::from(sqlx::Error::PoolTimedOut));
        assert!(matches!(err, SinkError::Transient(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn an_http_422_is_rejected_and_keeps_its_status_for_the_poll_loop() {
        let err = SinkError::from_http(
            ingest::HttpStatusError {
                prefix: "ingestion POST failed",
                status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                body: String::new(),
                retry_after: None,
            }
            .into(),
        );
        assert!(matches!(err, SinkError::Rejected(_)));
        assert_eq!(
            ingest::classify_failure(&err.into_anyhow()),
            ingest::FailureClass::Rejected
        );
    }
}

/// Against a real database (and, where noted, local Redis/valkey): the DB
/// sink against the api's write path, the publish order, and a Redis
/// outage. Resets `incident_feed_state`, so needs `--test-threads=1`.
///
/// They also run as the narrow `incidents` role (CI's per-service step),
/// which has no `DELETE`: the sinks write through `DATABASE_URL`, but
/// [`reset`] deletes through `MIGRATION_DATABASE_URL` (the schema owner
/// `test-postgres-roles.py` sets) when it is set, else `DATABASE_URL`.
#[cfg(test)]
mod db_tests {
    use std::sync::Mutex;

    use common::IncidentMessage;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const PREFIX: &str = "TEST-POLLER-SINK-";

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    /// A connection that may `DELETE` the test rows: the schema owner
    /// under `test-postgres-roles.py`, else `DATABASE_URL` itself (a
    /// superuser or the app role).
    async fn cleanup_pool() -> PgPool {
        let database_url = std::env::var("MIGRATION_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect to postgres for cleanup")
    }

    /// Deletes this module's rows and the feed state, through
    /// [`cleanup_pool`]: `_pool` (the sink's own role) may lack `DELETE`.
    async fn reset(_pool: &PgPool) {
        let pool = cleanup_pool().await;
        for sql in [
            "DELETE FROM incident_history WHERE incident_id LIKE 'TEST-POLLER-SINK-%'",
            "DELETE FROM incidents WHERE incident_id LIKE 'TEST-POLLER-SINK-%'",
            "DELETE FROM incident_feed_state",
        ] {
            sqlx::query(sql).execute(&pool).await.expect(sql);
        }
        pool.close().await;
    }

    fn incident(suffix: &str, summary: &str) -> IncidentMessage {
        IncidentMessage {
            incident_id: format!("{PREFIX}{suffix}"),
            summary: summary.to_string(),
            description: format!("Disruption between {suffix}ton and Elsewhere"),
            operators: vec!["ZZ".to_string()],
            affected_stations: vec![],
            priority: 2,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    /// What the publisher saw when it ran.
    #[derive(Debug, PartialEq, Eq)]
    struct AtPublish {
        ids: Vec<String>,
        committed_rows: i64,
        feed_state_rows: i64,
    }

    /// Records each publish with what another connection saw at that point.
    struct Recorder {
        pool: PgPool,
        seen: Mutex<Vec<AtPublish>>,
    }

    impl TextChangedPublisher for Recorder {
        async fn publish(&self, ids: Vec<String>) {
            let committed_rows: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM incidents WHERE incident_id LIKE 'TEST-POLLER-SINK-%'",
            )
            .fetch_one(&self.pool)
            .await
            .expect("count committed rows");
            let feed_state_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM incident_feed_state")
                    .fetch_one(&self.pool)
                    .await
                    .expect("count feed state");
            self.seen.lock().expect("not poisoned").push(AtPublish {
                ids,
                committed_rows,
                feed_state_rows,
            });
        }
    }

    fn sink<P>(pool: &PgPool, publisher: P) -> DbSink<P> {
        DbSink {
            pool: pool.clone(),
            matcher: LineMatcher::new(&[]),
            heartbeat: RowHeartbeat::On,
            publisher,
            budget: Duration::ZERO,
        }
    }

    fn complete(incidents: Vec<IncidentMessage>) -> IncidentSnapshot {
        IncidentSnapshot {
            incidents,
            complete: true,
            skipped: 0,
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p \
                poller-incidents sink::db_tests -- --ignored --test-threads=1`"]
    async fn the_text_changes_are_published_after_commit_and_before_inference() {
        let pool = test_pool().await;
        reset(&pool).await;
        let sink = sink(
            &pool,
            Recorder {
                pool: pool.clone(),
                seen: Mutex::new(Vec::new()),
            },
        );
        let batch: Vec<IncidentMessage> = (0..=incidents::UPSERT_CHUNK_SIZE)
            .map(|n| incident(&format!("{n:03}"), "Signal failure"))
            .collect();
        sink.publish(&complete(batch.clone()))
            .await
            .expect("written");
        // Unchanged: no second publish.
        sink.publish(&complete(batch.clone()))
            .await
            .expect("written");
        let seen = sink.publisher.seen.into_inner().expect("not poisoned");
        assert_eq!(
            seen,
            vec![AtPublish {
                ids: batch.iter().map(|i| i.incident_id.clone()).collect(),
                committed_rows: i64::try_from(batch.len()).expect("small"),
                feed_state_rows: 0,
            }],
            "once, after every chunk committed, before the inference made its baseline"
        );
        reset(&pool).await;
    }

    /// `(incidents, incident_history, affected_lines)` for this module's
    /// rows, without the times: what both sinks must agree on.
    async fn table_contents(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT format('%s|%s|%s|%s|%s|%s|%s|%s', i.incident_id, i.summary, i.is_cleared, \
                    i.affected_lines, i.source_missing_polls, i.source_removed_at IS NOT NULL, \
                    (SELECT count(*) FROM incident_history h WHERE h.incident_id = i.incident_id), \
                    i.active_since IS NOT NULL) \
               FROM incidents i WHERE i.incident_id LIKE 'TEST-POLLER-SINK-%' \
              ORDER BY i.incident_id",
        )
        .fetch_all(pool)
        .await
        .expect("table contents")
    }

    /// The repository's line catalogue, as the image carries it.
    fn catalogue() -> Vec<common::LineDefinition> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        common::config::parse_lines(dir.to_str().expect("utf-8 path"))
            .expect("parse lines/")
            .0
    }

    /// What the api receives from [`HttpSink`]: the snapshot POSTed to a
    /// stand-in api, read back with the api's own body parser
    /// (`ds_store::incidents::parse_snapshot`, its handler's first step).
    async fn through_http(snapshot: &IncidentSnapshot) -> IncidentSnapshot {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/incidents"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"upserted": 0})),
            )
            .mount(&server)
            .await;
        let tokens =
            common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
                token_url: format!("{}/token/", server.uri()),
                client_id: "test".to_string(),
                scope: "groups".to_string(),
                username: "test".to_string(),
                password: "test".to_string(),
            });
        let client = reqwest::Client::new();
        let url = format!("{}/private/incidents", server.uri());
        HttpSink {
            client: &client,
            url: &url,
            internal_oauth: &tokens,
            budget: Duration::ZERO,
        }
        .publish(snapshot)
        .await
        .expect("posted");
        let requests = server.received_requests().await.expect("recorded");
        let body = requests
            .iter()
            .find(|request| request.url.path() == "/private/incidents")
            .expect("the snapshot was POSTed")
            .body
            .clone();
        incidents::parse_snapshot(serde_json::from_slice(&body).expect("JSON"))
            .expect("the api parses it")
    }

    /// Both sinks give the same `incidents`, `incident_history` and
    /// `affected_lines` and the same inference outcomes for a fixture
    /// sequence (plan 2c.2). The HTTP side is the snapshot as the api
    /// receives it, written the way its handler writes it
    /// (`queries::upsert_incident_snapshot`: apply, publish, infer); the
    /// api crate itself is not a dependency of this one.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p \
                poller-incidents sink::db_tests -- --ignored --test-threads=1`"]
    async fn the_db_sink_writes_what_the_api_writes() {
        let pool = test_pool().await;
        let lines = catalogue();
        let operator = lines[0].operators[0].clone();
        let on = |mut message: IncidentMessage| {
            message.operators = vec![operator.clone()];
            message
        };
        let mut cleared_b = on(incident("B", "Points failure"));
        cleared_b.is_cleared = true;
        let sequence = [
            vec![
                on(incident("A", "Signal failure")),
                on(incident("B", "Points failure")),
                on(incident("C", "Flooding")),
            ],
            vec![
                on(incident("A", "Signal failure (updated)")),
                cleared_b.clone(),
                on(incident("C", "Flooding")),
            ],
            vec![
                on(incident("A", "Signal failure (updated)")),
                cleared_b.clone(),
            ],
            vec![on(incident("A", "Signal failure (updated)")), cleared_b],
        ];

        let mut results = Vec::new();
        for via_http in [true, false] {
            reset(&pool).await;
            let matcher = LineMatcher::new(&lines);
            let sink = DbSink {
                pool: pool.clone(),
                matcher: LineMatcher::new(&lines),
                heartbeat: RowHeartbeat::On,
                publisher: RedisPublisher(redis::Client::open("redis://127.0.0.1:1").expect("url")),
                budget: Duration::ZERO,
            };
            let mut outcomes = Vec::new();
            let mut contents = Vec::new();
            for batch in &sequence {
                sqlx::query(
                    "UPDATE incident_feed_state SET last_complete_at = now() - interval '10 minutes'",
                )
                .execute(&pool)
                .await
                .expect("backdate the baseline");
                let snapshot = complete(batch.clone());
                let inference = if via_http {
                    let received = through_http(&snapshot).await;
                    let applied = incidents::apply_snapshot(
                        &pool,
                        &matcher,
                        &received.incidents,
                        RowHeartbeat::On,
                    )
                    .await
                    .expect("apply");
                    assert!(applied.upserted > 0);
                    let ids: Vec<&str> = received
                        .incidents
                        .iter()
                        .map(|i| i.incident_id.as_str())
                        .collect();
                    incidents::removal::infer_removals(
                        &pool,
                        &ids,
                        received.complete,
                        RowHeartbeat::On,
                    )
                    .await
                    .expect("infer")
                } else {
                    sink.write_once(&snapshot).await.expect("db sink").inference
                };
                outcomes.push(inference);
                contents.push(table_contents(&pool).await);
            }
            results.push((outcomes, contents));
        }
        assert_eq!(results[1], results[0], "db sink == http sink");
        let first = &results[0].1[0];
        assert!(
            first.iter().all(|row| !row.contains("|{}|")),
            "the matcher filled affected_lines: {first:?}"
        );
        assert_eq!(
            results[0].0[2..],
            [
                incidents::removal::Inference::Applied {
                    missing: 1,
                    removed: 0
                },
                incidents::removal::Inference::Applied {
                    missing: 1,
                    removed: 1
                },
            ],
            "C missed once, then ended; the cleared B is never counted"
        );
        reset(&pool).await;
    }

    /// A Redis outage: the snapshot is still written (best-effort XADD).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p \
                poller-incidents sink::db_tests -- --ignored --test-threads=1`"]
    async fn a_redis_outage_still_ingests() {
        let pool = test_pool().await;
        reset(&pool).await;
        // Nothing listens on port 1: the connect is refused at once.
        let sink = sink(
            &pool,
            RedisPublisher(redis::Client::open("redis://127.0.0.1:1").expect("url")),
        );
        sink.publish(&complete(vec![incident("R", "Signal failure")]))
            .await
            .expect("written despite Redis");
        assert_eq!(table_contents(&pool).await.len(), 1);
        reset(&pool).await;
    }

    /// With local Redis/valkey (`REDIS_URL`): the XADD lands on the real
    /// stream, capped, after the rows are committed.
    #[tokio::test]
    #[ignore = "requires a live database and Redis; run with `DATABASE_URL=... \
                REDIS_URL=redis://127.0.0.1:6379 cargo test -p poller-incidents \
                sink::db_tests -- --ignored --test-threads=1`"]
    async fn the_text_change_reaches_the_stream() {
        let Ok(redis_url) = std::env::var("REDIS_URL") else {
            eprintln!("REDIS_URL unset; skipping");
            return;
        };
        let pool = test_pool().await;
        reset(&pool).await;
        let client = redis::Client::open(redis_url).expect("redis url");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("connect to redis");
        let before: redis::Value = redis::cmd("XREVRANGE")
            .arg(common::incident_text_changed::STREAM)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await
            .expect("xrevrange");
        let sink = sink(&pool, RedisPublisher(client));
        let id = format!("{PREFIX}STREAM");
        sink.publish(&complete(vec![incident("STREAM", "Signal failure")]))
            .await
            .expect("written");
        let after: Vec<(String, Vec<(String, String)>)> = redis::cmd("XREVRANGE")
            .arg(common::incident_text_changed::STREAM)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await
            .expect("xrevrange");
        assert_ne!(format!("{before:?}"), format!("{after:?}"), "a new entry");
        assert_eq!(after[0].1, vec![("incident_id".to_string(), id)]);
        reset(&pool).await;
    }
}
