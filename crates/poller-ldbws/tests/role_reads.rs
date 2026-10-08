//! poller-ldbws's direct sample-station read (ingest plan 4.5) as whichever
//! role `DATABASE_URL` is: CI runs it as the superuser and, in the
//! per-service step, as the narrow read-only `ldbws_ro` role itself (plan
//! 4.7), whose only grants are SELECT on the views
//! `ingest_sample_station_pins` and `ingest_custom_line_stations`. Proves
//! the schema gate and the `SAMPLE_STATIONS_SOURCE=db` queries against
//! exactly those grants. Read-only: no fixtures.

use std::time::Duration;

use ds_store::reads::sample_stations::SampleSelection;
use ds_store::schema::{DbRole, SchemaGate, wait_for_schema_with};

async fn pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    sqlx::PgPool::connect(&url).await.expect("connect")
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn the_schema_gate_passes_for_the_ldbws_ro_role() {
    let gate = SchemaGate {
        deadline: Duration::ZERO,
        ..SchemaGate::for_role(DbRole::LdbwsRo)
    };
    wait_for_schema_with(&pool().await, &gate, None)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn the_sample_station_reads_run_as_the_role() {
    let pool = pool().await;
    // Restricted, so the pin counts are read as well as the custom lines.
    let selection = SampleSelection {
        pinned_lines_only: true,
        max_stations: Some(10),
    };
    ds_store::reads::select_sample_stations_from(&pool, &[], selection)
        .await
        .expect("SELECT on the two views");
}
