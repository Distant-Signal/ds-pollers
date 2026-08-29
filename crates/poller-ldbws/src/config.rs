use clap::Parser;

/// CLI/env configuration for the `poller-ldbws` service.
///
/// `ldbws_base_url` deliberately has no default: research found two
/// different RDM product-slug segments in use across sources
/// (`1010-live-departure-board-dep` vs `...-dep1_2`) with no way to
/// reconcile which is currently correct without a live RDM subscription —
/// this must be supplied out of band once confirmed, not guessed.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Live Departure Board base URL, up to and including the
    /// `/LDBWS/api/20220120` segment. The poller appends
    /// `/GetDepBoardWithDetails/{crs}` itself (see `main.rs`).
    #[arg(long, env)]
    pub ldbws_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`). Community sources describe
    /// this as the "consumer key" specifically (as opposed to a paired
    /// "consumer secret") — unconfirmed against RDM's own docs, but
    /// consistent with how the other three pollers authenticate.
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// Number of services requested per station per cycle (LDBWS's own
    /// `numRows` query parameter). Kept at the upstream API's own default
    /// (10) rather than inventing a "better" number without evidence of
    /// what the aggregator's inference logic actually needs.
    #[arg(long, env, default_value_t = 10)]
    pub num_rows: u32,

    /// The `api` crate's endpoint for the deduplicated list of stations to
    /// sample (`GET /private/sample-stations`) — not an RDM endpoint.
    #[arg(long, env, default_value = "http://api:8080/private/sample-stations")]
    pub api_sample_stations_url: String,

    /// The `api` crate's ingestion endpoint for station samples.
    #[arg(long, env, default_value = "http://api:8080/private/station-samples")]
    pub api_ingest_url: String,

    /// Shared secret sent via `X-Internal-Token` to reach both `api`
    /// endpoints above (see `crates/api/src/auth.rs`).
    #[arg(long, env)]
    pub internal_token: String,

    /// DESIGN.md §4's aggregator polling cadence target is "30-60s"; 60 is
    /// the conservative end, given this feed's real rate limit is
    /// unconfirmed (see module docs in `main.rs`).
    #[arg(long, env, default_value_t = 60)]
    pub poll_interval_secs: u64,

    /// Port for this poller's Prometheus `/metrics` endpoint. See
    /// docs/superpowers/plans/2026-08-29-metrics.md's Global Constraints
    /// for why this differs from api.service.port -- api reuses its
    /// existing HTTP listener, this poller has none, so it needs a new one.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    /// Whether to start this service's Prometheus `/metrics` listener at
    /// all. Distinct from `metrics_port` (which port to use IF started) --
    /// this is what actually satisfies "metrics.enabled=false leaves the
    /// service working exactly as it does today" (see the Helm chart's
    /// `metrics.enabled` value and this branch's final whole-branch
    /// review, Important finding #2): omitting the containerPort/env/
    /// annotations in the chart alone does not stop the process from
    /// listening, since Kubernetes container ports are purely
    /// declarative.
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
}
