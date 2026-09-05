use clap::Parser;

/// CLI/env configuration for the `poller-tfl` service.
///
/// Unlike the four RDM pollers, `tfl_base_url` HAS a default: TfL's Unified
/// API is a published, stable, documented public endpoint, so there is no
/// "no confirmed endpoint path" gap to fail loudly over. The subscription
/// key still has none — an unset key must stop the process at startup
/// rather than have it poll anonymously and get rate-limited later.
#[derive(Debug, Parser)]
pub struct Config {
    /// TfL Unified API root, without a trailing path. The binary appends
    /// `/Line/Mode/{modes}/Status` itself.
    #[arg(long, env, default_value = "https://api.tfl.gov.uk")]
    pub tfl_base_url: String,

    /// TfL subscription key from api-portal.tfl.gov.uk, sent as the
    /// `Ocp-Apim-Subscription-Key` header (see `main.rs`).
    #[arg(long, env)]
    pub tfl_app_key: String,

    /// Comma-separated TfL modes to poll, passed straight through to TfL's
    /// own comma-separated `{modes}` path segment.
    ///
    /// `bus`, `river-bus`, `cable-car` and friends are deliberately absent
    /// — v1's scope is rail-like TfL modes. `national-rail` is absent for a
    /// different reason: this app already has four National Rail pollers
    /// and an aggregator producing far better status for it than TfL's
    /// summary view.
    #[arg(long, env, default_value = "tube,dlr,overground,elizabeth-line,tram")]
    pub tfl_modes: String,

    /// The `api` crate's ingestion endpoint for TfL line status.
    #[arg(long, env, default_value = "http://api:8080/private/tfl-line-status")]
    pub api_ingest_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// TfL publishes no recommended interval for the line-status endpoint,
    /// and offers no push, no webhook and no confirmed conditional-request
    /// support — polling is the only option. 300s mirrors
    /// `poller-incidents`, whose feed has a comparable update rhythm.
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,

    /// Enables the DLR arrivals-diffing pilot (see
    /// `docs/superpowers/plans/2026-08-22-dlr-arrivals-diffing-pilot.md`).
    ///
    /// Defaults **off**: the pilot was built without a real deployment to
    /// run it against (no Docker or Postgres in the sandbox), so it has
    /// never completed a clean cycle against the live API. It stays off
    /// until the plan's Task 8 manual verification checklist has been run
    /// clean once against a real deployment; after that it can be turned
    /// on via this flag or `DLR_PILOT_ENABLED` with no redeploy. Off means
    /// DLR reports `sample_stats: None`, same as every other TfL line.
    #[arg(long, env, default_value_t = false)]
    pub dlr_pilot_enabled: bool,

    /// Poplar's Naptan id, used as the `stopPointId` for the DLR
    /// Timetable poll. Not derived — this pilot covers one fixed station
    /// only (see the plan's Global Constraints).
    #[arg(long, env, default_value = "940GZZDLPOP")]
    pub dlr_pilot_stop_point_id: String,

    /// Port for this poller's Prometheus `/metrics` endpoint. Stays a
    /// plain field, not part of `MetricsArgs` -- its default differs per
    /// crate and `docker-compose.yml` relies on the code default.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}
