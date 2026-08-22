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

    /// Shared secret sent via `X-Internal-Token` to reach the ingestion
    /// endpoint (see `crates/api/src/auth.rs`).
    #[arg(long, env)]
    pub internal_token: String,

    /// TfL publishes no recommended interval for the line-status endpoint,
    /// and offers no push, no webhook and no confirmed conditional-request
    /// support — polling is the only option. 300s mirrors
    /// `poller-incidents`, whose feed has a comparable update rhythm.
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,
}
