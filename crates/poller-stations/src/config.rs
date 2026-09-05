use clap::Parser;

/// CLI/env configuration for the `poller-stations` service.
///
/// `rdm_stations_base_url` deliberately has no default: RSPS5050 P-03-00
/// Rev A §6 confirms the path suffix (`/stations`), but the host portion of
/// the URL is account-specific and not published in the spec, so a
/// missing/misconfigured URL must fail loudly at startup rather than
/// silently poll the wrong thing.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Stations feed base URL, e.g. `https://<host>/json/1.0`. The
    /// poller appends `/stations` itself (see `main.rs`).
    #[arg(long, env)]
    pub rdm_stations_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// The `api` crate's ingestion endpoint for stations.
    #[arg(long, env, default_value = "http://api:8080/private/stations")]
    pub api_ingest_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// RSPS5050 P-03-00 Rev A §6: "updated overnight; Poll frequency should
    /// only be once every 24 hours."
    #[arg(long, env, default_value_t = 86400)]
    pub poll_interval_secs: u64,

    /// Port for this poller's Prometheus `/metrics` endpoint. Stays a
    /// plain field, not part of `MetricsArgs` -- its default differs per
    /// crate and `docker-compose.yml` relies on the code default.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}
