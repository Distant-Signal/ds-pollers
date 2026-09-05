use clap::Parser;

/// CLI/env configuration for the `poller-tocs` service.
///
/// `rdm_tocs_base_url` deliberately has no default: RSPS5050 P-03-00 Rev A
/// §3 does not publish an endpoint path for this product in the current
/// spec edition (even the legacy internal-only URL from the 2017 edition
/// was removed), so a missing/misconfigured URL must fail loudly at
/// startup rather than silently poll the wrong thing.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Train Operating Company List feed base URL. GAP: no endpoint
    /// path is published in the current spec for this product — this must
    /// be supplied out of band once known.
    #[arg(long, env)]
    pub rdm_tocs_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// The `api` crate's ingestion endpoint for TOC references.
    #[arg(long, env, default_value = "http://api:8080/private/tocs")]
    pub api_ingest_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// RSPS5050 P-03-00 Rev A §3: "At least once every 24 hours."
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
