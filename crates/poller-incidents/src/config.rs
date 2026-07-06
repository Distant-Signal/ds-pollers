use clap::Parser;

/// CLI/env configuration for the `poller-incidents` service.
///
/// `rdm_incidents_base_url` deliberately has no default: RSPS5050 P-03-00
/// Rev A §10 does not publish an endpoint path for this product (only the
/// legacy NRE display page and the XSD filename `nre-incident-v5-0.xsd` are
/// given), so a missing/misconfigured URL must fail loudly at startup
/// rather than silently poll the wrong thing.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Knowledgebase Incidents feed base URL. GAP: no endpoint path is
    /// published in the current spec for this product — this must be
    /// supplied out of band once known.
    #[arg(long, env)]
    pub rdm_incidents_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// The `api` crate's ingestion endpoint for incidents.
    #[arg(long, env, default_value = "http://api:8080/private/incidents")]
    pub api_ingest_url: String,

    /// Shared secret sent via `X-Internal-Token` to reach the ingestion
    /// endpoint (see `crates/api/src/auth.rs`).
    #[arg(long, env)]
    pub internal_token: String,

    /// RSPS5050 P-03-00 Rev A §10: "Recommend every 5 minutes."
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,
}
