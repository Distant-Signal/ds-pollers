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

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 8 real callers) -- see
    /// docs/superpowers/specs/2026-09-02-internal-service-oauth2-design.md
    /// Decision 6.
    #[arg(long, env)]
    pub internal_oauth_token_url: String,
    #[arg(long, env)]
    pub internal_oauth_client_id: String,
    #[arg(long, env, default_value = "groups")]
    pub internal_oauth_scope: String,
    /// This service's own Authentik service-account credential --
    /// per-service, distinct from every other caller's. `username` is
    /// identifying, not itself the secret; `password` (an Authentik
    /// app-password) is the actual secret.
    #[arg(long, env)]
    pub internal_oauth_username: String,
    #[arg(long, env)]
    pub internal_oauth_password: String,

    /// RSPS5050 P-03-00 Rev A §10: "Recommend every 5 minutes."
    #[arg(long, env, default_value_t = 300)]
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
