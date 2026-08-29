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

    /// Shared secret sent via `X-Internal-Token` to reach the ingestion
    /// endpoint (see `crates/api/src/auth.rs`).
    #[arg(long, env)]
    pub internal_token: String,

    /// RSPS5050 P-03-00 Rev A §3: "At least once every 24 hours."
    #[arg(long, env, default_value_t = 86400)]
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
