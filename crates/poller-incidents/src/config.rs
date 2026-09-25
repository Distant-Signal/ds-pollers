use clap::Parser;

/// CLI/env configuration for the `poller-incidents` service.
///
/// `rdm_incidents_base_url` deliberately has no default: RSPS5050 P-03-00
/// Rev A §10 does not publish an endpoint path for this product (only the
/// legacy NRE display page and the XSD filename `nre-incident-v5-0.xsd` are
/// given), so a missing/misconfigured URL must fail loudly at startup
/// rather than silently poll the wrong thing.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `rdm_api_key` is a real
/// RDM Knowledgebase API key; a derived `Debug` would print it in full to
/// any future `tracing::debug!("{config:?}")`, matching the same class of
/// bug already fixed for `common::oauth_client::OAuthCredentials` and
/// `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
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
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// RSPS5050 P-03-00 Rev A §10: "Recommend every 5 minutes."
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,

    /// Port for this poller's Prometheus `/metrics` endpoint. Stays a
    /// plain field, not part of `MetricsArgs` -- its default differs per
    /// crate and `docker-compose.yml` relies on the code default.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rdm_incidents_base_url", &self.rdm_incidents_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("api_ingest_url", &self.api_ingest_url)
            .field("internal_oauth", &self.internal_oauth)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_rdm_api_key() {
        let config = Config::try_parse_from([
            "poller-incidents",
            "--rdm-incidents-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "super-secret-rdm-key",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "svc-password",
        ])
        .expect("required args should parse");

        let debug_output = format!("{config:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "rdm_api_key must be redacted: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-rdm-key"),
            "the real rdm_api_key must never appear in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("svc-password"),
            "the real internal_oauth password must never appear in Debug output: {debug_output}"
        );
    }
}
