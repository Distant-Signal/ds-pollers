use clap::Parser;

/// CLI/env configuration for the `poller-tocs` service.
///
/// `rdm_tocs_base_url` deliberately has no default: RSPS5050 P-03-00 Rev A
/// §3 does not publish an endpoint path for this product in the current
/// spec edition (even the legacy internal-only URL from the 2017 edition
/// was removed), so a missing/misconfigured URL must fail loudly at
/// startup rather than silently poll the wrong thing.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `rdm_api_key` is a real
/// RDM TOC List API key; a derived `Debug` would print it in full to any
/// future `tracing::debug!("{config:?}")`, matching the same class of bug
/// already fixed for `common::oauth_client::OAuthCredentials` and
/// `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
pub(crate) struct Config {
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

    /// Where each snapshot goes (ingest plan 3c.2, decision D8): `http`
    /// (the default: `POST` to `API_INGEST_URL`), `http+shadow` (that, plus
    /// a copy to `ds:ingest:reference`) or `stream` (the stream only; the
    /// ingest-writer applies it). See `ingest_stream::snapshot`.
    #[arg(long, env = "INGEST_SINK", default_value = "http")]
    pub ingest_sink: ingest_stream::snapshot::SinkMode,

    /// Redis for the stream sinks.
    #[command(flatten)]
    pub redis: ingest_stream::snapshot::RedisArgs,

    /// Shared, non-secret `OAuth2` client-credentials config (same value
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

    /// `/livez` listener and stall window (SVC-08).
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rdm_tocs_base_url", &self.rdm_tocs_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("api_ingest_url", &self.api_ingest_url)
            .field("ingest_sink", &self.ingest_sink)
            .field("redis", &self.redis)
            .field("internal_oauth", &self.internal_oauth)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .field("health", &self.health)
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
            "poller-tocs",
            "--rdm-tocs-base-url",
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

    /// Plan 3c.2: `http` by default; the stream sinks parse.
    #[test]
    fn ingest_sink_defaults_to_http() {
        use ingest_stream::snapshot::SinkMode;

        let args = [
            "poller-tocs",
            "--rdm-tocs-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "k",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "svc-password",
        ];
        let config = Config::try_parse_from(args).unwrap();
        assert_eq!(config.ingest_sink, SinkMode::Http);
        let config =
            Config::try_parse_from(args.iter().chain(&["--ingest-sink", "http+shadow"])).unwrap();
        assert_eq!(config.ingest_sink, SinkMode::HttpShadow);
    }
}
