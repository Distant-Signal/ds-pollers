use clap::Parser;
use common::secret::Secret;

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
pub(crate) struct Config {
    /// RDM Knowledgebase Incidents feed base URL. GAP: no endpoint path is
    /// published in the current spec for this product — this must be
    /// supplied out of band once known.
    #[arg(long, env)]
    pub rdm_incidents_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

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

    /// `/livez` listener and stall window (SVC-08).
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,

    /// The `distant_signal_incidents` role's connection (required).
    /// Pool size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 2.
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<Secret>,

    /// Directory of line-catalogue TOML files: the
    /// matcher that fills `incidents.affected_lines` is built from it at
    /// startup, as the api's is. The image carries the catalogue here.
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines")]
    pub lines_dir: String,

    /// Redis, for the `incident-text-changed` XADD (required). May
    /// carry a password (`redis://:pw@host`).
    #[arg(long, env, hide_env_values = true)]
    pub redis_url: Option<Secret>,

    /// Redis AUTH password (chart `redis.auth`, or the `poller-incidents`
    /// ACL user's own). Applied by
    /// `common::redis_auth::redis_url_with_credentials`, never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<Secret>,

    /// Redis ACL user (chart `redis.acl.clients.pollerIncidents`: user
    /// `poller-incidents`). Unset or empty: the `default` user.
    #[arg(long, env)]
    pub redis_username: Option<String>,

    /// The row heartbeat (plan 2c.6,
    /// `ds_store::incidents::RowHeartbeat`). **On by default** (today's
    /// behaviour). The chart sets it, and the api's, from one value.
    #[arg(long, env = "INCIDENTS_ROW_HEARTBEAT", default_value_t = true, action = clap::ArgAction::Set)]
    pub incidents_row_heartbeat: bool,
}

impl Config {
    /// The sink's required settings, checked once at startup.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.database_url.is_none() {
            anyhow::bail!("poller-incidents needs DATABASE_URL");
        }
        if self.redis_url.is_none() {
            anyhow::bail!(
                "poller-incidents needs REDIS_URL, for the incident-text-changed publish"
            );
        }
        Ok(())
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rdm_incidents_base_url", &self.rdm_incidents_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .field("health", &self.health)
            .field("database_url", &self.database_url)
            .field("lines_dir", &self.lines_dir)
            .field("redis_url", &self.redis_url)
            .field("redis_password", &self.redis_password)
            .field("redis_username", &self.redis_username)
            .field("incidents_row_heartbeat", &self.incidents_row_heartbeat)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    const REQUIRED: [&str; 5] = [
        "poller-incidents",
        "--rdm-incidents-base-url",
        "https://example.invalid",
        "--rdm-api-key",
        "super-secret-rdm-key",
    ];

    fn parse(extra: &[&str]) -> Config {
        Config::try_parse_from(REQUIRED.iter().chain(extra)).expect("args should parse")
    }

    /// The heartbeat is on by default (plan 2c.6).
    #[test]
    fn the_defaults_are_the_heartbeat_and_the_lines_dir() {
        let config = parse(&[]);
        assert!(config.incidents_row_heartbeat);
        assert_eq!(config.lines_dir, "/app/lines");
    }

    #[test]
    fn the_db_sink_needs_a_database_and_redis() {
        let mut config = parse(&[]);
        // Not from the environment (a DB test run sets both).
        config.database_url = None;
        config.redis_url = None;
        assert!(config.validate().is_err());
        let config = parse(&[
            "--database-url",
            "postgres://u:p@h/d",
            "--redis-url",
            "redis://h:6379",
            "--incidents-row-heartbeat",
            "false",
        ]);
        config.validate().expect("complete");
        assert!(!config.incidents_row_heartbeat);
    }

    #[test]
    fn debug_redacts_the_rdm_api_key() {
        let config = parse(&[
            "--database-url",
            "postgres://u:super-secret-db-password@h/d",
            "--redis-password",
            "super-secret-redis-password",
        ]);

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
            !debug_output.contains("super-secret-db-password")
                && !debug_output.contains("super-secret-redis-password"),
            "database and Redis credentials are Secrets: {debug_output}"
        );
    }
}
