use clap::{Parser, ValueEnum};
use common::secret::Secret;

/// Where a snapshot goes (`INGEST_SINK`, ingest architecture spec §9.1;
/// chart `pollers.incidents.ingest.sink`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum IngestSink {
    /// `POST /private/incidents` on the api (today's path, the default).
    Http,
    /// Straight into Postgres through `ds_store::incidents`, as the
    /// `distant_signal_incidents` role, with the text-changed XADD from
    /// here (plan 2c.2).
    Db,
}

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

    /// The `api` crate's ingestion endpoint for incidents.
    #[arg(long, env, default_value = "http://api:8080/private/incidents")]
    pub api_ingest_url: String,

    /// Shared, non-secret `OAuth2` client-credentials config (same value
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

    /// `/livez` listener and stall window (SVC-08).
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,

    /// `http` (default): POST each snapshot to `api_ingest_url`. `db`:
    /// write it to Postgres directly (`database_url`, `lines_dir`,
    /// `redis_url`), plan 2c.2.
    #[arg(long, env = "INGEST_SINK", value_enum, default_value_t = IngestSink::Http)]
    pub ingest_sink: IngestSink,

    /// The `distant_signal_incidents` role's connection (`db` sink only).
    /// Pool size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 2.
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<Secret>,

    /// Directory of line-catalogue TOML files (`db` sink only): the
    /// matcher that fills `incidents.affected_lines` is built from it at
    /// startup, as the api's is. The image carries the catalogue here.
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines")]
    pub lines_dir: String,

    /// Redis, for the `incident-text-changed` XADD (`db` sink only). May
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

    /// The row heartbeat (`db` sink only; plan 2c.6,
    /// `ds_store::incidents::RowHeartbeat`). **On by default** (today's
    /// behaviour). The chart sets it, and the api's, from one value.
    #[arg(long, env = "INCIDENTS_ROW_HEARTBEAT", default_value_t = true, action = clap::ArgAction::Set)]
    pub incidents_row_heartbeat: bool,
}

impl Config {
    /// The `db` sink's required settings, checked once at startup.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.ingest_sink == IngestSink::Db {
            if self.database_url.is_none() {
                anyhow::bail!("INGEST_SINK=db needs DATABASE_URL");
            }
            if self.redis_url.is_none() {
                anyhow::bail!(
                    "INGEST_SINK=db needs REDIS_URL, for the incident-text-changed publish"
                );
            }
        }
        Ok(())
    }
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
            .field("health", &self.health)
            .field("ingest_sink", &self.ingest_sink)
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

    use super::{Config, IngestSink};

    const REQUIRED: [&str; 13] = [
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
    ];

    fn parse(extra: &[&str]) -> Config {
        Config::try_parse_from(REQUIRED.iter().chain(extra)).expect("args should parse")
    }

    /// Off by default (plan 2c.2/2c.6): the http sink and the heartbeat.
    #[test]
    fn the_defaults_are_todays_behaviour() {
        let config = parse(&[]);
        assert_eq!(config.ingest_sink, IngestSink::Http);
        assert!(config.incidents_row_heartbeat);
        assert_eq!(config.lines_dir, "/app/lines");
        config.validate().expect("the http sink needs nothing more");
    }

    #[test]
    fn the_db_sink_needs_a_database_and_redis() {
        let mut config = parse(&["--ingest-sink", "db"]);
        // Not from the environment (a DB test run sets both).
        config.database_url = None;
        config.redis_url = None;
        assert!(config.validate().is_err());
        let config = parse(&[
            "--ingest-sink",
            "db",
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
            !debug_output.contains("svc-password"),
            "the real internal_oauth password must never appear in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-db-password")
                && !debug_output.contains("super-secret-redis-password"),
            "database and Redis credentials are Secrets: {debug_output}"
        );
    }
}
