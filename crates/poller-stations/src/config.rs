use clap::Parser;

/// CLI/env configuration for the `poller-stations` service.
///
/// `rdm_stations_base_url` deliberately has no default: RSPS5050 P-03-00
/// Rev A §6 confirms the path suffix (`/stations`), but the host portion of
/// the URL is account-specific and not published in the spec, so a
/// missing/misconfigured URL must fail loudly at startup rather than
/// silently poll the wrong thing.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `rdm_api_key` is a real
/// RDM Stations API key; a derived `Debug` would print it in full to any
/// future `tracing::debug!("{config:?}")`, matching the same class of bug
/// already fixed for `common::oauth_client::OAuthCredentials` and
/// `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
pub(crate) struct Config {
    /// RDM Stations feed base URL, e.g. `https://<host>/json/1.0`. The
    /// poller appends `/stations` itself (see `main.rs`).
    #[arg(long, env)]
    pub rdm_stations_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// The `api` crate's ingestion endpoint for stations. Used only with
    /// `INGEST_SINK=http`.
    #[arg(long, env, default_value = "http://api:8080/private/stations")]
    pub api_ingest_url: String,

    /// Where the parsed stations go (ingest architecture plan 2b.1): `http`
    /// (the default, today's behaviour) POSTs them to `API_INGEST_URL`;
    /// `db` writes them straight into Postgres
    /// (`ds_store::reference::upsert_stations`, as the `stations` role) and
    /// reads the startup cursor from `ingest_freshness`.
    #[arg(long, env, value_enum, default_value_t = IngestSink::Http)]
    pub ingest_sink: IngestSink,

    /// Postgres, for `INGEST_SINK=db` (required then, unused otherwise).
    /// Pool size and timeouts come from the shared `DATABASE_*` variables
    /// (`common::pg`); the default pool is 1 (spec §6.6: one sequential
    /// writer, role limit 2).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<common::secret::Secret>,

    /// Shared, non-secret `OAuth2` client-credentials config (same value
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

    /// `/livez` listener and stall window (SVC-08).
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

/// `INGEST_SINK`: see [`Config::ingest_sink`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum IngestSink {
    #[default]
    Http,
    Db,
}

impl Config {
    /// Cross-field checks clap cannot express: `db` needs a `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.ingest_sink == IngestSink::Db
            && self
                .database_url
                .as_ref()
                .is_none_or(common::secret::Secret::is_empty)
        {
            anyhow::bail!("INGEST_SINK=db needs DATABASE_URL");
        }
        Ok(())
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rdm_stations_base_url", &self.rdm_stations_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("api_ingest_url", &self.api_ingest_url)
            .field("ingest_sink", &self.ingest_sink)
            .field("database_url", &self.database_url)
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

    use super::{Config, IngestSink};

    const REQUIRED: [&str; 13] = [
        "poller-stations",
        "--rdm-stations-base-url",
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

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        Config::try_parse_from(REQUIRED.iter().chain(extra))
    }

    /// The default is today's behaviour: POST to the api, no database.
    #[test]
    fn the_sink_defaults_to_http_and_db_needs_a_database_url() {
        // `--database-url ""`: the test environment may set DATABASE_URL.
        let config = parse(&["--database-url", ""]).unwrap();
        assert_eq!(config.ingest_sink, IngestSink::Http);
        config.validate().unwrap();

        let db = parse(&["--ingest-sink", "db", "--database-url", ""]).unwrap();
        assert_eq!(db.ingest_sink, IngestSink::Db);
        assert!(db.validate().is_err(), "db without DATABASE_URL");

        let db = parse(&[
            "--ingest-sink",
            "db",
            "--database-url",
            "postgres://distant_signal_stations:pw@postgres/ds",
        ])
        .unwrap();
        db.validate().unwrap();
        assert!(
            !format!("{db:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );

        assert!(parse(&["--ingest-sink", "stream"]).is_err());
    }

    /// The chart sets these names (templates/poller-deployments.yaml).
    #[test]
    fn the_new_settings_read_the_chart_s_env_names() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .map(|env| env.to_string_lossy().into_owned())
        };
        assert_eq!(env("ingest_sink").as_deref(), Some("INGEST_SINK"));
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
    }

    #[test]
    fn debug_redacts_the_rdm_api_key() {
        let config = parse(&[]).expect("required args should parse");

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
