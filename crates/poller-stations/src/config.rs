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

    /// Postgres, as the `stations` role (required). Pool size and timeouts
    /// come from the shared `DATABASE_*` variables (`common::pg`); the
    /// default pool is 1 (spec §6.6: one sequential writer, role limit 2).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: common::secret::Secret,

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

impl Config {
    /// Cross-field checks clap cannot express: an empty `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.database_url.is_empty() {
            anyhow::bail!("DATABASE_URL must not be empty");
        }
        Ok(())
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rdm_stations_base_url", &self.rdm_stations_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("database_url", &self.database_url)
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

    const REQUIRED: [&str; 7] = [
        "poller-stations",
        "--rdm-stations-base-url",
        "https://example.invalid",
        "--rdm-api-key",
        "super-secret-rdm-key",
        "--database-url",
        "postgres://distant_signal_stations:pw@postgres/ds",
    ];

    fn parse(extra: &[&str]) -> Result<Config, clap::Error> {
        Config::try_parse_from(REQUIRED.iter().chain(extra))
    }

    /// An empty `DATABASE_URL` fails at startup, and the URL never reaches
    /// `Debug`.
    #[test]
    fn the_sink_needs_a_database_url() {
        let config = parse(&[]).unwrap();
        config.validate().unwrap();
        assert!(
            !format!("{config:?}").contains(":pw@"),
            "the database URL must not appear in Debug output"
        );
        let empty = Config::try_parse_from([
            "poller-stations",
            "--rdm-stations-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "k",
            "--database-url",
            "",
        ])
        .unwrap();
        assert!(empty.validate().is_err(), "an empty DATABASE_URL");
    }

    /// The chart sets `DATABASE_URL` (templates/poller-deployments.yaml).
    /// Release D (ingest phase 5, Q8) removed `INGEST_SINK`.
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
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        assert!(
            command
                .get_arguments()
                .all(|arg| arg.get_env().is_none_or(|env| env != "INGEST_SINK")),
            "INGEST_SINK was removed in release D"
        );
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
    }
}
