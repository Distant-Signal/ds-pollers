use clap::Parser;

/// CLI/env configuration for the `poller-ldbws` service.
///
/// `ldbws_base_url` deliberately has no default: research found two
/// different RDM product-slug segments in use across sources
/// (`1010-live-departure-board-dep` vs `...-dep1_2`) with no way to
/// reconcile which is currently correct without a live RDM subscription —
/// this must be supplied out of band once confirmed, not guessed.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `rdm_api_key` is a real
/// RDM Live Departure Board API key; a derived `Debug` would print it in
/// full to any future `tracing::debug!("{config:?}")`, matching the same
/// class of bug already fixed for `common::oauth_client::OAuthCredentials`
/// and `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
pub(crate) struct Config {
    /// RDM Live Departure Board base URL, up to and including the
    /// `/LDBWS/api/20220120` segment. The poller appends
    /// `/GetDepBoardWithDetails/{crs}` itself (see `main.rs`).
    #[arg(long, env)]
    pub ldbws_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`). Community sources describe
    /// this as the "consumer key" specifically (as opposed to a paired
    /// "consumer secret") — unconfirmed against RDM's own docs, but
    /// consistent with how the other three pollers authenticate.
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// Number of services requested per station per cycle (LDBWS's own
    /// `numRows` query parameter), used as the *first* attempt each cycle.
    /// Kept at the upstream API's own default (10) rather than lowering it
    /// globally: the repo owner's own empirical finding (a smaller
    /// `numRows` succeeds where 10 fails for a busy terminus like PAD
    /// during rush hour, evidenced by a persisting "500 Internal Server
    /// Error" from `GetDepBoardWithDetails`) points at a busyness-scaled
    /// upstream limit, not a value that's simply too high everywhere --
    /// lowering the global default would needlessly shrink every quiet
    /// station's data too, while still potentially being wrong for the
    /// busiest days at the busiest few. `main.rs`'s `fetch_departures`
    /// instead retries a 500 with progressively smaller `numRows` values
    /// (see `numrows_step_down`) *per station, per cycle*, so this value
    /// stays the richest one that's actually asked for, with a station-
    /// local fallback only when the upstream evidence says it's needed.
    /// See docs/superpowers/specs/2026-08-31-sample-data-availability-design.md's
    /// Correction 1: the aggregator's `min_sample_size` default is only 3
    /// relevant departures, pooled across a whole line's stations, so a
    /// reduced `numRows` at one busy station is far from a lossy trade.
    #[arg(long, env, default_value_t = 10)]
    pub num_rows: u32,

    /// Where the samples go (`INGEST_SINK`, ingest architecture plan 3a.7)
    /// and the Redis they are `XADD`ed to. See `sink.rs`.
    #[command(flatten)]
    pub ingest: IngestArgs,

    /// DESIGN.md §4's aggregator polling cadence target is "30-60s"; 60 is
    /// the conservative end. The resulting request volume (one request per
    /// sample station, about 560 of them, as many as fit `main.rs`'s 45 s
    /// `CYCLE_TIME_BUDGET` each cycle) was accepted by the repo owner under
    /// the Rail Data Marketplace terms on 2026-09-27 (LEG-18); the three
    /// knobs below exist so an operator can cut it without a code change.
    #[arg(long, env, default_value_t = 60)]
    pub poll_interval_secs: u64,

    /// LEG-18 operator knob: at most this many `GetDepBoardWithDetails`
    /// requests in any rolling hour, spread evenly across the hour's
    /// cycles (see `budget.rs`). Stations a cycle cannot afford are
    /// skipped (counted in `ldbws_budget_skipped_polls_total`) and the
    /// rotation picks them up first next cycle. 0, the default, means no
    /// budget.
    #[arg(long, env, default_value_t = 0)]
    pub hourly_request_budget: u32,

    /// LEG-18 operator knob: only sample stations on lines at least one
    /// user has pinned (`sample_source::selection`). Off by default. With nothing pinned, nothing is sampled.
    #[arg(long, env, default_value_t = false)]
    pub sample_pinned_lines_only: bool,

    /// LEG-18 operator knob: at most this many sample stations, chosen
    /// line-fairly with the most-pinned lines first (see
    /// `ds_store::reads::sample_stations::select_sample_stations`). 0, the
    /// default, means no cap.
    #[arg(long, env, default_value_t = 0)]
    pub sample_max_stations: u32,

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

    /// `SAMPLE_STATIONS_SOURCE`, `DATABASE_URL` and `LINES_DIR` (ingest
    /// architecture plan 4.5): see `sample_source.rs`.
    #[command(flatten)]
    pub reads: crate::sample_source::SampleStationsArgs,
}

/// `INGEST_SINK` and its Redis (ingest architecture plan 3a.7, spec §13.1).
#[derive(clap::Args, Clone, Debug, Default)]
pub(crate) struct IngestArgs {
    /// `stream` only: XADD each cycle's samples to
    /// `ds:ingest:station-samples`, keeping the latest unsent snapshot
    /// while Redis is down, and read the startup cursor from the stream.
    /// Release B (ingest phase 5, docs/ingest-phase5-runbook.md) removed
    /// `http` and `http+shadow` with the api's `/private` routes; an old
    /// value fails at startup. The next release drops the variable.
    #[arg(long, env, default_value = "stream", value_parser = ["stream"])]
    pub ingest_sink: String,

    /// Redis for the stream (required).
    #[arg(long, env)]
    pub redis_url: Option<String>,

    /// The `poller-ldbws` ACL user (chart `redis.acl.clients.pollerLdbws`).
    /// Unset or empty: the `default` user.
    #[arg(long, env)]
    pub redis_username: Option<String>,

    /// That user's password (or the `redis.auth` one). Never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<common::secret::Secret>,
}

impl IngestArgs {
    /// The Redis client for the stream.
    pub(crate) fn redis_client(&self) -> anyhow::Result<redis::Client> {
        let Some(url) = self.redis_url.as_deref().filter(|url| !url.is_empty()) else {
            anyhow::bail!("poller-ldbws needs REDIS_URL, for ds:ingest:station-samples");
        };
        let url = common::redis_auth::redis_url_with_credentials(
            url,
            self.redis_username.as_deref(),
            self.redis_password.as_ref(),
        )?;
        // No `.context(url)`: it carries the password.
        let client = redis::Client::open(url.expose())
            .map_err(|_| anyhow::anyhow!("REDIS_URL is not a valid Redis URL (value not shown)"))?;
        Ok(client)
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("ldbws_base_url", &self.ldbws_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("num_rows", &self.num_rows)
            .field("ingest", &self.ingest)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("hourly_request_budget", &self.hourly_request_budget)
            .field("sample_pinned_lines_only", &self.sample_pinned_lines_only)
            .field("sample_max_stations", &self.sample_max_stations)
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .field("health", &self.health)
            .field("reads", &self.reads)
            .finish()
    }
}

#[cfg(test)]
mod config_debug_tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_rdm_api_key() {
        let config = Config::try_parse_from([
            "poller-ldbws",
            "--ldbws-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "super-secret-rdm-key",
            "--redis-password",
            "redis-password",
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
            !debug_output.contains("redis-password"),
            "the Redis password must never appear in Debug output: {debug_output}"
        );
    }

    /// LEG-18: the three volume knobs are off unless set.
    #[test]
    fn the_volume_knobs_default_to_off() {
        let config = Config::try_parse_from([
            "poller-ldbws",
            "--ldbws-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "key",
        ])
        .expect("required args should parse");
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.hourly_request_budget, 0);
        assert!(!config.sample_pinned_lines_only);
        assert_eq!(config.sample_max_stations, 0);
        // Release B: `stream` is the only sink.
        assert_eq!(config.ingest.ingest_sink, "stream");
    }

    /// The stream needs `REDIS_URL`; the removed `http` and `http+shadow`
    /// fail at startup.
    #[test]
    fn the_stream_sink_needs_redis_and_http_is_gone() {
        use super::IngestArgs;

        let mut args = IngestArgs::default();
        assert!(args.redis_client().is_err());
        args.redis_url = Some("redis://redis:6379".to_owned());
        assert!(args.redis_client().is_ok());
        for removed in ["http", "http+shadow"] {
            assert!(
                Config::try_parse_from([
                    "poller-ldbws",
                    "--ldbws-base-url",
                    "https://example.invalid",
                    "--rdm-api-key",
                    "key",
                    "--ingest-sink",
                    removed,
                ])
                .is_err(),
                "{removed}"
            );
        }
    }
}

/// The LEG-18 knobs are only useful if an operator can set them under
/// Helm. `poller-deployments.yaml` renders every poller from one loop, so
/// check the template names each env var this crate declares for them.
#[cfg(test)]
mod chart_env_wiring_tests {
    use clap::CommandFactory;

    use super::Config;

    const KNOB_ENV_VARS: &[&str] = &[
        "HOURLY_REQUEST_BUDGET",
        "SAMPLE_PINNED_LINES_ONLY",
        "SAMPLE_MAX_STATIONS",
    ];

    #[test]
    fn every_volume_knob_is_wired_into_the_poller_template() {
        let template = common::manifest_dir!()
            .join("../../charts/distant-signal/templates/poller-deployments.yaml");
        let rendered = std::fs::read_to_string(&template)
            .unwrap_or_else(|err| panic!("read {}: {err}", template.display()));
        let declared: Vec<String> = Config::command()
            .get_arguments()
            .filter_map(|arg| arg.get_env().and_then(|env| env.to_str()))
            .map(str::to_string)
            .collect();
        for env in KNOB_ENV_VARS {
            assert!(
                declared.iter().any(|d| d == env),
                "sanity check: crates/poller-ldbws/src/config.rs must still declare {env}"
            );
            assert!(
                rendered.contains(&format!("- name: {env}")),
                "{env} is declared by crates/poller-ldbws/src/config.rs but never set in \
                 charts/distant-signal/templates/poller-deployments.yaml"
            );
        }
    }
}
