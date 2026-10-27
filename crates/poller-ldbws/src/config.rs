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

    /// The Redis the samples are `XADD`ed to (ingest architecture plan
    /// 3a.7). See `sink.rs`.
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

    /// The tracked-stop lane (DS-127, `tracked_stops.rs`): also poll the
    /// boards of tracked trains' own stops, narrowed to each train's booked
    /// time. Off by default: it adds requests (up to the cap below a
    /// cycle).
    #[arg(long, env, default_value_t = false)]
    pub tracked_stop_poll_enabled: bool,

    /// The tracked-stop lane's own budget: at most this many requests a
    /// cycle, soonest departure first; the rest wait for the next cycle.
    /// Separate from `HOURLY_REQUEST_BUDGET`, which caps the station
    /// rotation.
    #[arg(long, env, default_value_t = 20)]
    pub tracked_stop_max_requests_per_cycle: u32,

    /// The arrivals sub-lane of the tracked-stop lane (DS-152,
    /// `tracked_stops.rs`): also poll the LDBWS arrivals product
    /// (`GetArrBoardWithDetails`) for each tracked train's alighting stops,
    /// narrowed to its booked arrival. Departure boards never list a
    /// terminating or set-down-only call, so this is the only board that
    /// shows a platform there. Off by default; needs
    /// `LDBWS_ARRIVALS_URL` and `LDBWS_ARRIVALS_CONSUMER_KEY`. Independent of
    /// `TRACKED_STOP_POLL_ENABLED`.
    #[arg(long, env, default_value_t = false)]
    pub tracked_stop_arrivals_enabled: bool,

    /// The arrivals sub-lane's own budget: at most this many requests a
    /// cycle, soonest arrival first. Separate from the departure lane's
    /// `TRACKED_STOP_MAX_REQUESTS_PER_CYCLE`: the arrivals product is its
    /// own RDM subscription.
    /// Six: about what fits its 3 s share of the cycle
    /// (`tracked_stops::ARRIVALS_TIME_BUDGET`).
    #[arg(long, env, default_value_t = 6)]
    pub tracked_stop_arrivals_max_requests_per_cycle: u32,

    /// The RDM arrivals product's URL (the `url` key of the chart's
    /// arrivals Secret). How the request URL is built from it
    /// (`tracked_stops::arrivals_url`): a `{crs}` placeholder is replaced
    /// by the station; else a URL whose last path segment starts with
    /// `Get` (an operation, e.g. `.../GetArrBoardWithDetails`) gets
    /// `/{crs}` appended; else (the product's base, up to
    /// `/LDBWS/api/20220120`) it gets `/GetArrBoardWithDetails/{crs}`.
    /// `timeOffset`/`timeWindow`/`numRows` are added as query parameters.
    #[arg(long, env)]
    pub ldbws_arrivals_url: Option<String>,

    /// The arrivals product's consumer key, sent as the `x-apikey` header
    /// (how RDM API products authenticate, as `RDM_API_KEY` does for the
    /// departure boards).
    #[arg(long, env, hide_env_values = true)]
    pub ldbws_arrivals_consumer_key: Option<common::secret::Secret>,

    /// The arrivals product's consumer secret. Wired from the Secret for
    /// completeness but not sent: RDM's API products authenticate with
    /// the consumer key alone.
    #[arg(long, env, hide_env_values = true)]
    pub ldbws_arrivals_consumer_secret: Option<common::secret::Secret>,

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

    /// `DATABASE_URL` and `LINES_DIR` (ingest architecture plan 4.5): see
    /// `sample_source.rs`.
    #[command(flatten)]
    pub reads: crate::sample_source::SampleStationsArgs,
}

/// The station-samples stream's Redis (ingest architecture plan 3a.7, spec
/// §13.1). Each cycle's samples are `XADD`ed to
/// `ds:ingest:station-samples`, keeping the latest unsent snapshot while
/// Redis is down, and the startup cursor is read from the stream. Release
/// D (ingest phase 5) removed the `INGEST_SINK` switch (`stream` only since
/// release B).
#[derive(clap::Args, Clone, Debug, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct IngestArgs {
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
            .field("tracked_stop_poll_enabled", &self.tracked_stop_poll_enabled)
            .field(
                "tracked_stop_max_requests_per_cycle",
                &self.tracked_stop_max_requests_per_cycle,
            )
            .field(
                "tracked_stop_arrivals_enabled",
                &self.tracked_stop_arrivals_enabled,
            )
            .field(
                "tracked_stop_arrivals_max_requests_per_cycle",
                &self.tracked_stop_arrivals_max_requests_per_cycle,
            )
            .field("ldbws_arrivals_url", &self.ldbws_arrivals_url)
            .field(
                "ldbws_arrivals_consumer_key",
                &self
                    .ldbws_arrivals_consumer_key
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .field(
                "ldbws_arrivals_consumer_secret",
                &self
                    .ldbws_arrivals_consumer_secret
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
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
            "--ldbws-arrivals-consumer-key",
            "fake-arrivals-key",
            "--ldbws-arrivals-consumer-secret",
            "fake-arrivals-secret",
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
            !debug_output.contains("fake-arrivals-key")
                && !debug_output.contains("fake-arrivals-secret"),
            "the arrivals credentials must never appear in Debug output: {debug_output}"
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
        assert!(!config.tracked_stop_poll_enabled);
        assert_eq!(config.tracked_stop_max_requests_per_cycle, 20);
        assert!(!config.tracked_stop_arrivals_enabled);
        assert_eq!(config.tracked_stop_arrivals_max_requests_per_cycle, 6);
        assert!(config.ldbws_arrivals_url.is_none());
        assert!(config.ldbws_arrivals_consumer_key.is_none());
    }

    /// The stream needs `REDIS_URL`.
    #[test]
    fn the_stream_sink_needs_redis() {
        use super::IngestArgs;

        let mut args = IngestArgs::default();
        assert!(args.redis_client().is_err());
        args.redis_url = Some("redis://redis:6379".to_owned());
        assert!(args.redis_client().is_ok());
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
        "TRACKED_STOP_POLL_ENABLED",
        "TRACKED_STOP_MAX_REQUESTS_PER_CYCLE",
        "TRACKED_STOP_ARRIVALS_ENABLED",
        "TRACKED_STOP_ARRIVALS_MAX_REQUESTS_PER_CYCLE",
        "LDBWS_ARRIVALS_URL",
        "LDBWS_ARRIVALS_CONSUMER_KEY",
        "LDBWS_ARRIVALS_CONSUMER_SECRET",
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
