use clap::Parser;

/// CLI/env configuration for the `poller-tfl` service.
///
/// Unlike the four RDM pollers, `tfl_base_url` HAS a default: `TfL`'s Unified
/// API is a published, stable, documented public endpoint, so there is no
/// "no confirmed endpoint path" gap to fail loudly over. The subscription
/// key still has none — an unset key must stop the process at startup
/// rather than have it poll anonymously and get rate-limited later.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `tfl_app_key` is a real
/// `TfL` subscription key; a derived `Debug` would print it in full to any
/// future `tracing::debug!("{config:?}")`, matching the same class of bug
/// already fixed for `common::oauth_client::OAuthCredentials` and
/// `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
pub(crate) struct Config {
    /// `TfL` Unified API root, without a trailing path. The binary appends
    /// `/Line/Mode/{modes}/Status` itself.
    #[arg(long, env, default_value = "https://api.tfl.gov.uk")]
    pub tfl_base_url: String,

    /// `TfL` subscription key from api-portal.tfl.gov.uk, sent as the
    /// `Ocp-Apim-Subscription-Key` header (see `main.rs`).
    #[arg(long, env)]
    pub tfl_app_key: String,

    /// Comma-separated `TfL` modes to poll, passed straight through to `TfL`'s
    /// own comma-separated `{modes}` path segment.
    ///
    /// `bus`, `river-bus`, `cable-car` and friends are deliberately absent
    /// — v1's scope is rail-like `TfL` modes. `national-rail` is absent for a
    /// different reason: this app already has four National Rail pollers
    /// and an aggregator producing far better status for it than `TfL`'s
    /// summary view.
    #[arg(long, env, default_value = "tube,dlr,overground,elizabeth-line,tram")]
    pub tfl_modes: String,

    /// Where each snapshot goes: `stream` only, to `ds:ingest:tfl` (ingest
    /// plan 3c.2), which the ingest-writer applies. Release B (ingest phase
    /// 5, docs/ingest-phase5-runbook.md) removed `http` and `http+shadow`
    /// with the api's `/private` routes; an old value fails at startup. The
    /// next release drops the variable.
    #[arg(long, env = "INGEST_SINK", default_value = "stream", value_parser = ["stream"])]
    pub ingest_sink: String,

    /// Redis for the `ds:ingest:tfl` stream (`REDIS_URL` required).
    #[command(flatten)]
    pub redis: ingest_stream::snapshot::RedisArgs,

    /// `TfL` publishes no recommended interval for the line-status endpoint,
    /// and offers no push, no webhook and no confirmed conditional-request
    /// support — polling is the only option. 300s mirrors
    /// `poller-incidents`, whose feed has a comparable update rhythm.
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,

    /// The daily Tube, DLR and tram stops job
    /// (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md §1),
    /// published as `tfl-stops/1` on `ds:ingest:tfl-stops`. Off by default:
    /// turn it on once the ingest-writer applies that stream
    /// (`ingestWriter.streams.tfl-stops: apply`).
    #[arg(long, env, default_value_t = false, action = clap::ArgAction::Set)]
    pub tfl_stops_enabled: bool,

    /// Seconds between stops runs. `TfL`'s stop list changes a few times a
    /// year; a day is plenty.
    #[arg(long, env, default_value_t = 86_400)]
    pub tfl_stops_interval_secs: u64,

    /// The stops feed guard: a run whose stop count fell by more than this
    /// fraction of the last published snapshot's is refused.
    #[arg(long, env, default_value_t = 0.2)]
    pub tfl_stops_max_drop_fraction: f64,

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
            .field("tfl_base_url", &self.tfl_base_url)
            .field("tfl_app_key", &"[REDACTED]")
            .field("tfl_modes", &self.tfl_modes)
            .field("ingest_sink", &self.ingest_sink)
            .field("redis", &self.redis)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("tfl_stops_enabled", &self.tfl_stops_enabled)
            .field("tfl_stops_interval_secs", &self.tfl_stops_interval_secs)
            .field(
                "tfl_stops_max_drop_fraction",
                &self.tfl_stops_max_drop_fraction,
            )
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .field("health", &self.health)
            .finish()
    }
}

#[cfg(test)]
mod config_debug_tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_tfl_app_key() {
        let config = Config::try_parse_from([
            "poller-tfl",
            "--tfl-app-key",
            "super-secret-tfl-key",
            "--redis-password",
            "redis-password",
        ])
        .expect("required args should parse");

        let debug_output = format!("{config:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "tfl_app_key must be redacted: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-tfl-key"),
            "the real tfl_app_key must never appear in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("redis-password"),
            "the Redis password must never appear in Debug output: {debug_output}"
        );
    }

    #[test]
    fn the_stops_job_is_off_by_default() {
        let args = ["poller-tfl", "--tfl-app-key", "k"];
        let config = Config::try_parse_from(args).unwrap();
        assert!(!config.tfl_stops_enabled);
        assert_eq!(config.tfl_stops_interval_secs, 86_400);
        let on =
            Config::try_parse_from(args.iter().chain(&["--tfl-stops-enabled", "true"])).unwrap();
        assert!(on.tfl_stops_enabled);
    }

    /// Release B: `stream` is the default and the only value; the removed
    /// `http` and `http+shadow` fail at startup.
    #[test]
    fn ingest_sink_is_stream_only() {
        let args = ["poller-tfl", "--tfl-app-key", "k"];
        let config = Config::try_parse_from(args).unwrap();
        assert_eq!(config.ingest_sink, "stream");
        for removed in ["http", "http+shadow", "db"] {
            assert!(
                Config::try_parse_from(args.iter().chain(&["--ingest-sink", removed])).is_err(),
                "{removed}"
            );
        }
    }
}
