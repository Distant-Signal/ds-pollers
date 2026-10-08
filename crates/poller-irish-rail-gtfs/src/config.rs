use clap::Parser;

/// CLI/env configuration for the `poller-irish-rail-gtfs` service.
///
/// `gtfs_url` DOES have a working default, unlike every RDM poller's own
/// `baseUrl` (which is account-specific and unpublished): the friction doc
/// (docs/superpowers/specs/2026-09-05-ireland-vs-northern-ireland-friction-research.md
/// §1) confirms this is a real, public, key-free, anonymous-GET URL,
/// downloaded and verified directly in that research session -- matching
/// `poller-tfl`'s own precedent (`TFL_BASE_URL` defaults to the real `TfL`
/// API root) for "a genuinely public endpoint gets a working default,
/// unlike an account-gated one."
///
/// Ingest plan 3c.2 (decision D8): snapshots go to the
/// `ds:ingest:island-of-ireland` stream only, so there is no api URL or
/// internal OAuth credential any more. Does not derive `Debug`: the hand
/// impl below keeps the Redis credentials redacted (`RedisArgs`).
#[derive(Parser)]
pub(crate) struct Config {
    /// Transport for Ireland's public GTFS zip for Iarnród Éireann.
    #[arg(
        long,
        env,
        default_value = "https://www.transportforireland.ie/transitData/Data/GTFS_Irish_Rail.zip"
    )]
    pub gtfs_url: String,

    /// Where snapshots go: `stream` is the only sink (decision D8: the
    /// island-of-Ireland pollers keep no HTTP path). Kept so `INGEST_SINK`
    /// is uniform across the stream producers.
    #[arg(long, env = "INGEST_SINK", default_value = "stream", value_parser = ["stream"])]
    pub ingest_sink: String,

    /// Redis for the `ds:ingest:island-of-ireland` stream.
    #[command(flatten)]
    pub redis: ingest_stream::snapshot::RedisArgs,

    /// The friction doc confirms `feed_start_date`/`feed_end_date` show "a
    /// live, rolling one-year window" but never states how often the feed
    /// itself is regenerated -- unlike RDM's `stations`/`tocs` feeds, whose
    /// spec explicitly recommends a 24-hour poll. Defaulted to the same
    /// 24-hour cadence as `poller-stations`/`poller-tocs`
    /// (`crates/poller-stations/src/config.rs`'s own `poll_interval_secs`
    /// default) as the conservative, already-established convention for
    /// "static reference data with an unconfirmed real refresh cadence" --
    /// not a confirmed fact about this specific feed's own update
    /// frequency.
    #[arg(long, env, default_value_t = 86400)]
    pub poll_interval_secs: u64,

    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,

    /// `/livez` listener and stall window (SVC-08).
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("gtfs_url", &self.gtfs_url)
            .field("ingest_sink", &self.ingest_sink)
            .field("redis", &self.redis)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics_enabled", &self.metrics_enabled)
            .field("health", &self.health)
            .finish()
    }
}

#[cfg(test)]
mod config_debug_tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_redis_password() {
        let config = Config::try_parse_from([
            "poller-irish-rail-gtfs",
            "--redis-url",
            "redis://redis:6379",
            "--redis-password",
            "super-secret-password",
        ])
        .expect("args should parse");

        let debug_output = format!("{config:?}");
        assert!(
            !debug_output.contains("super-secret-password"),
            "the Redis password must never appear in Debug output: {debug_output}"
        );
    }

    /// Plan 3c.2 (D8): `stream` is the default and the only sink.
    #[test]
    fn ingest_sink_is_stream_only() {
        let config = Config::try_parse_from(["poller-irish-rail-gtfs"]).unwrap();
        assert_eq!(config.ingest_sink, "stream");
        assert!(
            Config::try_parse_from(["poller-irish-rail-gtfs", "--ingest-sink", "http"]).is_err()
        );
    }
}
