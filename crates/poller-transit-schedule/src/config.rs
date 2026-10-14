use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// CLI/env configuration for `poller-transit-schedule` (transit spec §6,
/// §13.1): one process per network. Every URL is a public, key-free BODS
/// or `NaPTAN` download (spec §3), so each has a working default.
///
/// Does not derive `Debug`: the hand impl keeps the Redis credentials
/// redacted (`RedisArgs`).
#[derive(Parser)]
#[command(name = "poller-transit-schedule")]
pub(crate) struct Config {
    /// The network this process serves (`metrolink`): one of the files in
    /// `networks/`, compiled in. Any other value refuses to start.
    #[arg(long, env = "TRANSIT_NETWORK")]
    pub network: String,

    /// The timetable source adapter. Only `gtfs` is built (`TransXChange`,
    /// for Nottingham NET, is later work, D6).
    #[arg(long, env = "TRANSIT_ADAPTER", default_value = "gtfs", value_parser = ["gtfs"])]
    pub adapter: String,

    /// The GTFS zip: the BODS North West region (OGL), filtered to the
    /// network's NOCs. `TfGM`'s own GTFS (`ODbL`) is the fallback (spec §4).
    #[arg(
        long,
        env = "TRANSIT_GTFS_URL",
        default_value = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/north_west/"
    )]
    pub gtfs_url: String,

    /// The GTFS download's byte cap (400 MiB; the North West zip was
    /// 123 MB on 2026-10-09).
    #[arg(long, env = "TRANSIT_GTFS_MAX_BYTES", default_value_t = 400 * 1024 * 1024)]
    pub gtfs_max_bytes: u64,

    /// The BODS SIRI-SX bulk archive (a 302 to a signed URL).
    #[arg(
        long,
        env = "TRANSIT_SITUATIONS_URL",
        default_value = "https://data.bus-data.dft.gov.uk/disruptions/download/bulk_archive"
    )]
    pub situations_url: String,

    /// `NaPTAN` access nodes for the network's area, as CSV. `{area}` is
    /// replaced with the network file's `naptan_area`.
    #[arg(
        long,
        env = "TRANSIT_NAPTAN_URL",
        default_value = "https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes={area}&dataFormat=csv"
    )]
    pub naptan_url: String,

    /// Seconds between timetable fetches (daily; the BODS file is rebuilt
    /// nightly).
    #[arg(
        long,
        env = "TRANSIT_TIMETABLE_INTERVAL_SECS",
        default_value_t = 86_400
    )]
    pub timetable_interval_secs: u64,

    /// Seconds between situations fetches.
    #[arg(long, env = "TRANSIT_SITUATIONS_INTERVAL_SECS", default_value_t = 300)]
    pub situations_interval_secs: u64,

    /// Run the situations loop. Off by default until the ingest-writer has
    /// the `transit-situations/1` handler (transit plan batch 4): before
    /// that an applied stream would dead-letter every snapshot.
    #[arg(long, env = "TRANSIT_SITUATIONS_ENABLED", default_value_t = false, action = clap::ArgAction::Set)]
    pub situations_enabled: bool,

    /// Days of service dates stored after today (Q8: 60).
    #[arg(long, env = "TRANSIT_HORIZON_DAYS", default_value_t = 60, value_parser = clap::value_parser!(u32).range(1..=400))]
    pub horizon_days: u32,

    /// Sharp-drop guard: a feed whose trip, stop or line count fell by more
    /// than this fraction since the last commit is refused. 1.0 turns the
    /// check off (an empty feed is still refused).
    #[arg(long, env = "FEED_MAX_DROP_FRACTION", default_value_t = 0.3, value_parser = parse_fraction)]
    pub feed_max_drop_fraction: f64,

    /// Where the GTFS zip is downloaded (a zip needs a seekable file): the
    /// chart's `emptyDir` at `/work`.
    #[arg(long, env = "TRANSIT_WORK_DIR", default_value = "/work")]
    pub work_dir: PathBuf,

    /// Redis for the `ds:ingest:transit-<network>` stream, as the network's
    /// own ACL user (`poller-transit-<network>`).
    #[command(flatten)]
    pub redis: ingest_stream::snapshot::RedisArgs,

    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,

    /// `/livez` listener and stall window. The chart sets an hour: one
    /// 123 MB download, its parse and the produce.
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

impl Config {
    pub(crate) fn timetable_interval(&self) -> Duration {
        Duration::from_secs(self.timetable_interval_secs)
    }

    pub(crate) fn situations_interval(&self) -> Duration {
        Duration::from_secs(self.situations_interval_secs)
    }

    /// The `NaPTAN` URL for `area`.
    pub(crate) fn naptan_url_for(&self, area: &str) -> String {
        self.naptan_url.replace("{area}", area)
    }
}

/// A fraction in `0.0..=1.0`.
fn parse_fraction(raw: &str) -> Result<f64, String> {
    let value: f64 = raw.parse().map_err(|err| format!("{err}"))?;
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(format!("{value} is not a fraction between 0 and 1"))
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("network", &self.network)
            .field("adapter", &self.adapter)
            .field("gtfs_url", &self.gtfs_url)
            .field("gtfs_max_bytes", &self.gtfs_max_bytes)
            .field("situations_url", &self.situations_url)
            .field("naptan_url", &self.naptan_url)
            .field("timetable_interval_secs", &self.timetable_interval_secs)
            .field("situations_interval_secs", &self.situations_interval_secs)
            .field("situations_enabled", &self.situations_enabled)
            .field("horizon_days", &self.horizon_days)
            .field("feed_max_drop_fraction", &self.feed_max_drop_fraction)
            .field("work_dir", &self.work_dir)
            .field("redis", &self.redis)
            .field("metrics_port", &self.metrics_port)
            .field("metrics_enabled", &self.metrics_enabled)
            .field("health", &self.health)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn defaults_are_the_bods_feeds() {
        let config = Config::try_parse_from(["p", "--network", "metrolink"]).unwrap();
        assert_eq!(config.adapter, "gtfs");
        assert!(
            config
                .gtfs_url
                .starts_with("https://data.bus-data.dft.gov.uk/")
        );
        assert_eq!(config.gtfs_max_bytes, 419_430_400);
        assert_eq!(config.horizon_days, 60);
        assert_eq!(config.timetable_interval(), Duration::from_secs(86_400));
        assert_eq!(config.situations_interval(), Duration::from_secs(300));
        assert_eq!(config.work_dir, PathBuf::from("/work"));
        assert_eq!(
            config.naptan_url_for("940"),
            "https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes=940&dataFormat=csv"
        );
    }

    #[test]
    fn a_network_is_required_and_only_gtfs_is_built() {
        assert!(Config::try_parse_from(["p"]).is_err());
        assert!(
            Config::try_parse_from(["p", "--network", "metrolink", "--adapter", "txc"]).is_err()
        );
        assert!(
            Config::try_parse_from(["p", "--network", "metrolink", "--horizon-days", "0"]).is_err()
        );
    }

    #[test]
    fn debug_redacts_the_redis_password() {
        let config = Config::try_parse_from([
            "p",
            "--network",
            "metrolink",
            "--redis-password",
            "super-secret-password",
        ])
        .unwrap();
        assert!(!format!("{config:?}").contains("super-secret-password"));
    }

    #[test]
    fn only_fractions_between_zero_and_one_parse() {
        assert_eq!(parse_fraction("0.3"), Ok(0.3));
        for bad in ["-0.1", "1.5", "NaN", ""] {
            assert!(parse_fraction(bad).is_err(), "{bad}");
        }
    }

    /// The chart's template sets every variable this config reads, and
    /// nothing it does not (a renamed variable would be silently ignored).
    #[test]
    fn the_chart_template_sets_exactly_the_config_env() {
        use clap::CommandFactory;
        let template = common::manifest_dir!()
            .join("../../charts/distant-signal/templates/poller-transit-schedule-deployment.yaml");
        let text = std::fs::read_to_string(&template).unwrap();
        let set: std::collections::BTreeSet<&str> = text
            .lines()
            .filter_map(|l| l.trim().strip_prefix("- name: "))
            .filter(|n| n.chars().all(|c| c.is_ascii_uppercase() || c == '_'))
            .collect();
        let declared: std::collections::BTreeSet<String> = Config::command()
            .get_arguments()
            .filter_map(|arg| arg.get_env().and_then(|env| env.to_str()))
            .map(str::to_owned)
            .collect();
        // REDIS_USERNAME/REDIS_PASSWORD come from redisClientAuthEnv;
        // RUST_LOG is the logging filter, not a Config field.
        for name in &set {
            assert!(
                declared.contains(*name) || *name == "RUST_LOG",
                "the template sets {name}, which Config does not read"
            );
        }
        for name in &declared {
            assert!(
                set.contains(name.as_str())
                    || ["REDIS_USERNAME", "REDIS_PASSWORD"].contains(&name.as_str()),
                "Config reads {name}, which the template never sets"
            );
        }
    }
}
