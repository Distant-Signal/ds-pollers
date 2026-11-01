use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// CLI/env configuration for `poller-transit-schedule` (transit spec §6,
/// §13.1): one process per network. Every URL but TNDS's is a public,
/// key-free BODS or `NaPTAN` download (spec §3), so each has a working
/// default; TNDS needs a Traveline login (NET design §5).
///
/// Does not derive `Debug`: the hand impl keeps the Redis and TNDS
/// credentials redacted (`RedisArgs`, `txc_password`).
#[derive(Parser)]
#[command(name = "poller-transit-schedule")]
pub(crate) struct Config {
    /// The network this process serves (`metrolink`): one of the files in
    /// `networks/`, compiled in. Any other value refuses to start.
    #[arg(long, env = "TRANSIT_NETWORK")]
    pub network: String,

    /// The timetable source adapter: `gtfs` (Metrolink) or `txc`
    /// (`TransXChange`, Nottingham NET). Must be the network file's
    /// `adapter`; anything else refuses to start.
    #[arg(long, env = "TRANSIT_ADAPTER", default_value = "gtfs", value_parser = ["gtfs", "txc"])]
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

    /// The `TransXChange` zip (`TRANSIT_ADAPTER=txc`): the Traveline
    /// National Dataset's East Midlands region over FTP (OGL; a login), or
    /// any `https://` zip of TXC documents.
    #[arg(
        long,
        env = "TRANSIT_TXC_URL",
        default_value = "ftp://ftp.tnds.co.uk/EM.zip"
    )]
    pub txc_url: String,

    /// The TXC download's byte cap (400 MiB).
    #[arg(long, env = "TRANSIT_TXC_MAX_BYTES", default_value_t = 400 * 1024 * 1024)]
    pub txc_max_bytes: u64,

    /// The FTP login for an `ftp://` TXC URL (TNDS): from the chart's
    /// `txcCredentials` Secret. Required with an `ftp://` URL.
    #[arg(long, env = "TRANSIT_TXC_USERNAME", default_value = "")]
    pub txc_username: String,

    /// Its password (never logged).
    #[arg(
        long,
        env = "TRANSIT_TXC_PASSWORD",
        default_value = "",
        hide_env_values = true
    )]
    pub txc_password: String,

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

    /// Which of the two loops this process runs (docs/scheduled-jobs.md):
    /// `both` (the default, one Deployment), or `timetable` / `situations`
    /// alone, so the daily timetable can run as a `CronJob` while the
    /// five-minute situations loop stays a small Deployment. `situations`
    /// needs `TRANSIT_SITUATIONS_ENABLED`; once mode (`RUN_ONCE`) needs
    /// `timetable`.
    #[arg(long, env = "TRANSIT_LOOPS", value_enum, default_value_t = Loops::Both)]
    pub loops: Loops,

    /// Days of service dates stored after today (Q8: 60).
    #[arg(long, env = "TRANSIT_HORIZON_DAYS", default_value_t = 60, value_parser = clap::value_parser!(u32).range(1..=400))]
    pub horizon_days: u32,

    /// Sharp-drop guard: a feed whose trip, stop or line count fell by more
    /// than this fraction since the last commit is refused. 1.0 turns the
    /// check off (an empty feed is still refused).
    #[arg(long, env = "FEED_MAX_DROP_FRACTION", default_value_t = 0.3, value_parser = parse_fraction)]
    pub feed_max_drop_fraction: f64,

    /// A one-shot timetable refresh request (chart `timetableRefresh`;
    /// docs/transit-networks.md). Empty (the default): none. Otherwise a
    /// token (`2026-10-15`, `2`): at start the producer fetches and
    /// publishes the timetable at once, even an unchanged one, unless the
    /// newest commit on the stream already answers this token
    /// (`source.refresh`). Change the token to ask again; it never needs
    /// unsetting.
    #[arg(long, env = "TRANSIT_TIMETABLE_REFRESH", default_value = "", value_parser = parse_refresh)]
    pub timetable_refresh: String,

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

    /// Whether the TXC source is TNDS-style FTP (and so needs the login).
    pub(crate) fn txc_over_ftp(&self) -> bool {
        self.txc_url.starts_with("ftp://")
    }

    /// The checks that need the network file (NET design §5, §6): the
    /// adapter is the file's, an `ftp://` TXC URL has its login, and the
    /// situations loop runs only for a network with a SIRI-SX participant.
    pub(crate) fn check_against(
        &self,
        network: &crate::network::NetworkConfig,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.adapter == network.adapter.as_str(),
            "TRANSIT_ADAPTER is {:?} but networks/{}.toml is written for {:?}",
            self.adapter,
            network.id,
            network.adapter.as_str()
        );
        if self.adapter == "txc" {
            anyhow::ensure!(
                self.txc_url.starts_with("ftp://") || self.txc_url.starts_with("https://"),
                "TRANSIT_TXC_URL must be ftp:// or https://"
            );
            anyhow::ensure!(
                !self.txc_over_ftp() || !self.txc_username.is_empty(),
                "TRANSIT_TXC_USERNAME (and TRANSIT_TXC_PASSWORD) are required for an ftp:// \
                 TRANSIT_TXC_URL: the chart's txcCredentials.existingSecret"
            );
        }
        anyhow::ensure!(
            !self.situations_enabled || !network.situations.participant_refs.is_empty(),
            "TRANSIT_SITUATIONS_ENABLED is on but networks/{}.toml names no SIRI-SX \
             participant: no source publishes its disruptions",
            network.id
        );
        anyhow::ensure!(
            self.loops != Loops::Situations || self.situations_enabled,
            "TRANSIT_LOOPS is situations but TRANSIT_SITUATIONS_ENABLED is off: \
             this process would run no loop"
        );
        Ok(())
    }

    /// Once mode (`RUN_ONCE`, a scheduled Job) runs one timetable cycle and
    /// exits, so it needs the timetable loop alone: with both, whichever
    /// loop returned first would end the run.
    pub(crate) fn check_once_mode(&self, once: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            !once || self.loops == Loops::Timetable,
            "RUN_ONCE needs TRANSIT_LOOPS=timetable (it is {:?}): a scheduled run is one \
             timetable cycle",
            self.loops
        );
        Ok(())
    }
}

/// The loops a process runs (`TRANSIT_LOOPS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Loops {
    /// The daily timetable only.
    Timetable,
    /// The situations (SIRI-SX) loop only.
    Situations,
    /// Both, in one process.
    Both,
}

impl Loops {
    /// Whether the timetable loop runs.
    pub(crate) fn timetable(self) -> bool {
        matches!(self, Self::Timetable | Self::Both)
    }

    /// Whether the situations loop may run (it still needs
    /// `TRANSIT_SITUATIONS_ENABLED`).
    pub(crate) fn situations(self) -> bool {
        matches!(self, Self::Situations | Self::Both)
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

fn parse_refresh(raw: &str) -> Result<String, String> {
    if raw.is_empty() || common::transit::is_refresh_token(raw) {
        Ok(raw.to_owned())
    } else {
        Err(format!(
            "a refresh token is 1 to {} of A-Z a-z 0-9 . _ : -",
            common::transit::MAX_REFRESH_TOKEN_LEN
        ))
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("network", &self.network)
            .field("adapter", &self.adapter)
            .field("gtfs_url", &self.gtfs_url)
            .field("gtfs_max_bytes", &self.gtfs_max_bytes)
            .field("txc_url", &self.txc_url)
            .field("txc_max_bytes", &self.txc_max_bytes)
            .field("txc_username", &self.txc_username)
            .field(
                "txc_password",
                &if self.txc_password.is_empty() {
                    ""
                } else {
                    "<redacted>"
                },
            )
            .field("situations_url", &self.situations_url)
            .field("naptan_url", &self.naptan_url)
            .field("timetable_interval_secs", &self.timetable_interval_secs)
            .field("situations_interval_secs", &self.situations_interval_secs)
            .field("situations_enabled", &self.situations_enabled)
            .field("loops", &self.loops)
            .field("horizon_days", &self.horizon_days)
            .field("feed_max_drop_fraction", &self.feed_max_drop_fraction)
            .field("timetable_refresh", &self.timetable_refresh)
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
            config.timetable_refresh, "",
            "no refresh request by default"
        );
        assert_eq!(
            config.naptan_url_for("940"),
            "https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes=940&dataFormat=csv"
        );
    }

    #[test]
    fn a_network_is_required_and_the_adapter_is_gtfs_or_txc() {
        assert!(Config::try_parse_from(["p"]).is_err());
        assert!(
            Config::try_parse_from(["p", "--network", "metrolink", "--adapter", "netex"]).is_err()
        );
        let txc = Config::try_parse_from(["p", "--network", "net", "--adapter", "txc"]).unwrap();
        assert_eq!(txc.txc_url, "ftp://ftp.tnds.co.uk/EM.zip");
        assert_eq!(txc.txc_max_bytes, 419_430_400);
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
            "--txc-password",
            "tnds-secret-password",
        ])
        .unwrap();
        let text = format!("{config:?}");
        assert!(!text.contains("super-secret-password"));
        assert!(!text.contains("tnds-secret-password"), "{text}");
    }

    #[test]
    fn the_network_file_decides_the_adapter_login_and_situations() {
        use crate::network::NetworkConfig;
        let net = NetworkConfig::load("net").unwrap();
        let metrolink = NetworkConfig::load("metrolink").unwrap();
        let parse = |args: &[&str]| {
            Config::try_parse_from(["p", "--network", "net"].iter().chain(args)).unwrap()
        };
        // NET: txc, with the TNDS login.
        let ok = parse(&[
            "--adapter",
            "txc",
            "--txc-username",
            "u",
            "--txc-password",
            "p",
        ]);
        ok.check_against(&net).unwrap();
        // The wrong adapter for the file.
        assert!(parse(&[]).check_against(&net).is_err());
        assert!(ok.check_against(&metrolink).is_err());
        // ftp:// without a login; https:// needs none.
        assert!(parse(&["--adapter", "txc"]).check_against(&net).is_err());
        parse(&[
            "--adapter",
            "txc",
            "--txc-url",
            "https://example.org/em.zip",
        ])
        .check_against(&net)
        .unwrap();
        assert!(
            parse(&["--adapter", "txc", "--txc-url", "http://example.org/em.zip"])
                .check_against(&net)
                .is_err()
        );
        // No SIRI-SX participant publishes NET: its situations loop refuses.
        let situations = parse(&[
            "--adapter",
            "txc",
            "--txc-username",
            "u",
            "--situations-enabled",
            "true",
        ]);
        let err = situations.check_against(&net).unwrap_err();
        assert!(err.to_string().contains("participant"), "{err}");
        // Metrolink is unchanged.
        let metrolink_config = Config::try_parse_from([
            "p",
            "--network",
            "metrolink",
            "--situations-enabled",
            "true",
        ])
        .unwrap();
        metrolink_config.check_against(&metrolink).unwrap();
    }

    #[test]
    fn the_loop_selector_defaults_to_both_and_checks_its_partners() {
        let parse = |args: &[&str]| {
            Config::try_parse_from([&["p", "--network", "metrolink"], args].concat()).unwrap()
        };
        let metrolink = crate::network::NetworkConfig::load("metrolink").unwrap();
        let both = parse(&[]);
        assert_eq!(both.loops, Loops::Both);
        assert!(both.loops.timetable() && both.loops.situations());
        let timetable = parse(&["--loops", "timetable"]);
        assert!(timetable.loops.timetable() && !timetable.loops.situations());
        timetable.check_against(&metrolink).unwrap();
        // Situations alone without the situations loop on runs nothing.
        let err = parse(&["--loops", "situations"])
            .check_against(&metrolink)
            .unwrap_err();
        assert!(err.to_string().contains("no loop"), "{err}");
        let situations = parse(&["--loops", "situations", "--situations-enabled", "true"]);
        assert!(!situations.loops.timetable() && situations.loops.situations());
        situations.check_against(&metrolink).unwrap();
        // Once mode is one timetable cycle.
        timetable.check_once_mode(true).unwrap();
        both.check_once_mode(false).unwrap();
        assert!(both.check_once_mode(true).is_err());
        assert!(situations.check_once_mode(true).is_err());
        assert!(Config::try_parse_from(["p", "--network", "metrolink", "--loops", "x"]).is_err());
    }

    #[test]
    fn only_fractions_between_zero_and_one_parse() {
        assert_eq!(parse_fraction("0.3"), Ok(0.3));
        for bad in ["-0.1", "1.5", "NaN", ""] {
            assert!(parse_fraction(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_refresh_token_is_short_and_plain() {
        for ok in ["2026-10-15", "2", "after-fix_1.0:a"] {
            let config =
                Config::try_parse_from(["p", "--network", "metrolink", "--timetable-refresh", ok])
                    .unwrap();
            assert_eq!(config.timetable_refresh, ok);
        }
        for bad in ["has space", "x/y", &"a".repeat(65)] {
            assert!(
                Config::try_parse_from(["p", "--network", "metrolink", "--timetable-refresh", bad])
                    .is_err(),
                "{bad}"
            );
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
