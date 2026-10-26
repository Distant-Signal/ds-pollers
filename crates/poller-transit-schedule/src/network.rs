//! The per-network config files (transit spec §6.4):
//! `networks/<network>.toml`, compiled in with `include_str!`, so the
//! network list is fixed at build time and every file is checked by the
//! unit tests as well as at startup ([`NetworkConfig::validate`]).

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use common::transit::{NETWORKS, NetworkId, TransitAttribution, is_id};
use serde::Deserialize;

/// Every network file, by id. Must list exactly `common::transit::NETWORKS`
/// (a unit test checks it), which the writer's stream pinning reads.
const FILES: &[(&str, &str)] = &[
    ("metrolink", include_str!("../networks/metrolink.toml")),
    ("net", include_str!("../networks/net.toml")),
];

/// The timetable source adapter a network's file is written for
/// (`TRANSIT_ADAPTER` must match it).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AdapterKind {
    /// GTFS (`adapter::gtfs`): BODS, Metrolink.
    #[default]
    Gtfs,
    /// `TransXChange` (`adapter::txc`): TNDS, Nottingham NET.
    Txc,
}

impl AdapterKind {
    /// The `TRANSIT_ADAPTER` value, and the `source` label of
    /// `transit_feed_fetch_total`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Gtfs => "gtfs",
            Self::Txc => "txc",
        }
    }
}

/// One network's config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NetworkConfig {
    pub id: String,
    pub name: String,
    /// Display only (`tram`); not a `SUPPORTED_MODES` value (D4).
    pub mode: String,
    pub timezone: String,
    /// The source adapter this file's line names are for (`gtfs` when
    /// absent).
    #[serde(default)]
    pub adapter: AdapterKind,
    /// The National Operator Codes whose routes are this network: GTFS
    /// `agency_noc`, TXC `NationalOperatorCode` (or `OperatorCode`).
    pub nocs: Vec<String>,
    /// Route names (GTFS `route_short_name`, TXC `LineName`) whose trips
    /// are replacement buses (Q3: on boards, labelled as buses; in no
    /// line).
    pub replacement_bus_route_names: Vec<String>,
    /// The `NaPTAN` `atcoAreaCodes` value of the network's stops.
    pub naptan_area: String,
    /// The ATCO code prefix of every station and platform.
    pub atco_prefix: String,
    /// Suffixes removed from stop names and headsigns, after the
    /// ` (<name>)` one (`NaPTAN` names NET's stops `Basford Tram Stop`).
    #[serde(default)]
    pub stop_name_suffixes: Vec<String>,
    /// Prefixes removed from `NaPTAN` platform indicators (`NET NE-bound`).
    #[serde(default)]
    pub indicator_prefixes: Vec<String>,
    pub attribution: TransitAttribution,
    pub situations: SituationFilter,
    pub lines: Vec<LineConfig>,
}

/// Which SIRI-SX situations are this network's (spec §10.1).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SituationFilter {
    pub participant_refs: Vec<String>,
    pub vehicle_modes: Vec<String>,
    pub operator_refs: Vec<String>,
}

/// One line.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LineConfig {
    /// The local id ([`is_id`]); public id `<network>:<id>`.
    pub id: String,
    pub name: String,
    /// The GTFS `route_short_name`s that are this line (a `gtfs`
    /// network). A route of the network matching no line and no
    /// replacement-bus name refuses the feed (`unmapped_route`): a new line
    /// is a config change.
    #[serde(default)]
    pub gtfs_route_names: Vec<String>,
    /// The TXC `LineName`s that are this line (a `txc` network), refused
    /// in the same way when unmapped.
    #[serde(default)]
    pub txc_line_names: Vec<String>,
    /// The SIRI-SX `LineRef` aliases of this line.
    pub siri_line_refs: Vec<String>,
    /// Display colour, `#rrggbb`.
    pub colour: String,
}

impl NetworkConfig {
    /// The compiled-in network `id`, parsed and validated.
    pub(crate) fn load(id: &str) -> Result<Self> {
        let Some((_, text)) = FILES.iter().find(|(name, _)| *name == id) else {
            bail!(
                "unknown transit network {id:?}: this build knows {}",
                FILES.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
            );
        };
        let config: Self =
            toml::from_str(text).with_context(|| format!("networks/{id}.toml does not parse"))?;
        config
            .validate(id)
            .with_context(|| format!("networks/{id}.toml is invalid"))?;
        Ok(config)
    }

    pub(crate) fn network_id(&self) -> NetworkId {
        // `validate` checked the grammar.
        NetworkId::parse(&self.id).unwrap_or_else(|_| unreachable!("validated network id"))
    }

    /// The checks of spec §6.4: the id is the file's name, a known network
    /// and in the grammar; every line has a valid unique id, a GTFS route
    /// name no other line or replacement-bus name claims, and a colour.
    pub(crate) fn validate(&self, file_name: &str) -> Result<()> {
        ensure!(
            self.id == file_name,
            "id {:?} is not the file's name {file_name:?}",
            self.id
        );
        ensure!(is_id(&self.id), "id {:?} is not [a-z0-9-]+", self.id);
        ensure!(
            NETWORKS.contains(&self.id.as_str()),
            "{:?} is not in common::transit::NETWORKS",
            self.id
        );
        ensure!(
            self.timezone == "Europe/London",
            "timezone {:?}: only Europe/London is supported",
            self.timezone
        );
        ensure!(!self.nocs.is_empty(), "no NOC");
        ensure!(!self.name.trim().is_empty(), "no name");
        ensure!(
            !self.atco_prefix.is_empty()
                && self
                    .atco_prefix
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
            "atco_prefix {:?}",
            self.atco_prefix
        );
        ensure!(!self.lines.is_empty(), "no line");
        for affix in self
            .stop_name_suffixes
            .iter()
            .chain(&self.indicator_prefixes)
        {
            ensure!(!affix.trim().is_empty(), "an empty name suffix or prefix");
        }
        let mut ids = BTreeSet::new();
        let mut routes: BTreeSet<&str> = self
            .replacement_bus_route_names
            .iter()
            .map(String::as_str)
            .collect();
        let mut refs = BTreeSet::new();
        for line in &self.lines {
            ensure!(is_id(&line.id), "line id {:?} is not [a-z0-9-]+", line.id);
            ensure!(ids.insert(line.id.as_str()), "line {:?} twice", line.id);
            let (names, other, kind) = match self.adapter {
                AdapterKind::Gtfs => (&line.gtfs_route_names, &line.txc_line_names, "GTFS route"),
                AdapterKind::Txc => (&line.txc_line_names, &line.gtfs_route_names, "TXC line"),
            };
            ensure!(!names.is_empty(), "line {:?} has no {kind} name", line.id);
            ensure!(
                other.is_empty(),
                "line {:?} has names for the other adapter (this file is {})",
                line.id,
                self.adapter.as_str()
            );
            for route in names {
                ensure!(
                    routes.insert(route.as_str()),
                    "{kind} name {route:?} claimed twice"
                );
            }
            for siri in &line.siri_line_refs {
                ensure!(refs.insert(siri.as_str()), "SIRI LineRef {siri:?} twice");
            }
            ensure!(
                is_colour(&line.colour),
                "line {:?} colour {:?} is not #rrggbb",
                line.id,
                line.colour
            );
        }
        Ok(())
    }

    /// The source route names (GTFS `route_short_name` or TXC `LineName`)
    /// of `line`, for this network's adapter.
    pub(crate) fn route_names<'a>(&self, line: &'a LineConfig) -> &'a [String] {
        match self.adapter {
            AdapterKind::Gtfs => &line.gtfs_route_names,
            AdapterKind::Txc => &line.txc_line_names,
        }
    }

    /// The index of the line `route_name` is, if any.
    pub(crate) fn line_for_route(&self, route_name: &str) -> Option<usize> {
        self.lines
            .iter()
            .position(|l| self.route_names(l).iter().any(|n| n == route_name))
    }

    /// A stop name or headsign without the ` (<network name>)` suffix
    /// (BODS's GTFS) or any of `stop_name_suffixes` (`NaPTAN`'s
    /// ` Tram Stop`). A name that is only a suffix is kept.
    pub(crate) fn clean_stop_name(&self, name: &str) -> String {
        let mut out = crate::adapter::gtfs::clean_name(name, &self.name);
        for suffix in &self.stop_name_suffixes {
            if let Some(rest) = out.strip_suffix(suffix.as_str())
                && !rest.trim().is_empty()
            {
                out = rest.trim_end().to_owned();
            }
        }
        out
    }

    /// A `NaPTAN` platform indicator without any of `indicator_prefixes`
    /// (`NET NE-bound` to `NE-bound`).
    pub(crate) fn clean_indicator(&self, indicator: &str) -> String {
        let trimmed = indicator.trim();
        for prefix in &self.indicator_prefixes {
            if let Some(rest) = trimmed.strip_prefix(prefix.as_str())
                && !rest.trim().is_empty()
            {
                return rest.trim().to_owned();
            }
        }
        trimmed.to_owned()
    }
}

fn is_colour(value: &str) -> bool {
    value.len() == 7 && value.starts_with('#') && value[1..].bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_network_file_is_valid_and_listed() {
        let files: Vec<&str> = FILES.iter().map(|(n, _)| *n).collect();
        assert_eq!(files, NETWORKS);
        for (id, _) in FILES {
            let config = NetworkConfig::load(id).unwrap();
            assert_eq!(config.network_id().as_str(), *id);
        }
    }

    #[test]
    fn metrolink_has_its_seven_lines() {
        let config = NetworkConfig::load("metrolink").unwrap();
        let ids: Vec<&str> = config.lines.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(
            ids,
            ["blue", "green", "navy", "pink", "purple", "red", "yellow"]
        );
        assert_eq!(config.nocs, ["METL"]);
        assert_eq!(config.replacement_bus_route_names, ["Replacement Bus"]);
    }

    #[test]
    fn net_is_a_txc_network_of_two_lines() {
        let config = NetworkConfig::load("net").unwrap();
        assert_eq!(config.adapter, AdapterKind::Txc);
        assert_eq!(config.nocs, ["NEXT"]);
        assert_eq!(config.atco_prefix, "9400ZZNO");
        let ids: Vec<&str> = config.lines.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(config.line_for_route("1"), Some(0));
        assert_eq!(config.line_for_route("Line 2"), Some(1));
        assert_eq!(config.line_for_route("3"), None);
        // No SIRI-SX participant publishes NET (NET design §2.3).
        assert!(config.situations.participant_refs.is_empty());
        assert_eq!(config.clean_stop_name("Basford Tram Stop"), "Basford");
        assert_eq!(config.clean_stop_name("Tram Stop"), "Tram Stop");
        assert_eq!(config.clean_indicator("NET NE-bound"), "NE-bound");
        assert_eq!(config.clean_indicator("Platform 2"), "Platform 2");
        assert!(
            config
                .attribution
                .text
                .contains("Traveline National Dataset")
        );
    }

    #[test]
    fn metrolink_keeps_its_gtfs_names_and_suffix() {
        let config = metrolink();
        assert_eq!(config.adapter, AdapterKind::Gtfs);
        assert_eq!(config.line_for_route("Blue Line"), Some(0));
        assert_eq!(
            config.clean_stop_name("Piccadilly (Manchester Metrolink)"),
            "Piccadilly"
        );
        assert_eq!(config.clean_indicator(" To Ashton "), "To Ashton");
    }

    #[test]
    fn names_for_the_other_adapter_are_refused() {
        let mut gtfs = metrolink();
        gtfs.lines[0].txc_line_names = vec!["1".into()];
        assert!(gtfs.validate("metrolink").is_err());
        let mut txc = NetworkConfig::load("net").unwrap();
        txc.lines[0].txc_line_names.clear();
        assert!(txc.validate("net").is_err());
        let mut twice = NetworkConfig::load("net").unwrap();
        twice.lines[1].txc_line_names = vec!["1".into()];
        assert!(twice.validate("net").is_err());
        let mut empty = NetworkConfig::load("net").unwrap();
        empty.stop_name_suffixes = vec![" ".into()];
        assert!(empty.validate("net").is_err());
    }

    #[test]
    fn an_unknown_network_refuses_to_start() {
        let err = NetworkConfig::load("supertram").unwrap_err();
        assert!(
            format!("{err:#}").contains("unknown transit network"),
            "{err:#}"
        );
    }

    fn metrolink() -> NetworkConfig {
        NetworkConfig::load("metrolink").unwrap()
    }

    #[test]
    fn a_file_whose_id_is_not_its_name_fails() {
        let err = metrolink().validate("tram").unwrap_err();
        assert!(err.to_string().contains("not the file's name"), "{err}");
    }

    #[test]
    fn bad_lines_fail_validation() {
        let mut twice = metrolink();
        twice.lines[1].gtfs_route_names = vec!["Blue Line".into()];
        assert!(twice.validate("metrolink").is_err());
        let mut no_route = metrolink();
        no_route.lines[0].gtfs_route_names.clear();
        assert!(no_route.validate("metrolink").is_err());
        let mut colour = metrolink();
        colour.lines[0].colour = "blue".into();
        assert!(colour.validate("metrolink").is_err());
        let mut id = metrolink();
        id.lines[0].id = "Blue".into();
        assert!(id.validate("metrolink").is_err());
        let mut bus = metrolink();
        bus.replacement_bus_route_names = vec!["Red Line".into()];
        assert!(bus.validate("metrolink").is_err());
    }

    #[test]
    fn unknown_fields_are_refused() {
        let text = FILES[0].1.replace("nocs =", "operators = [\"x\"]\nnocs =");
        assert!(toml::from_str::<NetworkConfig>(&text).is_err());
    }
}
