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
const FILES: &[(&str, &str)] = &[("metrolink", include_str!("../networks/metrolink.toml"))];

/// One network's config.
#[derive(Debug, Clone, Deserialize)]
#[allow(
    dead_code,
    reason = "the GTFS, NaPTAN and SIRI-SX adapters (transit plan 2.2-2.4) read the rest"
)]
#[serde(deny_unknown_fields)]
pub(crate) struct NetworkConfig {
    pub id: String,
    pub name: String,
    /// Display only (`tram`); not a `SUPPORTED_MODES` value (D4).
    pub mode: String,
    pub timezone: String,
    /// The GTFS `agency_noc` values whose routes are this network.
    pub nocs: Vec<String>,
    /// GTFS `route_short_name`s whose trips are replacement buses (Q3:
    /// on boards, labelled as buses; in no line).
    pub replacement_bus_route_names: Vec<String>,
    /// The `NaPTAN` `atcoAreaCodes` value of the network's stops.
    pub naptan_area: String,
    /// The ATCO code prefix of every station and platform.
    pub atco_prefix: String,
    pub attribution: TransitAttribution,
    pub situations: SituationFilter,
    pub lines: Vec<LineConfig>,
}

/// Which SIRI-SX situations are this network's (spec §10.1).
#[derive(Debug, Clone, Deserialize)]
#[allow(
    dead_code,
    reason = "the GTFS, NaPTAN and SIRI-SX adapters (transit plan 2.2-2.4) read the rest"
)]
#[serde(deny_unknown_fields)]
pub(crate) struct SituationFilter {
    pub participant_refs: Vec<String>,
    pub vehicle_modes: Vec<String>,
    pub operator_refs: Vec<String>,
}

/// One line.
#[derive(Debug, Clone, Deserialize)]
#[allow(
    dead_code,
    reason = "the GTFS, NaPTAN and SIRI-SX adapters (transit plan 2.2-2.4) read the rest"
)]
#[serde(deny_unknown_fields)]
pub(crate) struct LineConfig {
    /// The local id ([`is_id`]); public id `<network>:<id>`.
    pub id: String,
    pub name: String,
    /// The GTFS `route_short_name`s that are this line. A route of the
    /// network matching no line and no replacement-bus name refuses the
    /// feed (`unmapped_route`): a new line is a config change.
    pub gtfs_route_names: Vec<String>,
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
            ensure!(
                !line.gtfs_route_names.is_empty(),
                "line {:?} has no GTFS route name",
                line.id
            );
            for route in &line.gtfs_route_names {
                ensure!(
                    routes.insert(route.as_str()),
                    "GTFS route name {route:?} claimed twice"
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
    fn an_unknown_network_refuses_to_start() {
        let err = NetworkConfig::load("net").unwrap_err();
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
