//! `NaPTAN` names (transit spec §6.3): the access nodes of the network's
//! area, as CSV (`naptan.api.dft.gov.uk`, OGL, no key). A station takes
//! its `ShortCommonName` (`Piccadilly`), a platform its `Indicator`
//! (`To Ashton`). A failed fetch never blocks a timetable: the GTFS names
//! (suffix removed) stay, and the failure is counted
//! (`transit_enrichment_failed_total{source="naptan"}`).

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use common::transit::StopKind;

use crate::model::TransitModel;

/// The `NaPTAN` download's cap (area 940 was 618 KB on 2026-10-09).
pub(crate) const MAX_BYTES: u64 = 16 * 1024 * 1024;

/// One access node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Node {
    pub common_name: String,
    pub short_name: String,
    pub indicator: String,
    pub stop_type: String,
    pub active: bool,
}

/// Access nodes by ATCO code.
pub(crate) type Nodes = HashMap<String, Node>;

/// Parses the CSV, keeping the rows whose ATCO code starts with `prefix`.
pub(crate) fn parse(text: &str, prefix: &str) -> Result<Nodes> {
    let mut reader = csv::Reader::from_reader(text.as_bytes());
    let headers = reader.headers()?.clone();
    let at = |name: &str| {
        headers
            .iter()
            .position(|h| h.trim_start_matches('\u{feff}') == name)
            .with_context(|| format!("no {name} column"))
    };
    let (atco, common, short, indicator, stop_type, status) = (
        at("ATCOCode")?,
        at("CommonName")?,
        at("ShortCommonName")?,
        at("Indicator")?,
        at("StopType")?,
        at("Status")?,
    );
    let mut nodes = Nodes::new();
    for record in reader.records() {
        let record = record?;
        let get = |i: usize| record.get(i).unwrap_or("").trim().to_owned();
        let code = get(atco);
        if !code.starts_with(prefix) {
            continue;
        }
        nodes.insert(
            code,
            Node {
                common_name: get(common),
                short_name: get(short),
                indicator: get(indicator),
                stop_type: get(stop_type),
                active: get(status) == "active",
            },
        );
    }
    if nodes.is_empty() {
        bail!("no {prefix} rows");
    }
    Ok(nodes)
}

/// Applies `nodes` to `model`'s stops: a station's name becomes its
/// `ShortCommonName` (else its `CommonName` without the network suffix),
/// a platform's indicator its `Indicator`. The stops changed.
pub(crate) fn apply(model: &mut TransitModel, nodes: &Nodes, network_name: &str) -> usize {
    let mut changed = 0;
    for stop in &mut model.stops {
        let Some(node) = nodes.get(&stop.stop_id) else {
            continue;
        };
        match stop.kind {
            StopKind::Station => {
                let name = if node.short_name.is_empty() {
                    crate::adapter::gtfs::clean_name(&node.common_name, network_name)
                } else {
                    node.short_name.clone()
                };
                if !name.is_empty() {
                    stop.name = name;
                    changed += 1;
                }
            }
            StopKind::Platform => {
                if !node.indicator.is_empty() {
                    stop.indicator = Some(node.indicator.clone());
                    changed += 1;
                }
            }
        }
    }
    changed
}

/// Downloads the area's CSV.
pub(crate) async fn fetch(client: &reqwest::Client, url: &str) -> Result<String> {
    let mut response = client.get(url).send().await?.error_for_status()?;
    if response.content_length().is_some_and(|len| len > MAX_BYTES) {
        bail!("NaPTAN response over {MAX_BYTES} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        body.extend_from_slice(&chunk);
        if body.len() as u64 > MAX_BYTES {
            bail!("NaPTAN response over {MAX_BYTES} bytes");
        }
    }
    Ok(String::from_utf8(body)?)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use common::transit::{Horizon, TransitStop};

    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/naptan-940-mini.csv");

    fn stop(id: &str, kind: StopKind) -> TransitStop {
        TransitStop {
            stop_id: id.to_owned(),
            kind,
            parent_stop_id: None,
            name: "GTFS name".to_owned(),
            indicator: None,
            lat: None,
            lon: None,
            wheelchair: None,
        }
    }

    fn model(stops: Vec<TransitStop>) -> TransitModel {
        let day = NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        TransitModel {
            lines: vec![],
            stops,
            patterns: vec![],
            trips: vec![],
            service_days: vec![],
            horizon: Horizon { from: day, to: day },
            source_feed_version: None,
        }
    }

    #[test]
    fn names_and_indicators_are_applied() {
        let nodes = parse(FIXTURE, "9400ZZMA").unwrap();
        let pic = &nodes["9400ZZMAPIC"];
        assert_eq!(pic.short_name, "Piccadilly");
        assert_eq!(pic.stop_type, "MET");
        assert!(pic.active);
        let mut m = model(vec![
            stop("9400ZZMAPIC", StopKind::Station),
            stop("9400ZZMAAIR1", StopKind::Platform),
            stop("9400ZZMAXXX", StopKind::Station),
        ]);
        assert_eq!(apply(&mut m, &nodes, "Manchester Metrolink"), 2);
        assert_eq!(m.stops[0].name, "Piccadilly");
        assert_eq!(m.stops[1].indicator.as_deref(), Some("To Manchester"));
        // No NaPTAN row: the GTFS name stays.
        assert_eq!(m.stops[2].name, "GTFS name");
    }

    #[test]
    fn the_suffix_strip_is_the_fallback() {
        let mut nodes = parse(FIXTURE, "9400ZZMA").unwrap();
        nodes.get_mut("9400ZZMAPIC").unwrap().short_name.clear();
        let mut m = model(vec![stop("9400ZZMAPIC", StopKind::Station)]);
        apply(&mut m, &nodes, "Manchester Metrolink");
        assert_eq!(m.stops[0].name, "Piccadilly");
    }

    #[test]
    fn a_file_without_the_networks_rows_is_an_error() {
        assert!(parse(FIXTURE, "9400ZZNE").is_err());
        assert!(parse("a,b\n1,2\n", "9400").is_err());
    }
}
