//! `NaPTAN` names (transit spec §6.3): the access nodes of the network's
//! area, as CSV (`naptan.api.dft.gov.uk`, OGL, no key). A station takes
//! its `ShortCommonName` (`Piccadilly`), a platform its `Indicator`
//! (`To Ashton`), each cleaned by the network file's `stop_name_suffixes`
//! and `indicator_prefixes` (NET's `Basford Tram Stop`, `NET NE-bound`).
//! A stop the source gave no coordinates (TXC) takes `NaPTAN`'s. A failed
//! fetch never blocks a timetable: the source names (suffix removed) stay,
//! and the failure is counted
//! (`transit_enrichment_failed_total{source="naptan"}`).

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use common::transit::StopKind;

use crate::model::TransitModel;
use crate::network::NetworkConfig;

/// The `NaPTAN` download's cap (area 940 was 618 KB on 2026-10-09).
pub(crate) const MAX_BYTES: u64 = 16 * 1024 * 1024;

/// One access node.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Node {
    pub common_name: String,
    pub short_name: String,
    pub indicator: String,
    pub stop_type: String,
    pub active: bool,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
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
    let optional = |name: &str| {
        headers
            .iter()
            .position(|h| h.trim_start_matches('\u{feff}') == name)
    };
    let (lat, lon) = (optional("Latitude"), optional("Longitude"));
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
        let coordinate = |at: Option<usize>| {
            at.and_then(|i| record.get(i))
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| v.is_finite())
        };
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
                lat: coordinate(lat),
                lon: coordinate(lon),
            },
        );
    }
    if nodes.is_empty() {
        bail!("no {prefix} rows");
    }
    Ok(nodes)
}

/// Applies `nodes` to `model`'s stops: a station's name becomes its
/// `ShortCommonName` (else its `CommonName`, cleaned by the network file),
/// a platform's indicator its `Indicator` (cleaned), and a stop with no
/// coordinates takes the node's. The stops changed.
pub(crate) fn apply(model: &mut TransitModel, nodes: &Nodes, network: &NetworkConfig) -> usize {
    let mut changed = 0;
    for stop in &mut model.stops {
        let Some(node) = nodes.get(&stop.stop_id) else {
            continue;
        };
        let mut touched = false;
        match stop.kind {
            StopKind::Station => {
                let name = if node.short_name.is_empty() {
                    network.clean_stop_name(&node.common_name)
                } else {
                    node.short_name.clone()
                };
                if !name.is_empty() {
                    stop.name = name;
                    touched = true;
                }
            }
            StopKind::Platform => {
                let indicator = network.clean_indicator(&node.indicator);
                if !indicator.is_empty() {
                    stop.indicator = Some(indicator);
                    touched = true;
                }
            }
        }
        if stop.lat.is_none()
            && stop.lon.is_none()
            && let (Some(lat), Some(lon)) = (node.lat, node.lon)
        {
            stop.lat = Some(lat);
            stop.lon = Some(lon);
            touched = true;
        }
        if touched {
            changed += 1;
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
        assert_eq!(apply(&mut m, &nodes, &metrolink()), 2);
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
        apply(&mut m, &nodes, &metrolink());
        assert_eq!(m.stops[0].name, "Piccadilly");
    }

    fn metrolink() -> NetworkConfig {
        NetworkConfig::load("metrolink").unwrap()
    }

    /// NET's rows, copied from `NaPTAN` area 940 (2026-10-24): no
    /// `ShortCommonName`, a ` Tram Stop` suffix and `NET` indicators.
    const NET_ROWS: &str = "ATCOCode,CommonName,ShortCommonName,Indicator,StopType,Status,Longitude,Latitude\n\
        9400ZZNOBCF,Beaconsfield Street Tram Stop,,,MET,active,-1.17079292361,52.97023327015\n\
        9400ZZNOBCF1,Beaconsfield Street Tram Stop,,NET15,PLT,active,-1.17039420086,52.97005968923\n\
        9400ZZNOBFD1,Basford Tram Stop,,NET NE-bound,PLT,active,-1.1797,52.9786\n";

    #[test]
    fn net_names_lose_their_suffix_and_prefix_and_gain_coordinates() {
        let net = NetworkConfig::load("net").unwrap();
        let nodes = parse(NET_ROWS, "9400ZZNO").unwrap();
        let mut m = model(vec![
            stop("9400ZZNOBCF", StopKind::Station),
            stop("9400ZZNOBCF1", StopKind::Platform),
            stop("9400ZZNOBFD1", StopKind::Platform),
        ]);
        assert_eq!(apply(&mut m, &nodes, &net), 3);
        assert_eq!(m.stops[0].name, "Beaconsfield Street");
        assert_eq!(m.stops[0].lat, Some(52.970_233_270_15));
        assert_eq!(m.stops[0].lon, Some(-1.170_792_923_61));
        assert_eq!(m.stops[1].indicator.as_deref(), Some("NET15"));
        assert_eq!(m.stops[2].indicator.as_deref(), Some("NE-bound"));
        // A source's own coordinates are kept.
        let mut kept = model(vec![stop("9400ZZNOBCF", StopKind::Station)]);
        kept.stops[0].lat = Some(1.0);
        kept.stops[0].lon = Some(2.0);
        apply(&mut kept, &nodes, &net);
        assert_eq!(
            (kept.stops[0].lat, kept.stops[0].lon),
            (Some(1.0), Some(2.0))
        );
    }

    #[test]
    fn a_file_without_the_networks_rows_is_an_error() {
        assert!(parse(FIXTURE, "9400ZZNE").is_err());
        assert!(parse("a,b\n1,2\n", "9400").is_err());
    }
}
