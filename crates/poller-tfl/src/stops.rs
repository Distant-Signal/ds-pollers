//! The daily stops job (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md
//! §1): every Tube, DLR and tram stop from `TfL`'s `StopPoint` API, with
//! the National Rail station at the same interchange hub, published as
//! `tfl-stops/1` on `ds:ingest:tfl`.
//!
//! Two calls shapes, both parsed loosely (one odd stop point never fails
//! the run):
//!
//! - `GET /StopPoint/Mode/tube,dlr,tram?page=N`: a `StopPointsResponse`
//!   (`stopPoints`, `pageSize`, `total`, `page`), 1000 stop points a page,
//!   of every type (stations, platforms, entrances, hubs). A bare array is
//!   accepted too.
//! - `GET /StopPoint/{ids}` for the hubs (`HUBEUS,...`): an array, or one
//!   object for a single id, each hub with its `children`.
//!
//! Only stop areas (`940G…`) with an in-scope mode are kept; children are
//! walked, so a nested response gives the same stops as a flat one.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use common::tfl_stops::{MODES, NAME_MAX_CHARS, TflStop, TflStopLine, clean_stop_name, is_stop_id};
use serde::Deserialize;

/// `TfL`'s page size for `/StopPoint/Mode`.
pub(crate) const PAGE_SIZE: usize = 1000;

/// Pages read at most (20,000 stop points; the three modes have a few
/// thousand).
pub(crate) const MAX_PAGES: u32 = 20;

/// Hub ids per `/StopPoint/{ids}` call.
pub(crate) const HUB_BATCH: usize = 20;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StopPoint {
    #[serde(default)]
    pub naptan_id: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub common_name: Option<String>,
    #[serde(default)]
    pub modes: Vec<String>,
    #[serde(default)]
    pub lines: Vec<Line>,
    #[serde(default)]
    pub line_mode_groups: Vec<LineModeGroup>,
    #[serde(default)]
    pub hub_naptan_code: Option<String>,
    #[serde(default)]
    pub lat: Option<f64>,
    #[serde(default)]
    pub lon: Option<f64>,
    /// `false` for a stop `TfL` lists as closed.
    #[serde(default)]
    pub status: Option<bool>,
    /// Kept raw and parsed one by one, as the top level is.
    #[serde(default)]
    pub children: Vec<serde_json::Value>,
}

impl StopPoint {
    fn naptan(&self) -> Option<&str> {
        self.naptan_id.as_deref().or(self.id.as_deref())
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Line {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LineModeGroup {
    pub mode_name: String,
    #[serde(default)]
    pub line_identifier: Vec<String>,
}

/// One page of `/StopPoint/Mode`.
#[derive(Debug, Default)]
pub(crate) struct Page {
    pub stop_points: Vec<StopPoint>,
    /// Stop points on the page before any was skipped: the paging stops
    /// when this is under [`PAGE_SIZE`].
    pub raw_count: usize,
}

/// Parses a `/StopPoint/Mode` page (or a `/StopPoint/{ids}` body): a
/// `StopPointsResponse`, a bare array or a single stop point. A stop point
/// that does not parse is skipped and logged.
pub(crate) fn parse_page(json: &str) -> Result<Page> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let raw: Vec<serde_json::Value> = match value {
        serde_json::Value::Array(items) => items,
        serde_json::Value::Object(mut object) => match object.remove("stopPoints") {
            Some(serde_json::Value::Array(items)) => items,
            Some(_) => bail!("stopPoints is not an array"),
            None if object.contains_key("naptanId") || object.contains_key("id") => {
                vec![serde_json::Value::Object(object)]
            }
            None => bail!("neither a StopPointsResponse nor a stop point"),
        },
        _ => bail!("neither a StopPointsResponse nor a stop point"),
    };
    let raw_count = raw.len();
    Ok(Page {
        stop_points: parse_points(raw),
        raw_count,
    })
}

fn parse_points(raw: Vec<serde_json::Value>) -> Vec<StopPoint> {
    raw.into_iter()
        .filter_map(|value| match serde_json::from_value::<StopPoint>(value) {
            Ok(point) => Some(point),
            Err(err) => {
                tracing::warn!(error = %err, "skipping a TfL stop point that does not parse");
                None
            }
        })
        .collect()
}

/// Every stop point in `points` and their children, depth first.
fn flatten(points: Vec<StopPoint>, out: &mut Vec<StopPoint>) {
    for mut point in points {
        let children = parse_points(std::mem::take(&mut point.children));
        out.push(point);
        flatten(children, out);
    }
}

/// The stops of the in-scope modes, by id, from every page's stop points
/// (hubs not yet applied). A stop seen twice keeps its first sighting.
pub(crate) fn collect_stops(pages: Vec<StopPoint>) -> BTreeMap<String, TflStop> {
    let mut flat = Vec::new();
    flatten(pages, &mut flat);
    let mut stops = BTreeMap::new();
    for point in flat {
        if let Some(stop) = to_stop(&point) {
            stops.entry(stop.id.clone()).or_insert(stop);
        }
    }
    stops
}

fn to_stop(point: &StopPoint) -> Option<TflStop> {
    let id = point.naptan()?.trim();
    if !id.starts_with("940G") || !is_stop_id(id) || point.status == Some(false) {
        return None;
    }
    let modes: Vec<String> = MODES
        .iter()
        .filter(|mode| point.modes.iter().any(|m| m == *mode))
        .map(|mode| (*mode).to_owned())
        .collect();
    if modes.is_empty() {
        return None;
    }
    let name = clean_stop_name(point.common_name.as_deref().unwrap_or_default());
    if name.is_empty() {
        return None;
    }
    let name: String = name.chars().take(NAME_MAX_CHARS).collect();
    Some(TflStop {
        id: id.to_owned(),
        name,
        lines: lines_in_modes(point, &modes),
        modes,
        hub_code: point
            .hub_naptan_code
            .as_deref()
            .map(str::trim)
            .filter(|hub| !hub.is_empty())
            .map(str::to_owned),
        lat: point.lat.filter(|lat| lat.is_finite()),
        lon: point.lon.filter(|lon| lon.is_finite()),
        crs: Vec::new(),
        rail_tiplocs: Vec::new(),
    })
}

/// The stop's lines that run in `modes`, by `lineModeGroups`; every listed
/// line when `TfL` sends no groups.
fn lines_in_modes(point: &StopPoint, modes: &[String]) -> Vec<TflStopLine> {
    let in_scope: Option<BTreeSet<&str>> = (!point.line_mode_groups.is_empty()).then(|| {
        point
            .line_mode_groups
            .iter()
            .filter(|group| modes.contains(&group.mode_name))
            .flat_map(|group| group.line_identifier.iter().map(String::as_str))
            .collect()
    });
    let mut seen = BTreeSet::new();
    point
        .lines
        .iter()
        .filter(|line| {
            in_scope
                .as_ref()
                .is_none_or(|ids| ids.contains(line.id.as_str()))
        })
        .filter(|line| seen.insert(line.id.clone()))
        .map(|line| TflStopLine {
            id: line.id.clone(),
            name: line.name.clone().unwrap_or_else(|| line.id.clone()),
        })
        .collect()
}

/// The hub codes the stops name, sorted.
pub(crate) fn hub_codes(stops: &BTreeMap<String, TflStop>) -> Vec<String> {
    stops
        .values()
        .filter_map(|stop| stop.hub_code.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// What a hub says about National Rail: its rail stations' `TIPLOCs`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct HubRail {
    pub tiplocs: BTreeSet<String>,
}

/// The rail `TIPLOCs` of every hub in `hubs` (a parsed `/StopPoint/{ids}`
/// body): each hub's `910G<TIPLOC>` children.
pub(crate) fn hub_rail(hubs: Vec<StopPoint>) -> BTreeMap<String, HubRail> {
    let mut out = BTreeMap::new();
    for mut hub in hubs {
        let Some(code) = hub.naptan().map(|code| code.trim().to_owned()) else {
            continue;
        };
        let mut flat = Vec::new();
        flatten(parse_points(std::mem::take(&mut hub.children)), &mut flat);
        let tiplocs = flat
            .iter()
            .filter_map(|child| child.naptan()?.trim().strip_prefix("910G"))
            .filter(|tiploc| {
                (1..=7).contains(&tiploc.len())
                    && tiploc
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
            .map(str::to_owned)
            .collect();
        out.insert(code, HubRail { tiplocs });
    }
    out
}

/// A hub code's CRS: `HUBEUS` -> `EUS`.
fn hub_crs(hub: &str) -> Option<String> {
    let letters = hub.strip_prefix("HUB")?;
    (letters.len() == 3 && letters.bytes().all(|b| b.is_ascii_uppercase()))
        .then(|| letters.to_owned())
}

/// Gives every stop at a hub with a National Rail station that station:
/// its `TIPLOCs`, and the hub code's CRS.
pub(crate) fn apply_hubs(stops: &mut BTreeMap<String, TflStop>, hubs: &BTreeMap<String, HubRail>) {
    for stop in stops.values_mut() {
        let Some(rail) = stop.hub_code.as_ref().and_then(|hub| hubs.get(hub)) else {
            continue;
        };
        if rail.tiplocs.is_empty() {
            continue;
        }
        stop.rail_tiplocs = rail.tiplocs.iter().cloned().collect();
        stop.crs = stop
            .hub_code
            .as_deref()
            .and_then(hub_crs)
            .into_iter()
            .collect();
    }
}

/// Why the feed guard refused a snapshot: the `reason` of
/// `distant_signal_tfl_stops_refused_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    Empty,
    SharpDrop { was: usize, now: usize },
}

impl Refusal {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::SharpDrop { .. } => "sharp_drop",
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("TfL returned no Tube, DLR or tram stops"),
            Self::SharpDrop { was, now } => {
                write!(
                    f,
                    "the stop count fell from {was} to {now}, past the drop limit"
                )
            }
        }
    }
}

/// The feed guard: refuses an empty snapshot, and one whose stop count
/// fell by more than `max_drop_fraction` from `baseline` (the last
/// published snapshot's count).
pub(crate) fn check(
    count: usize,
    baseline: Option<usize>,
    max_drop_fraction: f64,
) -> Result<(), Refusal> {
    if count == 0 {
        return Err(Refusal::Empty);
    }
    let Some(was) = baseline.filter(|was| *was > 0) else {
        return Ok(());
    };
    #[expect(clippy::cast_precision_loss, reason = "stop counts are a few hundred")]
    let dropped = (was.saturating_sub(count)) as f64 / was as f64;
    if dropped > max_drop_fraction {
        return Err(Refusal::SharpDrop { was, now: count });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written in the shape of `GET /StopPoint/Mode/tube,dlr,tram?page=1`
    /// (`StopPointsResponse`). Every id is invented (`ZZLUX..`, `HUBXA.`),
    /// so nothing here is `TfL` data.
    const PAGE_JSON: &str = r#"{
      "$type": "Tfl.Api.Presentation.Entities.StopPointsResponse, Tfl.Api.Presentation.Entities",
      "centrePoint": [],
      "stopPoints": [
        {
          "$type": "Tfl.Api.Presentation.Entities.StopPoint, Tfl.Api.Presentation.Entities",
          "naptanId": "940GZZLUXAA",
          "modes": ["bus", "tube"],
          "icsCode": "1000001",
          "stopType": "NaptanMetroStation",
          "stationNaptan": "940GZZLUXAA",
          "hubNaptanCode": "HUBXAA",
          "lines": [
            {"id": "testline", "name": "Testline", "uri": "/Line/testline", "type": "Line"},
            {"id": "99", "name": "99", "uri": "/Line/99", "type": "Line"}
          ],
          "lineModeGroups": [
            {"modeName": "bus", "lineIdentifier": ["99"]},
            {"modeName": "tube", "lineIdentifier": ["testline"]}
          ],
          "status": true,
          "id": "940GZZLUXAA",
          "commonName": "Aardvark Road Underground Station",
          "placeType": "StopPoint",
          "additionalProperties": [],
          "children": [
            {
              "naptanId": "9400ZZLUXAA1",
              "modes": ["tube"],
              "stopType": "NaptanMetroPlatform",
              "commonName": "Aardvark Road Underground Station",
              "lines": [], "lineModeGroups": [], "children": [],
              "lat": 51.5, "lon": -0.1
            }
          ],
          "lat": 51.5001,
          "lon": -0.1001
        },
        {
          "naptanId": "940GZZDLXBB",
          "modes": ["dlr"],
          "stopType": "NaptanMetroStation",
          "lines": [{"id": "dlr", "name": "DLR"}],
          "lineModeGroups": [{"modeName": "dlr", "lineIdentifier": ["dlr"]}],
          "status": true,
          "commonName": "Badger Quay DLR Station",
          "children": [],
          "lat": 51.51,
          "lon": -0.02
        },
        {
          "naptanId": "940GZZCRXCC",
          "modes": ["tram"],
          "stopType": "NaptanMetroStation",
          "lines": [{"id": "tram", "name": "Tram"}],
          "status": true,
          "commonName": "Coypu Lane Tram Stop",
          "children": [],
          "lat": 51.4,
          "lon": -0.08
        },
        {
          "naptanId": "940GZZLUXDD",
          "modes": ["tube"],
          "stopType": "NaptanMetroStation",
          "status": false,
          "commonName": "Dormouse Underground Station",
          "children": []
        },
        {
          "naptanId": "910GXEEEEE",
          "modes": ["national-rail"],
          "stopType": "NaptanRailStation",
          "commonName": "Eel Street Rail Station",
          "children": []
        },
        {
          "naptanId": "940GZZLUXFF",
          "modes": "not-a-list",
          "commonName": "Broken Underground Station"
        }
      ],
      "pageSize": 1000,
      "total": 6,
      "page": 1
    }"#;

    /// Hand-written in the shape of `GET /StopPoint/HUBXAA,HUBXGG`.
    const HUBS_JSON: &str = r#"[
      {
        "naptanId": "HUBXAA",
        "stopType": "TransportInterchange",
        "commonName": "Aardvark Road",
        "modes": ["bus", "national-rail", "tube"],
        "children": [
          {"naptanId": "940GZZLUXAA", "stopType": "NaptanMetroStation", "modes": ["tube"], "children": []},
          {"naptanId": "910GAARDVK", "stopType": "NaptanRailStation", "modes": ["national-rail"], "children": []},
          {"naptanId": "910GAARDV2", "stopType": "NaptanRailStation", "modes": ["overground"], "children": []},
          {"naptanId": "490000001A", "stopType": "NaptanPublicBusCoachTram", "modes": ["bus"], "children": []}
        ]
      },
      {
        "naptanId": "HUBXGG",
        "stopType": "TransportInterchange",
        "commonName": "Gopher Gate",
        "children": [
          {"naptanId": "940GZZLUXGG", "stopType": "NaptanMetroStation", "modes": ["tube"], "children": []}
        ]
      }
    ]"#;

    fn stops() -> BTreeMap<String, TflStop> {
        let page = parse_page(PAGE_JSON).unwrap();
        assert_eq!(page.raw_count, 6, "paging counts every stop point");
        collect_stops(page.stop_points)
    }

    #[test]
    fn keeps_the_in_scope_stations_with_clean_names_and_their_own_lines() {
        let stops = stops();
        let ids: Vec<&str> = stops.keys().map(String::as_str).collect();
        // No platform (9400...), no rail station (910G...), no closed stop,
        // and the malformed one skipped rather than failing the page.
        assert_eq!(ids, ["940GZZCRXCC", "940GZZDLXBB", "940GZZLUXAA"]);

        let tube = &stops["940GZZLUXAA"];
        assert_eq!(tube.name, "Aardvark Road");
        assert_eq!(tube.modes, ["tube"], "bus is out of scope");
        assert_eq!(
            tube.lines,
            [TflStopLine {
                id: "testline".into(),
                name: "Testline".into()
            }],
            "the bus route is not a Tube line"
        );
        assert_eq!(tube.hub_code.as_deref(), Some("HUBXAA"));
        assert_eq!((tube.lat, tube.lon), (Some(51.5001), Some(-0.1001)));

        assert_eq!(stops["940GZZDLXBB"].name, "Badger Quay");
        assert_eq!(stops["940GZZDLXBB"].modes, ["dlr"]);
        let tram = &stops["940GZZCRXCC"];
        assert_eq!(tram.name, "Coypu Lane");
        // No lineModeGroups: every listed line.
        assert_eq!(tram.lines.len(), 1);
        assert_eq!(tram.hub_code, None);
    }

    #[test]
    fn a_hub_with_a_rail_station_gives_its_stops_a_crs_and_tiplocs() {
        let mut stops = stops();
        assert_eq!(hub_codes(&stops), ["HUBXAA"]);
        let hubs = hub_rail(parse_page(HUBS_JSON).unwrap().stop_points);
        assert_eq!(
            hubs["HUBXAA"].tiplocs.iter().collect::<Vec<_>>(),
            ["AARDV2", "AARDVK"]
        );
        assert!(hubs["HUBXGG"].tiplocs.is_empty());
        apply_hubs(&mut stops, &hubs);
        let tube = &stops["940GZZLUXAA"];
        assert_eq!(tube.crs, ["XAA"]);
        assert_eq!(tube.rail_tiplocs, ["AARDV2", "AARDVK"]);
        assert!(stops["940GZZDLXBB"].crs.is_empty());
    }

    #[test]
    fn a_hub_without_rail_gives_no_crs() {
        let mut stops = BTreeMap::new();
        stops.insert(
            "940GZZLUXGG".to_owned(),
            TflStop {
                id: "940GZZLUXGG".into(),
                name: "Gopher Gate".into(),
                modes: vec!["tube".into()],
                lines: vec![],
                hub_code: Some("HUBXGG".into()),
                lat: None,
                lon: None,
                crs: vec![],
                rail_tiplocs: vec![],
            },
        );
        apply_hubs(
            &mut stops,
            &hub_rail(parse_page(HUBS_JSON).unwrap().stop_points),
        );
        assert!(stops["940GZZLUXGG"].crs.is_empty());
    }

    #[test]
    fn a_single_hub_object_and_a_bare_array_both_parse() {
        let one = parse_page(r#"{"naptanId": "HUBXAA", "children": []}"#).unwrap();
        assert_eq!(one.stop_points.len(), 1);
        let bare =
            parse_page(r#"[{"naptanId": "940GZZLUXAA", "modes": ["tube"], "commonName": "A"}]"#)
                .unwrap();
        assert_eq!(collect_stops(bare.stop_points).len(), 1);
        assert!(parse_page("not json").is_err());
        assert!(parse_page(r#"{"message": "nope"}"#).is_err());
    }

    #[test]
    fn hub_codes_map_to_crs_only_when_shaped_like_one() {
        assert_eq!(hub_crs("HUBEUS").as_deref(), Some("EUS"));
        assert_eq!(hub_crs("HUBEU"), None);
        assert_eq!(hub_crs("HUBEUS1"), None);
        assert_eq!(hub_crs("940GZZLUEUS"), None);
    }

    #[test]
    fn the_feed_guard_refuses_empty_and_sharp_drops() {
        assert_eq!(check(0, None, 0.2), Err(Refusal::Empty));
        assert_eq!(check(400, None, 0.2), Ok(()));
        assert_eq!(
            check(330, Some(400), 0.2),
            Ok(()),
            "a 17.5% drop is allowed"
        );
        assert_eq!(
            check(300, Some(400), 0.2),
            Err(Refusal::SharpDrop { was: 400, now: 300 })
        );
        assert_eq!(check(900, Some(400), 0.2), Ok(()), "growth is fine");
        assert_eq!(Refusal::Empty.reason(), "empty");
        assert_eq!(Refusal::SharpDrop { was: 1, now: 0 }.reason(), "sharp_drop");
    }
}
