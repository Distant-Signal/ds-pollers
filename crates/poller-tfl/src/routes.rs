//! The daily route-sequences job's parsing (DS-151;
//! docs/superpowers/specs/2026-10-23-trips-plan-tfl-stop-ends-design.md §3):
//! `TfL`'s Tube, DLR and tram lines (`GET /Line/Mode/tube,dlr,tram`), then
//! each line's `GET /Line/{id}/Route/Sequence/{inbound,outbound}`, kept as
//! its `orderedLineRoutes`: every route end to end, its stops in running
//! order. Published as `tfl-line-routes/1` on `ds:ingest:tfl-stops`.
//!
//! The ordered routes, not the `stopPointSequences` branches: a route is
//! what one train runs (`Uxbridge ↔ Aldgate`), so "one route has both
//! stops, in this order" is exactly "no change of train", and two routes
//! that share a stop are a change there. The branches would need stitching
//! back together through their `nextBranchIds` to say the same.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use common::tfl_routes::{MAX_STOPS, NAME_MAX_CHARS, TflLineRoute, is_line_id};
use common::tfl_stops::{MODES, is_stop_id};
use serde::Deserialize;

/// One line of `GET /Line/Mode/{modes}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LineJson {
    id: String,
    #[serde(default)]
    mode_name: Option<String>,
}

/// The in-scope ([`MODES`]) line ids of `GET /Line/Mode/{modes}`, sorted,
/// each once.
pub(crate) fn parse_lines(json: &str) -> Result<Vec<String>> {
    let lines: Vec<LineJson> =
        serde_json::from_str(json).context("TfL's line list is not a JSON array of lines")?;
    let ids: BTreeSet<String> = lines
        .into_iter()
        .filter(|line| {
            line.mode_name
                .as_deref()
                .is_some_and(|mode| MODES.contains(&mode))
                && is_line_id(&line.id)
        })
        .map(|line| line.id)
        .collect();
    Ok(ids.into_iter().collect())
}

/// `GET /Line/{id}/Route/Sequence/{direction}` (`RouteSequence`), the
/// parts this job reads.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SequenceJson {
    #[serde(default)]
    ordered_line_routes: Vec<OrderedRouteJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderedRouteJson {
    #[serde(default)]
    name: String,
    #[serde(default)]
    naptan_ids: Vec<String>,
    #[serde(default)]
    service_type: Option<String>,
}

/// What [`parse_sequence`] kept and left out.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Parsed {
    pub routes: Vec<TflLineRoute>,
    /// Routes left out: a stop id that is not a `NaPTAN` stop area, fewer
    /// than two stops or more than [`MAX_STOPS`]. A route missing a stop
    /// would measure short, so it goes whole.
    pub invalid: usize,
}

/// The `Regular` routes of one line's sequence in `direction` (`Night`
/// routes are left out: the Night Tube is not modelled). A stop repeated
/// back to back is kept once.
pub(crate) fn parse_sequence(line_id: &str, direction: &str, json: &str) -> Result<Parsed> {
    let sequence: SequenceJson = serde_json::from_str(json)
        .with_context(|| format!("TfL's {line_id} {direction} route sequence is not JSON"))?;
    let mut parsed = Parsed::default();
    for route in sequence.ordered_line_routes {
        if route
            .service_type
            .as_deref()
            .is_some_and(|kind| !kind.eq_ignore_ascii_case("regular"))
        {
            continue;
        }
        let mut stop_ids: Vec<String> = Vec::with_capacity(route.naptan_ids.len());
        for id in route.naptan_ids {
            if stop_ids.last() != Some(&id) {
                stop_ids.push(id);
            }
        }
        if stop_ids.len() < 2
            || stop_ids.len() > MAX_STOPS
            || !stop_ids.iter().all(|id| is_stop_id(id))
        {
            parsed.invalid += 1;
            continue;
        }
        parsed.routes.push(TflLineRoute {
            line_id: line_id.to_owned(),
            direction: direction.to_owned(),
            name: route_name(&route.name),
            stop_ids,
        });
    }
    Ok(parsed)
}

/// `TfL`'s route name with its HTML entities decoded and its whitespace
/// collapsed: `Uxbridge  &harr;  Aldgate ` -> `Uxbridge ↔ Aldgate`.
fn route_name(raw: &str) -> String {
    let decoded = raw.replace("&harr;", "↔").replace("&amp;", "&");
    decoded
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(NAME_MAX_CHARS)
        .collect()
}

/// `routes` with exact repeats (same line, direction and stops) dropped,
/// in a stable order.
pub(crate) fn dedup(mut routes: Vec<TflLineRoute>) -> Vec<TflLineRoute> {
    let mut seen = BTreeSet::new();
    routes.retain(|route| {
        seen.insert((
            route.line_id.clone(),
            route.direction.clone(),
            route.stop_ids.clone(),
        ))
    });
    routes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written in the shape of `GET /Line/Mode/tube,dlr,tram`. The
    /// line ids are invented.
    const LINES_JSON: &str = r#"[
      {"$type": "Tfl.Api.Presentation.Entities.Line, Tfl.Api.Presentation.Entities",
       "id": "testline", "name": "Testline", "modeName": "tube", "lineStatuses": []},
      {"id": "test-city", "name": "Test & City", "modeName": "tube"},
      {"id": "testlight", "name": "Testlight", "modeName": "dlr"},
      {"id": "testline", "name": "Testline", "modeName": "tube"},
      {"id": "testbus", "name": "Testbus", "modeName": "bus"},
      {"id": "Bad Id", "name": "Bad", "modeName": "tube"},
      {"id": "nomode", "name": "No mode"}
    ]"#;

    /// Hand-written in the shape of `GET /Line/testline/Route/Sequence/
    /// outbound` (`RouteSequence`): a line with a trunk (`XTA` to `XTC`)
    /// and two branches (`XBA`, `XBB`) that meet at `XTB`. Every id is
    /// invented (`940GZZLUX..`), so nothing here is `TfL` data.
    const SEQUENCE_JSON: &str = r#"{
      "$type": "Tfl.Api.Presentation.Entities.RouteSequence, Tfl.Api.Presentation.Entities",
      "lineId": "testline",
      "lineName": "Testline",
      "direction": "outbound",
      "isOutboundOnly": false,
      "mode": "tube",
      "lineStrings": ["[[[-0.1,51.5],[-0.2,51.6]]]"],
      "stations": [],
      "stopPointSequences": [
        {"lineId": "testline", "branchId": 0, "nextBranchIds": [2], "prevBranchIds": [],
         "direction": "outbound", "serviceType": "Regular",
         "stopPoint": [{"id": "940GZZLUXBA", "name": "Branch A Underground Station"}]}
      ],
      "orderedLineRoutes": [
        {"$type": "Tfl.Api.Presentation.Entities.OrderedRoute, Tfl.Api.Presentation.Entities",
         "name": "Branch A  &harr;  Trunk End ",
         "naptanIds": ["940GZZLUXBA", "940GZZLUXTB", "940GZZLUXTB", "940GZZLUXTC"],
         "serviceType": "Regular"},
        {"name": "Branch B  &harr;  Trunk End ",
         "naptanIds": ["940GZZLUXBB", "940GZZLUXTB", "940GZZLUXTC"],
         "serviceType": "Regular"},
        {"name": "Night  &harr;  Trunk End ",
         "naptanIds": ["940GZZLUXBB", "940GZZLUXTC"],
         "serviceType": "Night"},
        {"name": "Broken",
         "naptanIds": ["940GZZLUXBB", "not a stop", "940GZZLUXTC"],
         "serviceType": "Regular"},
        {"name": "Too short", "naptanIds": ["940GZZLUXBB"], "serviceType": "Regular"}
      ]
    }"#;

    #[test]
    fn the_line_list_keeps_in_scope_lines_once() {
        assert_eq!(
            parse_lines(LINES_JSON).unwrap(),
            ["test-city", "testlight", "testline"]
        );
        assert!(parse_lines("{}").is_err());
    }

    #[test]
    fn a_sequence_gives_its_regular_routes_in_running_order() {
        let parsed = parse_sequence("testline", "outbound", SEQUENCE_JSON).unwrap();
        assert_eq!(parsed.invalid, 2, "the broken and the one-stop routes");
        assert_eq!(
            parsed.routes,
            [
                TflLineRoute {
                    line_id: "testline".into(),
                    direction: "outbound".into(),
                    name: "Branch A ↔ Trunk End".into(),
                    // The repeated stop is kept once.
                    stop_ids: vec![
                        "940GZZLUXBA".into(),
                        "940GZZLUXTB".into(),
                        "940GZZLUXTC".into()
                    ],
                },
                TflLineRoute {
                    line_id: "testline".into(),
                    direction: "outbound".into(),
                    name: "Branch B ↔ Trunk End".into(),
                    stop_ids: vec![
                        "940GZZLUXBB".into(),
                        "940GZZLUXTB".into(),
                        "940GZZLUXTC".into()
                    ],
                },
            ]
        );
    }

    #[test]
    fn a_sequence_without_routes_is_empty_and_junk_is_an_error() {
        let parsed = parse_sequence("testline", "inbound", r#"{"lineId": "testline"}"#).unwrap();
        assert_eq!(parsed, Parsed::default());
        assert!(parse_sequence("testline", "inbound", "<html>").is_err());
    }

    #[test]
    fn repeated_routes_are_kept_once() {
        let parsed = parse_sequence("testline", "outbound", SEQUENCE_JSON).unwrap();
        let mut twice = parsed.routes.clone();
        twice.extend(parsed.routes.clone());
        assert_eq!(dedup(twice), parsed.routes);
    }
}
