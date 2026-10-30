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
//!
//! Running times (DS-206): each route's `GET /Line/{id}/Timetable/{first}/
//! to/{last}` lists the stopping patterns of the trains from its first stop
//! (`stationIntervals`), each stop with its minutes from there
//! (`timeToArrival`). Two stops one pattern calls at in turn give a sample
//! of the running time between them ([`StepSamples::add_timetable`]); the
//! samples of every timetable of a line, averaged, time each step of its
//! routes ([`StepSamples::step_seconds`]). `TfL`'s minutes are whole, so a
//! step is a minute out either way, but the steps of one pattern add up to
//! its whole time.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use common::tfl_routes::{MAX_STEP_SECONDS, MAX_STOPS, NAME_MAX_CHARS, TflLineRoute, is_line_id};
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
            step_seconds: None,
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

/// `GET /Line/{id}/Timetable/{from}/to/{to}` (`TimetableResponse`), the
/// parts the running times are read from.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimetableResponseJson {
    #[serde(default)]
    timetable: Option<TimetableJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimetableJson {
    #[serde(default)]
    departure_stop_id: Option<String>,
    #[serde(default)]
    routes: Vec<TimetableRouteJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimetableRouteJson {
    #[serde(default)]
    station_intervals: Vec<StationIntervalJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StationIntervalJson {
    #[serde(default)]
    intervals: Vec<IntervalJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IntervalJson {
    stop_id: String,
    time_to_arrival: f64,
}

/// Running-time samples between consecutive stops of a line's stopping
/// patterns, by `(line id, from stop, to stop)`: their sum in minutes and
/// how many.
#[derive(Debug, Default)]
pub(crate) struct StepSamples {
    by_step: BTreeMap<(String, String, String), (f64, u32)>,
}

impl StepSamples {
    /// Adds the samples of one of `line_id`'s timetables. A pattern that
    /// goes back in time, or a time that is not finite, is passed over.
    pub(crate) fn add_timetable(&mut self, line_id: &str, json: &str) -> Result<()> {
        let response: TimetableResponseJson = serde_json::from_str(json)
            .with_context(|| format!("TfL's {line_id} timetable is not JSON"))?;
        let Some(timetable) = response.timetable else {
            return Ok(());
        };
        let Some(first) = timetable.departure_stop_id.filter(|id| is_stop_id(id)) else {
            return Ok(());
        };
        for route in timetable.routes {
            for pattern in route.station_intervals {
                let mut previous = (first.as_str(), 0.0_f64);
                for interval in &pattern.intervals {
                    let at = interval.time_to_arrival;
                    if !at.is_finite() || at < previous.1 || !is_stop_id(&interval.stop_id) {
                        break;
                    }
                    if interval.stop_id != previous.0 {
                        let sample = self
                            .by_step
                            .entry((
                                line_id.to_owned(),
                                previous.0.to_owned(),
                                interval.stop_id.clone(),
                            ))
                            .or_default();
                        sample.0 += at - previous.1;
                        sample.1 += 1;
                    }
                    previous = (&interval.stop_id, at);
                }
            }
        }
        Ok(())
    }

    /// `route`'s running time from each stop to the next, in whole
    /// seconds, the mean of the samples; `None` for a step with none, and
    /// `None` in all when no step has one.
    pub(crate) fn step_seconds(&self, route: &TflLineRoute) -> Option<Vec<Option<u32>>> {
        let steps: Vec<Option<u32>> = route
            .stop_ids
            .windows(2)
            .map(|pair| {
                let key = (route.line_id.clone(), pair[0].clone(), pair[1].clone());
                let &(sum, count) = self.by_step.get(&key)?;
                let seconds = (sum / f64::from(count) * 60.0).round();
                // Whole minutes from `TfL`, so at most an hour's worth (the
                // check) of a small count: in range for a `u32`.
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "a non-negative whole number of seconds, at most an hour"
                )]
                let seconds = seconds as u32;
                (seconds <= MAX_STEP_SECONDS).then_some(seconds)
            })
            .collect();
        steps.iter().any(Option::is_some).then_some(steps)
    }
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
                    step_seconds: None,
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
                    step_seconds: None,
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

    /// Hand-written in the shape of `GET /Line/testline/Timetable/
    /// 940GZZLUXBA/to/940GZZLUXTC` (`TimetableResponse`): three stopping
    /// patterns from `XBA`, one short of `XTB` and one that misses `XTB`
    /// out (so `XBA` to `XTC` in turn, not a step of the route), and a
    /// broken one. Every id is invented.
    const TIMETABLE_JSON: &str = r#"{
      "$type": "Tfl.Api.Presentation.Entities.TimetableResponse, Tfl.Api.Presentation.Entities",
      "lineId": "testline",
      "direction": "outbound",
      "timetable": {
        "departureStopId": "940GZZLUXBA",
        "routes": [{
          "stationIntervals": [
            {"id": "0", "intervals": [
              {"stopId": "940GZZLUXTB", "timeToArrival": 3.0},
              {"stopId": "940GZZLUXTC", "timeToArrival": 7.0}]},
            {"id": "1", "intervals": [
              {"stopId": "940GZZLUXTB", "timeToArrival": 4.0},
              {"stopId": "940GZZLUXTC", "timeToArrival": 7.0}]},
            {"id": "2", "intervals": [
              {"stopId": "940GZZLUXTC", "timeToArrival": 6.0}]},
            {"id": "3", "intervals": [
              {"stopId": "940GZZLUXTB", "timeToArrival": 2.0},
              {"stopId": "940GZZLUXTC", "timeToArrival": 1.0}]}
          ],
          "schedules": []
        }]
      }
    }"#;

    fn branch_a() -> TflLineRoute {
        TflLineRoute {
            line_id: "testline".into(),
            direction: "outbound".into(),
            name: "Branch A ↔ Trunk End".into(),
            stop_ids: vec![
                "940GZZLUXBA".into(),
                "940GZZLUXTB".into(),
                "940GZZLUXTC".into(),
                "940GZZLUXTD".into(),
            ],
            step_seconds: None,
        }
    }

    #[test]
    fn a_timetable_times_each_step_of_a_route() {
        let mut samples = StepSamples::default();
        samples.add_timetable("testline", TIMETABLE_JSON).unwrap();
        // XBA-XTB: 3, 4 and 2 (the broken pattern's first step is sound);
        // XTB-XTC: 4 and 3 (the broken one goes back in time); XTC-XTD:
        // none.
        assert_eq!(
            samples.step_seconds(&branch_a()),
            Some(vec![Some(180), Some(210), None])
        );
        // Another line's route has none of them.
        let other = TflLineRoute {
            line_id: "otherline".into(),
            ..branch_a()
        };
        assert_eq!(samples.step_seconds(&other), None);
    }

    #[test]
    fn a_timetable_without_patterns_adds_nothing_and_junk_is_an_error() {
        let mut samples = StepSamples::default();
        samples
            .add_timetable("testline", r#"{"lineId": "testline"}"#)
            .unwrap();
        samples
            .add_timetable(
                "testline",
                r#"{"timetable": {"departureStopId": "not a stop", "routes": []}}"#,
            )
            .unwrap();
        assert_eq!(samples.step_seconds(&branch_a()), None);
        assert!(samples.add_timetable("testline", "<html>").is_err());
    }
}
