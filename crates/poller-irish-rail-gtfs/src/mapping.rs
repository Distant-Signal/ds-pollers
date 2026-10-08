//! Maps a parsed `gtfs_structures::Gtfs` feed onto
//! `common::island_of_ireland::{IslandOfIrelandStation, IslandOfIrelandLineDefinition}`.
//!
//! Field provenance, confirmed against docs.rs/gtfs-structures/latest
//! (v0.50.0) directly in this crate's planning pass:
//! - `Gtfs.stops: HashMap<String, Arc<Stop>>`, `Gtfs.routes: HashMap<String, Route>`,
//!   `Gtfs.trips: HashMap<String, Trip>`.
//! - `Stop.id: String`, `Stop.name: Option<String>`, `Stop.latitude`/`longitude: Option<f64>`,
//!   `Stop.parent_station: Option<String>`.
//! - `Route.id: String`, `Route.long_name`/`short_name: Option<String>` (both optional).
//! - `Trip.id: String`, `Trip.route_id: String`, `Trip.stop_times: Vec<StopTime>`.
//! - `StopTime.stop: Arc<Stop>`, `StopTime.stop_sequence: u32`.
//!
//! Every Iarnród Éireann row is tagged `RepublicOfIreland` unconditionally
//! -- no border-station filtering. Design spec §4 already decided this
//! (Iarnród Éireann is the sole source for the Belfast-area stations/the
//! Enterprise line), and the friction doc (§4) confirms GTFS's own
//! `stops.txt` already contains no NIR-side signalling junctions (those
//! only appear in the live API's `getAllStationsXML`) -- so there is
//! nothing to filter out even if this crate wanted to.
//!
//! The per-item sanity checks (GitHub issue #1's evaluation, done in-poller
//! instead of adding gtfs-analyzer) live here too: see [`FeedIssue`] for
//! what is repaired or dropped, and `feed_guard` for the whole-feed
//! sharp-drop check.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;

use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
};
use gtfs_structures::{Gtfs, Stop};

/// A data-quality problem found while mapping one feed. Each is counted in
/// `distant_signal_gtfs_feed_issues_total{kind}` and summarised in one log
/// line per feed (see [`FeedIssues::record`]), never one line per item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FeedIssue {
    /// A route with no trips at all: dropped (it would be a line with no
    /// stations).
    RouteWithoutTrips,
    /// A route whose longest trip has no stop times, or none left once
    /// dropped stops are removed: dropped for the same reason.
    RouteWithoutStops,
    /// A stop with a missing or blank name that took its parent station's
    /// name instead. Kept; counted so a feed-wide regression is visible.
    StopNameFromParent,
    /// A stop with a missing or blank name and no named parent station:
    /// dropped (a nameless station is useless in every UI that lists it).
    StopWithoutName,
    /// A stop with missing, partial or implausible coordinates: kept with
    /// both coordinates omitted.
    StopInvalidCoordinates,
    /// A line's reference to a dropped stop, removed from that line's
    /// station list so no line points at a station that was not published.
    LineStopDropped,
}

impl FeedIssue {
    /// The metric's `kind` label value.
    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::RouteWithoutTrips => "route_without_trips",
            Self::RouteWithoutStops => "route_without_stops",
            Self::StopNameFromParent => "stop_name_from_parent",
            Self::StopWithoutName => "stop_without_name",
            Self::StopInvalidCoordinates => "stop_invalid_coordinates",
            Self::LineStopDropped => "line_stop_dropped",
        }
    }
}

/// Per-feed tally of [`FeedIssue`]s.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct FeedIssues(BTreeMap<FeedIssue, u64>);

impl FeedIssues {
    fn add(&mut self, issue: FeedIssue) {
        *self.0.entry(issue).or_default() += 1;
    }

    /// How many times `issue` was seen in this feed.
    #[cfg(test)]
    pub(crate) fn count(&self, issue: FeedIssue) -> u64 {
        self.0.get(&issue).copied().unwrap_or_default()
    }

    /// Adds this feed's counts to `distant_signal_gtfs_feed_issues_total`
    /// and logs one summary line (only when there is anything to report).
    pub(crate) fn record(&self) {
        for (issue, count) in &self.0 {
            metrics::counter!(
                common::metrics::metric_name("gtfs_feed_issues_total"),
                "kind" => issue.kind()
            )
            .increment(*count);
        }
        if !self.0.is_empty() {
            let summary: Vec<String> = self
                .0
                .iter()
                .map(|(issue, count)| format!("{}={count}", issue.kind()))
                .collect();
            tracing::warn!(
                issues = %summary.join(" "),
                "GTFS feed had data-quality issues; the affected items were repaired or dropped"
            );
        }
    }
}

/// One feed mapped onto the catalogue types, after the per-item checks.
#[derive(Debug)]
pub(crate) struct MappedFeed {
    pub stations: Vec<IslandOfIrelandStation>,
    pub lines: Vec<IslandOfIrelandLineDefinition>,
    pub issues: FeedIssues,
}

/// Maps `gtfs` and applies the per-item checks: [`map_stations`], then
/// [`map_lines`] restricted to the stations that survived.
pub(crate) fn map_feed(gtfs: &Gtfs) -> MappedFeed {
    let mut issues = FeedIssues::default();
    let stations = map_stations(gtfs, &mut issues);
    let kept: HashSet<&str> = stations.iter().map(|s| s.id.as_str()).collect();
    let lines = map_lines(gtfs, &kept, &mut issues);
    MappedFeed {
        stations,
        lines,
        issues,
    }
}

/// `name` if it has any non-whitespace content.
fn non_blank(name: Option<&String>) -> Option<&String> {
    name.filter(|n| !n.trim().is_empty())
}

/// Loose bounding box around the island of Ireland (the real network spans
/// roughly 51.8..55.2 N, 10.0..5.9 W). Generous on purpose: it catches a
/// zeroed, swapped or out-of-range pair, not stations near the edge.
const LATITUDE_RANGE: RangeInclusive<f64> = 51.0..=56.0;
const LONGITUDE_RANGE: RangeInclusive<f64> = -11.0..=-5.0;

/// Both coordinates, if the stop has a plausible pair; `None` otherwise.
fn plausible_coordinates(stop: &Stop) -> Option<(f64, f64)> {
    let (lat, lon) = (stop.latitude?, stop.longitude?);
    (LATITUDE_RANGE.contains(&lat) && LONGITUDE_RANGE.contains(&lon)).then_some((lat, lon))
}

/// Maps every stop to a station, with these policies (the output's
/// `latitude`/`longitude` are optional, its `name` is not):
/// - **Name** missing or blank: use the parent station's name when the stop
///   has a parent with one (GTFS platforms and boarding areas often leave
///   their own name to the parent). Otherwise drop the stop: a station with
///   no name cannot be shown or searched for, and an empty string would
///   overwrite a good name already in the catalogue.
/// - **Coordinates** missing, only half present, or outside
///   [`LATITUDE_RANGE`]/[`LONGITUDE_RANGE`]: keep the stop but omit both
///   coordinates. Its id and name are still good, a station with no
///   position is better than a missing one (lines that call there stay
///   whole), and much better than one pinned in the wrong place.
pub(crate) fn map_stations(gtfs: &Gtfs, issues: &mut FeedIssues) -> Vec<IslandOfIrelandStation> {
    gtfs.stops
        .values()
        .filter_map(|stop| {
            let name = if let Some(name) = non_blank(stop.name.as_ref()) {
                name.clone()
            } else if let Some(parent_name) = stop
                .parent_station
                .as_ref()
                .and_then(|parent| gtfs.stops.get(parent))
                .and_then(|parent| non_blank(parent.name.as_ref()))
            {
                issues.add(FeedIssue::StopNameFromParent);
                parent_name.clone()
            } else {
                issues.add(FeedIssue::StopWithoutName);
                return None;
            };

            let coordinates = plausible_coordinates(stop);
            if coordinates.is_none() {
                issues.add(FeedIssue::StopInvalidCoordinates);
            }

            Some(IslandOfIrelandStation {
                id: stop.id.clone(),
                name,
                network: IslandOfIrelandNetwork::RepublicOfIreland,
                latitude: coordinates.map(|(lat, _)| lat),
                longitude: coordinates.map(|(_, lon)| lon),
            })
        })
        .collect()
}

/// For each route, picks that route's LONGEST trip (most `stop_times`) as
/// the representative stopping pattern. A route can have multiple trips
/// with different stopping patterns (e.g. a peak express skipping stops an
/// off-peak service calls at); GTFS carries no single "canonical" stop
/// sequence per route, only per-trip sequences. "Longest trip wins" is a
/// concrete, defensible v1 choice (it captures the fullest possible
/// picture of a route's own stations, at the cost of not distinguishing
/// express/stopping variants) -- deliberately not a general timetable
/// model. A future pass wanting real trip-variant awareness needs a
/// different `IslandOfIrelandLineDefinition.stations` shape entirely, not a
/// tweak to this function.
///
/// Stops missing from `kept_stations` (dropped by [`map_stations`]) are
/// removed from the pattern, and a route left with no stations (no trips,
/// an empty longest trip, or only dropped stops) is dropped rather than
/// emitted as a line with an empty station list.
pub(crate) fn map_lines(
    gtfs: &Gtfs,
    kept_stations: &HashSet<&str>,
    issues: &mut FeedIssues,
) -> Vec<IslandOfIrelandLineDefinition> {
    let mut trips_by_route: HashMap<&str, Vec<&gtfs_structures::Trip>> = HashMap::new();
    for trip in gtfs.trips.values() {
        trips_by_route
            .entry(trip.route_id.as_str())
            .or_default()
            .push(trip);
    }

    gtfs.routes
        .values()
        .filter_map(|route| {
            let Some(trip) = trips_by_route
                .get(route.id.as_str())
                .and_then(|trips| trips.iter().max_by_key(|t| t.stop_times.len()))
            else {
                issues.add(FeedIssue::RouteWithoutTrips);
                return None;
            };

            let mut stop_times: Vec<&gtfs_structures::StopTime> = trip.stop_times.iter().collect();
            stop_times.sort_by_key(|st| st.stop_sequence);
            let mut stations = Vec::with_capacity(stop_times.len());
            for st in stop_times {
                if kept_stations.contains(st.stop.id.as_str()) {
                    stations.push(st.stop.id.clone());
                } else {
                    issues.add(FeedIssue::LineStopDropped);
                }
            }
            if stations.is_empty() {
                issues.add(FeedIssue::RouteWithoutStops);
                return None;
            }

            let name = route
                .long_name
                .clone()
                .filter(|n| !n.is_empty())
                .or_else(|| route.short_name.clone())
                .unwrap_or_else(|| route.id.clone());

            Some(IslandOfIrelandLineDefinition {
                id: route.id.clone(),
                name,
                network: IslandOfIrelandNetwork::RepublicOfIreland,
                stations,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGENCY: &str = "agency_id,agency_name,agency_url,agency_timezone\nIR,Iarnrod Eireann,https://example.invalid,Europe/Dublin\n";
    const CALENDAR: &str = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWEEKDAY,1,1,1,1,1,0,0,20260101,20271231\n";
    const STOPS_HEADER: &str = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n";
    const ROUTES_HEADER: &str = "route_id,agency_id,route_short_name,route_long_name,route_type\n";
    const TRIPS_HEADER: &str = "route_id,service_id,trip_id\n";
    const STOP_TIMES_HEADER: &str = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n";

    /// Builds a GTFS feed in-memory from the given CSV rows (headers above)
    /// and round-trips it through a real zip, so these tests exercise the
    /// same `Gtfs::from_reader` code path `main.rs` uses, not a hand-built
    /// `Gtfs` struct literal (whose exact field set could drift from what
    /// the crate actually requires).
    fn feed(stops: &str, routes: &str, trips: &str, stop_times: &str) -> Gtfs {
        use std::io::Write;

        let files: [(&str, String); 6] = [
            ("agency.txt", AGENCY.to_owned()),
            ("stops.txt", format!("{STOPS_HEADER}{stops}")),
            ("routes.txt", format!("{ROUTES_HEADER}{routes}")),
            ("trips.txt", format!("{TRIPS_HEADER}{trips}")),
            ("stop_times.txt", format!("{STOP_TIMES_HEADER}{stop_times}")),
            ("calendar.txt", CALENDAR.to_owned()),
        ];

        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
            for (name, contents) in &files {
                zip.start_file(*name, options).unwrap();
                zip.write_all(contents.as_bytes()).unwrap();
            }
            zip.finish().unwrap();
        }
        Gtfs::from_reader(std::io::Cursor::new(buf)).expect("parse test feed")
    }

    /// Two good stops on one route with one trip.
    fn test_feed() -> Gtfs {
        feed(
            "STOP_A,Zesttown,53.0,-6.0,,\nSTOP_B,Zorough,53.1,-6.1,,\n",
            "ROUTE_1,IR,,Zesttown - Zorough,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\n",
            "TRIP_1,08:00:00,08:00:00,STOP_A,1\nTRIP_1,08:10:00,08:10:00,STOP_B,2\n",
        )
    }

    fn station<'a>(mapped: &'a MappedFeed, id: &str) -> Option<&'a IslandOfIrelandStation> {
        mapped.stations.iter().find(|s| s.id == id)
    }

    #[test]
    fn map_stations_maps_id_name_coordinates_and_tags_republic_of_ireland() {
        let mapped = map_feed(&test_feed());
        assert_eq!(mapped.stations.len(), 2);
        let a = station(&mapped, "STOP_A").expect("STOP_A present");
        assert_eq!(a.name, "Zesttown");
        assert_eq!(a.network, IslandOfIrelandNetwork::RepublicOfIreland);
        assert_eq!(a.latitude, Some(53.0));
        assert_eq!(a.longitude, Some(-6.0));
        assert_eq!(mapped.issues, FeedIssues::default());
    }

    #[test]
    fn map_lines_uses_long_name_and_orders_stations_by_stop_sequence() {
        let mapped = map_feed(&test_feed());
        assert_eq!(mapped.lines.len(), 1);
        let route = &mapped.lines[0];
        assert_eq!(route.id, "ROUTE_1");
        assert_eq!(route.name, "Zesttown - Zorough");
        assert_eq!(route.network, IslandOfIrelandNetwork::RepublicOfIreland);
        assert_eq!(
            route.stations,
            vec!["STOP_A".to_string(), "STOP_B".to_string()]
        );
    }

    #[test]
    fn a_route_with_no_trips_is_dropped_and_counted() {
        let mapped = map_feed(&feed(
            "STOP_A,Zesttown,53.0,-6.0,,\nSTOP_B,Zorough,53.1,-6.1,,\n",
            "ROUTE_1,IR,,Zesttown - Zorough,2\nROUTE_EMPTY,IR,,Nowhere Express,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\n",
            "TRIP_1,08:00:00,08:00:00,STOP_A,1\nTRIP_1,08:10:00,08:10:00,STOP_B,2\n",
        ));
        let ids: Vec<&str> = mapped.lines.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(ids, ["ROUTE_1"]);
        assert_eq!(mapped.issues.count(FeedIssue::RouteWithoutTrips), 1);
    }

    #[test]
    fn a_route_whose_longest_trip_has_no_stops_is_dropped_and_counted() {
        let mapped = map_feed(&feed(
            "STOP_A,Zesttown,53.0,-6.0,,\n",
            "ROUTE_1,IR,,Zesttown - Zorough,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\n",
            "",
        ));
        assert!(mapped.lines.is_empty());
        assert_eq!(mapped.issues.count(FeedIssue::RouteWithoutStops), 1);
    }

    #[test]
    fn a_nameless_stop_with_a_named_parent_takes_the_parents_name() {
        let mapped = map_feed(&feed(
            "PARENT,Zesttown,53.0,-6.0,1,\nPLATFORM_1,,53.0,-6.0,0,PARENT\nPLATFORM_2,   ,53.0,-6.0,0,PARENT\n",
            "ROUTE_1,IR,,Zesttown Shuttle,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\n",
            "TRIP_1,08:00:00,08:00:00,PLATFORM_1,1\nTRIP_1,08:10:00,08:10:00,PLATFORM_2,2\n",
        ));
        assert_eq!(mapped.stations.len(), 3);
        for id in ["PLATFORM_1", "PLATFORM_2"] {
            assert_eq!(station(&mapped, id).expect(id).name, "Zesttown");
        }
        assert_eq!(mapped.issues.count(FeedIssue::StopNameFromParent), 2);
        assert_eq!(mapped.issues.count(FeedIssue::StopWithoutName), 0);
        assert_eq!(mapped.lines[0].stations, ["PLATFORM_1", "PLATFORM_2"]);
    }

    #[test]
    fn a_nameless_stop_without_a_named_parent_is_dropped_and_pruned_from_lines() {
        let mapped = map_feed(&feed(
            "STOP_A,Zesttown,53.0,-6.0,,\nORPHAN,,53.1,-6.1,,\nNAMELESS_PARENT,,53.2,-6.2,1,\nCHILD,,53.2,-6.2,0,NAMELESS_PARENT\n",
            "ROUTE_1,IR,,Zesttown - Orphan,2\nROUTE_2,IR,,Orphan only,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\nROUTE_2,WEEKDAY,TRIP_2\n",
            "TRIP_1,08:00:00,08:00:00,STOP_A,1\nTRIP_1,08:10:00,08:10:00,ORPHAN,2\nTRIP_2,09:00:00,09:00:00,ORPHAN,1\n",
        ));
        let ids: Vec<&str> = mapped.stations.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["STOP_A"]);
        assert_eq!(mapped.issues.count(FeedIssue::StopWithoutName), 3);
        // ROUTE_1 keeps its named stop; ROUTE_2 called only at the dropped one.
        assert_eq!(mapped.lines.len(), 1);
        assert_eq!(mapped.lines[0].stations, ["STOP_A"]);
        assert_eq!(mapped.issues.count(FeedIssue::LineStopDropped), 2);
        assert_eq!(mapped.issues.count(FeedIssue::RouteWithoutStops), 1);
    }

    #[test]
    fn a_stop_with_bad_coordinates_is_kept_without_them() {
        let mapped = map_feed(&feed(
            // Missing, half-present, (0, 0), swapped, and out of range.
            "MISSING,Missing,,,,\nHALF,Half,53.0,,,\nZERO,Zero,0,0,,\nSWAPPED,Swapped,-6.0,53.0,,\nFAR,Far,95.0,-6.0,,\nGOOD,Good,53.0,-6.0,,\n",
            "ROUTE_1,IR,,Line,2\n",
            "ROUTE_1,WEEKDAY,TRIP_1\n",
            "TRIP_1,08:00:00,08:00:00,GOOD,1\nTRIP_1,08:10:00,08:10:00,ZERO,2\n",
        ));
        assert_eq!(mapped.stations.len(), 6);
        for id in ["MISSING", "HALF", "ZERO", "SWAPPED", "FAR"] {
            let s = station(&mapped, id).expect(id);
            assert_eq!((s.latitude, s.longitude), (None, None), "{id}");
        }
        let good = station(&mapped, "GOOD").expect("GOOD");
        assert_eq!((good.latitude, good.longitude), (Some(53.0), Some(-6.0)));
        assert_eq!(mapped.issues.count(FeedIssue::StopInvalidCoordinates), 5);
        // A kept stop stays on its line.
        assert_eq!(mapped.lines[0].stations, ["GOOD", "ZERO"]);
    }
}
