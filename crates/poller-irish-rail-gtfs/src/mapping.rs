//! Maps a parsed `gtfs_structures::Gtfs` feed onto
//! `common::island_of_ireland::{IslandOfIrelandStation, IslandOfIrelandLineDefinition}`.
//!
//! Field provenance, confirmed against docs.rs/gtfs-structures/latest
//! (v0.50.0) directly in this crate's planning pass:
//! - `Gtfs.stops: HashMap<String, Arc<Stop>>`, `Gtfs.routes: HashMap<String, Route>`,
//!   `Gtfs.trips: HashMap<String, Trip>`.
//! - `Stop.id: String`, `Stop.name: Option<String>`, `Stop.latitude`/`longitude: Option<f64>`.
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

use std::collections::HashMap;

use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
};
use gtfs_structures::Gtfs;

pub fn map_stations(gtfs: &Gtfs) -> Vec<IslandOfIrelandStation> {
    gtfs.stops
        .values()
        .map(|stop| IslandOfIrelandStation {
            id: stop.id.clone(),
            // `Stop.name` is `Option<String>` in gtfs-structures; GTFS's
            // own spec requires it for a `location_type` of "stop"
            // (ordinary passenger stations), so an empty fallback here is
            // defensive, not an expected real-world case for this feed.
            name: stop.name.clone().unwrap_or_default(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            latitude: stop.latitude,
            longitude: stop.longitude,
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
pub fn map_lines(gtfs: &Gtfs) -> Vec<IslandOfIrelandLineDefinition> {
    let mut trips_by_route: HashMap<&str, Vec<&gtfs_structures::Trip>> = HashMap::new();
    for trip in gtfs.trips.values() {
        trips_by_route
            .entry(trip.route_id.as_str())
            .or_default()
            .push(trip);
    }

    gtfs.routes
        .values()
        .map(|route| {
            let name = route
                .long_name
                .clone()
                .filter(|n| !n.is_empty())
                .or_else(|| route.short_name.clone())
                .unwrap_or_else(|| route.id.clone());

            let stations = trips_by_route
                .get(route.id.as_str())
                .and_then(|trips| trips.iter().max_by_key(|t| t.stop_times.len()))
                .map(|trip| {
                    let mut stop_times: Vec<&gtfs_structures::StopTime> =
                        trip.stop_times.iter().collect();
                    stop_times.sort_by_key(|st| st.stop_sequence);
                    stop_times
                        .into_iter()
                        .map(|st| st.stop.id.clone())
                        .collect()
                })
                .unwrap_or_default();

            IslandOfIrelandLineDefinition {
                id: route.id.clone(),
                name,
                network: IslandOfIrelandNetwork::RepublicOfIreland,
                stations,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal, valid GTFS feed in-memory (three files are the
    /// GTFS-required floor for `Gtfs::from_reader` to succeed at all:
    /// `agency.txt`, `stops.txt`, `routes.txt`, plus `trips.txt`/
    /// `stop_times.txt` for this test's own assertions) and round-trips it
    /// through a real zip so this test exercises the same
    /// `Gtfs::from_reader` code path `main.rs` uses, not a hand-built
    /// `Gtfs` struct literal (whose exact field set could drift from what
    /// the crate actually requires).
    fn build_test_feed_zip() -> Vec<u8> {
        use std::io::Write;

        let files: &[(&str, &str)] = &[
            (
                "agency.txt",
                "agency_id,agency_name,agency_url,agency_timezone\nIR,Iarnrod Eireann,https://example.invalid,Europe/Dublin\n",
            ),
            (
                "stops.txt",
                "stop_id,stop_name,stop_lat,stop_lon\nSTOP_A,Zesttown,53.0,-6.0\nSTOP_B,Zorough,53.1,-6.1\n",
            ),
            (
                "routes.txt",
                "route_id,agency_id,route_short_name,route_long_name,route_type\nROUTE_1,IR,,Zesttown - Zorough,2\n",
            ),
            (
                "trips.txt",
                "route_id,service_id,trip_id\nROUTE_1,WEEKDAY,TRIP_1\n",
            ),
            (
                "stop_times.txt",
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nTRIP_1,08:00:00,08:00:00,STOP_A,1\nTRIP_1,08:10:00,08:10:00,STOP_B,2\n",
            ),
            (
                "calendar.txt",
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWEEKDAY,1,1,1,1,1,0,0,20260101,20271231\n",
            ),
        ];

        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options: zip::write::FileOptions<()> = zip::write::FileOptions::default();
            for (name, contents) in files {
                zip.start_file(*name, options).unwrap();
                zip.write_all(contents.as_bytes()).unwrap();
            }
            zip.finish().unwrap();
        }
        buf
    }

    fn test_feed() -> Gtfs {
        let bytes = build_test_feed_zip();
        Gtfs::from_reader(std::io::Cursor::new(bytes)).expect("parse test feed")
    }

    #[test]
    fn map_stations_maps_id_name_coordinates_and_tags_republic_of_ireland() {
        let gtfs = test_feed();
        let stations = map_stations(&gtfs);
        assert_eq!(stations.len(), 2);
        let a = stations
            .iter()
            .find(|s| s.id == "STOP_A")
            .expect("STOP_A present");
        assert_eq!(a.name, "Zesttown");
        assert_eq!(a.network, IslandOfIrelandNetwork::RepublicOfIreland);
        assert_eq!(a.latitude, Some(53.0));
        assert_eq!(a.longitude, Some(-6.0));
    }

    #[test]
    fn map_lines_uses_long_name_and_orders_stations_by_stop_sequence() {
        let gtfs = test_feed();
        let lines = map_lines(&gtfs);
        assert_eq!(lines.len(), 1);
        let route = &lines[0];
        assert_eq!(route.id, "ROUTE_1");
        assert_eq!(route.name, "Zesttown - Zorough");
        assert_eq!(route.network, IslandOfIrelandNetwork::RepublicOfIreland);
        assert_eq!(
            route.stations,
            vec!["STOP_A".to_string(), "STOP_B".to_string()]
        );
    }
}
