//! The GTFS adapter (transit spec §6.2, steps 2-10).
//!
//! The North West BODS zip is 159 MB (2026-10-14), 1 GB unzipped, with a
//! 470 MB `stop_times.txt` and a 510 MB `shapes.txt`. So the zip is read
//! from a file, entry by entry, each through a [`CappedReader`], and every
//! file is streamed row by row: only the network's rows are kept.
//! `shapes.txt` is never opened.
//!
//! 1. `agency.txt`: the agencies whose `agency_noc` is one of the network's.
//! 2. `routes.txt`: their routes, each mapped to a line by its
//!    `route_short_name`, or marked as a replacement bus; any other name
//!    refuses the feed ([`FeedError::UnmappedRoute`]).
//! 3. `trips.txt`, then `stop_times.txt`: those routes' trips and calls.
//! 4. `stops.txt`: the stops called at, and their parent stations, whose
//!    id becomes the station's `NaPTAN` `MET` code (`940G…` to `9400…`,
//!    spec §6.3).
//! 5. `calendar.txt` and `calendar_dates.txt`: each service's dates from
//!    yesterday (GTFS times run past 24:00) to today plus the horizon.
//! 6. Folding: each trip becomes a pattern (line, direction, the calls'
//!    platforms and offsets from the first departure) plus its start time.
//! 7. Each line's station order per direction: its longest pattern's,
//!    merged with its other patterns' stations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek};

use chrono::{Datelike, Days, NaiveDate, Weekday};
use common::transit::{
    Direction, Horizon, StopKind, TransitLine, TransitLineDirection, TransitPattern,
    TransitPatternStop, TransitServiceDays, TransitStop, TransitTrip, is_stop_id,
};
use zip::ZipArchive;

use super::{CappedReader, FeedError};
use crate::model::TransitModel;
use crate::network::NetworkConfig;

/// The inflated-size cap of one zip entry. `stop_times.txt` was 470 MB on
/// 2026-10-14; the cap bounds the time a hostile zip can cost (the parse
/// streams, so memory does not depend on it).
pub(crate) const MAX_ENTRY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// How to read a feed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GtfsOptions {
    /// Today in the network's timezone: the horizon starts the day before.
    pub today: NaiveDate,
    /// Days stored after today (Q8: 60).
    pub horizon_days: u32,
    pub max_entry_bytes: u64,
}

/// What a route of the network is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteKind {
    /// The index of its line in the network file.
    Line(usize),
    ReplacementBus,
}

struct TripInfo {
    route: RouteKind,
    service_id: String,
    headsign: Option<String>,
    direction: Direction,
}

/// One `stop_times.txt` row of a kept trip.
struct Call {
    seq: u32,
    stop: u32,
    arrival: Option<i32>,
    departure: Option<i32>,
    pickup: u8,
    drop_off: u8,
}

/// A pattern's identity: line, direction and calls.
type PatternKey = (Option<usize>, Direction, Vec<(String, i32, i32, u8, u8)>);

fn malformed(what: impl std::fmt::Display) -> FeedError {
    FeedError::Malformed(what.to_string())
}

/// A CSV file's column positions.
struct Columns(HashMap<String, usize>);

impl Columns {
    fn new(headers: &csv::StringRecord) -> Self {
        Self(
            headers
                .iter()
                .enumerate()
                .map(|(i, name)| (name.trim_start_matches('\u{feff}').trim().to_owned(), i))
                .collect(),
        )
    }

    /// A required column.
    fn need(&self, file: &str, name: &str) -> Result<usize, FeedError> {
        self.0
            .get(name)
            .copied()
            .ok_or_else(|| malformed(format!("{file} has no {name} column")))
    }

    fn get(&self, name: &str) -> Option<usize> {
        self.0.get(name).copied()
    }
}

fn field(record: &csv::StringRecord, at: Option<usize>) -> &str {
    at.and_then(|i| record.get(i)).unwrap_or("").trim()
}

/// Streams `file` from the zip through `row` (with its columns). `false`
/// when the file is absent.
fn each_row<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    file: &str,
    cap: u64,
    mut row: impl FnMut(&Columns, &csv::StringRecord) -> Result<(), FeedError>,
) -> Result<bool, FeedError> {
    let entry = match archive.by_name(file) {
        Ok(entry) => entry,
        Err(zip::result::ZipError::FileNotFound) => return Ok(false),
        Err(err) => return Err(malformed(format!("{file}: {err}"))),
    };
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(CappedReader::new(entry, cap, file));
    let columns = Columns::new(reader.headers().map_err(|err| read_error(file, &err))?);
    let mut record = csv::StringRecord::new();
    while reader
        .read_record(&mut record)
        .map_err(|err| read_error(file, &err))?
    {
        row(&columns, &record)?;
    }
    Ok(true)
}

fn read_error(file: &str, err: &csv::Error) -> FeedError {
    if err.to_string().contains("inflated-size cap") {
        FeedError::Oversize(format!("{file} (inflated)"))
    } else {
        malformed(format!("{file}: {err}"))
    }
}

/// `HH:MM:SS` (hours may pass 24) as seconds; empty is `None`.
fn parse_time(text: &str) -> Result<Option<i32>, FeedError> {
    if text.is_empty() {
        return Ok(None);
    }
    let mut parts = text.split(':');
    let (Some(h), Some(m), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed(format!("time {text:?}")));
    };
    let num = |p: &str| {
        p.parse::<i32>()
            .map_err(|_| malformed(format!("time {text:?}")))
    };
    let (h, m, s) = (num(h)?, num(m)?, num(s)?);
    if !(0..48).contains(&h) || !(0..60).contains(&m) || !(0..60).contains(&s) {
        return Err(malformed(format!("time {text:?}")));
    }
    Ok(Some(h * 3600 + m * 60 + s))
}

fn parse_date(text: &str) -> Result<NaiveDate, FeedError> {
    NaiveDate::parse_from_str(text, "%Y%m%d").map_err(|_| malformed(format!("date {text:?}")))
}

/// A GTFS pickup or drop-off type (empty is 0, regular).
fn parse_pickup(text: &str) -> Result<u8, FeedError> {
    if text.is_empty() {
        return Ok(0);
    }
    match text.parse::<u8>() {
        Ok(v) if v <= common::transit::MAX_PICKUP_DROP_OFF => Ok(v),
        _ => Err(malformed(format!("pickup or drop-off type {text:?}"))),
    }
}

/// A station's id from its GTFS `parent_station`: the stop area
/// `940GZZMAPIC` is the `NaPTAN` `MET` node `9400ZZMAPIC` (spec §6.3).
pub(crate) fn station_id(parent: &str) -> String {
    match parent.strip_prefix("940G") {
        Some(rest) => format!("9400{rest}"),
        None => parent.to_owned(),
    }
}

/// `name` without the network's ` (Manchester Metrolink)` suffix.
pub(crate) fn clean_name(name: &str, network_name: &str) -> String {
    let suffix = format!(" ({network_name})");
    name.trim()
        .strip_suffix(&suffix)
        .unwrap_or(name.trim())
        .to_owned()
}

/// Reads `archive` as one network's timetable. See the module docs.
#[expect(
    clippy::too_many_lines,
    reason = "the module docs' steps, in order, each a short pass over one file"
)]
pub(crate) fn parse<R: Read + Seek>(
    reader: R,
    network: &NetworkConfig,
    options: &GtfsOptions,
) -> Result<TransitModel, FeedError> {
    let mut archive = ZipArchive::new(reader).map_err(|err| malformed(format!("zip: {err}")))?;
    let cap = options.max_entry_bytes;

    // 1. Agencies.
    let mut agencies = BTreeSet::new();
    let found = each_row(&mut archive, "agency.txt", cap, |cols, rec| {
        let noc = field(rec, cols.get("agency_noc"));
        if network.nocs.iter().any(|n| n == noc) {
            agencies.insert(field(rec, Some(cols.need("agency.txt", "agency_id")?)).to_owned());
        }
        Ok(())
    })?;
    if !found || agencies.is_empty() {
        return Err(malformed(format!(
            "no agency with NOC {:?} in agency.txt",
            network.nocs
        )));
    }

    // 2. Routes.
    let mut routes: HashMap<String, RouteKind> = HashMap::new();
    each_row(&mut archive, "routes.txt", cap, |cols, rec| {
        if !agencies.contains(field(rec, cols.get("agency_id"))) {
            return Ok(());
        }
        let name = field(rec, cols.get("route_short_name"));
        let kind = if network
            .replacement_bus_route_names
            .iter()
            .any(|n| n == name)
        {
            RouteKind::ReplacementBus
        } else if let Some(line) = network
            .lines
            .iter()
            .position(|l| l.gtfs_route_names.iter().any(|n| n == name))
        {
            RouteKind::Line(line)
        } else {
            return Err(FeedError::UnmappedRoute(name.to_owned()));
        };
        routes.insert(
            field(rec, Some(cols.need("routes.txt", "route_id")?)).to_owned(),
            kind,
        );
        Ok(())
    })?;

    // 3. Trips and their calls.
    let mut trip_ids: Vec<String> = Vec::new();
    let mut trip_index: HashMap<String, usize> = HashMap::new();
    let mut trips: Vec<TripInfo> = Vec::new();
    each_row(&mut archive, "trips.txt", cap, |cols, rec| {
        let Some(route) = routes.get(field(rec, cols.get("route_id"))).copied() else {
            return Ok(());
        };
        let trip_id = field(rec, Some(cols.need("trips.txt", "trip_id")?)).to_owned();
        let direction = match field(rec, cols.get("direction_id")) {
            "1" => Direction::Inbound,
            _ => Direction::Outbound,
        };
        let headsign = Some(clean_name(
            field(rec, cols.get("trip_headsign")),
            &network.name,
        ))
        .filter(|h| !h.is_empty());
        trip_index.insert(trip_id.clone(), trips.len());
        trip_ids.push(trip_id);
        trips.push(TripInfo {
            route,
            service_id: field(rec, Some(cols.need("trips.txt", "service_id")?)).to_owned(),
            headsign,
            direction,
        });
        Ok(())
    })?;

    let mut stop_names: Vec<String> = Vec::new();
    let mut stop_index: HashMap<String, u32> = HashMap::new();
    let mut calls: Vec<Vec<Call>> = (0..trips.len()).map(|_| Vec::new()).collect();
    each_row(&mut archive, "stop_times.txt", cap, |cols, rec| {
        let Some(&trip) = trip_index.get(field(rec, cols.get("trip_id"))) else {
            return Ok(());
        };
        let stop_id = field(rec, Some(cols.need("stop_times.txt", "stop_id")?));
        let stop = if let Some(&i) = stop_index.get(stop_id) {
            i
        } else {
            let i = u32::try_from(stop_names.len()).map_err(|_| malformed("too many stops"))?;
            stop_index.insert(stop_id.to_owned(), i);
            stop_names.push(stop_id.to_owned());
            i
        };
        calls[trip].push(Call {
            seq: field(rec, cols.get("stop_sequence"))
                .parse()
                .map_err(|_| malformed("stop_sequence"))?,
            stop,
            arrival: parse_time(field(rec, cols.get("arrival_time")))?,
            departure: parse_time(field(rec, cols.get("departure_time")))?,
            pickup: parse_pickup(field(rec, cols.get("pickup_type")))?,
            drop_off: parse_pickup(field(rec, cols.get("drop_off_type")))?,
        });
        Ok(())
    })?;

    // 4. Stops.
    let used: BTreeSet<&str> = stop_names.iter().map(String::as_str).collect();
    let stops = read_stops(&mut archive, cap, &used, &network.name)?;

    // 5. Service dates.
    let from = options
        .today
        .checked_sub_days(Days::new(1))
        .ok_or_else(|| malformed("date range"))?;
    let to = options
        .today
        .checked_add_days(Days::new(u64::from(options.horizon_days)))
        .ok_or_else(|| malformed("date range"))?;
    let services: BTreeSet<&str> = trips.iter().map(|t| t.service_id.as_str()).collect();
    let service_days = read_service_days(&mut archive, cap, &services, from, to)?;

    let mut source_feed_version = None;
    each_row(&mut archive, "feed_info.txt", cap, |cols, rec| {
        let version = field(rec, cols.get("feed_version"));
        if !version.is_empty() {
            source_feed_version = Some(version.to_owned());
        }
        Ok(())
    })?;

    // 6, 7.
    let (patterns, trip_rows) = fold(network, &trips, &trip_ids, &mut calls, &stop_names)?;
    let lines = line_orders(network, &patterns, &trip_rows, &stops);
    Ok(TransitModel {
        lines,
        stops: stops.into_rows(),
        patterns,
        trips: trip_rows,
        service_days,
        horizon: Horizon { from, to },
        source_feed_version,
    })
}

/// The stops called at, and their stations.
struct Stops {
    /// Platform (or bus stop) id to its station's id, if it has one.
    station_of: BTreeMap<String, Option<String>>,
    stations: BTreeMap<String, TransitStop>,
    platforms: BTreeMap<String, TransitStop>,
}

impl Stops {
    /// The station a call at `stop` belongs to (itself if it has none).
    fn station(&self, stop: &str) -> String {
        self.station_of
            .get(stop)
            .cloned()
            .flatten()
            .unwrap_or_else(|| stop.to_owned())
    }

    fn into_rows(self) -> Vec<TransitStop> {
        self.stations
            .into_values()
            .chain(self.platforms.into_values())
            .collect()
    }
}

fn read_stops<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    cap: u64,
    used: &BTreeSet<&str>,
    network_name: &str,
) -> Result<Stops, FeedError> {
    struct Row {
        name: String,
        lat: Option<f64>,
        lon: Option<f64>,
        wheelchair: Option<bool>,
        parent: String,
    }
    fn row(cols: &Columns, rec: &csv::StringRecord) -> Row {
        Row {
            name: field(rec, cols.get("stop_name")).to_owned(),
            lat: field(rec, cols.get("stop_lat")).parse().ok(),
            lon: field(rec, cols.get("stop_lon")).parse().ok(),
            wheelchair: match field(rec, cols.get("wheelchair_boarding")) {
                "1" => Some(true),
                "2" => Some(false),
                _ => None,
            },
            parent: field(rec, cols.get("parent_station")).to_owned(),
        }
    }
    let mut platforms: BTreeMap<String, Row> = BTreeMap::new();
    let found = each_row(archive, "stops.txt", cap, |cols, rec| {
        let id = field(rec, cols.get("stop_id"));
        if used.contains(id) {
            platforms.insert(id.to_owned(), row(cols, rec));
        }
        Ok(())
    })?;
    if !found {
        return Err(malformed("no stops.txt"));
    }
    if let Some(missing) = used.iter().find(|id| !platforms.contains_key(**id)) {
        return Err(malformed(format!("stop {missing} is not in stops.txt")));
    }
    let parents: BTreeSet<String> = platforms
        .values()
        .filter(|p| !p.parent.is_empty())
        .map(|p| p.parent.clone())
        .collect();
    let mut parent_rows: BTreeMap<String, Row> = BTreeMap::new();
    if !parents.is_empty() {
        each_row(archive, "stops.txt", cap, |cols, rec| {
            let id = field(rec, cols.get("stop_id"));
            if parents.contains(id) {
                parent_rows.insert(id.to_owned(), row(cols, rec));
            }
            Ok(())
        })?;
    }
    let mut out = Stops {
        station_of: BTreeMap::new(),
        stations: BTreeMap::new(),
        platforms: BTreeMap::new(),
    };
    for (id, platform) in &platforms {
        let station = (!platform.parent.is_empty()).then(|| station_id(&platform.parent));
        for checked in std::iter::once(id).chain(station.as_ref()) {
            if !is_stop_id(checked) {
                return Err(malformed(format!("stop id {checked:?}")));
            }
        }
        if let Some(station) = &station {
            let parent = parent_rows.get(&platform.parent);
            out.stations
                .entry(station.clone())
                .or_insert_with(|| TransitStop {
                    stop_id: station.clone(),
                    kind: StopKind::Station,
                    parent_stop_id: None,
                    name: clean_name(&parent.unwrap_or(platform).name, network_name),
                    indicator: None,
                    lat: parent.and_then(|p| p.lat).or(platform.lat),
                    lon: parent.and_then(|p| p.lon).or(platform.lon),
                    wheelchair: parent.and_then(|p| p.wheelchair),
                });
        }
        out.platforms.insert(
            id.clone(),
            TransitStop {
                stop_id: id.clone(),
                kind: StopKind::Platform,
                parent_stop_id: station.clone(),
                name: clean_name(&platform.name, network_name),
                indicator: None,
                lat: platform.lat,
                lon: platform.lon,
                wheelchair: platform.wheelchair,
            },
        );
        out.station_of.insert(id.clone(), station);
    }
    // A stop with no parent whose id is another platform's station (BODS
    // 2026-10-14: `9400ZZMASLE`, Sale, beside platforms of `940GZZMASLE`)
    // is that station: a call there is a call at the station, and it gets
    // no platform row of its own.
    let stations: BTreeSet<String> = out.stations.keys().cloned().collect();
    out.platforms.retain(|id, _| !stations.contains(id));
    Ok(out)
}

fn read_service_days<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    cap: u64,
    services: &BTreeSet<&str>,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<Vec<TransitServiceDays>, FeedError> {
    let mut dates: BTreeMap<String, BTreeSet<NaiveDate>> = BTreeMap::new();
    let has_calendar = each_row(archive, "calendar.txt", cap, |cols, rec| {
        let service = field(rec, cols.get("service_id"));
        if !services.contains(service) {
            return Ok(());
        }
        let start = parse_date(field(rec, cols.get("start_date")))?.max(from);
        let end = parse_date(field(rec, cols.get("end_date")))?.min(to);
        let runs = |day: Weekday| {
            let name = match day {
                Weekday::Mon => "monday",
                Weekday::Tue => "tuesday",
                Weekday::Wed => "wednesday",
                Weekday::Thu => "thursday",
                Weekday::Fri => "friday",
                Weekday::Sat => "saturday",
                Weekday::Sun => "sunday",
            };
            field(rec, cols.get(name)) == "1"
        };
        let set = dates.entry(service.to_owned()).or_default();
        let mut day = start;
        while day <= end {
            if runs(day.weekday()) {
                set.insert(day);
            }
            day = day.succ_opt().ok_or_else(|| malformed("date range"))?;
        }
        Ok(())
    })?;
    let has_dates = each_row(archive, "calendar_dates.txt", cap, |cols, rec| {
        let service = field(rec, cols.get("service_id"));
        if !services.contains(service) {
            return Ok(());
        }
        let date = parse_date(field(rec, cols.get("date")))?;
        if date < from || date > to {
            return Ok(());
        }
        let set = dates.entry(service.to_owned()).or_default();
        match field(rec, cols.get("exception_type")) {
            "1" => {
                set.insert(date);
            }
            "2" => {
                set.remove(&date);
            }
            other => return Err(malformed(format!("exception_type {other:?}"))),
        }
        Ok(())
    })?;
    if !has_calendar && !has_dates {
        return Err(malformed("neither calendar.txt nor calendar_dates.txt"));
    }
    Ok(dates
        .into_iter()
        .filter(|(_, d)| !d.is_empty())
        .map(|(service_id, d)| TransitServiceDays {
            service_id,
            dates: d.into_iter().collect(),
        })
        .collect())
}

/// Folds each trip into a pattern (spec §6.2 step 9); patterns are
/// numbered in key order, so the ids are stable for the same content.
fn fold(
    network: &NetworkConfig,
    trips: &[TripInfo],
    trip_ids: &[String],
    calls: &mut [Vec<Call>],
    stop_names: &[String],
) -> Result<(Vec<TransitPattern>, Vec<TransitTrip>), FeedError> {
    // Trip index -> (key, start).
    let mut keyed: Vec<Option<(PatternKey, i32)>> = Vec::with_capacity(trips.len());
    for (i, trip) in trips.iter().enumerate() {
        let trip_calls = &mut calls[i];
        if trip_calls.is_empty() {
            keyed.push(None);
            continue;
        }
        trip_calls.sort_by_key(|c| c.seq);
        let mut last: Option<i32> = None;
        let mut start: Option<i32> = None;
        let mut key_calls = Vec::with_capacity(trip_calls.len());
        for call in trip_calls.iter() {
            let arrival = call.arrival.or(call.departure).or(last);
            let departure = call.departure.or(arrival);
            let (Some(arrival), Some(departure)) = (arrival, departure) else {
                return Err(malformed(format!("trip {} has no times", trip_ids[i])));
            };
            let t0 = *start.get_or_insert(departure);
            let arr = (arrival - t0).max(0);
            let dep = (departure - t0).max(arr);
            last = Some(departure);
            key_calls.push((
                stop_names[call.stop as usize].clone(),
                arr,
                dep,
                call.pickup,
                call.drop_off,
            ));
        }
        let line = match trip.route {
            RouteKind::Line(l) => Some(l),
            RouteKind::ReplacementBus => None,
        };
        keyed.push(Some((
            (line, trip.direction, key_calls),
            start.unwrap_or(0),
        )));
    }
    let mut by_key: BTreeMap<&PatternKey, Vec<usize>> = BTreeMap::new();
    for (i, entry) in keyed.iter().enumerate() {
        if let Some((key, _)) = entry {
            by_key.entry(key).or_default().push(i);
        }
    }
    let mut pattern_of: HashMap<usize, String> = HashMap::new();
    let mut patterns = Vec::with_capacity(by_key.len());
    for (n, (key, members)) in by_key.iter().enumerate() {
        let id = format!("p{}", n + 1);
        let (line, direction, key_calls) = key;
        let first = members
            .iter()
            .min_by_key(|&&i| &trip_ids[i])
            .copied()
            .unwrap_or(members[0]);
        patterns.push(TransitPattern {
            pattern_id: id.clone(),
            line_id: line.map(|l| network.lines[l].id.clone()),
            direction: *direction,
            headsign: trips[first].headsign.clone(),
            stops: key_calls
                .iter()
                .map(|(stop, arr, dep, pickup, drop_off)| TransitPatternStop {
                    stop_id: stop.clone(),
                    arr_offset_s: *arr,
                    dep_offset_s: *dep,
                    pickup: *pickup,
                    drop_off: *drop_off,
                })
                .collect(),
        });
        for &i in members {
            pattern_of.insert(i, id.clone());
        }
    }
    let mut trip_rows: Vec<TransitTrip> = keyed
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            let (key, start) = entry.as_ref()?;
            Some(TransitTrip {
                trip_id: trip_ids[i].clone(),
                line_id: key.0.map(|l| network.lines[l].id.clone()),
                pattern_id: pattern_of.get(&i)?.clone(),
                service_id: trips[i].service_id.clone(),
                start_s: *start,
                headsign: trips[i].headsign.clone(),
                replacement_bus: matches!(trips[i].route, RouteKind::ReplacementBus),
            })
        })
        .collect();
    trip_rows.sort_by(|a, b| a.trip_id.cmp(&b.trip_id));
    Ok((patterns, trip_rows))
}

/// Each configured line, with its station order per direction (spec §6.2
/// step 10): the longest pattern's stations, then each other pattern's
/// missing stations inserted after the station they follow there.
fn line_orders(
    network: &NetworkConfig,
    patterns: &[TransitPattern],
    trips: &[TransitTrip],
    stops: &Stops,
) -> Vec<TransitLine> {
    let mut lines = Vec::with_capacity(network.lines.len());
    for (n, line) in network.lines.iter().enumerate() {
        let mut directions = Vec::new();
        for direction in [Direction::Outbound, Direction::Inbound] {
            let mut sequences: Vec<Vec<String>> = patterns
                .iter()
                .filter(|p| p.line_id.as_deref() == Some(&line.id) && p.direction == direction)
                .map(|p| {
                    let mut seq: Vec<String> = Vec::new();
                    for call in &p.stops {
                        let station = stops.station(&call.stop_id);
                        if seq.last() != Some(&station) {
                            seq.push(station);
                        }
                    }
                    seq
                })
                .collect();
            if sequences.is_empty() {
                continue;
            }
            // Longest first; ties by content, for a stable order.
            sequences.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
            let mut order = sequences[0].clone();
            for seq in &sequences[1..] {
                for (i, station) in seq.iter().enumerate() {
                    if order.contains(station) {
                        continue;
                    }
                    let at = i
                        .checked_sub(1)
                        .and_then(|prev| order.iter().position(|s| *s == seq[prev]))
                        .map_or(0, |p| p + 1);
                    order.insert(at, station.clone());
                }
            }
            let mut headsigns: BTreeMap<&str, usize> = BTreeMap::new();
            for trip in trips
                .iter()
                .filter(|t| t.line_id.as_deref() == Some(&line.id))
            {
                let in_direction = patterns
                    .iter()
                    .any(|p| p.pattern_id == trip.pattern_id && p.direction == direction);
                if let (true, Some(h)) = (in_direction, trip.headsign.as_deref()) {
                    *headsigns.entry(h).or_default() += 1;
                }
            }
            let headsign = headsigns
                .iter()
                .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(h, _)| (*h).to_owned());
            directions.push(TransitLineDirection {
                direction,
                headsign,
                stations: order,
            });
        }
        lines.push(TransitLine {
            line_id: line.id.clone(),
            name: line.name.clone(),
            colour: Some(line.colour.clone()),
            sort_order: i32::try_from(n + 1).unwrap_or(i32::MAX),
            directions,
        });
    }
    lines
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::io::{Cursor, Write};

    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../../tests/fixtures/bods-nw-metl-mini.zip");

    fn metrolink() -> NetworkConfig {
        NetworkConfig::load("metrolink").unwrap()
    }

    fn options(today: &str) -> GtfsOptions {
        GtfsOptions {
            today: NaiveDate::parse_from_str(today, "%Y-%m-%d").unwrap(),
            horizon_days: 60,
            max_entry_bytes: MAX_ENTRY_BYTES,
        }
    }

    fn fixture(today: &str) -> TransitModel {
        parse(Cursor::new(FIXTURE), &metrolink(), &options(today)).unwrap()
    }

    #[test]
    fn the_fixture_parses_to_the_networks_rows_only() {
        let model = fixture("2026-10-19");
        // 14 Blue, 14 Green and 4 replacement-bus trips; not the other
        // operator's 2.
        assert_eq!(model.trips.len(), 32);
        let buses: Vec<_> = model.trips.iter().filter(|t| t.replacement_bus).collect();
        assert_eq!(buses.len(), 4);
        assert!(buses.iter().all(|t| t.line_id.is_none()));
        assert!(
            model
                .trips
                .iter()
                .filter(|t| !t.replacement_bus)
                .all(|t| matches!(t.line_id.as_deref(), Some("blue" | "green")))
        );
        assert_eq!(
            model.source_feed_version.as_deref(),
            Some("20261014_030249")
        );
        // Patterns dedupe: far fewer than trips, and every trip has one.
        assert!(
            model.patterns.len() < model.trips.len(),
            "{}",
            model.patterns.len()
        );
        let ids: BTreeSet<&str> = model
            .patterns
            .iter()
            .map(|p| p.pattern_id.as_str())
            .collect();
        assert!(
            model
                .trips
                .iter()
                .all(|t| ids.contains(t.pattern_id.as_str()))
        );
        // Stations are MET codes, platforms point at them, names are clean.
        let pic = model.stops.iter().find(|s| s.stop_id == "9400ZZMAPIC");
        if let Some(pic) = pic {
            assert_eq!(pic.kind, StopKind::Station);
            assert_eq!(pic.name, "Piccadilly");
        }
        for stop in &model.stops {
            assert!(
                !stop.name.contains("(Manchester Metrolink)"),
                "{}",
                stop.name
            );
            if let Some(parent) = &stop.parent_stop_id {
                assert!(parent.starts_with("9400ZZMA"), "{parent}");
            }
        }
        // Every call is at a stop the model has.
        let stop_ids: BTreeSet<&str> = model.stops.iter().map(|s| s.stop_id.as_str()).collect();
        for p in &model.patterns {
            assert!(
                p.stops
                    .iter()
                    .all(|c| stop_ids.contains(c.stop_id.as_str()))
            );
        }
        // All seven lines, in file order; blue and green have stations.
        let lines: Vec<&str> = model.lines.iter().map(|l| l.line_id.as_str()).collect();
        assert_eq!(
            lines,
            ["blue", "green", "navy", "pink", "purple", "red", "yellow"]
        );
        assert!(!model.lines[0].directions.is_empty());
        assert!(model.lines[2].directions.is_empty());
        // The horizon: yesterday to today + 60.
        assert_eq!(model.horizon.from.to_string(), "2026-10-18");
        assert_eq!(model.horizon.to.to_string(), "2026-12-18");
        assert!(model.service_day_count() > 0);
        // Deterministic.
        assert_eq!(fixture("2026-10-19"), model);
    }

    #[test]
    fn a_calendar_dates_removal_drops_a_day() {
        // Service 39665 runs Monday to Thursday from 2026-10-19 and not on
        // 2026-12-24 (a Thursday), by a calendar_dates removal.
        let model = fixture("2026-12-01");
        let days = model
            .service_days
            .iter()
            .find(|s| s.service_id == "39665")
            .unwrap();
        let date = |d: &str| NaiveDate::parse_from_str(d, "%Y-%m-%d").unwrap();
        assert!(days.dates.contains(&date("2026-12-23")));
        assert!(!days.dates.contains(&date("2026-12-24")));
        assert!(days.dates.iter().all(|d| *d >= date("2026-11-30")));
    }

    /// A small zip with `files`.
    fn zip(files: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = zip::ZipWriter::new(Cursor::new(&mut buf));
            let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
            for (name, text) in files {
                writer.start_file(*name, options).unwrap();
                writer.write_all(text.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }
        buf
    }

    const AGENCY: &str = "agency_id,agency_name,agency_noc\nA1,Metrolink,METL\nA2,Bus,XXXX\n";
    const STOPS: &str = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
        9400ZZMAAAA1,Aaa (Manchester Metrolink),53.1,-2.1,0,940GZZMAAAA\n\
        940GZZMAAAA,Aaa (Manchester Metrolink),53.1,-2.1,1,\n\
        9400ZZMABBB1,Bbb (Manchester Metrolink),53.2,-2.2,0,940GZZMABBB\n\
        940GZZMABBB,Bbb (Manchester Metrolink),53.2,-2.2,1,\n";
    const CALENDAR: &str = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n\
        S,1,1,1,1,1,1,1,20261001,20261231\n";

    fn feed(routes: &str, stop_times: &str) -> Vec<u8> {
        zip(&[
            ("agency.txt", AGENCY),
            ("routes.txt", routes),
            (
                "trips.txt",
                "route_id,service_id,trip_id,trip_headsign,direction_id\n\
                 R1,S,T1,Bbb (Manchester Metrolink),0\n\
                 R1,S,T2,Bbb (Manchester Metrolink),0\n\
                 R9,S,T9,,0\n",
            ),
            ("stop_times.txt", stop_times),
            ("stops.txt", STOPS),
            ("calendar.txt", CALENDAR),
        ])
    }

    const ROUTES: &str = "route_id,agency_id,route_short_name,route_type\n\
        R1,A1,Blue Line,0\nR9,A2,99,3\n";
    const STOP_TIMES: &str = "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\n\
        T1,23:50:00,23:50:00,9400ZZMAAAA1,1,0,1\n\
        T1,24:10:00,24:10:00,9400ZZMABBB1,2,1,0\n\
        T2,10:00:00,10:00:00,9400ZZMAAAA1,1,0,1\n\
        T2,10:20:00,10:20:00,9400ZZMABBB1,2,1,0\n\
        T9,10:00:00,10:00:00,1800XX1,1,0,0\n";

    #[test]
    fn times_past_midnight_survive_and_same_shape_trips_share_a_pattern() {
        let model = parse(
            Cursor::new(feed(ROUTES, STOP_TIMES)),
            &metrolink(),
            &options("2026-10-15"),
        )
        .unwrap();
        assert_eq!(model.trips.len(), 2, "the other operator's trip is dropped");
        assert_eq!(model.trips[0].trip_id, "T1");
        assert_eq!(model.trips[0].start_s, 23 * 3600 + 50 * 60);
        assert_eq!(model.trips[0].headsign.as_deref(), Some("Bbb"));
        // T1 runs 20 minutes, T2 20: one pattern? No: the same offsets
        // and stops, so yes.
        assert_eq!(model.patterns.len(), 1);
        let p = &model.patterns[0];
        assert_eq!(p.stops[1].arr_offset_s, 1200);
        assert_eq!((p.stops[0].pickup, p.stops[0].drop_off), (0, 1));
        assert_eq!(
            model.lines[0].directions[0].stations,
            ["9400ZZMAAAA", "9400ZZMABBB"]
        );
        assert_eq!(model.stops.len(), 4);
        assert_eq!(model.stops[0].stop_id, "9400ZZMAAAA");
        assert_eq!(model.stops[0].name, "Aaa");
    }

    #[test]
    fn a_parentless_stop_named_like_a_station_is_that_station() {
        let stops = format!("{STOPS}9400ZZMABBB,Bbb (Manchester Metrolink),53.2,-2.2,0,\n");
        let stop_times = STOP_TIMES.replace(
            "T2,10:20:00,10:20:00,9400ZZMABBB1",
            "T2,10:20:00,10:20:00,9400ZZMABBB",
        );
        let bytes = zip(&[
            ("agency.txt", AGENCY),
            ("routes.txt", ROUTES),
            (
                "trips.txt",
                "route_id,service_id,trip_id,trip_headsign,direction_id\n\
                 R1,S,T1,,0\nR1,S,T2,,0\n",
            ),
            ("stop_times.txt", &stop_times),
            ("stops.txt", &stops),
            ("calendar.txt", CALENDAR),
        ]);
        let model = parse(Cursor::new(bytes), &metrolink(), &options("2026-10-15")).unwrap();
        let ids: Vec<&str> = model.stops.iter().map(|s| s.stop_id.as_str()).collect();
        // One row per id: the station, not a platform as well.
        assert_eq!(
            ids,
            ["9400ZZMAAAA", "9400ZZMABBB", "9400ZZMAAAA1", "9400ZZMABBB1"]
        );
        assert_eq!(model.stops[1].kind, StopKind::Station);
        assert_eq!(
            model.lines[0].directions[0].stations,
            ["9400ZZMAAAA", "9400ZZMABBB"]
        );
    }

    #[test]
    fn an_unmapped_route_refuses_the_feed() {
        let routes = "route_id,agency_id,route_short_name,route_type\n\
            R1,A1,Orange Line,0\nR9,A2,99,3\n";
        let err = parse(
            Cursor::new(feed(routes, STOP_TIMES)),
            &metrolink(),
            &options("2026-10-15"),
        )
        .unwrap_err();
        assert_eq!(err.reason(), "unmapped_route", "{err}");
        assert!(err.to_string().contains("Orange Line"));
    }

    #[test]
    fn an_entry_over_its_cap_aborts() {
        let mut opts = options("2026-10-15");
        opts.max_entry_bytes = 200;
        let err = parse(Cursor::new(feed(ROUTES, STOP_TIMES)), &metrolink(), &opts).unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");
    }

    #[test]
    fn a_large_stop_times_is_streamed_and_filtered() {
        // 200k rows of another operator's trip around the network's: only
        // the network's 4 calls are kept (rows are filtered as read, never
        // collected).
        let mut stop_times = String::from(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\n",
        );
        for i in 0..200_000 {
            writeln!(stop_times, "T9,10:00:00,10:00:00,1800XX{i},{i},0,0").unwrap();
        }
        stop_times.push_str(&STOP_TIMES[STOP_TIMES.find('\n').unwrap() + 1..]);
        let model = parse(
            Cursor::new(feed(ROUTES, &stop_times)),
            &metrolink(),
            &options("2026-10-15"),
        )
        .unwrap();
        assert_eq!(model.trips.len(), 2);
        assert_eq!(model.stops.len(), 4);
    }

    #[test]
    fn helpers() {
        assert_eq!(parse_time("25:01:02").unwrap(), Some(25 * 3600 + 62));
        assert_eq!(parse_time("").unwrap(), None);
        assert!(parse_time("1:2").is_err());
        assert!(parse_time("10:61:00").is_err());
        assert_eq!(station_id("940GZZMAPIC"), "9400ZZMAPIC");
        assert_eq!(station_id("1800XX"), "1800XX");
        assert_eq!(
            clean_name("Piccadilly (Manchester Metrolink)", "Manchester Metrolink"),
            "Piccadilly"
        );
        assert_eq!(clean_name("Bus stop", "Manchester Metrolink"), "Bus stop");
        assert!(parse_pickup("4").is_err());
    }

    /// The whole North West file (`TRANSIT_REAL_GTFS`, a local copy that is
    /// never committed): the counts of spec §3.2 and the parse time.
    #[test]
    #[ignore = "needs TRANSIT_REAL_GTFS, a downloaded BODS North West GTFS zip"]
    fn the_real_north_west_feed() {
        let Ok(path) = std::env::var("TRANSIT_REAL_GTFS") else {
            return;
        };
        let started = std::time::Instant::now();
        let file = std::io::BufReader::new(std::fs::File::open(path).unwrap());
        let today = common::transit::service_date(chrono::Utc::now());
        let model = parse(
            file,
            &metrolink(),
            &GtfsOptions {
                today,
                horizon_days: 60,
                max_entry_bytes: MAX_ENTRY_BYTES,
            },
        )
        .unwrap();
        let replacement = model.trips.iter().filter(|t| t.replacement_bus).count();
        eprintln!(
            "parsed in {:?}: {} lines, {} stops, {} patterns, {} trips ({replacement} replacement bus), {} services, {} service days",
            started.elapsed(),
            model.lines.len(),
            model.stops.len(),
            model.patterns.len(),
            model.trips.len(),
            model.service_days.len(),
            model.service_day_count(),
        );
        assert!(model.trips.len() > 10_000);
    }
}
