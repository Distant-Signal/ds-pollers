//! The `TransXChange` adapter (NET design §4): a zip of TXC documents (the
//! Traveline National Dataset's regional `EM.zip`, one document per
//! service) read into the same [`TransitModel`] as the GTFS adapter.
//!
//! 1. **Which documents.** Every `.xml` entry, in name order, is read
//!    through a [`CappedReader`]; one that does not even mention a network
//!    NOC is skipped unparsed. The rest are parsed into a small element
//!    tree (route geometry and other unused sections skipped), and kept
//!    only if an `Operators` entry's `NationalOperatorCode` (or
//!    `OperatorCode`) is one of the network's. Mode tags are not trusted.
//! 2. **Lines.** A journey's `LineRef` names a `Line`, whose `LineName`
//!    maps to a DS line (`txc_line_names`) or a replacement bus; any other
//!    name refuses the feed ([`FeedError::UnmappedRoute`]).
//! 3. **Calls.** A journey pattern's timing links, in order: the first
//!    stop departs at `DepartureTime`, each link adds its `RunTime`, and a
//!    stop's dwell is the previous link's `To/WaitTime` plus the next
//!    link's `From/WaitTime`; a journey's `VehicleJourneyTimingLink`s
//!    override them. `pass` stops are not calls.
//! 4. **Dates.** The journey's operating profile (else its pattern's, else
//!    its service's), expanded from yesterday to today plus the horizon,
//!    inside the service's `OperatingPeriod`; a later document of the same
//!    `ServiceCode` takes over from its start date.
//! 5. **Folding**, as GTFS: patterns by (line, direction, calls), service
//!    ids by date set, each line's station order.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, Read, Seek};

use chrono::{Datelike, Days, NaiveDate, Weekday};
use common::transit::{
    Direction, Horizon, StopKind, TransitPattern, TransitPatternStop, TransitServiceDays,
    TransitStop, TransitTrip, is_stop_id,
};
use quick_xml::events::Event;
use zip::ZipArchive;

use super::{CappedReader, FeedError};
use crate::model::TransitModel;
use crate::network::NetworkConfig;

/// The inflated-size cap of one TXC document (a whole entry is held while
/// it is parsed; TNDS documents are one service each).
pub(crate) const MAX_ENTRY_BYTES: u64 = 256 * 1024 * 1024;

/// The deepest element nesting accepted (TXC needs about 10).
const MAX_DEPTH: usize = 64;

/// Sections never read: route geometry and other metadata. Skipped
/// unparsed, so they cost no memory.
const SKIPPED: &[&[u8]] = &[
    b"RouteSections",
    b"Routes",
    b"ServicedOrganisations",
    b"Garages",
    b"Registrations",
    b"SupportingDocuments",
    b"NptgLocalities",
];

/// How to read a feed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TxcOptions {
    /// Today in the network's timezone: the horizon starts the day before.
    pub today: NaiveDate,
    /// Days stored after today (Q8: 60).
    pub horizon_days: u32,
    pub max_entry_bytes: u64,
}

fn malformed(what: impl std::fmt::Display) -> FeedError {
    FeedError::Malformed(format!("TXC: {what}"))
}

// --- A small element tree --------------------------------------------------

/// One element: its local name, attributes, direct text and children.
#[derive(Debug, Default)]
struct Node {
    name: String,
    attrs: Vec<(String, String)>,
    text: String,
    children: Vec<Node>,
}

impl Node {
    fn child(&self, name: &str) -> Option<&Node> {
        self.children.iter().find(|c| c.name == name)
    }

    fn children<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Node> + 'a {
        self.children.iter().filter(move |c| c.name == name)
    }

    fn path(&self, names: &[&str]) -> Option<&Node> {
        names.iter().try_fold(self, |node, name| node.child(name))
    }

    /// The trimmed text at `names`, if not empty.
    fn text_at(&self, names: &[&str]) -> Option<&str> {
        self.path(names)
            .map(|n| n.text.trim())
            .filter(|t| !t.is_empty())
    }

    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Parses a document into a tree, skipping [`SKIPPED`] sections.
fn parse_tree<R: BufRead>(input: R) -> Result<Node, FeedError> {
    let mut reader = quick_xml::Reader::from_reader(input);
    let mut buf = Vec::new();
    let mut skip = Vec::new();
    let mut stack: Vec<Node> = Vec::new();
    let mut root: Option<Node> = None;
    loop {
        let event = reader.read_event_into(&mut buf).map_err(malformed)?;
        match event {
            Event::Start(e) => {
                if SKIPPED.contains(&e.local_name().as_ref()) {
                    let end = e.name().as_ref().to_vec();
                    reader
                        .read_to_end_into(quick_xml::name::QName(&end), &mut skip)
                        .map_err(malformed)?;
                    skip.clear();
                } else {
                    if stack.len() >= MAX_DEPTH {
                        return Err(malformed("elements nested too deeply"));
                    }
                    stack.push(element(&e)?);
                }
            }
            Event::Empty(e) => {
                let node = element(&e)?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(node),
                    None => return Err(malformed("an empty root element")),
                }
            }
            Event::Text(t) => {
                if let Some(top) = stack.last_mut() {
                    let text = t.decode().map_err(malformed)?;
                    if !text.trim().is_empty() || !top.text.is_empty() {
                        top.text.push_str(&text);
                    }
                }
            }
            Event::CData(t) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&String::from_utf8_lossy(&t));
                }
            }
            Event::GeneralRef(r) => {
                if let Some(top) = stack.last_mut() {
                    if let Ok(Some(ch)) = r.resolve_char_ref() {
                        top.text.push(ch);
                    } else {
                        let name = r.decode().map_err(malformed)?;
                        top.text.push_str(match name.as_ref() {
                            "amp" => "&",
                            "lt" => "<",
                            "gt" => ">",
                            "quot" => "\"",
                            "apos" => "'",
                            _ => "",
                        });
                    }
                }
            }
            Event::End(_) => {
                let node = stack.pop().ok_or_else(|| malformed("unbalanced end tag"))?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(node),
                    None => root = Some(node),
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    if !stack.is_empty() {
        return Err(malformed("the document ends inside an element"));
    }
    root.ok_or_else(|| malformed("no root element"))
}

fn element(e: &quick_xml::events::BytesStart<'_>) -> Result<Node, FeedError> {
    let mut attrs = Vec::new();
    for attr in e.attributes() {
        let attr = attr.map_err(malformed)?;
        attrs.push((
            String::from_utf8_lossy(attr.key.local_name().as_ref()).into_owned(),
            String::from_utf8_lossy(&attr.value).into_owned(),
        ));
    }
    Ok(Node {
        name: String::from_utf8_lossy(e.local_name().as_ref()).into_owned(),
        attrs,
        text: String::new(),
        children: Vec::new(),
    })
}

// --- Values ------------------------------------------------------------------

/// `HH:MM:SS` or `HH:MM` as seconds after midnight.
fn parse_time(text: &str) -> Result<i32, FeedError> {
    let parts: Vec<&str> = text.trim().split(':').collect();
    let num = |p: &str| {
        p.parse::<i32>()
            .map_err(|_| malformed(format!("time {text:?}")))
    };
    let (h, m, s) = match parts.as_slice() {
        [h, m, s] => (num(h)?, num(m)?, num(s)?),
        [h, m] => (num(h)?, num(m)?, 0),
        _ => return Err(malformed(format!("time {text:?}"))),
    };
    if !(0..24).contains(&h) || !(0..60).contains(&m) || !(0..60).contains(&s) {
        return Err(malformed(format!("time {text:?}")));
    }
    Ok(h * 3600 + m * 60 + s)
}

/// An ISO 8601 duration (`PT6M`, `PT1M30S`, `P0DT1H`) as whole seconds.
fn parse_duration(text: &str) -> Result<i32, FeedError> {
    let bad = || malformed(format!("duration {text:?}"));
    let rest = text.trim().strip_prefix('P').ok_or_else(bad)?;
    let (days, time) = match rest.split_once('T') {
        Some((d, t)) => (d, t),
        None => (rest, ""),
    };
    let mut total: f64 = 0.0;
    let mut components = 0usize;
    for (part, units) in [
        (days, &[('D', 86_400.0)][..]),
        (time, &[('H', 3600.0), ('M', 60.0), ('S', 1.0)][..]),
    ] {
        let mut number = String::new();
        for ch in part.chars() {
            if ch.is_ascii_digit() || ch == '.' {
                number.push(ch);
            } else {
                let scale = units
                    .iter()
                    .find(|(unit, _)| *unit == ch)
                    .map(|(_, s)| *s)
                    .ok_or_else(bad)?;
                let value: f64 = number.parse().map_err(|_| bad())?;
                total += value * scale;
                components += 1;
                number.clear();
            }
        }
        if !number.is_empty() {
            return Err(bad());
        }
    }
    if components == 0 || !(0.0..=7.0 * 86_400.0).contains(&total) {
        return Err(bad());
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "bounded to a week above, rounded to whole seconds"
    )]
    Ok(total.round() as i32)
}

fn parse_date(text: &str) -> Result<NaiveDate, FeedError> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
        .map_err(|_| malformed(format!("date {text:?}")))
}

/// A stop's `Activity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Activity {
    PickUpAndSetDown,
    PickUp,
    SetDown,
    Pass,
}

fn activity(end: &Node) -> Option<Activity> {
    Some(match end.text_at(&["Activity"])? {
        "pickUp" => Activity::PickUp,
        "setDown" => Activity::SetDown,
        "pass" => Activity::Pass,
        _ => Activity::PickUpAndSetDown,
    })
}

/// The station of a call at `stop`: the platform code without its
/// trailing digits when that keeps the network's prefix
/// (`9400ZZNOBCF1` to `9400ZZNOBCF`; every NET `PLT` row follows the rule,
/// NET design §2.4). `None` for a stop outside the network (a bus stop) or
/// that is itself a station.
pub(crate) fn station_of(stop: &str, prefix: &str) -> Option<String> {
    if !stop.starts_with(prefix) {
        return None;
    }
    let station = stop.trim_end_matches(|c: char| c.is_ascii_digit());
    (station.len() < stop.len() && station.len() > prefix.len()).then(|| station.to_owned())
}

// --- Operating profiles ------------------------------------------------------

/// A bank holiday as TXC names it, or `OtherPublicHoliday`'s date.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Holiday {
    Named(String),
    Date(NaiveDate),
}

/// One operating profile, read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Profile {
    /// Monday first.
    days: [bool; 7],
    bank_on: Vec<Holiday>,
    bank_off: Vec<Holiday>,
    special_on: Vec<(NaiveDate, NaiveDate)>,
    special_off: Vec<(NaiveDate, NaiveDate)>,
}

impl Default for Profile {
    /// Every day (a profile with no `RegularDayType`).
    fn default() -> Self {
        Self {
            days: [true; 7],
            bank_on: Vec::new(),
            bank_off: Vec::new(),
            special_on: Vec::new(),
            special_off: Vec::new(),
        }
    }
}

/// The weekdays a `DaysOfWeek` child names (Monday first).
fn days_named(name: &str) -> Option<[bool; 7]> {
    let only = |i: usize| {
        let mut d = [false; 7];
        d[i] = true;
        d
    };
    let not = |i: usize| {
        let mut d = [true; 7];
        d[i] = false;
        d
    };
    Some(match name {
        "Monday" => only(0),
        "Tuesday" => only(1),
        "Wednesday" => only(2),
        "Thursday" => only(3),
        "Friday" => only(4),
        "Saturday" => only(5),
        "Sunday" => only(6),
        "MondayToFriday" => [true, true, true, true, true, false, false],
        "MondayToSaturday" => [true, true, true, true, true, true, false],
        "MondayToSunday" => [true; 7],
        "Weekend" => [false, false, false, false, false, true, true],
        "NotMonday" => not(0),
        "NotTuesday" => not(1),
        "NotWednesday" => not(2),
        "NotThursday" => not(3),
        "NotFriday" => not(4),
        "NotSaturday" => not(5),
        "NotSunday" => not(6),
        _ => return None,
    })
}

fn holidays(node: Option<&Node>) -> Result<Vec<Holiday>, FeedError> {
    let mut out = Vec::new();
    for child in node.map(|n| n.children.as_slice()).unwrap_or_default() {
        if child.name == "OtherPublicHoliday" {
            if let Some(date) = child.text_at(&["Date"]) {
                out.push(Holiday::Date(parse_date(date)?));
            }
        } else {
            out.push(Holiday::Named(child.name.clone()));
        }
    }
    Ok(out)
}

fn ranges(node: Option<&Node>) -> Result<Vec<(NaiveDate, NaiveDate)>, FeedError> {
    let mut out = Vec::new();
    for range in node.into_iter().flat_map(|n| n.children("DateRange")) {
        let start = parse_date(
            range
                .text_at(&["StartDate"])
                .ok_or_else(|| malformed("a DateRange without its StartDate"))?,
        )?;
        let end = match range.text_at(&["EndDate"]) {
            Some(end) => parse_date(end)?,
            None => start,
        };
        out.push((start, end));
    }
    Ok(out)
}

fn read_profile(node: &Node) -> Result<Profile, FeedError> {
    let mut out = Profile::default();
    if let Some(regular) = node.child("RegularDayType") {
        out.days = [false; 7];
        if let Some(days) = regular.child("DaysOfWeek") {
            for day in &days.children {
                let mask = days_named(&day.name)
                    .ok_or_else(|| malformed(format!("DaysOfWeek {:?}", day.name)))?;
                for (have, add) in out.days.iter_mut().zip(mask) {
                    *have |= add;
                }
            }
        }
        // `HolidaysOnly`: no regular day; the bank holidays add the days.
    }
    if let Some(bank) = node.child("BankHolidayOperation") {
        out.bank_on = holidays(bank.child("DaysOfOperation"))?;
        out.bank_off = holidays(bank.child("DaysOfNonOperation"))?;
    }
    if let Some(special) = node.child("SpecialDaysOperation") {
        out.special_on = ranges(special.child("DaysOfOperation"))?;
        out.special_off = ranges(special.child("DaysOfNonOperation"))?;
    }
    Ok(out)
}

/// Easter Sunday (the anonymous Gregorian computus).
#[expect(
    clippy::many_single_char_names,
    reason = "the computus's own variable names"
)]
fn easter(year: i32) -> Option<NaiveDate> {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = (h + l - 7 * m + 114) % 31 + 1;
    NaiveDate::from_ymd_opt(year, u32::try_from(month).ok()?, u32::try_from(day).ok()?)
}

/// The `n`th (1-based) `weekday` of a month, or the last when `n` is 0.
fn nth_weekday(year: i32, month: u32, weekday: Weekday, n: u32) -> Option<NaiveDate> {
    if n == 0 {
        let next = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1)?
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1)?
        };
        let mut day = next.pred_opt()?;
        while day.weekday() != weekday {
            day = day.pred_opt()?;
        }
        Some(day)
    } else {
        NaiveDate::from_weekday_of_month_opt(year, month, weekday, u8::try_from(n).ok()?)
    }
}

/// England and Wales's bank holidays of `year` by their TXC names, with
/// TXC's groups (NET design §4.4). One-off holidays are not known.
fn holiday_dates(year: i32) -> BTreeMap<&'static str, Vec<NaiveDate>> {
    let date = |m: u32, d: u32| NaiveDate::from_ymd_opt(year, m, d);
    let mut named: BTreeMap<&'static str, Vec<NaiveDate>> = BTreeMap::new();
    let mut put = |name: &'static str, day: Option<NaiveDate>| {
        if let Some(day) = day {
            named.entry(name).or_default().push(day);
        }
    };
    let new_year = date(1, 1);
    put("NewYearsDay", new_year);
    put(
        "NewYearsDayHoliday",
        new_year.and_then(|d| match d.weekday() {
            Weekday::Sat => date(1, 3),
            Weekday::Sun => date(1, 2),
            _ => None,
        }),
    );
    let easter_day = easter(year);
    put(
        "GoodFriday",
        easter_day.and_then(|e| e.checked_sub_days(Days::new(2))),
    );
    put(
        "EasterMonday",
        easter_day.and_then(|e| e.checked_add_days(Days::new(1))),
    );
    put("MayDay", nth_weekday(year, 5, Weekday::Mon, 1));
    put("SpringBank", nth_weekday(year, 5, Weekday::Mon, 0));
    put(
        "LateSummerBankHolidayNotScotland",
        nth_weekday(year, 8, Weekday::Mon, 0),
    );
    put("ChristmasEve", date(12, 24));
    put("ChristmasDay", date(12, 25));
    put("BoxingDay", date(12, 26));
    put("NewYearsEve", date(12, 31));
    // Substitutes: Christmas on a Saturday moves to Monday 27th and Boxing
    // Day to Tuesday 28th; on a Sunday, Christmas moves to Tuesday 27th;
    // Boxing Day on a Saturday (Christmas on a Friday) moves to Monday 28th.
    match date(12, 25).map(|d| d.weekday()) {
        Some(Weekday::Sat) => {
            put("ChristmasDayHoliday", date(12, 27));
            put("BoxingDayHoliday", date(12, 28));
        }
        Some(Weekday::Sun) => put("ChristmasDayHoliday", date(12, 27)),
        Some(Weekday::Fri) => put("BoxingDayHoliday", date(12, 28)),
        _ => {}
    }
    let group = |names: &[&str]| -> Vec<NaiveDate> {
        names
            .iter()
            .flat_map(|n| named.get(n).cloned().unwrap_or_default())
            .collect()
    };
    let displacement = group(&[
        "NewYearsDayHoliday",
        "ChristmasDayHoliday",
        "BoxingDayHoliday",
    ]);
    let christmas = group(&["ChristmasDay", "BoxingDay"]);
    let early_run_off = group(&["ChristmasEve", "NewYearsEve"]);
    let holiday_mondays = group(&[
        "EasterMonday",
        "MayDay",
        "SpringBank",
        "LateSummerBankHolidayNotScotland",
    ]);
    let except_christmas = group(&[
        "NewYearsDay",
        "NewYearsDayHoliday",
        "GoodFriday",
        "EasterMonday",
        "MayDay",
        "SpringBank",
        "LateSummerBankHolidayNotScotland",
    ]);
    let all = group(&[
        "NewYearsDay",
        "NewYearsDayHoliday",
        "GoodFriday",
        "EasterMonday",
        "MayDay",
        "SpringBank",
        "LateSummerBankHolidayNotScotland",
        "ChristmasDay",
        "ChristmasDayHoliday",
        "BoxingDay",
        "BoxingDayHoliday",
    ]);
    named.insert("DisplacementHolidays", displacement);
    named.insert("Christmas", christmas);
    named.insert("EarlyRunOff", early_run_off);
    named.insert("HolidayMondays", holiday_mondays);
    named.insert("AllHolidaysExceptChristmas", except_christmas);
    named.insert("AllBankHolidays", all);
    named
}

/// The bank holiday dates in `[from, to]`, by TXC name.
struct HolidayCalendar(BTreeMap<&'static str, BTreeSet<NaiveDate>>);

impl HolidayCalendar {
    fn new(from: NaiveDate, to: NaiveDate) -> Self {
        let mut out: BTreeMap<&'static str, BTreeSet<NaiveDate>> = BTreeMap::new();
        for year in from.year()..=to.year() {
            for (name, dates) in holiday_dates(year) {
                out.entry(name).or_default().extend(dates);
            }
        }
        Self(out)
    }

    fn is(&self, holiday: &Holiday, day: NaiveDate) -> bool {
        match holiday {
            Holiday::Date(date) => *date == day,
            Holiday::Named(name) => self.0.get(name.as_str()).is_some_and(|d| d.contains(&day)),
        }
    }
}

/// The dates `profile` runs on in `[from, to]` (both already clipped to
/// the service's period).
fn expand(
    profile: &Profile,
    from: NaiveDate,
    to: NaiveDate,
    calendar: &HolidayCalendar,
) -> BTreeSet<NaiveDate> {
    let within = |ranges: &[(NaiveDate, NaiveDate)], day: NaiveDate| {
        ranges
            .iter()
            .any(|(start, end)| *start <= day && day <= *end)
    };
    let mut out = BTreeSet::new();
    let mut day = from;
    while day <= to {
        let mut runs = profile.days[day.weekday().num_days_from_monday() as usize];
        if profile.bank_off.iter().any(|h| calendar.is(h, day)) {
            runs = false;
        }
        if profile.bank_on.iter().any(|h| calendar.is(h, day)) {
            runs = true;
        }
        if within(&profile.special_on, day) {
            runs = true;
        }
        if within(&profile.special_off, day) {
            runs = false;
        }
        if runs {
            out.insert(day);
        }
        let Some(next) = day.succ_opt() else { break };
        day = next;
    }
    out
}

// --- Documents ---------------------------------------------------------------

/// A kept document, by its zip entry name.
struct Document {
    entry: String,
    root: Node,
}

/// One end of a timing link.
#[derive(Debug, Clone)]
struct End {
    stop: String,
    activity: Option<Activity>,
    wait: i32,
}

#[derive(Debug, Clone)]
struct Link {
    id: Option<String>,
    from: End,
    to: End,
    run: i32,
}

fn end(link: &Node, which: &str) -> Result<End, FeedError> {
    let node = link
        .child(which)
        .ok_or_else(|| malformed(format!("a timing link without its {which}")))?;
    Ok(End {
        stop: node
            .text_at(&["StopPointRef"])
            .ok_or_else(|| malformed(format!("a timing link's {which} without a stop")))?
            .to_owned(),
        activity: activity(node),
        wait: node
            .text_at(&["WaitTime"])
            .map(parse_duration)
            .transpose()?
            .unwrap_or(0),
    })
}

/// A journey's override of one link.
#[derive(Debug, Default, Clone, Copy)]
struct Override {
    run: Option<i32>,
    from_wait: Option<i32>,
    to_wait: Option<i32>,
}

/// What a route name is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RouteKind {
    Line(usize),
    ReplacementBus,
}

/// One journey, resolved: absolute times (seconds after the service
/// day's midnight) and its dates.
struct Journey {
    trip_id: String,
    route: RouteKind,
    direction: Direction,
    headsign: Option<String>,
    /// (platform, arrival, departure, pickup, drop-off).
    calls: Vec<(String, i32, i32, u8, u8)>,
    dates: BTreeSet<NaiveDate>,
}

/// Whether `doc` is one of the network's: an operator with one of its
/// NOCs. The matching operator ids.
fn network_operators(doc: &Node, nocs: &[String]) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let Some(operators) = doc.child("Operators") else {
        return ids;
    };
    for operator in &operators.children {
        let codes = [
            operator.text_at(&["NationalOperatorCode"]),
            operator.text_at(&["OperatorCode"]),
        ];
        if codes
            .iter()
            .flatten()
            .any(|code| nocs.iter().any(|n| n == code))
        {
            ids.insert(operator.attr("id").unwrap_or_default().to_owned());
        }
    }
    ids
}

/// Whether `bytes` mentions any of `nocs` as an element's whole text
/// (`>NEXT<`): a cheap test before a document is parsed.
fn mentions(bytes: &[u8], nocs: &[String]) -> bool {
    nocs.iter().any(|noc| {
        let needle = format!(">{noc}<");
        bytes
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
    })
}

/// Reads every `.xml` entry of `archive` that is one of the network's.
fn documents<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    network: &NetworkConfig,
    cap: u64,
) -> Result<Vec<Document>, FeedError> {
    let mut names: Vec<String> = archive
        .file_names()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| malformed(format!("zip: {err}")))?
        .into_iter()
        .map(std::borrow::Cow::into_owned)
        .collect();
    names.sort();
    let mut kept = Vec::new();
    let mut nested = 0usize;
    for name in names {
        let extension = std::path::Path::new(&name)
            .extension()
            .map(std::ffi::OsStr::to_ascii_lowercase);
        match extension.as_ref().and_then(|e| e.to_str()) {
            Some("xml") => {}
            Some("zip") => {
                nested += 1;
                continue;
            }
            _ => continue,
        }
        let entry = archive
            .by_name(&name)
            .map_err(|err| malformed(format!("{name}: {err}")))?;
        let mut bytes = Vec::new();
        CappedReader::new(entry, cap, name.clone())
            .read_to_end(&mut bytes)
            .map_err(|err| {
                if err.to_string().contains("inflated-size cap") {
                    FeedError::Oversize(format!("{name} (inflated)"))
                } else {
                    malformed(format!("{name}: {err}"))
                }
            })?;
        if !mentions(&bytes, &network.nocs) {
            continue;
        }
        let root = parse_tree(bytes.as_slice()).map_err(|err| match err {
            FeedError::Malformed(what) => FeedError::Malformed(format!("{name}: {what}")),
            other => other,
        })?;
        if !network_operators(&root, &network.nocs).is_empty() {
            kept.push(Document { entry: name, root });
        }
    }
    if nested > 0 {
        tracing::info!(nested, "skipped zip entries inside the TXC zip");
    }
    Ok(kept)
}

/// The days each document serves each of its services on: from its
/// `OperatingPeriod` start, to its end or the day before a later
/// document of the same `ServiceCode` starts (a new timetable takes over).
/// Keyed by (document index, service code).
fn service_periods(docs: &[Document]) -> Result<ServicePeriods, FeedError> {
    let mut by_code: BTreeMap<String, Vec<(NaiveDate, String, usize, NaiveDate)>> = BTreeMap::new();
    for (i, doc) in docs.iter().enumerate() {
        let revision = doc
            .root
            .attr("ModificationDateTime")
            .unwrap_or_default()
            .to_owned();
        for service in doc
            .root
            .child("Services")
            .into_iter()
            .flat_map(|s| s.children("Service"))
        {
            let code = service.text_at(&["ServiceCode"]).ok_or_else(|| {
                malformed(format!("{}: a service without its ServiceCode", doc.entry))
            })?;
            let start = service
                .text_at(&["OperatingPeriod", "StartDate"])
                .map(parse_date)
                .transpose()?
                .unwrap_or(NaiveDate::MIN);
            let end = service
                .text_at(&["OperatingPeriod", "EndDate"])
                .map(parse_date)
                .transpose()?
                .unwrap_or(NaiveDate::MAX);
            by_code
                .entry(code.to_owned())
                .or_default()
                .push((start, revision.clone(), i, end));
        }
    }
    let mut out = HashMap::new();
    for (code, mut versions) in by_code {
        versions.sort();
        for (n, (start, _, doc, end)) in versions.iter().enumerate() {
            let end = match versions.get(n + 1) {
                Some((next, ..)) => (*end).min(next.pred_opt().unwrap_or(NaiveDate::MIN)),
                None => *end,
            };
            out.insert((*doc, code.clone()), (*start, end));
        }
    }
    Ok(out)
}

/// The kind of route `name` is in `network`.
fn route_kind(network: &NetworkConfig, name: &str) -> Result<RouteKind, FeedError> {
    if network
        .replacement_bus_route_names
        .iter()
        .any(|n| n == name)
    {
        Ok(RouteKind::ReplacementBus)
    } else if let Some(line) = network.line_for_route(name) {
        Ok(RouteKind::Line(line))
    } else {
        Err(FeedError::UnmappedRoute(name.to_owned()))
    }
}

/// The first and last day each (document, service code) serves.
type ServicePeriods = HashMap<(usize, String), (NaiveDate, NaiveDate)>;

/// A journey pattern of a document, read.
struct Pattern<'a> {
    direction: Direction,
    destination: Option<&'a str>,
    sections: Vec<&'a str>,
    profile: Option<&'a Node>,
}

/// A service of a document, read.
struct Service<'a> {
    lines: HashMap<&'a str, RouteKind>,
    only_line: Option<RouteKind>,
    patterns: HashMap<&'a str, Pattern<'a>>,
    profile: Option<&'a Node>,
    period: (NaiveDate, NaiveDate),
}

/// A pattern's identity: line, direction and calls (platform, arrival
/// and departure offsets, pickup, drop-off).
type PatternKey = (Option<usize>, Direction, Vec<(String, i32, i32, u8, u8)>);

/// Reads one document's journeys into `out`, its stop names into `names`.
#[expect(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "one pass over a document's services and journeys, in TXC's order"
)]
fn journeys(
    doc_index: usize,
    doc: &Document,
    network: &NetworkConfig,
    periods: &ServicePeriods,
    window: (NaiveDate, NaiveDate),
    calendar: &HolidayCalendar,
    names: &mut BTreeMap<String, String>,
    out: &mut Vec<Journey>,
) -> Result<(), FeedError> {
    let root = &doc.root;
    let entry = &doc.entry;
    let in_doc = |what: String| malformed(format!("{entry}: {what}"));
    let operators = network_operators(root, &network.nocs);

    if let Some(stops) = root.child("StopPoints") {
        for stop in &stops.children {
            let (code, name) = match stop.name.as_str() {
                "AnnotatedStopPointRef" => (
                    stop.text_at(&["StopPointRef"]),
                    stop.text_at(&["CommonName"]),
                ),
                "StopPoint" => (
                    stop.text_at(&["AtcoCode"]),
                    stop.text_at(&["Descriptor", "CommonName"]),
                ),
                _ => continue,
            };
            if let (Some(code), Some(name)) = (code, name) {
                names
                    .entry(code.to_owned())
                    .or_insert_with(|| network.clean_stop_name(name));
            }
        }
    }

    let mut sections: HashMap<&str, Vec<Link>> = HashMap::new();
    for section in root
        .child("JourneyPatternSections")
        .into_iter()
        .flat_map(|s| s.children("JourneyPatternSection"))
    {
        let id = section
            .attr("id")
            .ok_or_else(|| in_doc("a JourneyPatternSection without its id".to_owned()))?;
        let mut links = Vec::new();
        for link in section.children("JourneyPatternTimingLink") {
            links.push(Link {
                id: link.attr("id").map(str::to_owned),
                from: end(link, "From")?,
                to: end(link, "To")?,
                run: link
                    .text_at(&["RunTime"])
                    .map(parse_duration)
                    .transpose()?
                    .unwrap_or(0),
            });
        }
        sections.insert(id, links);
    }

    let mut services: HashMap<&str, Service<'_>> = HashMap::new();
    for service in root
        .child("Services")
        .into_iter()
        .flat_map(|s| s.children("Service"))
    {
        let Some(code) = service.text_at(&["ServiceCode"]) else {
            continue;
        };
        if let Some(operator) = service.text_at(&["RegisteredOperatorRef"])
            && !operators.contains(operator)
        {
            continue;
        }
        let mut lines = HashMap::new();
        for line in service
            .child("Lines")
            .into_iter()
            .flat_map(|l| l.children("Line"))
        {
            let name = line
                .text_at(&["LineName"])
                .ok_or_else(|| in_doc(format!("service {code}: a Line without its LineName")))?;
            lines.insert(
                line.attr("id").unwrap_or_default(),
                route_kind(network, name)?,
            );
        }
        let only_line = (lines.len() == 1)
            .then(|| lines.values().copied().next())
            .flatten();
        let mut patterns = HashMap::new();
        for pattern in service
            .path(&["StandardService"])
            .into_iter()
            .flat_map(|s| s.children("JourneyPattern"))
        {
            let id = pattern.attr("id").ok_or_else(|| {
                in_doc(format!("service {code}: a JourneyPattern without its id"))
            })?;
            let direction = match pattern.text_at(&["Direction"]) {
                Some("inbound" | "antiClockwise") => Direction::Inbound,
                _ => Direction::Outbound,
            };
            patterns.insert(
                id,
                Pattern {
                    direction,
                    destination: pattern.text_at(&["DestinationDisplay"]),
                    sections: pattern
                        .children("JourneyPatternSectionRefs")
                        .filter_map(|r| Some(r.text.trim()).filter(|t| !t.is_empty()))
                        .collect(),
                    profile: pattern.child("OperatingProfile"),
                },
            );
        }
        let Some(period) = periods.get(&(doc_index, code.to_owned())).copied() else {
            continue;
        };
        services.insert(
            code,
            Service {
                lines,
                only_line,
                patterns,
                profile: service.child("OperatingProfile"),
                period,
            },
        );
    }

    let vehicle_journeys: Vec<&Node> = root
        .child("VehicleJourneys")
        .into_iter()
        .flat_map(|v| v.children("VehicleJourney"))
        .collect();
    // A journey may borrow another's pattern (`VehicleJourneyRef`).
    let pattern_of: HashMap<&str, &str> = vehicle_journeys
        .iter()
        .filter_map(|vj| {
            Some((
                vj.text_at(&["VehicleJourneyCode"])?,
                vj.text_at(&["JourneyPatternRef"])?,
            ))
        })
        .collect();
    let mut frequent = 0usize;
    for (n, vj) in vehicle_journeys.iter().enumerate() {
        let Some(service_code) = vj.text_at(&["ServiceRef"]) else {
            continue;
        };
        let Some(service) = services.get(service_code) else {
            continue;
        };
        if let Some(operator) = vj.text_at(&["OperatorRef"])
            && !operators.contains(operator)
        {
            continue;
        }
        if vj.child("Frequency").is_some() {
            frequent += 1;
        }
        let code = vj
            .text_at(&["VehicleJourneyCode"])
            .or_else(|| vj.text_at(&["PrivateCode"]))
            .map_or_else(|| format!("vj{}", n + 1), str::to_owned);
        let route = match vj.text_at(&["LineRef"]) {
            Some(line) => *service
                .lines
                .get(line)
                .ok_or_else(|| in_doc(format!("journey {code}: no Line {line:?}")))?,
            None => service
                .only_line
                .ok_or_else(|| in_doc(format!("journey {code}: no LineRef")))?,
        };
        let pattern_ref = vj
            .text_at(&["JourneyPatternRef"])
            .or_else(|| {
                vj.text_at(&["VehicleJourneyRef"])
                    .and_then(|r| pattern_of.get(r).copied())
            })
            .ok_or_else(|| in_doc(format!("journey {code}: no JourneyPatternRef")))?;
        let pattern = service
            .patterns
            .get(pattern_ref)
            .ok_or_else(|| in_doc(format!("journey {code}: no JourneyPattern {pattern_ref:?}")))?;
        let mut links: Vec<&Link> = Vec::new();
        for section in &pattern.sections {
            links.extend(
                sections
                    .get(section)
                    .ok_or_else(|| in_doc(format!("no JourneyPatternSection {section:?}")))?,
            );
        }
        if links.is_empty() {
            return Err(in_doc(format!("journey {code}: a pattern without links")));
        }
        let mut overrides: HashMap<&str, Override> = HashMap::new();
        for link in vj.children("VehicleJourneyTimingLink") {
            let Some(id) = link.text_at(&["JourneyPatternTimingLinkRef"]) else {
                continue;
            };
            let wait = |which: &str| {
                link.text_at(&[which, "WaitTime"])
                    .map(parse_duration)
                    .transpose()
            };
            overrides.insert(
                id,
                Override {
                    run: link.text_at(&["RunTime"]).map(parse_duration).transpose()?,
                    from_wait: wait("From")?,
                    to_wait: wait("To")?,
                },
            );
        }
        let over = |link: &Link| {
            link.id
                .as_deref()
                .and_then(|id| overrides.get(id))
                .copied()
                .unwrap_or_default()
        };

        let departure = parse_time(
            vj.text_at(&["DepartureTime"])
                .ok_or_else(|| in_doc(format!("journey {code}: no DepartureTime")))?,
        )?;
        // (stop, arrival, departure, activity), every stop including passes.
        let mut stops: Vec<(String, i32, i32, Activity)> = Vec::with_capacity(links.len() + 1);
        stops.push((
            links[0].from.stop.clone(),
            departure,
            departure,
            links[0].from.activity.unwrap_or(Activity::PickUpAndSetDown),
        ));
        let mut time = departure;
        for (k, link) in links.iter().enumerate() {
            let this = over(link);
            let arrival = time + this.run.unwrap_or(link.run);
            let mut wait = this.to_wait.unwrap_or(link.to.wait);
            let next = links.get(k + 1);
            if let Some(next) = next {
                wait += over(next).from_wait.unwrap_or(next.from.wait);
            }
            time = arrival + wait;
            let activity = next
                .and_then(|n| n.from.activity)
                .or(link.to.activity)
                .unwrap_or(Activity::PickUpAndSetDown);
            stops.push((link.to.stop.clone(), arrival, time, activity));
        }
        stops.retain(|(.., activity)| *activity != Activity::Pass);
        if stops.len() < 2 {
            continue;
        }
        let last = stops.len() - 1;
        let calls = stops
            .into_iter()
            .enumerate()
            .map(|(i, (stop, arrival, departure, activity))| {
                let (mut pickup, mut drop_off) = match activity {
                    Activity::PickUp => (0, 1),
                    Activity::SetDown => (1, 0),
                    Activity::PickUpAndSetDown | Activity::Pass => (0, 0),
                };
                if i == 0 {
                    drop_off = 1;
                }
                if i == last {
                    pickup = 1;
                }
                (stop, arrival, departure, pickup, drop_off)
            })
            .collect();

        let profile_node = vj
            .child("OperatingProfile")
            .or(pattern.profile)
            .or(service.profile);
        let profile = profile_node
            .map(read_profile)
            .transpose()?
            .unwrap_or_default();
        let from = window.0.max(service.period.0);
        let to = window.1.min(service.period.1);
        let dates = if from <= to {
            expand(&profile, from, to, calendar)
        } else {
            BTreeSet::new()
        };
        let headsign = vj
            .text_at(&["DestinationDisplay"])
            .or(pattern.destination)
            .map(|h| network.clean_stop_name(h))
            .filter(|h| !h.is_empty());
        out.push(Journey {
            trip_id: format!("{service_code}:{code}"),
            route,
            direction: pattern.direction,
            headsign,
            calls,
            dates,
        });
    }
    if frequent > 0 {
        tracing::warn!(
            entry = %entry,
            frequent,
            "frequency-based journeys read as single departures (NET design §13)"
        );
    }
    Ok(())
}

/// Reads `reader` (the TXC zip) as one network's timetable. See the module
/// docs.
#[expect(
    clippy::too_many_lines,
    reason = "the module docs' steps 4 and 5 in order: ids, service days, patterns, trips, stops"
)]
pub(crate) fn parse<R: Read + Seek>(
    reader: R,
    network: &NetworkConfig,
    options: &TxcOptions,
) -> Result<TransitModel, FeedError> {
    let mut archive = ZipArchive::new(reader).map_err(|err| malformed(format!("zip: {err}")))?;
    let docs = documents(&mut archive, network, options.max_entry_bytes)?;
    if docs.is_empty() {
        return Err(malformed(format!(
            "no document with an operator of NOC {:?}",
            network.nocs
        )));
    }
    let from = options
        .today
        .checked_sub_days(Days::new(1))
        .ok_or_else(|| malformed("date range"))?;
    let to = options
        .today
        .checked_add_days(Days::new(u64::from(options.horizon_days)))
        .ok_or_else(|| malformed("date range"))?;
    let calendar = HolidayCalendar::new(from, to);
    let periods = service_periods(&docs)?;
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    let mut all: Vec<Journey> = Vec::new();
    for (i, doc) in docs.iter().enumerate() {
        journeys(
            i,
            doc,
            network,
            &periods,
            (from, to),
            &calendar,
            &mut names,
            &mut all,
        )?;
    }
    all.retain(|j| !j.dates.is_empty());

    // Unique trip ids: a later version of a service may reuse a code.
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for journey in &mut all {
        let n = seen.entry(journey.trip_id.clone()).or_default();
        *n += 1;
        if *n > 1 {
            journey.trip_id = format!("{}:{n}", journey.trip_id);
        }
    }

    // Service ids by date set, numbered in date-set order.
    let date_sets: BTreeSet<&BTreeSet<NaiveDate>> = all.iter().map(|j| &j.dates).collect();
    let service_of: BTreeMap<&BTreeSet<NaiveDate>, String> = date_sets
        .iter()
        .enumerate()
        .map(|(n, dates)| (*dates, format!("s{}", n + 1)))
        .collect();
    let service_days: Vec<TransitServiceDays> = service_of
        .iter()
        .map(|(dates, id)| TransitServiceDays {
            service_id: id.clone(),
            dates: dates.iter().copied().collect(),
        })
        .collect();

    // Patterns, numbered in key order (stable for the same content).
    let keyed: Vec<(PatternKey, i32)> = all
        .iter()
        .map(|j| {
            let start = j.calls[0].2;
            let line = match j.route {
                RouteKind::Line(l) => Some(l),
                RouteKind::ReplacementBus => None,
            };
            let calls = j
                .calls
                .iter()
                .map(|(stop, arr, dep, pickup, drop_off)| {
                    let arr = (arr - start).max(0);
                    (
                        stop.clone(),
                        arr,
                        (dep - start).max(arr),
                        *pickup,
                        *drop_off,
                    )
                })
                .collect();
            ((line, j.direction, calls), start)
        })
        .collect();
    let mut by_key: BTreeMap<&PatternKey, Vec<usize>> = BTreeMap::new();
    for (i, (key, _)) in keyed.iter().enumerate() {
        by_key.entry(key).or_default().push(i);
    }
    let mut pattern_of: HashMap<usize, String> = HashMap::new();
    let mut patterns = Vec::with_capacity(by_key.len());
    for (n, (key, members)) in by_key.iter().enumerate() {
        let id = format!("p{}", n + 1);
        let (line, direction, calls) = key;
        let first = members
            .iter()
            .min_by_key(|&&i| &all[i].trip_id)
            .copied()
            .unwrap_or(members[0]);
        patterns.push(TransitPattern {
            pattern_id: id.clone(),
            line_id: line.map(|l| network.lines[l].id.clone()),
            direction: *direction,
            headsign: all[first].headsign.clone(),
            stops: calls
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
    let mut trips: Vec<TransitTrip> = all
        .iter()
        .enumerate()
        .filter_map(|(i, j)| {
            Some(TransitTrip {
                trip_id: j.trip_id.clone(),
                line_id: match j.route {
                    RouteKind::Line(l) => Some(network.lines[l].id.clone()),
                    RouteKind::ReplacementBus => None,
                },
                pattern_id: pattern_of.get(&i)?.clone(),
                service_id: service_of.get(&j.dates)?.clone(),
                start_s: keyed[i].1,
                headsign: j.headsign.clone(),
                replacement_bus: matches!(j.route, RouteKind::ReplacementBus),
            })
        })
        .collect();
    trips.sort_by(|a, b| a.trip_id.cmp(&b.trip_id));

    let stops = stop_rows(network, &patterns, &names)?;
    let prefix = network.atco_prefix.as_str();
    let lines = super::gtfs::line_orders(network, &patterns, &trips, |stop| {
        station_of(stop, prefix).unwrap_or_else(|| stop.to_owned())
    });
    let source_feed_version = docs
        .iter()
        .filter_map(|d| d.root.attr("ModificationDateTime"))
        .max()
        .map(|t| {
            let digits: String = t.chars().filter(char::is_ascii_digit).collect();
            match (digits.get(..8), digits.get(8..14)) {
                (Some(day), Some(time)) => format!("{day}_{time}"),
                _ => digits,
            }
        })
        .filter(|v| !v.is_empty());
    Ok(TransitModel {
        lines,
        stops,
        patterns,
        trips,
        service_days,
        horizon: Horizon { from, to },
        source_feed_version,
    })
}

/// The stations then platforms the patterns call at, each sorted by id.
fn stop_rows(
    network: &NetworkConfig,
    patterns: &[TransitPattern],
    names: &BTreeMap<String, String>,
) -> Result<Vec<TransitStop>, FeedError> {
    let prefix = network.atco_prefix.as_str();
    let used: BTreeSet<&str> = patterns
        .iter()
        .flat_map(|p| p.stops.iter().map(|s| s.stop_id.as_str()))
        .collect();
    let mut stations: BTreeMap<String, TransitStop> = BTreeMap::new();
    let mut platforms: BTreeMap<String, TransitStop> = BTreeMap::new();
    let row = |id: &str, kind: StopKind, parent: Option<String>, name: String| TransitStop {
        stop_id: id.to_owned(),
        kind,
        parent_stop_id: parent,
        name,
        indicator: None,
        lat: None,
        lon: None,
        wheelchair: None,
    };
    for id in used {
        if !is_stop_id(id) {
            return Err(malformed(format!("stop id {id:?}")));
        }
        let name = names.get(id).cloned().unwrap_or_else(|| id.to_owned());
        let is_station = id.starts_with(prefix)
            && id.len() > prefix.len()
            && !id.ends_with(|c: char| c.is_ascii_digit());
        if is_station {
            stations
                .entry(id.to_owned())
                .or_insert_with(|| row(id, StopKind::Station, None, name));
            continue;
        }
        let station = station_of(id, prefix);
        if let Some(station) = &station {
            stations
                .entry(station.clone())
                .or_insert_with(|| row(station, StopKind::Station, None, name.clone()));
        }
        platforms.insert(id.to_owned(), row(id, StopKind::Platform, station, name));
    }
    Ok(stations
        .into_values()
        .chain(platforms.into_values())
        .collect())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/tnds-em-net-mini.xml");

    fn net() -> NetworkConfig {
        NetworkConfig::load("net").unwrap()
    }

    fn options(today: &str) -> TxcOptions {
        TxcOptions {
            today: date(today),
            horizon_days: 60,
            max_entry_bytes: MAX_ENTRY_BYTES,
        }
    }

    fn date(d: &str) -> NaiveDate {
        NaiveDate::parse_from_str(d, "%Y-%m-%d").unwrap()
    }

    /// A zip of `files`.
    fn zip(files: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = zip::ZipWriter::new(Cursor::new(&mut buf));
            let options = zip::write::SimpleFileOptions::default();
            for (name, text) in files {
                writer.start_file(*name, options).unwrap();
                writer.write_all(text.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }
        buf
    }

    /// Another operator's document (a bus), which must be left out.
    const OTHER: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<TransXChange xmlns="http://www.transxchange.org.uk/" ModificationDateTime="2026-10-21T08:00:00">
  <StopPoints><AnnotatedStopPointRef><StopPointRef>3390BA14</StopPointRef><CommonName>Hucknall Road</CommonName></AnnotatedStopPointRef>
  <AnnotatedStopPointRef><StopPointRef>3390SH19</StopPointRef><CommonName>Hucknall Road</CommonName></AnnotatedStopPointRef></StopPoints>
  <JourneyPatternSections><JourneyPatternSection id="S1"><JourneyPatternTimingLink id="L1">
    <From><StopPointRef>3390BA14</StopPointRef></From><To><StopPointRef>3390SH19</StopPointRef></To><RunTime>PT5M</RunTime>
  </JourneyPatternTimingLink></JourneyPatternSection></JourneyPatternSections>
  <Operators><Operator id="O1"><NationalOperatorCode>NCTR</NationalOperatorCode></Operator></Operators>
  <Services><Service><ServiceCode>EM_NC_17</ServiceCode><Lines><Line id="X"><LineName>17</LineName></Line></Lines>
    <OperatingPeriod><StartDate>2026-01-01</StartDate></OperatingPeriod>
    <StandardService><JourneyPattern id="P1"><JourneyPatternSectionRefs>S1</JourneyPatternSectionRefs></JourneyPattern></StandardService>
  </Service></Services>
  <VehicleJourneys><VehicleJourney><VehicleJourneyCode>B1</VehicleJourneyCode><ServiceRef>EM_NC_17</ServiceRef><LineRef>X</LineRef>
    <JourneyPatternRef>P1</JourneyPatternRef><DepartureTime>09:00:00</DepartureTime></VehicleJourney></VehicleJourneys>
</TransXChange>"#;

    fn fixture(today: &str) -> TransitModel {
        let bytes = zip(&[
            ("EM/EM_NC_17.xml", OTHER),
            ("EM/EM_NE_NET.xml", FIXTURE),
            ("EM/readme.txt", "not TXC"),
        ]);
        parse(Cursor::new(bytes), &net(), &options(today)).unwrap()
    }

    fn trip<'a>(model: &'a TransitModel, id: &str) -> &'a TransitTrip {
        model.trips.iter().find(|t| t.trip_id == id).unwrap()
    }

    fn pattern<'a>(model: &'a TransitModel, trip_id: &str) -> &'a TransitPattern {
        let id = &trip(model, trip_id).pattern_id;
        model.patterns.iter().find(|p| &p.pattern_id == id).unwrap()
    }

    fn dates(model: &TransitModel, trip_id: &str) -> BTreeSet<NaiveDate> {
        let service = &trip(model, trip_id).service_id;
        model
            .service_days
            .iter()
            .find(|s| &s.service_id == service)
            .unwrap()
            .dates
            .iter()
            .copied()
            .collect()
    }

    #[test]
    fn the_fixture_parses_to_nets_rows_only() {
        let model = fixture("2026-12-01");
        let ids: Vec<&str> = model.trips.iter().map(|t| t.trip_id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "EM_NE_NET1:VJ1",
                "EM_NE_NET1:VJ2",
                "EM_NE_NET1:VJ3",
                "EM_NE_NET1:VJ4",
                "EM_NE_NET2:VJ5"
            ]
        );
        assert!(model.trips.iter().all(|t| !t.replacement_bus));
        assert_eq!(trip(&model, "EM_NE_NET2:VJ5").line_id.as_deref(), Some("2"));
        assert_eq!(trip(&model, "EM_NE_NET1:VJ1").line_id.as_deref(), Some("1"));
        // VJ1 and VJ2 share a pattern; VJ4's run-time override makes its own.
        assert_eq!(
            trip(&model, "EM_NE_NET1:VJ1").pattern_id,
            trip(&model, "EM_NE_NET1:VJ2").pattern_id
        );
        assert_ne!(
            trip(&model, "EM_NE_NET1:VJ1").pattern_id,
            trip(&model, "EM_NE_NET1:VJ4").pattern_id
        );
        assert_eq!(model.patterns.len(), 4);
        // Stations from the platform codes; names lose " Tram Stop".
        let stations: Vec<&str> = model
            .stops
            .iter()
            .filter(|s| s.kind == StopKind::Station)
            .map(|s| s.stop_id.as_str())
            .collect();
        assert_eq!(
            stations,
            [
                "9400ZZNOBWL",
                "9400ZZNOCLS",
                "9400ZZNOHKL",
                "9400ZZNONOT",
                "9400ZZNOOMS",
                "9400ZZNOPHP",
                "9400ZZNOTOT"
            ]
        );
        let not1 = model
            .stops
            .iter()
            .find(|s| s.stop_id == "9400ZZNONOT1")
            .unwrap();
        assert_eq!(not1.kind, StopKind::Platform);
        assert_eq!(not1.parent_stop_id.as_deref(), Some("9400ZZNONOT"));
        assert_eq!(not1.name, "Nottingham Station");
        assert!(model.stops.iter().all(|s| !s.name.contains("Tram Stop")));
        assert!(
            model
                .stops
                .iter()
                .all(|s| s.stop_id.starts_with("9400ZZNO"))
        );
        // Line 1's stations in order, both ways; line 2 one way.
        let line1 = &model.lines[0];
        assert_eq!(line1.line_id, "1");
        assert_eq!(
            line1.directions[0].stations,
            ["9400ZZNOHKL", "9400ZZNOBWL", "9400ZZNONOT", "9400ZZNOTOT"]
        );
        assert_eq!(line1.directions[0].direction, Direction::Outbound);
        assert_eq!(line1.directions[1].direction, Direction::Inbound);
        assert_eq!(line1.directions[1].headsign.as_deref(), Some("Hucknall"));
        assert_eq!(model.lines[1].directions.len(), 1);
        assert_eq!(
            model.source_feed_version.as_deref(),
            Some("20261020_101500")
        );
        assert_eq!(model.horizon.from, date("2026-11-30"));
        assert_eq!(model.horizon.to, date("2027-01-30"));
        // Deterministic.
        assert_eq!(fixture("2026-12-01"), model);
    }

    #[test]
    fn times_follow_run_and_wait_times() {
        let model = fixture("2026-12-01");
        assert_eq!(trip(&model, "EM_NE_NET1:VJ1").start_s, 6 * 3600);
        let p = pattern(&model, "EM_NE_NET1:VJ1");
        let offsets: Vec<(&str, i32, i32)> = p
            .stops
            .iter()
            .map(|s| (s.stop_id.as_str(), s.arr_offset_s, s.dep_offset_s))
            .collect();
        // 6 min, then 14 min and a 1 min wait (To/WaitTime), then 20 min.
        assert_eq!(
            offsets,
            [
                ("9400ZZNOHKL1", 0, 0),
                ("9400ZZNOBWL1", 360, 360),
                ("9400ZZNONOT1", 1200, 1260),
                ("9400ZZNOTOT1", 2460, 2460)
            ]
        );
        // First call: no drop-off; last: no pickup (and setDown says so).
        assert_eq!((p.stops[0].pickup, p.stops[0].drop_off), (0, 1));
        assert_eq!((p.stops[3].pickup, p.stops[3].drop_off), (1, 0));
        assert_eq!(p.headsign.as_deref(), Some("Toton Lane"));
        // VJ4: link 2 overridden to 16 minutes.
        let p4 = pattern(&model, "EM_NE_NET1:VJ4");
        assert_eq!(p4.stops[2].arr_offset_s, 1320);
        assert_eq!(trip(&model, "EM_NE_NET1:VJ4").start_s, 7 * 3600);
        // Line 2: a From/WaitTime of 30 s at Old Market Square.
        let p5 = pattern(&model, "EM_NE_NET2:VJ5");
        assert_eq!(
            (p5.stops[1].arr_offset_s, p5.stops[1].dep_offset_s),
            (900, 930)
        );
        assert_eq!(p5.stops[2].arr_offset_s, 2130);
        assert_eq!(p5.headsign.as_deref(), Some("Clifton South"));
        // VJ3 leaves at 23:50 and arrives after midnight: offsets carry it.
        let p3 = pattern(&model, "EM_NE_NET1:VJ3");
        assert_eq!(trip(&model, "EM_NE_NET1:VJ3").start_s, 23 * 3600 + 50 * 60);
        assert_eq!(p3.stops[3].arr_offset_s, 2400);
        assert_eq!(p3.direction, Direction::Inbound);
    }

    #[test]
    fn operating_profiles_expand_to_dates() {
        let model = fixture("2026-12-01");
        // VJ1: its own profile, Monday to Friday, not 24 December; the
        // service's "not Christmas Day" does not apply to it.
        let vj1 = dates(&model, "EM_NE_NET1:VJ1");
        assert!(vj1.contains(&date("2026-12-23")));
        assert!(!vj1.contains(&date("2026-12-24")));
        assert!(vj1.contains(&date("2026-12-25")));
        assert!(!vj1.contains(&date("2026-12-26")));
        assert!(!vj1.contains(&date("2026-11-29")), "before the horizon");
        assert!(vj1.contains(&date("2026-11-30")));
        // VJ2: the service's profile, every day but Christmas Day.
        let vj2 = dates(&model, "EM_NE_NET1:VJ2");
        assert!(vj2.contains(&date("2026-12-24")));
        assert!(!vj2.contains(&date("2026-12-25")));
        assert!(vj2.contains(&date("2026-12-26")));
        assert_eq!(vj2.len(), 61, "62 days, less Christmas Day");
        assert_eq!(vj2, dates(&model, "EM_NE_NET1:VJ3"));
        // VJ5: Sundays, plus the Boxing Day substitute (Monday 28th, as
        // Boxing Day 2026 is a Saturday) and New Year's Day.
        let vj5 = dates(&model, "EM_NE_NET2:VJ5");
        assert!(vj5.contains(&date("2026-12-06")));
        assert!(!vj5.contains(&date("2026-12-07")));
        assert!(vj5.contains(&date("2026-12-28")));
        assert!(vj5.contains(&date("2027-01-01")));
        assert!(!vj5.contains(&date("2026-12-26")));
        // Same date sets share a service id.
        assert_eq!(
            trip(&model, "EM_NE_NET1:VJ2").service_id,
            trip(&model, "EM_NE_NET1:VJ3").service_id
        );
        assert_eq!(model.service_days.len(), 3);
    }

    #[test]
    fn the_operating_period_bounds_the_dates_and_a_later_version_takes_over() {
        // Service 1 ends 2027-03-31: from 2027-03-01, 60 days reach past it.
        let model = fixture("2027-03-01");
        let vj2 = dates(&model, "EM_NE_NET1:VJ2");
        assert_eq!(vj2.last(), Some(&date("2027-03-31")));
        // A second version of service 1 from 2027-03-15 takes over then.
        let newer = FIXTURE
            .replace(
                "ModificationDateTime=\"2026-10-20T10:15:00\"",
                "ModificationDateTime=\"2027-02-01T00:00:00\"",
            )
            .replace(
                "<StartDate>2026-10-01</StartDate><EndDate>2027-03-31</EndDate>",
                "<StartDate>2027-03-15</StartDate>",
            );
        let bytes = zip(&[("a.xml", FIXTURE), ("b.xml", &newer)]);
        let model = parse(Cursor::new(bytes), &net(), &options("2027-03-01")).unwrap();
        let old = dates(&model, "EM_NE_NET1:VJ2");
        assert_eq!(old.last(), Some(&date("2027-03-14")));
        let new = dates(&model, "EM_NE_NET1:VJ2:2");
        assert_eq!(new.first(), Some(&date("2027-03-15")));
        assert_eq!(
            model.source_feed_version.as_deref(),
            Some("20270201_000000")
        );
    }

    #[test]
    fn an_unmapped_line_name_refuses_the_feed() {
        let renamed = FIXTURE.replace("<LineName>2</LineName>", "<LineName>Tram 3</LineName>");
        let err = parse(
            Cursor::new(zip(&[("n.xml", &renamed)])),
            &net(),
            &options("2026-12-01"),
        )
        .unwrap_err();
        assert_eq!(err.reason(), "unmapped_route", "{err}");
        assert!(err.to_string().contains("Tram 3"), "{err}");
    }

    #[test]
    fn a_zip_without_the_network_is_refused() {
        let err = parse(
            Cursor::new(zip(&[("o.xml", OTHER)])),
            &net(),
            &options("2026-12-01"),
        )
        .unwrap_err();
        assert_eq!(err.reason(), "malformed", "{err}");
        assert!(err.to_string().contains("NEXT"), "{err}");
    }

    #[test]
    fn an_entry_over_its_cap_aborts() {
        let mut opts = options("2026-12-01");
        opts.max_entry_bytes = 1000;
        let err = parse(Cursor::new(zip(&[("n.xml", FIXTURE)])), &net(), &opts).unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");
    }

    #[test]
    fn a_missing_pattern_section_is_malformed() {
        let broken = FIXTURE.replace(
            "<JourneyPatternSectionRefs>JPS3</JourneyPatternSectionRefs>",
            "<JourneyPatternSectionRefs>JPS9</JourneyPatternSectionRefs>",
        );
        let err = parse(
            Cursor::new(zip(&[("n.xml", &broken)])),
            &net(),
            &options("2026-12-01"),
        )
        .unwrap_err();
        assert_eq!(err.reason(), "malformed", "{err}");
        assert!(err.to_string().contains("JPS9"), "{err}");
    }

    #[test]
    fn a_passed_stop_is_not_a_call() {
        let passing = FIXTURE.replace(
            "<From><WaitTime>PT30S</WaitTime><StopPointRef>9400ZZNOOMS1</StopPointRef></From>",
            "<From><Activity>pass</Activity><StopPointRef>9400ZZNOOMS1</StopPointRef></From>",
        );
        let model = parse(
            Cursor::new(zip(&[("n.xml", &passing)])),
            &net(),
            &options("2026-12-01"),
        )
        .unwrap();
        let p = pattern(&model, "EM_NE_NET2:VJ5");
        let stops: Vec<&str> = p.stops.iter().map(|s| s.stop_id.as_str()).collect();
        assert_eq!(stops, ["9400ZZNOPHP1", "9400ZZNOCLS1"]);
        assert_eq!(p.stops[1].arr_offset_s, 2100);
    }

    #[test]
    fn values_parse() {
        assert_eq!(parse_time("06:00:00").unwrap(), 21_600);
        assert_eq!(parse_time("07:05").unwrap(), 25_500);
        assert!(parse_time("24:00:00").is_err());
        assert!(parse_time("7").is_err());
        assert_eq!(parse_duration("PT6M").unwrap(), 360);
        assert_eq!(parse_duration("PT1M30S").unwrap(), 90);
        assert_eq!(parse_duration("PT0S").unwrap(), 0);
        assert_eq!(parse_duration("P0DT1H").unwrap(), 3600);
        assert_eq!(parse_duration("PT1.5M").unwrap(), 90);
        for bad in ["6M", "PT", "PT5X", "-PT1M", "P30D"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
        assert_eq!(
            station_of("9400ZZNOBCF1", "9400ZZNO").as_deref(),
            Some("9400ZZNOBCF")
        );
        assert_eq!(station_of("9400ZZNOBCF", "9400ZZNO"), None);
        assert_eq!(station_of("3390BA14", "9400ZZNO"), None);
        assert_eq!(station_of("9400ZZNO1", "9400ZZNO"), None);
        assert!(mentions(
            b"<NationalOperatorCode>NEXT</NationalOperatorCode>",
            &["NEXT".to_owned()]
        ));
        assert!(!mentions(
            b"<OperatorCode>NEXTA</OperatorCode>",
            &["NEXT".to_owned()]
        ));
    }

    #[test]
    fn bank_holidays_follow_england_and_wales() {
        let y2026 = holiday_dates(2026);
        let one = |name: &str| y2026[name].clone();
        assert_eq!(one("GoodFriday"), [date("2026-04-03")]);
        assert_eq!(one("EasterMonday"), [date("2026-04-06")]);
        assert_eq!(one("MayDay"), [date("2026-05-04")]);
        assert_eq!(one("SpringBank"), [date("2026-05-25")]);
        assert_eq!(
            one("LateSummerBankHolidayNotScotland"),
            [date("2026-08-31")]
        );
        // 2026: Christmas on a Friday, Boxing Day on a Saturday.
        assert_eq!(one("BoxingDayHoliday"), [date("2026-12-28")]);
        assert!(!y2026.contains_key("ChristmasDayHoliday"));
        // 2027: Christmas on a Saturday; 2022: on a Sunday.
        assert_eq!(
            holiday_dates(2027)["ChristmasDayHoliday"],
            [date("2027-12-27")]
        );
        assert_eq!(
            holiday_dates(2027)["BoxingDayHoliday"],
            [date("2027-12-28")]
        );
        assert_eq!(
            holiday_dates(2022)["ChristmasDayHoliday"],
            [date("2022-12-27")]
        );
        // 2028: New Year's Day on a Saturday, so Monday 3rd.
        assert_eq!(
            holiday_dates(2028)["NewYearsDayHoliday"],
            [date("2028-01-03")]
        );
        assert_eq!(easter(2027), Some(date("2027-03-28")));
        assert_eq!(y2026["AllBankHolidays"].len(), 9);
        assert_eq!(y2026["Christmas"].len(), 2);
    }

    #[test]
    fn days_of_week_groups() {
        let doc = parse_tree(
            "<OperatingProfile><RegularDayType><DaysOfWeek><NotSaturday/></DaysOfWeek></RegularDayType></OperatingProfile>"
                .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            read_profile(&doc).unwrap().days,
            [true, true, true, true, true, false, true]
        );
        let holidays_only = parse_tree(
            "<OperatingProfile><RegularDayType><HolidaysOnly/></RegularDayType>\
             <BankHolidayOperation><DaysOfOperation><AllBankHolidays/></DaysOfOperation></BankHolidayOperation></OperatingProfile>"
                .as_bytes(),
        )
        .unwrap();
        let p = read_profile(&holidays_only).unwrap();
        assert_eq!(p.days, [false; 7]);
        let calendar = HolidayCalendar::new(date("2026-12-20"), date("2027-01-05"));
        let run = expand(&p, date("2026-12-20"), date("2027-01-05"), &calendar);
        assert_eq!(
            run.into_iter().collect::<Vec<_>>(),
            [
                date("2026-12-25"),
                date("2026-12-26"),
                date("2026-12-28"),
                date("2027-01-01")
            ]
        );
        let bad = parse_tree(
            "<OperatingProfile><RegularDayType><DaysOfWeek><Funday/></DaysOfWeek></RegularDayType></OperatingProfile>"
                .as_bytes(),
        )
        .unwrap();
        assert!(read_profile(&bad).is_err());
    }

    /// A whole TNDS region (`TRANSIT_REAL_TXC`, a downloaded `EM.zip` that
    /// is never committed): the counts and the parse time.
    #[test]
    #[ignore = "needs TRANSIT_REAL_TXC, a downloaded TNDS EM.zip"]
    fn the_real_tnds_feed() {
        let Ok(path) = std::env::var("TRANSIT_REAL_TXC") else {
            return;
        };
        let started = std::time::Instant::now();
        let file = std::io::BufReader::new(std::fs::File::open(path).unwrap());
        let today = common::transit::service_date(chrono::Utc::now());
        let model = parse(
            file,
            &net(),
            &TxcOptions {
                today,
                horizon_days: 60,
                max_entry_bytes: MAX_ENTRY_BYTES,
            },
        )
        .unwrap();
        eprintln!(
            "parsed in {:?}: {} lines, {} stops, {} patterns, {} trips, {} services, {} service days",
            started.elapsed(),
            model.lines.len(),
            model.stops.len(),
            model.patterns.len(),
            model.trips.len(),
            model.service_days.len(),
            model.service_day_count(),
        );
        assert!(!model.trips.is_empty());
    }
}
