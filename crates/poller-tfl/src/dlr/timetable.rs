//! Parses TfL's `GET /Line/dlr/Timetable/{stopPointId}` response for one
//! fixed pilot station (Poplar — see the plan's Global Constraints for why
//! this pilot doesn't cover the whole network). `knownJourneys[]` gives
//! each scheduled departure as an `hour`/`minute` pair with no date; this
//! module combines each with the `service_date` the caller is asking
//! about (the current *London* date — TfL's timetable service day is a
//! local one, not a UTC calendar day — threaded in the same way
//! `poller-tfl/src/schema.rs::parse_line_status` threads `now` — never
//! read directly, so parsing stays deterministic under test).
//!
//! Two properties of TfL's published times matter and are handled here:
//! `hour` uses the after-midnight service-day convention (`"24"`, `"25"`
//! mean 00:xx/01:xx the *following* morning), and every time is
//! Europe/London wall-clock, so it needs a real timezone conversion to
//! reach the `DateTime<Utc>` the rest of the pilot works in.

use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc, Weekday};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TimetableResponse {
    timetable: Timetable,
}

#[derive(Debug, Deserialize)]
struct Timetable {
    routes: Vec<Route>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Route {
    #[serde(default)]
    schedules: Vec<Schedule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Schedule {
    // Defaulted so one schedule entry missing `name` degrades into "no
    // day-type match, skip it" — the same graceful outcome as the
    // route-level skip in `parse_timetable` — rather than erroring the
    // whole Poplar timetable parse.
    #[serde(default)]
    name: String,
    #[serde(default)]
    known_journeys: Vec<KnownJourney>,
}

#[derive(Debug, Deserialize)]
struct KnownJourney {
    hour: String,
    minute: String,
    // A Task 2 live capture found this is a JSON integer in practice (e.g.
    // `0`, `1`), not the string the plan originally assumed — see
    // `crates/poller-tfl/tests/fixtures/README.md` finding 3. Deserializing
    // it into a `String` field fails against real data, so it's typed
    // numeric here instead.
    #[serde(default)]
    #[serde(rename = "intervalId")]
    interval_id: Option<i64>,
}

/// One scheduled DLR departure from the pilot station, resolved to a real
/// timestamp for `service_date`.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledTrip {
    pub scheduled_departure: DateTime<Utc>,
    /// TfL's Timetable response does not carry a destination per journey
    /// the way Arrivals does — only a route-level `intervalId` grouping.
    /// Matching (Task 5) does not use this field yet; kept for a future
    /// iteration that resolves `intervalId` to a real destination via
    /// `timetable.routes[].stationIntervals[]`, which this pilot does not
    /// parse.
    pub interval_id: Option<i64>,
}

/// The `schedules[].name` this pilot selects for a given `service_date`,
/// per the day-of-week rule below.
fn expected_schedule_name(service_date: NaiveDate) -> &'static str {
    // A Task 2 live capture found each route's `schedules[]` holds one
    // entry per day-type (observed names: "Monday - Friday", "Sunday",
    // "Saturdays and Public Holidays"), each carrying its own complete
    // `knownJourneys[]` list for that day-type only — not one flat pool of
    // departures shared across every day, as the plan originally assumed.
    // See `crates/poller-tfl/tests/fixtures/README.md` finding 4. Parsing
    // must therefore pick the one schedule matching `service_date`'s day
    // of week; flattening every schedule together would combine weekday
    // and weekend departures into a single inflated (~3x) trip count, and
    // Task 6 promotes unmatched scheduled trips to "cancelled" — so that
    // bug would manufacture large numbers of phantom cancellations. This
    // is a pilot-tier approximation: it does not special-case real public
    // holidays, so a Bank Holiday Monday will incorrectly use the weekday
    // schedule. That's accepted as out of scope for this pilot.
    match service_date.weekday() {
        Weekday::Sat => "Saturdays and Public Holidays",
        Weekday::Sun => "Sunday",
        _ => "Monday - Friday",
    }
}

/// Resolves a Europe/London wall-clock timetable time to the instant it
/// names. `knownJourneys[].hour`/`.minute` are local published times, so
/// constructing them as UTC directly would be silently an hour out for the
/// ~7 months of British Summer Time — far enough to push every scheduled
/// trip outside `inference::MATCH_WINDOW_MINUTES` and defeat matching
/// entirely.
///
/// Follows `crates/aggregator/src/aggregation.rs::next_rail_day_boundary`'s
/// `LocalResult` handling, with one deliberate difference: that function
/// only ever resolves local 02:00, which UK clock changes never make
/// ambiguous or nonexistent, so it panics on anything but `Single`. A DLR
/// timetable *does* contain 01:00-01:59 departures, which are exactly the
/// times that occur twice (autumn) or not at all (spring). Twice a year a
/// panic here would take the poller down, so those two cases degrade
/// instead: the ambiguous hour takes the first (BST) occurrence, and a
/// nonexistent local time yields `None` so the caller drops that one
/// journey rather than the whole timetable.
fn london_to_utc(naive: NaiveDateTime) -> Option<DateTime<Utc>> {
    match chrono_tz::Europe::London.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        chrono::LocalResult::None => None,
    }
}

pub fn parse_timetable(json: &str, service_date: NaiveDate) -> Result<Vec<ScheduledTrip>> {
    let response: TimetableResponse = serde_json::from_str(json)?;
    let expected_name = expected_schedule_name(service_date);
    let mut trips = Vec::new();
    for route in &response.timetable.routes {
        // If a route has no schedule matching the expected day-type name
        // (e.g. a spelling quirk on that particular route), skip its
        // contribution silently rather than erroring — a partial Poplar
        // timetable degrades better than one route's naming quirk
        // crashing the whole parse.
        let Some(schedule) = route.schedules.iter().find(|s| s.name == expected_name) else {
            continue;
        };
        for journey in &schedule.known_journeys {
            // `hour`/`minute` are TfL-supplied strings with no documented
            // upper bound. A single malformed or absurd value (a real
            // captured non-numeric value, or something like
            // `"4294967295"`, still a valid `u32`) must skip only THIS
            // journey, not fail the whole route/timetable via `?` -- one
            // bad entry in a 400+-journey response is exactly the partial-
            // failure shape this parser is otherwise careful to tolerate
            // (see the schedule-name-mismatch skip above).
            let (Ok(hour), Ok(minute)) =
                (journey.hour.parse::<u32>(), journey.minute.parse::<u32>())
            else {
                tracing::warn!(
                    hour = %journey.hour,
                    minute = %journey.minute,
                    "knownJourney hour/minute is not a valid non-negative integer; skipping this journey"
                );
                continue;
            };
            // TfL publishes after-midnight departures as part of the
            // previous service day, using hours 24, 25, ... (35 such
            // journeys exist in the captured Poplar timetable, across
            // every day-type). `NaiveDate::and_hms_opt` rejects hour >=
            // 24, so roll the excess into the date instead of erroring
            // — which would otherwise fail every real Timetable parse.
            // This also rejects an out-of-range `minute` (>= 60) the same
            // way, skipping just this journey.
            let Some(base) = service_date.and_hms_opt(hour % 24, minute, 0) else {
                tracing::warn!(
                    hour = %journey.hour,
                    minute = %journey.minute,
                    "invalid knownJourney time; skipping this journey"
                );
                continue;
            };
            // `NaiveDateTime`'s `Add<Duration>` impl (the previous `+
            // Duration::days(..)` here) panics on overflow via
            // `checked_add_signed(..).expect(..)` -- an absurd `hour`
            // value (e.g. `"4294967295"`, a valid u32) computes a
            // days-offset far outside what a `NaiveDate` can represent and
            // would crash the whole poll cycle. `checked_add_signed`
            // itself never panics; an out-of-range result degrades to
            // skipping just this journey instead.
            let Some(naive) = base.checked_add_signed(Duration::days(i64::from(hour / 24))) else {
                tracing::warn!(
                    hour = %journey.hour,
                    "knownJourney hour is too large to resolve to a representable date; \
                     skipping this journey"
                );
                continue;
            };
            // A local time that doesn't exist (the spring-forward gap)
            // names no instant, so there is no departure to report; skip
            // that journey rather than fail the day's whole timetable.
            let Some(scheduled_departure) = london_to_utc(naive) else {
                continue;
            };
            trips.push(ScheduledTrip {
                scheduled_departure,
                interval_id: journey.interval_id,
            });
        }
    }
    Ok(trips)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Adapted from crates/poller-tfl/tests/fixtures/dlr_timetable_poplar.json,
    // trimmed to two routes each carrying "Monday - Friday" and "Saturdays
    // and Public Holidays" schedules, so the day-of-week selection logic
    // has something to distinguish. intervalId is a JSON integer, matching
    // the real capture (see fixtures/README.md finding 3).
    const DLR_TIMETABLE_JSON: &str = r#"{
      "lineId": "dlr",
      "lineName": "DLR",
      "direction": "outbound",
      "timetable": {
        "departureStopId": "940GZZDLPOP",
        "routes": [
          {
            "stationIntervals": [],
            "schedules": [
              {
                "name": "Monday - Friday",
                "knownJourneys": [
                  { "hour": "10", "minute": "02", "intervalId": 1 },
                  { "hour": "10", "minute": "06", "intervalId": 2 }
                ]
              },
              {
                "name": "Sunday",
                "knownJourneys": [
                  { "hour": "11", "minute": "15", "intervalId": 0 }
                ]
              },
              {
                "name": "Saturdays and Public Holidays",
                "knownJourneys": [
                  { "hour": "09", "minute": "45", "intervalId": 0 }
                ]
              }
            ]
          },
          {
            "stationIntervals": [],
            "schedules": [
              {
                "name": "Monday - Friday",
                "knownJourneys": [
                  { "hour": "10", "minute": "04", "intervalId": 1 }
                ]
              },
              {
                "name": "Saturdays and Public Holidays",
                "knownJourneys": [
                  { "hour": "09", "minute": "50", "intervalId": 1 }
                ]
              }
            ]
          }
        ]
      }
    }"#;

    // The real captured Timetable response, used as-is. This crate's usual
    // test convention is a small inline constant, but a hand-trimmed
    // constant is exactly what hid the `hour: "24"`/`"25"` bug (the real
    // file has 35 such journeys and the trimmed one has none), so these
    // tests deliberately exercise the committed capture itself.
    const REAL_TIMETABLE_JSON: &str =
        include_str!("../../tests/fixtures/dlr_timetable_poplar.json");

    fn weekday_service_date() -> NaiveDate {
        // 2026-08-22 is a Saturday; use the following Monday for weekday tests.
        NaiveDate::from_ymd_opt(2026, 8, 24).unwrap()
    }

    fn saturday_service_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, 22).unwrap()
    }

    fn sunday_service_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, 23).unwrap()
    }

    #[test]
    fn parses_known_journeys_into_scheduled_trips_on_the_given_date() {
        let trips =
            parse_timetable(DLR_TIMETABLE_JSON, weekday_service_date()).expect("should parse");
        // 2 journeys from route 0's "Monday - Friday" + 1 from route 1's "Monday - Friday" = 3.
        assert_eq!(trips.len(), 3);
        // 2026-08-24 is in British Summer Time, so London 10:02 is 09:02Z.
        assert_eq!(
            trips[0].scheduled_departure,
            "2026-08-24T09:02:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(trips[0].interval_id, Some(1));
        assert_eq!(
            trips[1].scheduled_departure,
            "2026-08-24T09:06:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            trips[2].scheduled_departure,
            "2026-08-24T09:04:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_saturday_service_date_only_pulls_from_the_saturday_schedule() {
        let trips =
            parse_timetable(DLR_TIMETABLE_JSON, saturday_service_date()).expect("should parse");
        // 1 journey from route 0's "Saturdays and Public Holidays" + 1 from route 1's = 2.
        assert_eq!(trips.len(), 2);
        assert_eq!(
            trips[0].scheduled_departure,
            "2026-08-22T08:45:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            trips[1].scheduled_departure,
            "2026-08-22T08:50:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_summer_timetable_time_is_read_as_london_local_not_utc() {
        // The weekday fixture's first journey is 10:02. On 2026-08-24
        // (BST, UTC+1) that instant is 09:02Z — if this comes back as
        // 10:02Z the local-to-UTC conversion isn't happening at all.
        let trips =
            parse_timetable(DLR_TIMETABLE_JSON, weekday_service_date()).expect("should parse");
        assert_eq!(
            trips[0].scheduled_departure,
            "2026-08-24T09:02:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_winter_timetable_time_stays_at_the_same_clock_value_under_gmt() {
        // 2026-01-05 is a Monday in GMT (UTC+0), where London local and UTC
        // coincide — the same 10:02 journey must resolve to 10:02Z, proving
        // the BST case above is a real offset and not a blanket -1 hour.
        let winter_monday = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let trips = parse_timetable(DLR_TIMETABLE_JSON, winter_monday).expect("should parse");
        assert_eq!(
            trips[0].scheduled_departure,
            "2026-01-05T10:02:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn an_after_midnight_hour_rolls_over_into_the_next_day() {
        // TfL's service-day convention: hour 24/25 mean 00:xx/01:xx the
        // following morning, which `NaiveDate::and_hms_opt` rejects outright.
        let json = r#"{
          "lineId": "dlr",
          "lineName": "DLR",
          "direction": "outbound",
          "timetable": {
            "departureStopId": "940GZZDLPOP",
            "routes": [
              {
                "schedules": [
                  {
                    "name": "Monday - Friday",
                    "knownJourneys": [
                      { "hour": "24", "minute": "30", "intervalId": 0 },
                      { "hour": "25", "minute": "05", "intervalId": 0 }
                    ]
                  }
                ]
              }
            ]
          }
        }"#;
        let trips = parse_timetable(json, weekday_service_date()).expect("should parse");
        assert_eq!(trips.len(), 2);
        // 00:30 London on the 25th, in BST, is 23:30Z on the 24th.
        assert_eq!(
            trips[0].scheduled_departure,
            "2026-08-24T23:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            trips[1].scheduled_departure,
            "2026-08-25T00:05:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn the_real_captured_timetable_parses_for_a_weekday_service_date() {
        let trips = parse_timetable(REAL_TIMETABLE_JSON, weekday_service_date());
        assert!(
            trips.is_ok(),
            "real fixture must parse for a weekday: {:?}",
            trips.err()
        );
        // 243 + 195 "Monday - Friday" journeys across the capture's 2 routes.
        assert_eq!(trips.unwrap().len(), 438);
    }

    #[test]
    fn the_real_captured_timetable_parses_for_a_saturday_service_date() {
        let trips = parse_timetable(REAL_TIMETABLE_JSON, saturday_service_date());
        assert!(
            trips.is_ok(),
            "real fixture must parse for a Saturday: {:?}",
            trips.err()
        );
        // 233 + 163 "Saturdays and Public Holidays" journeys.
        assert_eq!(trips.unwrap().len(), 396);
    }

    #[test]
    fn the_real_captured_timetable_parses_for_a_sunday_service_date() {
        let trips = parse_timetable(REAL_TIMETABLE_JSON, sunday_service_date());
        assert!(
            trips.is_ok(),
            "real fixture must parse for a Sunday: {:?}",
            trips.err()
        );
        // 203 + 133 "Sunday" journeys.
        assert_eq!(trips.unwrap().len(), 336);
    }

    #[test]
    fn the_real_captured_timetables_after_midnight_journeys_land_on_the_next_day() {
        // Every day-type in the capture carries hour >= 24 entries (16, 16
        // and 1 respectively), so each must produce departures dated after
        // its own service date rather than erroring the parse. Compared in
        // London local time, since that is the calendar the timetable's own
        // rollover is expressed in.
        for service_date in [
            weekday_service_date(),
            saturday_service_date(),
            sunday_service_date(),
        ] {
            let trips = parse_timetable(REAL_TIMETABLE_JSON, service_date).expect("should parse");
            let rolled_over = trips
                .iter()
                .filter(|t| {
                    t.scheduled_departure
                        .with_timezone(&chrono_tz::Europe::London)
                        .date_naive()
                        > service_date
                })
                .count();
            assert!(
                rolled_over > 0,
                "expected after-midnight journeys for {service_date}"
            );
        }
    }

    #[test]
    fn a_route_missing_the_expected_schedule_name_is_skipped_not_errored() {
        let json = r#"{
          "lineId": "dlr",
          "lineName": "DLR",
          "direction": "outbound",
          "timetable": {
            "departureStopId": "940GZZDLPOP",
            "routes": [
              {
                "schedules": [
                  { "name": "Sunday", "knownJourneys": [{ "hour": "11", "minute": "00", "intervalId": 0 }] }
                ]
              }
            ]
          }
        }"#;
        let trips = parse_timetable(json, weekday_service_date()).expect("should parse");
        assert!(trips.is_empty());
    }

    #[test]
    fn an_absurdly_large_hour_is_skipped_not_a_panic() {
        // A real captured `intervalId` is a JSON integer, but `hour`/
        // `minute` are still bare strings with no documented upper bound.
        // `"4294967295"` parses fine as a `u32` (it IS `u32::MAX`), so the
        // old code's `+ Duration::days(..)` (a panicking `Add` impl) would
        // crash the whole poll cycle computing a days-offset far outside
        // what any `NaiveDate` can represent. This must instead skip just
        // the malformed journey and keep the well-formed one.
        let json = r#"{
          "lineId": "dlr",
          "lineName": "DLR",
          "direction": "outbound",
          "timetable": {
            "departureStopId": "940GZZDLPOP",
            "routes": [
              {
                "schedules": [
                  {
                    "name": "Monday - Friday",
                    "knownJourneys": [
                      { "hour": "4294967295", "minute": "00", "intervalId": 0 },
                      { "hour": "10", "minute": "02", "intervalId": 1 }
                    ]
                  }
                ]
              }
            ]
          }
        }"#;
        let trips = parse_timetable(json, weekday_service_date())
            .expect("a malformed journey must not fail the whole parse");
        assert_eq!(
            trips.len(),
            1,
            "only the well-formed journey should survive"
        );
        assert_eq!(trips[0].interval_id, Some(1));
    }

    #[test]
    fn a_non_numeric_hour_or_minute_skips_only_that_journey() {
        let json = r#"{
          "lineId": "dlr",
          "lineName": "DLR",
          "direction": "outbound",
          "timetable": {
            "departureStopId": "940GZZDLPOP",
            "routes": [
              {
                "schedules": [
                  {
                    "name": "Monday - Friday",
                    "knownJourneys": [
                      { "hour": "not-a-number", "minute": "02", "intervalId": 0 },
                      { "hour": "10", "minute": "not-a-number", "intervalId": 1 },
                      { "hour": "10", "minute": "04", "intervalId": 2 }
                    ]
                  }
                ]
              }
            ]
          }
        }"#;
        let trips = parse_timetable(json, weekday_service_date())
            .expect("a non-numeric hour/minute must not fail the whole parse -- previously this used `?` and errored the whole timetable");
        assert_eq!(
            trips.len(),
            1,
            "only the one well-formed journey should survive"
        );
        assert_eq!(trips[0].interval_id, Some(2));
    }

    #[test]
    fn an_out_of_range_minute_is_skipped_not_erroring_the_whole_parse() {
        let json = r#"{
          "lineId": "dlr",
          "lineName": "DLR",
          "direction": "outbound",
          "timetable": {
            "departureStopId": "940GZZDLPOP",
            "routes": [
              {
                "schedules": [
                  {
                    "name": "Monday - Friday",
                    "knownJourneys": [
                      { "hour": "10", "minute": "99", "intervalId": 0 },
                      { "hour": "10", "minute": "02", "intervalId": 1 }
                    ]
                  }
                ]
              }
            ]
          }
        }"#;
        let trips = parse_timetable(json, weekday_service_date()).expect("should parse");
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].interval_id, Some(1));
    }

    #[test]
    fn a_response_with_no_journeys_parses_to_an_empty_list() {
        let json = r#"{"lineId":"dlr","lineName":"DLR","direction":"outbound","timetable":{"departureStopId":"940GZZDLPOP","routes":[]}}"#;
        assert!(
            parse_timetable(json, weekday_service_date())
                .expect("should parse")
                .is_empty()
        );
    }
}
