//! RDM Live Departure Board (`GetDepBoardWithDetails`) JSON schema and its
//! mapping to `common::StationDeparture`.
//!
//! Field names below are transcribed verbatim from a Swagger 2.0 spec
//! fetched and parsed directly during planning (see the implementation
//! plan's "Current relevant code" section for the source and exact
//! `definitions` block). High confidence on field names/types; the base
//! URL's exact product-slug segment is the genuinely unconfirmed fact,
//! handled in `config.rs` (as are the request-volume knobs), not here.

use anyhow::Result;
use common::StationDeparture;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RdmStationBoard {
    #[serde(default, rename = "trainServices")]
    train_services: Vec<RdmServiceItem>,
}

#[derive(Debug, Deserialize)]
struct RdmServiceItem {
    /// RDM/LDBWS's `serviceID`: a board-relative token for chaining into
    /// `GetServiceDetails`. Darwin documents no stability guarantee for it,
    /// but in practice it embeds the Darwin RID serial and this station's
    /// own TIPLOC (see `common::service_id_tiploc`, used only as a hint).
    /// The public `GetDepBoardWithDetails` item this schema mirrors carries
    /// no Darwin `rid`, no CIF `uid` and no service-start date (`sdd`) --
    /// those appear only on the staff (`LDBSVWS`) API or the Darwin Push
    /// Port, neither of which this app consumes. `rsid` (below) is the one
    /// identifier it shares with the CIF timetable.
    #[serde(rename = "serviceID")]
    service_id: String,
    #[serde(rename = "operatorCode")]
    operator_code: String,
    destination: Vec<RdmServiceLocation>,
    std: String,
    etd: String,
    #[serde(rename = "isCancelled")]
    is_cancelled: bool,
    #[serde(default, rename = "cancelReason")]
    cancel_reason: Option<String>,
    #[serde(default, rename = "delayReason")]
    delay_reason: Option<String>,
    #[serde(default, rename = "subsequentCallingPoints")]
    subsequent_calling_points: Vec<RdmCallingPointList>,
    /// The CURRENT (not "planned") platform this station's own departure
    /// board reports for this service -- Darwin/RDM merges any late
    /// alteration into this single field and does not separately expose
    /// what was originally published. `None` both when the key is absent
    /// AND when it's present as JSON `null` (an unallocated platform) --
    /// this API gives no way to tell those two "unknown" cases apart, so
    /// neither is fabricated as a distinct state. `parse_departures` copies
    /// this straight into `StationDeparture.platform`; a genuine
    /// planned-vs-actual distinction is reconstructed downstream, across
    /// polls, by `platform_history::PlatformHistory` -- this feed has no
    /// such distinction of its own to parse.
    #[serde(default)]
    platform: Option<String>,
    /// The Retail Service ID (e.g. `"GW123400"`), the same value the CIF
    /// `BX` record carries (`schedule_destination_departures.rsid`). Not
    /// unique per day on its own; `api::data::train_resolve` and
    /// `api::data::stop_board` pair it with station and time. `None` when
    /// absent or `null`.
    #[serde(default)]
    rsid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RdmCallingPointList {
    #[serde(default, rename = "callingPoint")]
    calling_point: Vec<RdmCallingPoint>,
}

#[derive(Debug, Deserialize)]
struct RdmCallingPoint {
    crs: String,
    #[serde(default, rename = "locationName")]
    location_name: Option<String>,
    #[serde(default)]
    st: Option<String>,
    #[serde(default)]
    et: Option<String>,
    #[serde(default)]
    at: Option<String>,
    #[serde(default, rename = "isCancelled")]
    is_cancelled: bool,
}

#[derive(Debug, Deserialize)]
struct RdmServiceLocation {
    crs: String,
}

fn parse_hhmm(s: &str) -> Option<chrono::NaiveTime> {
    chrono::NaiveTime::parse_from_str(s, "%H:%M").ok()
}

/// Computes minutes of delay between a scheduled ("std") and estimated
/// ("etd") departure time-of-day string. LDBWS's `etd` field is not always
/// a time — it may be a status word like `"On time"`, `"Delayed"`, or
/// `"Cancelled"` — so this returns `0` whenever `etd` isn't itself a valid
/// "HH:MM" time (including `"On time"`: no delay to report, and any other
/// status word: `is_cancelled`/`delay_reason` already carry the more
/// precise signal, and there's no time to diff against).
///
/// Handles the midnight wraparound case (e.g. std="23:55", etd="00:05" is
/// a 10-minute delay, not -1430).
///
/// A small negative `diff` is NOT a wraparound, though -- Darwin routinely
/// publishes an `etd` a minute or two BEFORE `std` for a re-timed or
/// early-running service. Applying the +1440 correction uniformly to every
/// negative diff (the previous behaviour here) turned that ordinary "1
/// minute early" case into a reported ~1439-minute delay, which then
/// skewed the aggregator's averaged per-line delay stats into a false
/// "delays" severity tier from a single bad sample. A genuine midnight
/// wraparound instead produces a diff close to -1440 (std shortly before
/// midnight, etd shortly after) -- so only a diff more negative than
/// `WRAPAROUND_THRESHOLD_MINUTES` is treated as a wraparound; anything
/// less negative than that is just an early/on-time service and clamps to
/// 0 delay.
const WRAPAROUND_THRESHOLD_MINUTES: i64 = -720;

pub fn compute_delay_minutes(std: &str, etd: &str) -> i32 {
    let (Some(scheduled), Some(estimated)) = (parse_hhmm(std), parse_hhmm(etd)) else {
        return 0;
    };

    let diff = (estimated - scheduled).num_minutes();
    if diff < WRAPAROUND_THRESHOLD_MINUTES {
        (diff + 1440) as i32
    } else if diff < 0 {
        0
    } else {
        diff as i32
    }
}

/// Flattens every calling point Darwin marks `isCancelled: true` across all
/// of a service's `subsequentCallingPoints` entries (a service can report
/// more than one when it splits/joins) into a single CRS list. A calling
/// point that was never scheduled for this service doesn't appear in
/// `subsequentCallingPoints` at all, so nothing here can mistake a normal
/// fast-service stopping pattern for a genuine skip.
fn extract_skipped_stations(service: &RdmServiceItem) -> Vec<String> {
    service
        .subsequent_calling_points
        .iter()
        .flat_map(|list| list.calling_point.iter())
        .filter(|cp| cp.is_cancelled)
        .map(|cp| cp.crs.clone())
        .collect()
}

/// Flattens a service's `subsequentCallingPoints` lists (one per portion
/// of a split/joining service) into one compact list in board order. A
/// stop reported by more than one list (the shared part of a split) is
/// kept once, keyed on `(crs, st)`. Blank strings are stored as absent.
fn extract_calling_points(service: &RdmServiceItem) -> Vec<common::BoardCallingPoint> {
    fn non_blank(value: &Option<String>) -> Option<String> {
        value
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    }
    let mut out: Vec<common::BoardCallingPoint> = Vec::new();
    for cp in service
        .subsequent_calling_points
        .iter()
        .flat_map(|list| list.calling_point.iter())
    {
        let st = non_blank(&cp.st);
        if out
            .iter()
            .any(|seen| seen.crs.eq_ignore_ascii_case(&cp.crs) && seen.st == st)
        {
            continue;
        }
        out.push(common::BoardCallingPoint {
            crs: cp.crs.trim().to_uppercase(),
            location_name: non_blank(&cp.location_name),
            st,
            et: non_blank(&cp.et),
            at: non_blank(&cp.at),
            is_cancelled: cp.is_cancelled,
        });
    }
    out
}

/// Maps one RDM `GetDepBoardWithDetails` JSON response body into the
/// `StationDeparture`s for that station. Only `trainServices` are sampled
/// (see the implementation plan's Global Constraints). A service missing a
/// destination is skipped (logged, not fabricated) rather than guessing a
/// CRS. `headcode` is always `None`: confirmed absent from this API's
/// schema entirely.
pub fn parse_departures(json: &str) -> Result<Vec<StationDeparture>> {
    let board: RdmStationBoard = serde_json::from_str(json)?;

    Ok(board
        .train_services
        .iter()
        .filter_map(|service| {
            let Some(destination) = service.destination.first() else {
                tracing::warn!(service_id = %service.service_id, "service has no destination, skipping");
                return None;
            };

            Some(StationDeparture {
                service_id: service.service_id.clone(),
                operator: service.operator_code.clone(),
                destination_crs: destination.crs.clone(),
                scheduled: service.std.clone(),
                estimated: service.etd.clone(),
                is_cancelled: service.is_cancelled,
                delay_minutes: compute_delay_minutes(&service.std, &service.etd),
                cancel_reason: service.cancel_reason.clone(),
                delay_reason: service.delay_reason.clone(),
                headcode: None,
                skipped_stations: extract_skipped_stations(service),
                platform: service.platform.clone(),
                // See `StationDeparture.planned_platform`'s own doc
                // comment -- this single-poll parse has no history to draw
                // a "planned" value from; `platform_history::PlatformHistory`
                // fills this in afterwards, once per polled station, from
                // this process's own memory of earlier polls.
                planned_platform: None,
                rsid: service
                    .rsid
                    .as_deref()
                    .map(str::trim)
                    .filter(|r| !r.is_empty())
                    .map(str::to_string),
                calling_points: extract_calling_points(service),
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_time_etd_has_zero_delay() {
        assert_eq!(compute_delay_minutes("10:00", "On time"), 0);
    }

    #[test]
    fn normal_delay_is_computed() {
        assert_eq!(compute_delay_minutes("10:00", "10:05"), 5);
    }

    #[test]
    fn midnight_wraparound_is_handled() {
        assert_eq!(compute_delay_minutes("23:55", "00:05"), 10);
    }

    #[test]
    fn a_slightly_early_estimate_clamps_to_zero_not_1439_minutes() {
        // The real bug: a re-timed/early-running service with etd a minute
        // or two before std used to get the same +1440 wraparound
        // correction as a genuine midnight rollover, reporting ~1439
        // minutes of "delay" for a train that's actually early or on time.
        assert_eq!(compute_delay_minutes("10:05", "10:03"), 0);
        assert_eq!(compute_delay_minutes("10:05", "10:04"), 0);
        assert_eq!(compute_delay_minutes("00:10", "00:05"), 0);
    }

    #[test]
    fn a_genuine_midnight_wraparound_still_gets_the_1440_correction() {
        // Regression guard for the fix above: only a large negative diff
        // (std shortly before midnight, etd shortly after) should still be
        // treated as a wraparound, not just clamped to 0.
        assert_eq!(compute_delay_minutes("23:58", "00:02"), 4);
        assert_eq!(compute_delay_minutes("23:00", "00:30"), 90);
    }

    #[test]
    fn a_diff_right_at_the_wraparound_threshold_is_still_early_not_wrapped() {
        // -720 minutes (12 hours) is a plausible same-day "early" diff for
        // a std/etd pair on opposite sides of noon, not a real wraparound
        // -- it must clamp to 0, not add 1440 and report a 720-minute
        // delay.
        assert_eq!(compute_delay_minutes("12:00", "00:00"), 0);
    }

    #[test]
    fn non_time_status_word_has_zero_delay() {
        assert_eq!(compute_delay_minutes("10:00", "Cancelled"), 0);
        assert_eq!(compute_delay_minutes("10:00", "Delayed"), 0);
    }

    #[test]
    fn identical_times_have_zero_delay() {
        assert_eq!(compute_delay_minutes("10:00", "10:00"), 0);
    }

    const SAMPLE_JSON: &str = r#"
        {
            "generatedAt": "2026-07-06T10:00:00Z",
            "locationName": "London Paddington",
            "crs": "PAD",
            "trainServices": [
                {
                    "serviceID": "yjnJDu6rXAM6MhtwfOUZZg==",
                    "operator": "Great Western Railway",
                    "operatorCode": "GW",
                    "destination": [{"locationName": "Reading", "crs": "RDG"}],
                    "origin": [{"locationName": "London Paddington", "crs": "PAD"}],
                    "std": "10:00",
                    "etd": "10:05",
                    "platform": "6",
                    "isCancelled": false,
                    "cancelReason": null,
                    "delayReason": "This train has been delayed by a signalling problem",
                    "rsid": "GW123400",
                    "serviceType": "train"
                },
                {
                    "serviceID": "abc123==",
                    "operator": "Great Western Railway",
                    "operatorCode": "GW",
                    "destination": [{"locationName": "Oxford", "crs": "OXF"}],
                    "origin": [{"locationName": "London Paddington", "crs": "PAD"}],
                    "std": "10:15",
                    "etd": "On time",
                    "platform": "9",
                    "isCancelled": false,
                    "cancelReason": null,
                    "delayReason": null,
                    "rsid": "GW123500",
                    "serviceType": "train",
                    "subsequentCallingPoints": [
                        {
                            "callingPoint": [
                                {"locationName": "Didcot Parkway", "crs": "DID", "st": "10:22", "isCancelled": true},
                                {"locationName": "Oxford", "crs": "OXF", "st": "10:40", "isCancelled": false}
                            ]
                        }
                    ]
                },
                {
                    "serviceID": "def456==",
                    "operator": "Great Western Railway",
                    "operatorCode": "GW",
                    "destination": [{"locationName": "Bristol Temple Meads", "crs": "BRI"}],
                    "origin": [{"locationName": "London Paddington", "crs": "PAD"}],
                    "std": "10:30",
                    "etd": "Cancelled",
                    "platform": null,
                    "isCancelled": true,
                    "cancelReason": "This train has been cancelled because of a fault on this train",
                    "delayReason": null,
                    "rsid": "GW123600",
                    "serviceType": "train"
                }
            ]
        }
    "#;

    #[test]
    fn parses_sample_board_and_maps_every_field() {
        let departures = parse_departures(SAMPLE_JSON).expect("sample JSON should parse");
        assert_eq!(departures.len(), 3);

        let first = &departures[0];
        assert_eq!(first.service_id, "yjnJDu6rXAM6MhtwfOUZZg==");
        assert_eq!(first.operator, "GW");
        assert_eq!(first.destination_crs, "RDG");
        assert_eq!(first.scheduled, "10:00");
        assert_eq!(first.estimated, "10:05");
        assert!(!first.is_cancelled);
        assert_eq!(first.delay_minutes, 5);
        assert_eq!(first.cancel_reason, None);
        assert_eq!(
            first.delay_reason,
            Some("This train has been delayed by a signalling problem".to_string())
        );
        assert_eq!(first.headcode, None);
        assert_eq!(first.skipped_stations, Vec::<String>::new());
        assert_eq!(first.platform, Some("6".to_string()));
        // `planned_platform` is filled in later by `PlatformHistory` (see
        // `platform_history.rs`), never by this parsing step -- a single
        // JSON body carries no cross-poll history of its own.
        assert_eq!(first.planned_platform, None);
        assert_eq!(first.rsid.as_deref(), Some("GW123400"));
        assert!(first.calling_points.is_empty());

        let second = &departures[1];
        assert_eq!(second.estimated, "On time");
        assert_eq!(second.delay_minutes, 0);
        assert!(!second.is_cancelled);
        assert_eq!(second.skipped_stations, vec!["DID".to_string()]);
        assert_eq!(second.platform, Some("9".to_string()));
        assert_eq!(second.rsid.as_deref(), Some("GW123500"));
        assert_eq!(
            second.calling_points,
            vec![
                common::BoardCallingPoint {
                    crs: "DID".to_string(),
                    location_name: Some("Didcot Parkway".to_string()),
                    st: Some("10:22".to_string()),
                    et: None,
                    at: None,
                    is_cancelled: true,
                },
                common::BoardCallingPoint {
                    crs: "OXF".to_string(),
                    location_name: Some("Oxford".to_string()),
                    st: Some("10:40".to_string()),
                    et: None,
                    at: None,
                    is_cancelled: false,
                },
            ]
        );

        let third = &departures[2];
        assert!(third.is_cancelled);
        assert_eq!(third.delay_minutes, 0);
        assert_eq!(
            third.cancel_reason,
            Some("This train has been cancelled because of a fault on this train".to_string())
        );
        assert_eq!(third.skipped_stations, Vec::<String>::new());
        // The sample fixture's third service has `"platform": null` -- an
        // unallocated platform, distinct from the field being absent
        // entirely (also `None` on the wire, but a genuinely different
        // real-world fact -- see `RdmServiceItem::platform`'s own doc
        // comment).
        assert_eq!(third.platform, None);
    }

    #[test]
    fn skipped_stations_flattens_multiple_calling_point_lists() {
        // A split/joined service reports more than one callingPointList
        // (one per association) — both must be flattened into one result.
        let service = RdmServiceItem {
            service_id: "svc".to_string(),
            operator_code: "GW".to_string(),
            destination: vec![RdmServiceLocation {
                crs: "BRI".to_string(),
            }],
            std: "10:00".to_string(),
            etd: "On time".to_string(),
            is_cancelled: false,
            cancel_reason: None,
            delay_reason: None,
            subsequent_calling_points: vec![
                RdmCallingPointList {
                    calling_point: vec![
                        RdmCallingPoint {
                            crs: "DID".to_string(),
                            location_name: None,
                            st: None,
                            et: None,
                            at: None,
                            is_cancelled: true,
                        },
                        RdmCallingPoint {
                            crs: "SWI".to_string(),
                            location_name: None,
                            st: None,
                            et: None,
                            at: None,
                            is_cancelled: false,
                        },
                    ],
                },
                RdmCallingPointList {
                    calling_point: vec![RdmCallingPoint {
                        crs: "BRI".to_string(),
                        location_name: None,
                        st: None,
                        et: None,
                        at: None,
                        is_cancelled: true,
                    }],
                },
            ],
            platform: None,
            rsid: None,
        };
        let mut skipped = extract_skipped_stations(&service);
        skipped.sort();
        assert_eq!(skipped, vec!["BRI".to_string(), "DID".to_string()]);
    }

    #[test]
    fn skipped_stations_empty_when_no_calling_points_reported() {
        let service = RdmServiceItem {
            service_id: "svc".to_string(),
            operator_code: "GW".to_string(),
            destination: vec![RdmServiceLocation {
                crs: "BRI".to_string(),
            }],
            std: "10:00".to_string(),
            etd: "On time".to_string(),
            is_cancelled: false,
            cancel_reason: None,
            delay_reason: None,
            subsequent_calling_points: vec![],
            platform: None,
            rsid: None,
        };
        assert_eq!(extract_skipped_stations(&service), Vec::<String>::new());
    }

    #[test]
    fn service_with_no_destination_is_skipped() {
        let json = r#"
            {
                "trainServices": [
                    {
                        "serviceID": "x==",
                        "operator": "Test",
                        "operatorCode": "TT",
                        "destination": [],
                        "std": "10:00",
                        "etd": "On time",
                        "isCancelled": false
                    }
                ]
            }
        "#;
        let departures = parse_departures(json).expect("should parse despite empty destination");
        assert_eq!(departures.len(), 0);
    }

    #[test]
    fn a_missing_null_or_blank_rsid_decodes_as_none() {
        let json = r#"
            {
                "trainServices": [
                    {"serviceID": "a", "operatorCode": "SW", "destination": [{"crs": "WAT"}],
                     "std": "10:00", "etd": "On time", "isCancelled": false},
                    {"serviceID": "b", "operatorCode": "SW", "destination": [{"crs": "WAT"}],
                     "std": "10:05", "etd": "On time", "isCancelled": false, "rsid": null},
                    {"serviceID": "c", "operatorCode": "SW", "destination": [{"crs": "WAT"}],
                     "std": "10:10", "etd": "On time", "isCancelled": false, "rsid": " "}
                ]
            }
        "#;
        let departures = parse_departures(json).expect("should parse");
        assert_eq!(departures.len(), 3);
        assert!(departures.iter().all(|d| d.rsid.is_none()));
    }

    #[test]
    fn calling_points_keep_et_and_at_and_collapse_a_split_services_shared_stops() {
        let json = r#"
            {
                "trainServices": [
                    {"serviceID": "a", "operatorCode": "SN", "destination": [{"crs": "LIT"}, {"crs": "BOG"}],
                     "std": "10:00", "etd": "10:04", "isCancelled": false, "rsid": "SN123400",
                     "subsequentCallingPoints": [
                        {"callingPoint": [
                            {"locationName": "Clapham Junction", "crs": "CLJ", "st": "10:07", "et": "10:11"},
                            {"locationName": "Horsham", "crs": "HRH", "st": "10:50", "et": "Delayed"},
                            {"locationName": "Littlehampton", "crs": "LIT", "st": "11:40", "et": "Cancelled", "isCancelled": true}
                        ]},
                        {"callingPoint": [
                            {"locationName": "Clapham Junction", "crs": "CLJ", "st": "10:07", "et": "10:11"},
                            {"locationName": "Horsham", "crs": "HRH", "st": "10:50", "et": "Delayed"},
                            {"locationName": "Bognor Regis", "crs": "BOG", "st": "11:45", "at": "", "et": "On time"}
                        ]}
                     ]}
                ]
            }
        "#;
        let departures = parse_departures(json).expect("should parse");
        let cps = &departures[0].calling_points;
        let crs: Vec<&str> = cps.iter().map(|cp| cp.crs.as_str()).collect();
        assert_eq!(crs, vec!["CLJ", "HRH", "LIT", "BOG"]);
        assert_eq!(cps[1].et.as_deref(), Some("Delayed"));
        assert!(cps[2].is_cancelled);
        // A blank `at` is stored as absent, not as an empty string.
        assert_eq!(cps[3].at, None);
        assert_eq!(departures[0].skipped_stations, vec!["LIT".to_string()]);
    }

    #[test]
    fn stored_calling_points_are_compact_and_round_trip() {
        let departures = parse_departures(SAMPLE_JSON).expect("sample JSON should parse");
        let stored = serde_json::to_value(&departures[1]).unwrap();
        assert_eq!(
            stored["calling_points"],
            serde_json::json!([
                {"crs": "DID", "n": "Didcot Parkway", "st": "10:22", "x": true},
                {"crs": "OXF", "n": "Oxford", "st": "10:40"},
            ])
        );
        assert_eq!(stored["rsid"], "GW123500");
        let back: StationDeparture = serde_json::from_value(stored).unwrap();
        assert_eq!(back.calling_points, departures[1].calling_points);
    }
}
