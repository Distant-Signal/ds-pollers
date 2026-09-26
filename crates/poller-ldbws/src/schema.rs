//! RDM Live Departure Board (`GetDepBoardWithDetails`) JSON schema and its
//! mapping to `common::StationDeparture`.
//!
//! Field names below are transcribed verbatim from a Swagger 2.0 spec
//! fetched and parsed directly during planning (see the implementation
//! plan's "Current relevant code" section for the source and exact
//! `definitions` block). High confidence on field names/types; the base
//! URL's exact product-slug segment and this feed's rate limit are the
//! genuinely unconfirmed facts, both handled in `config.rs`, not here.

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
    /// RDM/LDBWS's `serviceID`: an opaque, board-relative token for
    /// chaining into `GetServiceDetails`, NOT a durable train identity --
    /// Darwin documents no stability guarantee for it across calls or
    /// boards. It is the ONLY per-service identifier this poller decodes.
    /// The public `GetDepBoardWithDetails` item this schema mirrors carries
    /// no Darwin `rid`, no CIF `uid` and no service-start date (`sdd`) at
    /// all -- those appear only on the staff (`LDBSVWS`) API or the Darwin
    /// Push Port, neither of which this app consumes. The payload does
    /// carry `rsid` (the retail service id, e.g. `"GW123400"`, see this
    /// file's own test fixture), but it is deliberately not decoded: it is
    /// not unique per day on its own, and nothing on the CIF side of this
    /// app (`schedule-query`'s `BX` decode) stores the matching RSID to
    /// join it against. See `routes::departures::get_station_departures`
    /// (`crates/api`) for the documented `serviceId -> (trainUid, date)`
    /// resolution path this leaves a caller with.
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
}

#[derive(Debug, Deserialize)]
struct RdmCallingPointList {
    #[serde(default, rename = "callingPoint")]
    calling_point: Vec<RdmCallingPoint>,
}

#[derive(Debug, Deserialize)]
struct RdmCallingPoint {
    crs: String,
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

        let second = &departures[1];
        assert_eq!(second.estimated, "On time");
        assert_eq!(second.delay_minutes, 0);
        assert!(!second.is_cancelled);
        assert_eq!(second.skipped_stations, vec!["DID".to_string()]);
        assert_eq!(second.platform, Some("9".to_string()));

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
                            is_cancelled: true,
                        },
                        RdmCallingPoint {
                            crs: "SWI".to_string(),
                            is_cancelled: false,
                        },
                    ],
                },
                RdmCallingPointList {
                    calling_point: vec![RdmCallingPoint {
                        crs: "BRI".to_string(),
                        is_cancelled: true,
                    }],
                },
            ],
            platform: None,
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
}
