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
pub fn compute_delay_minutes(std: &str, etd: &str) -> i32 {
    let (Some(scheduled), Some(estimated)) = (parse_hhmm(std), parse_hhmm(etd)) else {
        return 0;
    };

    let diff = (estimated - scheduled).num_minutes();
    if diff < 0 {
        (diff + 1440) as i32
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

        let second = &departures[1];
        assert_eq!(second.estimated, "On time");
        assert_eq!(second.delay_minutes, 0);
        assert!(!second.is_cancelled);
        assert_eq!(second.skipped_stations, vec!["DID".to_string()]);

        let third = &departures[2];
        assert!(third.is_cancelled);
        assert_eq!(third.delay_minutes, 0);
        assert_eq!(
            third.cancel_reason,
            Some("This train has been cancelled because of a fault on this train".to_string())
        );
        assert_eq!(third.skipped_stations, Vec::<String>::new());
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
