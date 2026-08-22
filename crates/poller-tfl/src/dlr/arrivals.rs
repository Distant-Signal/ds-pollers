//! Parses TfL's `GET /Line/dlr/Arrivals` response — a flat list of live
//! per-train predictions, one entry per (vehicle, next stop) pair, covering
//! the whole DLR network in a single call. Field names are transcribed
//! from TfL's public `Prediction` entity docs; see
//! `crates/poller-tfl/tests/fixtures/README.md` for what the live capture
//! actually confirmed.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;

// Transcribed to mirror TfL's full published `Prediction` entity (Task 3's
// design), for fidelity with the real API shape. Not all fields are read
// by current callers — `poller-tfl`'s DLR pilot (Task 7) reads
// `expected_arrival` (matching), `naptan_id` (scoping predictions to the
// pilot station) and `direction` (the Timetable half of the diff is
// fetched `?direction=outbound`, so inbound predictions at the same
// station must not be matched against it); `vehicle_id`, `station_name`,
// `destination_naptan_id`, `destination_name`, and `time_to_station` are
// unused today but kept for API fidelity/future consumers.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Prediction {
    pub vehicle_id: String,
    pub naptan_id: String,
    pub station_name: String,
    /// `"inbound"`/`"outbound"` in the live capture, and occasionally the
    /// empty string — defaulted so a missing or blank value degrades into
    /// "not outbound" (and so is simply not matched) rather than failing
    /// the whole Arrivals parse.
    #[serde(default)]
    pub direction: String,
    #[serde(default)]
    pub destination_naptan_id: String,
    #[serde(default)]
    pub destination_name: String,
    pub expected_arrival: DateTime<Utc>,
    pub time_to_station: i64,
}

pub fn parse_arrivals(json: &str) -> Result<Vec<Prediction>> {
    Ok(serde_json::from_str(json)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real data trimmed from crates/poller-tfl/tests/fixtures/dlr_arrivals.json,
    // confirming TfL's actual response structure (vehicleId always empty for DLR,
    // extra fields like $type/operationType/timing/bearing/towards/timeToLive
    // present in real data but safely ignored by serde).
    const DLR_ARRIVALS_JSON: &str = r#"[
      {
        "$type": "Tfl.Api.Presentation.Entities.Prediction, Tfl.Api.Presentation.Entities",
        "id": "-436894549",
        "operationType": 1,
        "vehicleId": "",
        "naptanId": "940GZZDLBOW",
        "stationName": "Bow Church DLR Station",
        "lineId": "dlr",
        "lineName": "DLR",
        "platformName": "Platform 1",
        "direction": "inbound",
        "bearing": "",
        "destinationNaptanId": "940GZZDLSTD",
        "destinationName": "Stratford DLR Station",
        "timestamp": "2026-08-22T13:25:32.53931Z",
        "timeToStation": 277,
        "currentLocation": "",
        "towards": "",
        "expectedArrival": "2026-08-22T13:30:09Z",
        "timeToLive": "2026-08-22T13:30:09Z",
        "modeName": "dlr",
        "timing": {
          "$type": "Tfl.Api.Presentation.Entities.PredictionTiming, Tfl.Api.Presentation.Entities",
          "countdownServerAdjustment": "00:00:00",
          "source": "0001-01-01T00:00:00",
          "insert": "0001-01-01T00:00:00",
          "read": "2026-08-22T13:26:09.216Z",
          "sent": "2026-08-22T13:25:32Z",
          "received": "0001-01-01T00:00:00"
        }
      },
      {
        "$type": "Tfl.Api.Presentation.Entities.Prediction, Tfl.Api.Presentation.Entities",
        "id": "-1340095081",
        "operationType": 1,
        "vehicleId": "",
        "naptanId": "940GZZDLBPK",
        "stationName": "Beckton Park DLR Station",
        "lineId": "dlr",
        "lineName": "DLR",
        "platformName": "Platform 2",
        "direction": "inbound",
        "bearing": "",
        "destinationNaptanId": "940GZZDLTWG",
        "destinationName": "Tower Gateway DLR Station",
        "timestamp": "2026-08-22T13:25:32.53931Z",
        "timeToStation": 156,
        "currentLocation": "",
        "towards": "",
        "expectedArrival": "2026-08-22T13:28:08Z",
        "timeToLive": "2026-08-22T13:28:08Z",
        "modeName": "dlr",
        "timing": {
          "$type": "Tfl.Api.Presentation.Entities.PredictionTiming, Tfl.Api.Presentation.Entities",
          "countdownServerAdjustment": "00:00:00",
          "source": "0001-01-01T00:00:00",
          "insert": "0001-01-01T00:00:00",
          "read": "2026-08-22T13:26:09.149Z",
          "sent": "2026-08-22T13:25:32Z",
          "received": "0001-01-01T00:00:00"
        }
      },
      {
        "$type": "Tfl.Api.Presentation.Entities.Prediction, Tfl.Api.Presentation.Entities",
        "id": "1482773628",
        "operationType": 1,
        "vehicleId": "",
        "naptanId": "940GZZDLPOP",
        "stationName": "Poplar DLR Station",
        "lineId": "dlr",
        "lineName": "DLR",
        "platformName": "Platform 2",
        "direction": "inbound",
        "bearing": "",
        "destinationNaptanId": "940GZZDLSTD",
        "destinationName": "Stratford DLR Station",
        "timestamp": "2026-08-22T13:25:32.53931Z",
        "timeToStation": 216,
        "currentLocation": "",
        "towards": "",
        "expectedArrival": "2026-08-22T13:29:08Z",
        "timeToLive": "2026-08-22T13:29:08Z",
        "modeName": "dlr",
        "timing": {
          "$type": "Tfl.Api.Presentation.Entities.PredictionTiming, Tfl.Api.Presentation.Entities",
          "countdownServerAdjustment": "00:00:00",
          "source": "0001-01-01T00:00:00",
          "insert": "0001-01-01T00:00:00",
          "read": "2026-08-22T13:26:09.149Z",
          "sent": "2026-08-22T13:25:32Z",
          "received": "0001-01-01T00:00:00"
        }
      }
    ]"#;

    #[test]
    fn parses_a_prediction_and_maps_every_field_this_pilot_needs() {
        let predictions = parse_arrivals(DLR_ARRIVALS_JSON).expect("should parse");
        assert_eq!(predictions.len(), 3);

        // First entry: Bow Church → Stratford (real data structure with extra fields)
        let p = &predictions[0];
        assert_eq!(p.vehicle_id, "");  // TfL always sends empty string for DLR vehicleId
        assert_eq!(p.naptan_id, "940GZZDLBOW");
        assert_eq!(p.station_name, "Bow Church DLR Station");
        assert_eq!(p.destination_name, "Stratford DLR Station");
        assert_eq!(p.direction, "inbound");
        assert_eq!(p.expected_arrival, "2026-08-22T13:30:09Z".parse::<DateTime<Utc>>().unwrap());
        assert_eq!(p.time_to_station, 277);

        // Third entry: Poplar (the pilot station)
        let p3 = &predictions[2];
        assert_eq!(p3.vehicle_id, "");
        assert_eq!(p3.naptan_id, "940GZZDLPOP");
        assert_eq!(p3.station_name, "Poplar DLR Station");
        assert_eq!(p3.destination_name, "Stratford DLR Station");
        assert_eq!(p3.expected_arrival, "2026-08-22T13:29:08Z".parse::<DateTime<Utc>>().unwrap());
        assert_eq!(p3.time_to_station, 216);
    }

    #[test]
    fn direction_parses_and_defaults_to_empty_when_absent() {
        // Real Poplar data carries both directions on the same station
        // (6 inbound and 6 outbound predictions in the capture), which is
        // why the caller has to filter on it — the Timetable half of the
        // diff is outbound-only.
        let json = r#"[
          {
            "vehicleId": "",
            "naptanId": "940GZZDLPOP",
            "stationName": "Poplar DLR Station",
            "direction": "outbound",
            "destinationNaptanId": "940GZZDLLEW",
            "destinationName": "Lewisham DLR Station",
            "expectedArrival": "2026-08-22T13:29:08Z",
            "timeToStation": 216
          },
          {
            "vehicleId": "",
            "naptanId": "940GZZDLPOP",
            "stationName": "Poplar DLR Station",
            "destinationNaptanId": "",
            "destinationName": "",
            "expectedArrival": "2026-08-22T13:31:08Z",
            "timeToStation": 336
          }
        ]"#;
        let predictions = parse_arrivals(json).expect("should parse");
        assert_eq!(predictions[0].direction, "outbound");
        assert_eq!(predictions[1].direction, "");
    }

    #[test]
    fn an_empty_response_parses_to_an_empty_list() {
        assert!(parse_arrivals("[]").expect("should parse").is_empty());
    }
}
