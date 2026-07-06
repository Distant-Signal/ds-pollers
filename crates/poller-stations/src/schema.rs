//! RDM Stations JSON schema and its mapping to `common::StationReference`.
//!
//! Per RSPS5050 P-03-00 Rev A, §6 (see `.superpowers/sdd/task-4-brief.md`),
//! the field names below (`CrsCode`, `Name`, `Latitude`, `Longitude`,
//! `StationOperator`, `Accessibility`) are transcribed verbatim from the
//! spec — but the spec only documents them via the *sibling XML schema*,
//! not the JSON OpenAPI spec directly, so the exact JSON casing (PascalCase
//! vs camelCase vs something else) is **unconfirmed**. `rename_all =
//! "PascalCase"` below is a best-effort guess matching the documented
//! spelling, not a confirmed fact.
//!
//! To avoid shipping that guess blind, `main.rs`'s `fetch_stations_json`
//! logs the raw response body at `debug` level *before* this module parses
//! it. When a real run against the account's RDM endpoint happens, enable
//! debug logging (`RUST_LOG=poller_stations=debug`), inspect the logged
//! body, and adjust `rename_all`/per-field `rename` here to match observed
//! reality if it differs.
//!
//! `Accessibility` is deliberately left as an opaque `serde_json::Value` —
//! Global Constraint 7 says JSONB passthrough only, not hand-modeling the
//! ~14 documented sub-fields (`Helpline`, `InductionLoop`,
//! `AccessibleTicketMachines`, `RampForTrainAccess`,
//! `StepFreeAccess.Coverage`, etc).
//!
//! Also unconfirmed: whether `GET /stations` returns a bare JSON array or
//! wraps it in an envelope object (the spec doesn't name one, unlike the
//! Incidents schema's `<Incidents>` root) — a bare array is the
//! least-invented assumption, so that's what `parse_stations` expects.

use anyhow::Result;
use common::StationReference;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RdmStation {
    pub crs_code: String,
    pub name: String,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub station_operator: Option<String>,
    /// JSONB passthrough — see module docs. Not decomposed into its ~14
    /// documented sub-fields.
    #[serde(default)]
    pub accessibility: serde_json::Value,
}

impl From<&RdmStation> for StationReference {
    fn from(station: &RdmStation) -> Self {
        StationReference {
            crs: station.crs_code.clone(),
            name: station.name.clone(),
            latitude: station.latitude,
            longitude: station.longitude,
            station_operator: station.station_operator.clone(),
            accessibility: station.accessibility.clone(),
        }
    }
}

/// Parse a full RDM `/stations` JSON response body into `StationReference`s.
///
/// Expects a bare JSON array (see module docs for why — no envelope name is
/// documented in the spec).
pub fn parse_stations(json: &str) -> Result<Vec<StationReference>> {
    let stations: Vec<RdmStation> = serde_json::from_str(json)?;
    Ok(stations.iter().map(StationReference::from).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written sample using the spec's documented field names
    /// (`CrsCode`, `Name`, `Longitude`/`Latitude`, `StationOperator`,
    /// `Accessibility`), including a nested `Accessibility` sub-object to
    /// confirm it round-trips as `serde_json::Value` without being
    /// decomposed into individual fields.
    const SAMPLE_JSON: &str = r#"
        [
            {
                "CrsCode": "EUS",
                "Name": "London Euston",
                "Latitude": 51.528308,
                "Longitude": -0.133541,
                "StationOperator": "VT",
                "Accessibility": {
                    "Helpline": "0345 000 0000",
                    "InductionLoop": true,
                    "AccessibleTicketMachines": true,
                    "RampForTrainAccess": true,
                    "StepFreeAccess": {
                        "Coverage": "Full"
                    }
                }
            },
            {
                "CrsCode": "ABC",
                "Name": "A Test Station",
                "Latitude": null,
                "Longitude": null,
                "StationOperator": null,
                "Accessibility": {}
            }
        ]
    "#;

    #[test]
    fn parses_sample_stations_and_maps_every_field() {
        let stations = parse_stations(SAMPLE_JSON).expect("sample JSON should parse");
        assert_eq!(stations.len(), 2);

        let euston = &stations[0];
        assert_eq!(euston.crs, "EUS");
        assert_eq!(euston.name, "London Euston");
        assert_eq!(euston.latitude, Some(51.528308));
        assert_eq!(euston.longitude, Some(-0.133541));
        assert_eq!(euston.station_operator, Some("VT".to_string()));

        // The accessibility sub-object must round-trip verbatim as a
        // `serde_json::Value`, not be decomposed into individual fields.
        let accessibility = euston.accessibility.as_object().expect("object");
        assert_eq!(
            accessibility.get("Helpline").and_then(|v| v.as_str()),
            Some("0345 000 0000")
        );
        assert_eq!(
            accessibility.get("InductionLoop").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            accessibility
                .get("StepFreeAccess")
                .and_then(|v| v.get("Coverage"))
                .and_then(|v| v.as_str()),
            Some("Full")
        );

        let second = &stations[1];
        assert_eq!(second.crs, "ABC");
        assert_eq!(second.latitude, None);
        assert_eq!(second.longitude, None);
        assert_eq!(second.station_operator, None);
        assert_eq!(second.accessibility, serde_json::json!({}));
    }
}
