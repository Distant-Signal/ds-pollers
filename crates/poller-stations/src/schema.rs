//! RDM Stations JSON schema and its mapping to `common::StationReference`.
//!
//! Field names and structure below are taken from the National Rail Station
//! API OpenAPI spec (v1.0.0, `paths./stations`, `components.schemas.Station`)
//! — camelCase, confirmed directly from the JSON schema rather than
//! transcribed from a sibling XML schema.
//!
//! The spec's `200` response schema for `GET /stations` documents a bare
//! JSON array of `Station`. The live API does not match that: a real
//! response body (observed after a `RDM_TOCS_BASE_URL` misconfiguration
//! pointed `poller-tocs` at this same product and it logged the response
//! it couldn't parse) shows the array wrapped in an envelope object,
//! `{"stations": [...]}`. `parse_stations` follows the observed reality,
//! not the spec doc, and unwraps that envelope — if the spec is ever
//! revised or the account's actual endpoint reconfirmed, re-check this.
//!
//! The spec's `Station` object has dozens of fields covering facilities,
//! accessibility, ticketing, transport links, car parks, etc. Only the
//! handful with a direct `StationReference` column (`crsCode`, `name`,
//! `location`, `stationOperator`) are modeled individually; everything else
//! (`stationAccessibility`, `staffAssistance`, `toiletsAndChanging`,
//! `transportLinks`, `lifts`, `ticketBuying`, `loungesAndWaiting`,
//! `stationFacilities`, `helpAndSupport`, `platformFacilities`, `cycling`,
//! `dropOffPickUp`, `carParks`, `changeHistory`, `slug`,
//! `sixteenCharacterName`, `nationalLocationCode`, `minimumConnectionTime`,
//! `address`, `stationAlerts`, `stationMap`, `staffingLevel`,
//! `informationServices`) is collected verbatim via `#[serde(flatten)]` into
//! the `accessibility` JSONB passthrough column — Global Constraint 7 says
//! don't hand-model this, and the DB schema only has one passthrough column
//! for it.
//!
//! The spec doesn't document a security scheme, so the `x-apikey` auth
//! header assumption (same as the other RDM pollers) is unchanged.

use anyhow::Result;
use common::StationReference;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdmLocation {
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdmStationOperator {
    #[serde(default)]
    pub operator_code: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdmStation {
    pub crs_code: String,
    pub name: String,
    #[serde(default)]
    pub location: Option<RdmLocation>,
    #[serde(default)]
    pub station_operator: Option<RdmStationOperator>,
    /// Every `Station` field not named above, captured verbatim — see
    /// module docs.
    #[serde(flatten)]
    pub rest: serde_json::Value,
}

impl From<&RdmStation> for StationReference {
    fn from(station: &RdmStation) -> Self {
        StationReference {
            crs: station.crs_code.clone(),
            name: station.name.clone(),
            latitude: station.location.as_ref().and_then(|l| l.latitude),
            longitude: station.location.as_ref().and_then(|l| l.longitude),
            station_operator: station
                .station_operator
                .as_ref()
                .and_then(|so| so.operator_code.clone()),
            accessibility: station.rest.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct StationsResponse {
    stations: Vec<RdmStation>,
}

/// Parse a full RDM `/stations` JSON response body into `StationReference`s.
///
/// Expects `{"stations": [...]}` (see module docs — the live API wraps the
/// array in an envelope despite the spec doc saying otherwise).
pub fn parse_stations(json: &str) -> Result<Vec<StationReference>> {
    let response: StationsResponse = serde_json::from_str(json)?;
    Ok(response
        .stations
        .iter()
        .map(StationReference::from)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written sample matching the confirmed live shape: envelope
    /// object (`{"stations": [...]}`), camelCase fields, nested `location`
    /// and `stationOperator` objects, and unmodeled `Station` fields
    /// (`slug`, `changeHistory`, ...) present to confirm they round-trip
    /// verbatim via `#[serde(flatten)]` rather than being decomposed.
    const SAMPLE_JSON: &str = r#"
        {
            "stations": [
                {
                    "crsCode": "EUS",
                    "name": "London Euston",
                    "location": {
                        "latitude": 51.528308,
                        "longitude": -0.133541
                    },
                    "stationOperator": {
                        "name": "Network Rail",
                        "slug": "network-rail",
                        "operatorCode": "NR"
                    },
                    "slug": "london-euston",
                    "changeHistory": {
                        "changedBy": "AAP2",
                        "lastChangedDate": "2026-06-23T21:37:34.000Z"
                    }
                },
                {
                    "crsCode": "ABC",
                    "name": "A Test Station",
                    "location": null,
                    "stationOperator": null,
                    "slug": "a-test-station"
                }
            ]
        }
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
        assert_eq!(euston.station_operator, Some("NR".to_string()));

        // Unmodeled `Station` fields must round-trip verbatim as a
        // `serde_json::Value`, not be decomposed into individual fields.
        let rest = euston.accessibility.as_object().expect("object");
        assert_eq!(
            rest.get("slug").and_then(|v| v.as_str()),
            Some("london-euston")
        );
        assert_eq!(
            rest.get("changeHistory")
                .and_then(|v| v.get("changedBy"))
                .and_then(|v| v.as_str()),
            Some("AAP2")
        );

        let second = &stations[1];
        assert_eq!(second.crs, "ABC");
        assert_eq!(second.latitude, None);
        assert_eq!(second.longitude, None);
        assert_eq!(second.station_operator, None);
        assert_eq!(
            second.accessibility.get("slug").and_then(|v| v.as_str()),
            Some("a-test-station")
        );
    }
}
