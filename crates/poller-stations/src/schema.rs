//! RDM Stations JSON schema and its mapping to `common::StationReference`.
//!
//! Field names and structure below are taken from the National Rail Station
//! API `OpenAPI` spec (v1.0.0, `paths./stations`, `components.schemas.Station`)
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
//! `informationServices`) is passed through verbatim (see [`parse_stations`]) into
//! the `accessibility` JSONB passthrough column — Global Constraint 7 says
//! don't hand-model this, and the DB schema only has one passthrough column
//! for it.
//!
//! The spec doesn't document a security scheme, so the `x-apikey` auth
//! header assumption (same as the other RDM pollers) is unchanged.

use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RdmLocation {
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RdmStationOperator {
    #[serde(default)]
    pub operator_code: Option<String>,
}

/// The `Station` fields with a `StationReference` column of their own.
/// Every other field is ignored here and kept, unparsed, by
/// [`parse_stations`] instead.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RdmStation {
    crs_code: String,
    name: String,
    #[serde(default)]
    location: Option<RdmLocation>,
    #[serde(default)]
    station_operator: Option<RdmStationOperator>,
}

/// The JSON keys [`RdmStation`] models. Every other top-level key of a
/// station goes into [`StationRecord::accessibility`].
const MODELED_KEYS: [&str; 4] = ["crsCode", "name", "location", "stationOperator"];

/// One station as `POSTed` to `/private/stations`: the same wire shape as
/// `common::StationReference` (which `api` deserializes it as), but with
/// the passthrough `accessibility` object held as `&RawValue` slices of
/// the fetched response body rather than as a `serde_json::Value` tree.
/// See [`parse_stations`] for why.
#[derive(Debug, Serialize)]
pub(crate) struct StationRecord<'a> {
    pub crs: String,
    pub name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub station_operator: Option<String>,
    /// Every `Station` field not modeled by [`RdmStation`], verbatim.
    pub accessibility: BTreeMap<String, &'a RawValue>,
}

#[derive(Debug, Deserialize)]
struct StationsResponse<'a> {
    #[serde(borrow)]
    stations: Vec<&'a RawValue>,
}

/// Parse a full RDM `/stations` JSON response body into [`StationRecord`]s
/// that borrow from `json`.
///
/// Expects `{"stations": [...]}` (see module docs — the live API wraps the
/// array in an envelope despite the spec doc saying otherwise).
///
/// **Memory.** The live body is ~37MB (2,613 stations, ~14KB of facilities
/// JSON each). This used to parse it into one `serde_json::Value` per
/// station, clone each one, deserialize it through `#[serde(flatten)]`
/// (which buffers the whole object again) and clone the flattened rest
/// into a `StationReference` -- several full in-memory copies of the feed,
/// each far larger than its JSON text. Peak RSS was ~320MB, and the pod
/// was `OOMKilled` on every start once its limit was cut to 192Mi on
/// 2026-09-26. Now nothing but the body itself and the serialized POST
/// body is ever feed-sized: each station is kept as a `&RawValue` slice of
/// `json`, the four modeled fields are parsed out of it, and the rest are
/// carried as top-level `&RawValue` slices too.
///
/// **One bad station does not fail the batch.** Each element is
/// deserialized independently, skipping (and logging) just the malformed
/// ones -- a missing required `crsCode`/`name`, say -- mirroring the
/// per-station isolation `poller-ldbws` does for its own batch. A
/// genuinely invalid JSON document still fails outright at the envelope
/// parse, which is not recoverable per element.
pub(crate) fn parse_stations(json: &[u8]) -> Result<Vec<StationRecord<'_>>> {
    let response: StationsResponse<'_> = serde_json::from_slice(json)?;
    Ok(response
        .stations
        .into_iter()
        .filter_map(|raw| match parse_station(raw) {
            Ok(station) => Some(station),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    station = raw.get(),
                    "skipping malformed station entry rather than failing the whole batch"
                );
                None
            }
        })
        .collect())
}

fn parse_station(raw: &RawValue) -> serde_json::Result<StationRecord<'_>> {
    let station: RdmStation = serde_json::from_str(raw.get())?;
    let mut accessibility: BTreeMap<String, &RawValue> = serde_json::from_str(raw.get())?;
    accessibility.retain(|key, _| !MODELED_KEYS.contains(&key.as_str()));
    let location = station.location.as_ref();
    Ok(StationRecord {
        crs: station.crs_code,
        name: station.name,
        latitude: location.and_then(|l| l.latitude),
        longitude: location.and_then(|l| l.longitude),
        station_operator: station.station_operator.and_then(|so| so.operator_code),
        accessibility,
    })
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
        let stations = parse_stations(SAMPLE_JSON.as_bytes()).expect("sample JSON should parse");
        assert_eq!(stations.len(), 2);

        let euston = &stations[0];
        assert_eq!(euston.crs, "EUS");
        assert_eq!(euston.name, "London Euston");
        assert_eq!(euston.latitude, Some(51.528_308));
        assert_eq!(euston.longitude, Some(-0.133_541));
        assert_eq!(euston.station_operator, Some("NR".to_string()));

        // Unmodeled `Station` fields must round-trip verbatim, not be
        // decomposed into individual fields -- and the modeled ones must
        // not be duplicated into the passthrough.
        let rest = accessibility_json(euston);
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
        for key in MODELED_KEYS {
            assert!(rest.get(key).is_none(), "{key} leaked into accessibility");
        }

        let second = &stations[1];
        assert_eq!(second.crs, "ABC");
        assert_eq!(second.latitude, None);
        assert_eq!(second.longitude, None);
        assert_eq!(second.station_operator, None);
        assert_eq!(
            accessibility_json(second)
                .get("slug")
                .and_then(|v| v.as_str()),
            Some("a-test-station")
        );
    }

    fn accessibility_json(station: &StationRecord<'_>) -> serde_json::Value {
        serde_json::to_value(&station.accessibility).expect("accessibility serializes")
    }

    /// `api` reads the POST body as `Vec<common::StationReference>`; a
    /// `StationRecord` must serialize to exactly what the old
    /// `StationReference`-based path sent.
    #[test]
    fn records_serialize_to_the_station_reference_wire_shape() {
        let stations = parse_stations(SAMPLE_JSON.as_bytes()).unwrap();
        let wire = serde_json::to_vec(&stations).unwrap();
        let decoded: Vec<common::StationReference> = serde_json::from_slice(&wire).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].crs, "EUS");
        assert_eq!(decoded[0].station_operator.as_deref(), Some("NR"));
        assert_eq!(decoded[0].latitude, Some(51.528_308));
        assert_eq!(
            decoded[0].accessibility,
            serde_json::json!({
                "slug": "london-euston",
                "changeHistory": {
                    "changedBy": "AAP2",
                    "lastChangedDate": "2026-06-23T21:37:34.000Z"
                }
            })
        );
        assert_eq!(
            decoded[1].accessibility,
            serde_json::json!({ "slug": "a-test-station" })
        );
    }

    /// 2026-09-27 OOM: `poller-stations` was `OOMKilled` at its 192Mi limit
    /// on every start. Parsing the ~37MB live feed built several
    /// `serde_json::Value` copies of it (peak RSS ~320MB). Parsing plus
    /// serializing the POST body (what `reqwest`'s `.json()` does) must now
    /// allocate no more than a small multiple of the body's own size.
    /// Measured on this synthetic ~5MB feed: the old `Value`-based parse
    /// peaked at ~21x the body size; this one at ~1.8x (almost all of it
    /// the growing serialized POST body).
    #[test]
    fn parsing_and_serializing_a_large_feed_stays_near_the_body_size() {
        let station = |i: usize| {
            serde_json::json!({
                "crsCode": format!("C{i:02}"),
                "name": format!("Station {i}"),
                "location": { "latitude": 51.5, "longitude": -0.1 },
                "stationOperator": { "operatorCode": "NR" },
                "slug": format!("station-{i}"),
                "stationFacilities": {
                    "items": (0..60)
                        .map(|n| serde_json::json!({ "id": n, "available": n % 2 == 0, "note": "x" }))
                        .collect::<Vec<_>>()
                },
            })
        };
        let feed = serde_json::to_vec(&serde_json::json!({
            "stations": (0..2_000).map(station).collect::<Vec<_>>()
        }))
        .unwrap();
        assert!(feed.len() > 4_000_000, "feed is {} bytes", feed.len());

        let (posted_len, peak) = crate::alloc_meter::peak_during(|| {
            let stations = parse_stations(&feed).unwrap();
            assert_eq!(stations.len(), 2_000);
            serde_json::to_vec(&stations).unwrap().len()
        });

        assert!(posted_len > feed.len() / 2);
        assert!(
            peak < feed.len() * 3,
            "parse + serialize peaked at {peak} bytes above baseline for a {}-byte feed",
            feed.len()
        );
    }

    #[test]
    fn one_malformed_station_is_skipped_not_the_whole_batch() {
        // The real bug: deserializing the whole envelope as one
        // `Vec<RdmStation>` in a single call meant one malformed station
        // entry (here, missing the required `crsCode`) failed the ENTIRE
        // batch, silently stopping every OTHER station in the response
        // from updating too.
        let json = r#"
            {
                "stations": [
                    { "crsCode": "EUS", "name": "London Euston" },
                    { "name": "Missing CRS Code Station" },
                    { "crsCode": "ABC", "name": "A Test Station" }
                ]
            }
        "#;

        let stations = parse_stations(json.as_bytes())
            .expect("one malformed station must not fail the whole batch parse");
        assert_eq!(
            stations.len(),
            2,
            "both well-formed stations must survive; only the malformed one is skipped"
        );
        let codes: Vec<&str> = stations.iter().map(|s| s.crs.as_str()).collect();
        assert_eq!(codes, vec!["EUS", "ABC"]);
    }

    #[test]
    fn genuinely_invalid_json_still_fails_the_whole_parse() {
        assert!(parse_stations(b"not json at all").is_err());
    }
}
