//! `api.irishrail.ie` legacy realtime XML schema and its mapping to
//! `common::island_of_ireland::{IslandOfIrelandDeparture, IslandOfIrelandStationSample}`.
//!
//! Field names were originally transcribed from the friction doc's own
//! live-fetch results (docs/superpowers/specs/2026-09-05-ireland-vs-northern-ireland-friction-research.md
//! sections 1 and 4), then **re-confirmed against a real, live fetch of
//! both endpoints during this crate's own implementation**
//! (`getAllStationsXML` and `getStationDataByCodeXML?StationCode=BFSTC`/`CNLLY`,
//! 2026-09-05): every field name below (`Traincode`, `Origin`,
//! `Destination`, `Scharrival`, `Schdepart`, `Exparrival`, `Expdepart`,
//! `Late`, `Status`, `Duein`, `StationCode`, `StationDesc`) matches the
//! real response body byte-for-byte. Two things the friction doc didn't
//! call out, discovered from that live fetch:
//! - The real response wraps every field in a default XML namespace
//!   (`xmlns="http://api.irishrail.ie/realtime/"` on the root element).
//!   `quick_xml::de` is not namespace-aware in this configuration and
//!   matches on local element name only, so this needs no code-level
//!   handling -- confirmed by the round-trip tests below, which embed
//!   real captured response bodies (namespace declarations included), not
//!   synthetic XML.
//! - `Traincode`'s value carries a trailing space in the real feed (e.g.
//!   `"A912 "`) -- `From<&ObjStationData>` trims it.
//! - The real response has many more fields than this crate models
//!   (`Servertime`, `Stationfullname`, `Stationcode`, `Querytime`,
//!   `Traindate`, `Origintime`, `Destinationtime`, `Lastlocation`,
//!   `Direction`, `Traintype`, `Locationtype`) -- left unmapped, and
//!   harmlessly ignored by `quick_xml::de` (no `deny_unknown_fields` is
//!   set anywhere in this module).
//! - `Late` is genuinely signed in the wild (a real live poll during this
//!   crate's implementation observed `Late = -2` for an Enterprise service
//!   running early), confirming `i32`, not `u32`.
//! - A `StationCode` for a real station with no scheduled services right
//!   now (e.g. this session's own `getStationDataByCodeXML?StationCode=DCNLL`
//!   probe against a station code that turned out not to exist) comes back
//!   as a self-closing empty root element
//!   (`<ArrayOfObjStationData .../>`), not an error -- `#[serde(default)]`
//!   on `station_data` handles this as zero departures, matching the
//!   public route's own "empty is a legitimate result" honesty split
//!   (`crates/api/src/routes/island_of_ireland.rs::get_station_departures`).
//!
//! GAP: `api.irishrail.ie`'s own real `StationCode` for Dublin Connolly is
//! `CNLLY`, not `DCNLL` -- this crate's own live-fetch pass during
//! implementation is what discovered that (a guessed code, never used in
//! any shipped config default here).

use anyhow::Result;
use common::island_of_ireland::{
    IslandOfIrelandDeparture, IslandOfIrelandNetwork, IslandOfIrelandStationSample,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ArrayOfObjStationData {
    #[serde(default, rename = "objStationData")]
    pub station_data: Vec<ObjStationData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObjStationData {
    pub traincode: String,
    pub origin: String,
    pub destination: String,
    #[serde(default)]
    pub scharrival: Option<String>,
    #[serde(default)]
    pub schdepart: Option<String>,
    #[serde(default)]
    pub exparrival: Option<String>,
    #[serde(default)]
    pub expdepart: Option<String>,
    pub late: i32,
    pub status: String,
    #[serde(default)]
    pub duein: Option<String>,
}

impl From<&ObjStationData> for IslandOfIrelandDeparture {
    fn from(d: &ObjStationData) -> Self {
        IslandOfIrelandDeparture {
            // The real feed pads `Traincode` with a trailing space (e.g.
            // "A912 ") -- trimmed here, not carried into stored/served
            // data verbatim.
            train_code: d.traincode.trim().to_string(),
            origin: d.origin.clone(),
            destination: d.destination.clone(),
            scheduled_arrival: d.scharrival.clone(),
            scheduled_departure: d.schdepart.clone(),
            expected_arrival: d.exparrival.clone(),
            expected_departure: d.expdepart.clone(),
            late_minutes: d.late,
            status: d.status.clone(),
            due_in_minutes: d.duein.as_deref().and_then(|s| s.parse().ok()),
        }
    }
}

pub fn parse_station_departures(xml: &str) -> Result<Vec<IslandOfIrelandDeparture>> {
    let response: ArrayOfObjStationData = quick_xml::de::from_str(xml)?;
    Ok(response
        .station_data
        .iter()
        .map(IslandOfIrelandDeparture::from)
        .collect())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ArrayOfObjStation {
    #[serde(default, rename = "objStation")]
    pub station: Vec<ObjStation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObjStation {
    pub station_code: String,
    // Real, confirmed field (`getAllStationsXML`'s own `StationDesc`),
    // kept here for schema-completeness/documentation even though
    // `parse_all_stations` only extracts `station_code` today -- not
    // dead code by accident, just unused.
    #[allow(dead_code)]
    #[serde(default)]
    pub station_desc: Option<String>,
}

pub fn parse_all_stations(xml: &str) -> Result<Vec<String>> {
    let response: ArrayOfObjStation = quick_xml::de::from_str(xml)?;
    Ok(response
        .station
        .into_iter()
        .map(|s| s.station_code)
        .collect())
}

pub fn to_sample(
    station_id: &str,
    departures: Vec<IslandOfIrelandDeparture>,
) -> IslandOfIrelandStationSample {
    IslandOfIrelandStationSample {
        station_id: station_id.to_string(),
        network: IslandOfIrelandNetwork::RepublicOfIreland,
        polled_at: chrono::Utc::now(),
        departures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real captured response body (`getStationDataByCodeXML?StationCode=BFSTC`,
    /// api.irishrail.ie, 2026-09-05, redacted only by trimming to two of
    /// its three real `objStationData` entries) -- not hand-typed sample
    /// XML. Exercises the real default-namespace declaration on the root
    /// element and the real `Lastlocation />` self-closing-empty-element
    /// shape (an unmapped field, so this also confirms unmapped real
    /// fields don't break parsing).
    const REAL_BFSTC_RESPONSE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
        <ArrayOfObjStationData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://api.irishrail.ie/realtime/">
          <objStationData>
            <Servertime>2026-09-05T16:59:57.427</Servertime>
            <Traincode>A120 </Traincode>
            <Stationfullname>Belfast</Stationfullname>
            <Stationcode>BFSTC</Stationcode>
            <Querytime>16:59:57</Querytime>
            <Traindate>05 Sep 2026</Traindate>
            <Origin>Dublin Connolly</Origin>
            <Destination>Belfast</Destination>
            <Origintime>15:50</Origintime>
            <Destinationtime>17:58</Destinationtime>
            <Status>En Route</Status>
            <Lastlocation>Departed Dundalk</Lastlocation>
            <Duein>61</Duein>
            <Late>2</Late>
            <Exparrival>18:00</Exparrival>
            <Expdepart>00:00</Expdepart>
            <Scharrival>17:58</Scharrival>
            <Schdepart>00:00</Schdepart>
            <Direction>Northbound</Direction>
            <Traintype>Train</Traintype>
            <Locationtype>D</Locationtype>
          </objStationData>
          <objStationData>
            <Servertime>2026-09-05T16:59:57.427</Servertime>
            <Traincode>A123 </Traincode>
            <Stationfullname>Belfast</Stationfullname>
            <Stationcode>BFSTC</Stationcode>
            <Querytime>16:59:57</Querytime>
            <Traindate>05 Sep 2026</Traindate>
            <Origin>Belfast</Origin>
            <Destination>Dublin Connolly</Destination>
            <Origintime>17:00</Origintime>
            <Destinationtime>19:15</Destinationtime>
            <Status>No Information</Status>
            <Lastlocation />
            <Duein>1</Duein>
            <Late>0</Late>
            <Exparrival>00:00</Exparrival>
            <Expdepart>17:00</Expdepart>
            <Scharrival>00:00</Scharrival>
            <Schdepart>17:00</Schdepart>
            <Direction>Southbound</Direction>
            <Traintype>Train</Traintype>
            <Locationtype>O</Locationtype>
          </objStationData>
        </ArrayOfObjStationData>"#;

    #[test]
    fn parses_a_real_captured_response_and_maps_every_field() {
        let departures =
            parse_station_departures(REAL_BFSTC_RESPONSE).expect("real response must parse");
        assert_eq!(departures.len(), 2);

        // Trailing space in the real feed's `Traincode` value is trimmed.
        assert_eq!(departures[0].train_code, "A120");
        assert_eq!(departures[0].origin, "Dublin Connolly");
        assert_eq!(departures[0].destination, "Belfast");
        assert_eq!(departures[0].scheduled_arrival, Some("17:58".to_string()));
        assert_eq!(departures[0].late_minutes, 2);
        assert_eq!(departures[0].status, "En Route");
        assert_eq!(departures[0].due_in_minutes, Some(61));

        assert_eq!(departures[1].train_code, "A123");
        assert_eq!(departures[1].status, "No Information");
    }

    /// A real captured response for an Enterprise service running early
    /// (`getStationDataByCodeXML?StationCode=CNLLY`, api.irishrail.ie,
    /// 2026-09-05) -- confirms `Late` is genuinely signed in the wild, not
    /// just theoretically per the friction doc.
    const REAL_NEGATIVE_LATE_RESPONSE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
        <ArrayOfObjStationData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://api.irishrail.ie/realtime/">
          <objStationData>
            <Servertime>2026-09-05T16:59:55.687</Servertime>
            <Traincode>A119 </Traincode>
            <Stationfullname>Dublin Connolly</Stationfullname>
            <Stationcode>CNLLY</Stationcode>
            <Querytime>16:59:55</Querytime>
            <Traindate>05 Sep 2026</Traindate>
            <Origin>Belfast</Origin>
            <Destination>Dublin Connolly</Destination>
            <Origintime>15:00</Origintime>
            <Destinationtime>17:15</Destinationtime>
            <Status>En Route</Status>
            <Lastlocation>Departed Portmarnock</Lastlocation>
            <Duein>14</Duein>
            <Late>-2</Late>
            <Exparrival>17:13</Exparrival>
            <Expdepart>00:00</Expdepart>
            <Scharrival>17:15</Scharrival>
            <Schdepart>00:00</Schdepart>
            <Direction>Southbound</Direction>
            <Traintype>Train</Traintype>
            <Locationtype>D</Locationtype>
          </objStationData>
        </ArrayOfObjStationData>"#;

    #[test]
    fn late_minutes_can_be_negative_in_a_real_response() {
        let departures = parse_station_departures(REAL_NEGATIVE_LATE_RESPONSE)
            .expect("real response must parse");
        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].late_minutes, -2);
        assert_eq!(departures[0].train_code, "A119");
    }

    /// A real captured response for a `StationCode` with no scheduled
    /// services matching right now (a self-closing empty root element,
    /// not an error) -- confirms this parses to zero departures rather
    /// than failing.
    const REAL_EMPTY_RESPONSE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
        <ArrayOfObjStationData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://api.irishrail.ie/realtime/" />"#;

    #[test]
    fn a_real_empty_response_parses_to_zero_departures_not_an_error() {
        let departures =
            parse_station_departures(REAL_EMPTY_RESPONSE).expect("empty response must parse");
        assert!(departures.is_empty());
    }

    /// A real captured excerpt of `getAllStationsXML` (api.irishrail.ie,
    /// 2026-09-05) -- includes the real field order (`StationDesc` before
    /// `StationCode`, unlike this crate's own declared field order, which
    /// doesn't matter to `quick_xml::de`'s by-name matching) and a real
    /// `StationAlias />` self-closing empty element (unmapped).
    const REAL_ALL_STATIONS_EXCERPT: &str = r#"<?xml version="1.0" encoding="utf-8"?>
        <ArrayOfObjStation xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://api.irishrail.ie/realtime/">
          <objStation>
            <StationDesc>Belfast</StationDesc>
            <StationAlias />
            <StationLatitude>54.6123</StationLatitude>
            <StationLongitude>-5.91744</StationLongitude>
            <StationCode>BFSTC</StationCode>
            <StationId>228</StationId>
          </objStation>
          <objStation>
            <StationDesc>Dublin Connolly</StationDesc>
            <StationAlias>Connolly</StationAlias>
            <StationLatitude>53.3531</StationLatitude>
            <StationLongitude>-6.24591</StationLongitude>
            <StationCode>CNLLY</StationCode>
            <StationId>100</StationId>
          </objStation>
        </ArrayOfObjStation>"#;

    #[test]
    fn parses_a_real_captured_all_stations_excerpt_into_a_code_list() {
        let codes = parse_all_stations(REAL_ALL_STATIONS_EXCERPT)
            .expect("real all-stations excerpt must parse");
        assert_eq!(codes, vec!["BFSTC".to_string(), "CNLLY".to_string()]);
    }
}
