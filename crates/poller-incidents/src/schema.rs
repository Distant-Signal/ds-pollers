//! RDM Knowledgebase Incidents XML schema (`Incidents` -> `PtIncident[]`),
//! per RSPS5050 P-03-00 Rev A, §10, and its mapping to
//! `common::IncidentMessage`.
//!
//! Field names below are transcribed verbatim from the spec (see
//! `.superpowers/sdd/task-3-brief.md`), not invented. Two spec facts drive
//! the shape here:
//! - `ValidityPeriod` is mandatory *and* repeatable (can occur more than
//!   once), so it's a `Vec`, not a single from/to pair.
//! - There is no structured CRS/station code field anywhere in this schema
//!   — `Affects.RoutesAffected` is free text only, and is deliberately left
//!   unparsed here (a separate, already-flagged DESIGN.md gap). That is why
//!   `affected_stations` below is `vec![]` and must stay that way rather
//!   than being guessed at: line attribution is done downstream, by
//!   `common::matcher` at ingest (`api`'s `upsert_incidents`), over the
//!   prose and the structured operator list — the same matcher the
//!   aggregator runs for live status.
//! - `IncidentPriority` has no documented value table, so it is carried as
//!   a raw integer with zero reinterpretation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{IncidentMessage, ValidityPeriod};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PtIncident {
    pub incident_number: String,
    pub summary: String,
    pub description: String,
    pub planned: bool,
    #[serde(default)]
    pub cleared_incident: bool,
    #[serde(default, rename = "ValidityPeriod")]
    pub validity_period: Vec<ValidityPeriodXml>,
    #[serde(default)]
    pub affects: Option<Affects>,
    pub incident_priority: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ValidityPeriodXml {
    pub start_time: DateTime<Utc>,
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Affects {
    #[serde(default)]
    pub operators: Option<Operators>,
    #[serde(default)]
    pub routes_affected: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Operators {
    #[serde(default, rename = "AffectedOperator")]
    pub affected_operator: Vec<AffectedOperator>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AffectedOperator {
    pub operator_ref: String,
    #[serde(default)]
    pub operator_name: Option<String>,
}

impl From<&PtIncident> for IncidentMessage {
    fn from(incident: &PtIncident) -> Self {
        let operators = incident
            .affects
            .as_ref()
            .and_then(|affects| affects.operators.as_ref())
            .map(|operators| {
                operators
                    .affected_operator
                    .iter()
                    .map(|op| op.operator_ref.clone())
                    .collect()
            })
            .unwrap_or_default();

        let validity = incident
            .validity_period
            .iter()
            .map(|vp| ValidityPeriod {
                from_date: vp.start_time,
                to_date: vp.end_time,
                is_now: vp.end_time.is_none(),
            })
            .collect();

        IncidentMessage {
            incident_id: incident.incident_number.clone(),
            summary: incident.summary.clone(),
            description: incident.description.clone(),
            operators,
            affected_stations: vec![],
            priority: incident.incident_priority,
            validity,
            is_planned: incident.planned,
            is_cleared: incident.cleared_incident,
        }
    }
}

/// Parse a full RDM `Incidents` XML document body into `IncidentMessage`s.
///
/// Deliberately does NOT deserialize the whole `<Incidents>` document as one
/// strongly-typed `Incidents { pt_incident: Vec<PtIncident> }` in a single
/// `quick_xml::de::from_str` call: one malformed `<PtIncident>` anywhere in
/// the batch (a missing required field, an unparseable `<StartTime>`, ...)
/// would fail that whole call, silently stopping every OTHER incident in the
/// response from updating too. Instead `parse_repeated_elements` isolates
/// each `<PtIncident>` element and deserializes it independently, skipping
/// (and logging) just the malformed ones -- mirroring the per-station
/// isolation `poller-ldbws` already does for its own batch of stations.
pub fn parse_incidents(xml: &str) -> Result<Vec<IncidentMessage>> {
    let incidents: Vec<PtIncident> = parse_repeated_elements(xml, "PtIncident")?;
    Ok(incidents.iter().map(IncidentMessage::from).collect())
}

/// Isolates every top-level `<{tag_name}>...</{tag_name}>` element in `xml`
/// and deserializes each one independently into `T`, skipping (and logging
/// a warning for) any element that fails to deserialize on its own, rather
/// than letting one malformed element fail deserialization of the entire
/// document via a single `quick_xml::de::from_str::<Vec<T>>` call.
///
/// Works by driving `quick_xml::Reader` directly: for each `Start`/`Empty`
/// event whose local name matches `tag_name`, it captures the exact raw
/// byte span from that tag's opening `<` through its closing `>` (using
/// `Reader::buffer_position()` before the tag and after `read_to_end`
/// consumes its matching end tag -- `quick_xml`'s own doctest for
/// `buffer_position` confirms this positions land exactly on those
/// boundaries) and re-parses that standalone fragment with
/// `quick_xml::de::from_str::<T>`. This only fails the WHOLE parse if the
/// document isn't well-formed XML at all (a genuinely unrecoverable input,
/// same as before); a single element that's well-formed XML but doesn't
/// match `T`'s shape is skipped on its own.
fn parse_repeated_elements<T: serde::de::DeserializeOwned>(
    xml: &str,
    tag_name: &str,
) -> Result<Vec<T>> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let tag_bytes = tag_name.as_bytes();
    let mut items = Vec::new();

    loop {
        let start_pos = reader.buffer_position() as usize;
        let event = reader.read_event().map_err(|err| {
            anyhow::anyhow!("malformed XML while scanning for <{tag_name}> elements: {err}")
        })?;
        match event {
            Event::Eof => break,
            Event::Empty(e) if e.name().as_ref() == tag_bytes => {
                let end_pos = reader.buffer_position() as usize;
                deserialize_fragment_or_warn(&mut items, &xml[start_pos..end_pos], tag_name);
            }
            Event::Start(e) if e.name().as_ref() == tag_bytes => {
                let end_tag = e.to_end().into_owned();
                if let Err(err) = reader.read_to_end(end_tag.name()) {
                    // A genuinely unbalanced `<{tag_name}>` (no matching
                    // close tag anywhere in the rest of the document) means
                    // the document itself is not well-formed XML -- there
                    // is no reliable fragment boundary to isolate, so this
                    // (unlike a single malformed element) does fail the
                    // whole parse.
                    return Err(anyhow::anyhow!(
                        "malformed XML: unterminated <{tag_name}> element: {err}"
                    ));
                }
                let end_pos = reader.buffer_position() as usize;
                deserialize_fragment_or_warn(&mut items, &xml[start_pos..end_pos], tag_name);
            }
            _ => {}
        }
    }

    Ok(items)
}

fn deserialize_fragment_or_warn<T: serde::de::DeserializeOwned>(
    items: &mut Vec<T>,
    fragment: &str,
    tag_name: &str,
) {
    match quick_xml::de::from_str::<T>(fragment) {
        Ok(item) => items.push(item),
        Err(err) => {
            tracing::warn!(
                tag = tag_name,
                error = %err,
                "skipping malformed <{tag_name}> element rather than failing the whole batch"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written sample using the spec's own example
    /// `IncidentNumber` value, and the documented field names/nesting for
    /// `ValidityPeriod` (repeated) and `Affects.Operators.AffectedOperator[]`.
    const SAMPLE_XML: &str = r#"
        <Incidents>
            <PtIncident>
                <IncidentNumber>8B68D83E08C1415A906022178722BDCB</IncidentNumber>
                <Summary>Signal failure at Reading</Summary>
                <Description>Disruption caused by a signal failure near Reading station.</Description>
                <Planned>false</Planned>
                <ClearedIncident>false</ClearedIncident>
                <ValidityPeriod>
                    <StartTime>2026-07-01T08:00:00Z</StartTime>
                    <EndTime>2026-07-01T12:00:00Z</EndTime>
                </ValidityPeriod>
                <ValidityPeriod>
                    <StartTime>2026-07-02T08:00:00Z</StartTime>
                </ValidityPeriod>
                <Affects>
                    <Operators>
                        <AffectedOperator>
                            <OperatorRef>GW</OperatorRef>
                            <OperatorName>Great Western Railway</OperatorName>
                        </AffectedOperator>
                        <AffectedOperator>
                            <OperatorRef>SW</OperatorRef>
                        </AffectedOperator>
                    </Operators>
                    <RoutesAffected>Reading to Oxford</RoutesAffected>
                </Affects>
                <IncidentPriority>2</IncidentPriority>
            </PtIncident>
        </Incidents>
    "#;

    #[test]
    fn parses_sample_incident_and_maps_every_field() {
        let messages = parse_incidents(SAMPLE_XML).expect("sample XML should parse");
        assert_eq!(messages.len(), 1);
        let message = &messages[0];

        assert_eq!(message.incident_id, "8B68D83E08C1415A906022178722BDCB");
        assert_eq!(message.summary, "Signal failure at Reading");
        assert_eq!(
            message.description,
            "Disruption caused by a signal failure near Reading station."
        );
        assert_eq!(message.operators, vec!["GW".to_string(), "SW".to_string()]);
        assert_eq!(message.affected_stations, Vec::<String>::new());
        assert_eq!(message.priority, 2);
        assert!(!message.is_planned);
        assert!(!message.is_cleared);

        assert_eq!(message.validity.len(), 2);

        let first = &message.validity[0];
        assert_eq!(
            first.from_date,
            DateTime::parse_from_rfc3339("2026-07-01T08:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(
            first.to_date,
            Some(
                DateTime::parse_from_rfc3339("2026-07-01T12:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
        assert!(!first.is_now);

        let second = &message.validity[1];
        assert_eq!(
            second.from_date,
            DateTime::parse_from_rfc3339("2026-07-02T08:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(second.to_date, None);
        assert!(second.is_now);
    }

    #[test]
    fn cleared_incident_absent_defaults_to_false() {
        let xml = r#"
            <Incidents>
                <PtIncident>
                    <IncidentNumber>ABC123</IncidentNumber>
                    <Summary>Summary</Summary>
                    <Description>Description</Description>
                    <Planned>true</Planned>
                    <ValidityPeriod>
                        <StartTime>2026-07-01T08:00:00Z</StartTime>
                    </ValidityPeriod>
                    <IncidentPriority>5</IncidentPriority>
                </PtIncident>
            </Incidents>
        "#;

        let messages = parse_incidents(xml).expect("sample XML should parse");
        assert_eq!(messages.len(), 1);
        assert!(!messages[0].is_cleared);
        assert!(messages[0].is_planned);
        assert_eq!(messages[0].operators, Vec::<String>::new());
    }

    #[test]
    fn one_malformed_incident_is_skipped_not_the_whole_batch() {
        // The real bug: deserializing the whole `<Incidents>` document as
        // one `Vec<PtIncident>` in a single call meant one malformed
        // `<PtIncident>` (here, a required `<IncidentPriority>` that isn't
        // an integer) failed the ENTIRE batch, silently stopping every
        // OTHER incident in the response from updating too.
        let xml = r#"
            <Incidents>
                <PtIncident>
                    <IncidentNumber>GOOD-1</IncidentNumber>
                    <Summary>Signal failure at Reading</Summary>
                    <Description>Disruption caused by a signal failure.</Description>
                    <Planned>false</Planned>
                    <ValidityPeriod>
                        <StartTime>2026-07-01T08:00:00Z</StartTime>
                    </ValidityPeriod>
                    <IncidentPriority>2</IncidentPriority>
                </PtIncident>
                <PtIncident>
                    <IncidentNumber>BAD-1</IncidentNumber>
                    <Summary>Malformed incident</Summary>
                    <Description>This one has a non-integer priority.</Description>
                    <Planned>false</Planned>
                    <ValidityPeriod>
                        <StartTime>2026-07-01T08:00:00Z</StartTime>
                    </ValidityPeriod>
                    <IncidentPriority>not-a-number</IncidentPriority>
                </PtIncident>
                <PtIncident>
                    <IncidentNumber>GOOD-2</IncidentNumber>
                    <Summary>Points failure at Oxford</Summary>
                    <Description>Disruption caused by a points failure.</Description>
                    <Planned>false</Planned>
                    <ValidityPeriod>
                        <StartTime>2026-07-01T08:00:00Z</StartTime>
                    </ValidityPeriod>
                    <IncidentPriority>3</IncidentPriority>
                </PtIncident>
            </Incidents>
        "#;

        let messages = parse_incidents(xml)
            .expect("one malformed incident must not fail the whole batch parse");
        assert_eq!(
            messages.len(),
            2,
            "both well-formed incidents must survive; only the malformed one is skipped"
        );
        let ids: Vec<&str> = messages.iter().map(|m| m.incident_id.as_str()).collect();
        assert_eq!(ids, vec!["GOOD-1", "GOOD-2"]);
    }

    #[test]
    fn genuinely_malformed_xml_still_fails_the_whole_parse() {
        // Not every failure is recoverable -- if the document itself isn't
        // well-formed XML (here, an unterminated `<PtIncident>` with no
        // matching close tag anywhere), there's no reliable fragment
        // boundary to isolate, so this must still surface as an error
        // rather than silently returning a partial/wrong result.
        let xml = r#"
            <Incidents>
                <PtIncident>
                    <IncidentNumber>UNCLOSED</IncidentNumber>
            </Incidents>
        "#;
        assert!(parse_incidents(xml).is_err());
    }
}
