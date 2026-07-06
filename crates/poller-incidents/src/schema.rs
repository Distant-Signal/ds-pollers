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
//!   unparsed here (a separate, already-flagged DESIGN.md gap).
//! - `IncidentPriority` has no documented value table, so it is carried as
//!   a raw integer with zero reinterpretation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{IncidentMessage, ValidityPeriod};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Incidents {
    #[serde(default, rename = "PtIncident")]
    pub pt_incident: Vec<PtIncident>,
}

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
pub fn parse_incidents(xml: &str) -> Result<Vec<IncidentMessage>> {
    let incidents: Incidents = quick_xml::de::from_str(xml)?;
    Ok(incidents
        .pt_incident
        .iter()
        .map(IncidentMessage::from)
        .collect())
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
}
