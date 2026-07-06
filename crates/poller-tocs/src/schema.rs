//! RDM Train Operating Company List XML schema
//! (`TrainOperatingCompanyList` -> `TrainOperatingCompany[]`), per RSPS5050
//! P-03-00 Rev A, §3, and its mapping to `common::TocReference`.
//!
//! Field names below are transcribed verbatim from the spec (see
//! `.superpowers/sdd/task-5-brief.md`), not invented. Only the fields
//! `common::TocReference` actually consumes are modeled here —
//! `ManagingDirector`, `Logo`, `NetworkMap`, `CompanyWebsite`, and any
//! contact-detail structures are present in the real schema but
//! deliberately left unparsed since nothing downstream needs them.

use anyhow::Result;
use common::TocReference;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrainOperatingCompanyList {
    #[serde(default, rename = "TrainOperatingCompany")]
    pub train_operating_company: Vec<TrainOperatingCompany>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrainOperatingCompany {
    pub atoc_code: String,
    pub name: String,
    pub legal_name: String,
    #[serde(default)]
    pub atoc_member: Option<bool>,
    #[serde(default)]
    pub station_operator: Option<bool>,
}

impl From<&TrainOperatingCompany> for TocReference {
    fn from(toc: &TrainOperatingCompany) -> Self {
        TocReference {
            atoc_code: toc.atoc_code.clone(),
            name: toc.name.clone(),
            legal_name: toc.legal_name.clone(),
            atoc_member: toc.atoc_member,
            station_operator: toc.station_operator,
        }
    }
}

/// Parse a full RDM `TrainOperatingCompanyList` XML document body into
/// `TocReference`s.
pub fn parse_tocs(xml: &str) -> Result<Vec<TocReference>> {
    let list: TrainOperatingCompanyList = quick_xml::de::from_str(xml)?;
    Ok(list
        .train_operating_company
        .iter()
        .map(TocReference::from)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-written sample using the spec's own example values.
    const SAMPLE_XML: &str = r#"
        <TrainOperatingCompanyList>
            <TrainOperatingCompany>
                <AtocCode>LE</AtocCode>
                <Name>Greater Anglia</Name>
                <LegalName>London Eastern Railways</LegalName>
                <AtocMember>true</AtocMember>
                <StationOperator>true</StationOperator>
            </TrainOperatingCompany>
        </TrainOperatingCompanyList>
    "#;

    #[test]
    fn parses_sample_toc_and_maps_every_field() {
        let tocs = parse_tocs(SAMPLE_XML).expect("sample XML should parse");
        assert_eq!(tocs.len(), 1);
        let toc = &tocs[0];

        assert_eq!(toc.atoc_code, "LE");
        assert_eq!(toc.name, "Greater Anglia");
        assert_eq!(toc.legal_name, "London Eastern Railways");
        assert_eq!(toc.atoc_member, Some(true));
        assert_eq!(toc.station_operator, Some(true));
    }

    #[test]
    fn missing_boolean_fields_default_to_none() {
        let xml = r#"
            <TrainOperatingCompanyList>
                <TrainOperatingCompany>
                    <AtocCode>GW</AtocCode>
                    <Name>Great Western Railway</Name>
                    <LegalName>Great Western Railway</LegalName>
                </TrainOperatingCompany>
            </TrainOperatingCompanyList>
        "#;

        let tocs = parse_tocs(xml).expect("sample XML should parse");
        assert_eq!(tocs.len(), 1);
        assert_eq!(tocs[0].atoc_member, None);
        assert_eq!(tocs[0].station_operator, None);
    }
}
