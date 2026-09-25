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
///
/// Deliberately does NOT deserialize the whole document as one
/// strongly-typed `Vec<TrainOperatingCompany>` in a single
/// `quick_xml::de::from_str` call: one malformed `<TrainOperatingCompany>`
/// anywhere in the list (a missing required field, ...) would fail that
/// whole call, silently stopping every OTHER operator in the response from
/// updating too. Instead `parse_repeated_elements` isolates each
/// `<TrainOperatingCompany>` element and deserializes it independently,
/// skipping (and logging) just the malformed ones -- mirroring the
/// per-station isolation `poller-ldbws` already does for its own batch of
/// stations.
pub fn parse_tocs(xml: &str) -> Result<Vec<TocReference>> {
    let tocs: Vec<TrainOperatingCompany> = parse_repeated_elements(xml, "TrainOperatingCompany")?;
    Ok(tocs.iter().map(TocReference::from).collect())
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
/// consumes its matching end tag) and re-parses that standalone fragment
/// with `quick_xml::de::from_str::<T>`. This only fails the WHOLE parse if
/// the document isn't well-formed XML at all (a genuinely unrecoverable
/// input, same as before); a single element that's well-formed XML but
/// doesn't match `T`'s shape is skipped on its own.
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

    #[test]
    fn one_malformed_operator_is_skipped_not_the_whole_batch() {
        // The real bug: deserializing the whole `TrainOperatingCompanyList`
        // as one `Vec<TrainOperatingCompany>` in a single call meant one
        // malformed `<TrainOperatingCompany>` (here, missing the required
        // `<LegalName>`) failed the ENTIRE batch, silently stopping every
        // OTHER operator in the response from updating too.
        let xml = r#"
            <TrainOperatingCompanyList>
                <TrainOperatingCompany>
                    <AtocCode>LE</AtocCode>
                    <Name>Greater Anglia</Name>
                    <LegalName>London Eastern Railways</LegalName>
                </TrainOperatingCompany>
                <TrainOperatingCompany>
                    <AtocCode>BAD</AtocCode>
                    <Name>Missing Legal Name Operator</Name>
                </TrainOperatingCompany>
                <TrainOperatingCompany>
                    <AtocCode>GW</AtocCode>
                    <Name>Great Western Railway</Name>
                    <LegalName>Great Western Railway</LegalName>
                </TrainOperatingCompany>
            </TrainOperatingCompanyList>
        "#;

        let tocs =
            parse_tocs(xml).expect("one malformed operator must not fail the whole batch parse");
        assert_eq!(
            tocs.len(),
            2,
            "both well-formed operators must survive; only the malformed one is skipped"
        );
        let codes: Vec<&str> = tocs.iter().map(|t| t.atoc_code.as_str()).collect();
        assert_eq!(codes, vec!["LE", "GW"]);
    }

    #[test]
    fn genuinely_malformed_xml_still_fails_the_whole_parse() {
        let xml = r#"
            <TrainOperatingCompanyList>
                <TrainOperatingCompany>
                    <AtocCode>UNCLOSED</AtocCode>
            </TrainOperatingCompanyList>
        "#;
        assert!(parse_tocs(xml).is_err());
    }
}
