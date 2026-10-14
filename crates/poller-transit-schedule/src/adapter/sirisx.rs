//! The SIRI-SX adapter (transit spec §10.1): the BODS bulk archive (a zip
//! holding one `sirisx.xml` of every operator's situations, about 5.6 MB)
//! streamed with `quick-xml`, filtered to the network's situations and
//! normalised to [`TransitSituation`]s.
//!
//! A situation is the network's when its `ParticipantRef` is one of the
//! network file's `participant_refs` (`TfGM`) **and** one of its
//! consequences names a `VehicleMode` in `vehicle_modes` (`tram`), a stop
//! with the network's ATCO prefix, or an `OperatorRef` in `operator_refs`.
//! (South Yorkshire's Supertram situations have another participant.)
//!
//! Normalising: `LineRef`s map to line ids through the lines'
//! `siri_line_refs`, and an unmapped one is kept raw and counted; stop
//! refs fold to their station (`9400ZZMAPIC1` to `9400ZZMAPIC`); text is
//! whitespace-collapsed and capped. The set is sorted by situation id.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Read, Seek};

use chrono::{DateTime, Utc};
use common::transit::{Period, SituationScope, TransitConsequence, TransitSituation, is_stop_id};
use quick_xml::events::Event;

use super::{CappedReader, FeedError};
use crate::network::NetworkConfig;

/// The XML entry's inflated-size cap (5.6 MB on 2026-10-09).
pub(crate) const MAX_XML_BYTES: u64 = 256 * 1024 * 1024;
/// The longest summary, description or advice kept (characters).
const MAX_TEXT: usize = 4000;
/// The length of a station's ATCO code (`9400ZZMAPIC`); a platform adds
/// digits.
const STATION_ATCO_LEN: usize = 11;

/// One `PtSituationElement` as read, before filtering.
#[derive(Debug, Default)]
struct Raw {
    participant: String,
    number: String,
    version: Option<i32>,
    progress: String,
    planned: bool,
    reason: Option<String>,
    summary: String,
    description: Option<String>,
    info_url: Option<String>,
    validity: Vec<Period>,
    publication: Vec<Period>,
    consequences: Vec<RawConsequence>,
}

#[derive(Debug, Default)]
struct RawConsequence {
    condition: Option<String>,
    severity: Option<String>,
    all_lines: bool,
    modes: BTreeSet<String>,
    operators: BTreeSet<String>,
    lines: Vec<String>,
    stops: Vec<String>,
    advice: Option<String>,
}

/// What the adapter found.
#[derive(Debug, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "the situations, as the schema calls them"
)]
pub(crate) struct Situations {
    /// The network's situations, sorted by id.
    pub situations: Vec<TransitSituation>,
    /// `LineRef`s with no alias, and how often each occurred.
    pub unmapped_line_refs: BTreeMap<String, u64>,
    /// Every situation in the file.
    pub seen: usize,
}

fn malformed(what: impl std::fmt::Display) -> FeedError {
    FeedError::Malformed(format!("SIRI-SX: {what}"))
}

/// Collapses whitespace and caps the length.
fn clean(text: &str) -> String {
    let mut out: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if out.chars().count() > MAX_TEXT {
        out = out.chars().take(MAX_TEXT).collect();
    }
    out
}

fn time(text: &str) -> Result<DateTime<Utc>, FeedError> {
    DateTime::parse_from_rfc3339(text.trim())
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| malformed(format!("time {text:?}")))
}

/// Reads the archive's XML entry and returns the network's situations.
pub(crate) fn parse_archive<R: Read + Seek>(
    reader: R,
    network: &NetworkConfig,
) -> Result<Situations, FeedError> {
    let mut archive =
        zip::ZipArchive::new(reader).map_err(|err| malformed(format!("zip: {err}")))?;
    let name = (0..archive.len())
        .filter_map(|i| archive.name_for_index(i).map(str::to_owned))
        .find(|n| {
            std::path::Path::new(n)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("xml"))
        })
        .ok_or_else(|| malformed("no .xml entry in the archive"))?;
    let entry = archive
        .by_name(&name)
        .map_err(|err| malformed(format!("{name}: {err}")))?;
    parse_xml(
        std::io::BufReader::new(CappedReader::new(entry, MAX_XML_BYTES, name)),
        network,
    )
}

/// Streams a SIRI-SX document and returns the network's situations.
#[expect(
    clippy::too_many_lines,
    reason = "one event loop over the element paths SIRI-SX uses"
)]
pub(crate) fn parse_xml<R: BufRead>(
    input: R,
    network: &NetworkConfig,
) -> Result<Situations, FeedError> {
    let mut reader = quick_xml::Reader::from_reader(input);
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut text = String::new();
    let mut current: Option<Raw> = None;
    let mut consequence: Option<RawConsequence> = None;
    let mut period: (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = (None, None);
    let mut out = Situations::default();
    let mut raws = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|err| {
            if err.to_string().contains("inflated-size cap") {
                FeedError::Oversize("the SIRI-SX XML (inflated)".to_owned())
            } else {
                malformed(err)
            }
        })?;
        match event {
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                match name.as_str() {
                    "PtSituationElement" => current = Some(Raw::default()),
                    "Consequence" if current.is_some() => {
                        consequence = Some(RawConsequence::default());
                    }
                    "ValidityPeriod" | "PublicationWindow" => period = (None, None),
                    _ => {}
                }
                path.push(name);
                text.clear();
            }
            Event::Empty(e) => {
                if e.local_name().as_ref() == b"AllLines"
                    && let Some(c) = consequence.as_mut()
                {
                    c.all_lines = true;
                }
            }
            Event::Text(t) => text.push_str(&t.decode().map_err(malformed)?),
            Event::CData(t) => text.push_str(&String::from_utf8_lossy(&t)),
            Event::GeneralRef(r) => {
                if let Ok(Some(ch)) = r.resolve_char_ref() {
                    text.push(ch);
                } else {
                    let name = r.decode().map_err(malformed)?;
                    text.push_str(match name.as_ref() {
                        "amp" => "&",
                        "lt" => "<",
                        "gt" => ">",
                        "quot" => "\"",
                        "apos" => "'",
                        _ => "",
                    });
                }
            }
            Event::End(_) => {
                let Some(name) = path.pop() else {
                    return Err(malformed("unbalanced end tag"));
                };
                let parent = path.last().map_or("", String::as_str);
                let value = text.trim().to_owned();
                text.clear();
                if name == "PtSituationElement" {
                    if let Some(raw) = current.take() {
                        out.seen += 1;
                        raws.push(raw);
                    }
                    continue;
                }
                let Some(raw) = current.as_mut() else {
                    continue;
                };
                if name == "Consequence" {
                    if let Some(c) = consequence.take() {
                        raw.consequences.push(c);
                    }
                    continue;
                }
                if let Some(c) = consequence.as_mut() {
                    match name.as_str() {
                        "Condition" => c.condition = Some(value).filter(|v| !v.is_empty()),
                        "Severity" => c.severity = Some(value).filter(|v| !v.is_empty()),
                        "VehicleMode" => {
                            c.modes.insert(value);
                        }
                        "OperatorRef" => {
                            c.operators.insert(value);
                        }
                        "LineRef" => c.lines.push(value),
                        "StopPointRef" => c.stops.push(value),
                        "Details" if path.iter().any(|p| p == "Advice") => {
                            c.advice = Some(clean(&value)).filter(|v| !v.is_empty());
                        }
                        _ => {}
                    }
                    continue;
                }
                match (name.as_str(), parent) {
                    ("ParticipantRef", "PtSituationElement") => raw.participant = value,
                    ("SituationNumber", "PtSituationElement") => raw.number = value,
                    ("Version", "PtSituationElement") => raw.version = value.parse().ok(),
                    ("Progress", "PtSituationElement") => raw.progress = value,
                    ("Planned", "PtSituationElement") => raw.planned = value == "true",
                    ("Summary", "PtSituationElement") => raw.summary = clean(&value),
                    ("Description", "PtSituationElement") => {
                        raw.description = Some(clean(&value)).filter(|v| !v.is_empty());
                    }
                    ("Uri", "InfoLink") if raw.info_url.is_none() => {
                        raw.info_url = Some(value).filter(|v| v.starts_with("https://"));
                    }
                    ("StartTime", "ValidityPeriod" | "PublicationWindow") => {
                        period.0 = Some(time(&value)?);
                    }
                    ("EndTime", "ValidityPeriod" | "PublicationWindow") => {
                        period.1 = Some(time(&value)?);
                    }
                    ("ValidityPeriod" | "PublicationWindow", _) => {
                        if let (Some(start), end) = period {
                            let p = Period { start, end };
                            if name == "ValidityPeriod" {
                                raw.validity.push(p);
                            } else {
                                raw.publication.push(p);
                            }
                        }
                    }
                    (reason, "PtSituationElement") if reason.ends_with("Reason") => {
                        let kind = reason.trim_end_matches("Reason").to_lowercase();
                        raw.reason = Some(format!("{kind}:{value}"));
                    }
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    for raw in raws {
        if let Some(situation) = normalise(raw, network, &mut out.unmapped_line_refs) {
            out.situations.push(situation);
        }
    }
    out.situations
        .sort_by(|a, b| a.situation_id.cmp(&b.situation_id));
    out.situations
        .dedup_by(|a, b| a.situation_id == b.situation_id);
    Ok(out)
}

/// A stop ref as the station's ATCO code, if it is the network's.
fn station_of(stop: &str, prefix: &str) -> Option<String> {
    if !stop.starts_with(prefix) || !is_stop_id(stop) {
        return None;
    }
    Some(
        if stop.len() > STATION_ATCO_LEN
            && stop[STATION_ATCO_LEN..].bytes().all(|b| b.is_ascii_digit())
        {
            stop[..STATION_ATCO_LEN].to_owned()
        } else {
            stop.to_owned()
        },
    )
}

fn normalise(
    raw: Raw,
    network: &NetworkConfig,
    unmapped: &mut BTreeMap<String, u64>,
) -> Option<TransitSituation> {
    let filter = &network.situations;
    if !filter.participant_refs.contains(&raw.participant) || raw.number.is_empty() {
        return None;
    }
    let ours = raw.consequences.iter().any(|c| {
        c.modes.iter().any(|m| filter.vehicle_modes.contains(m))
            || c.stops.iter().any(|s| s.starts_with(&network.atco_prefix))
            || c.operators.iter().any(|o| filter.operator_refs.contains(o))
    });
    if !ours {
        return None;
    }
    let mut line_ids = BTreeSet::new();
    let mut stop_ids = BTreeSet::new();
    let consequences: Vec<TransitConsequence> = raw
        .consequences
        .into_iter()
        .map(|c| {
            let mut lines = BTreeSet::new();
            let mut unmapped_refs = BTreeSet::new();
            for line_ref in &c.lines {
                if let Some(line) = network
                    .lines
                    .iter()
                    .find(|l| l.siri_line_refs.contains(line_ref))
                {
                    lines.insert(line.id.clone());
                } else {
                    *unmapped.entry(line_ref.clone()).or_default() += 1;
                    unmapped_refs.insert(line_ref.clone());
                }
            }
            let stops: BTreeSet<String> = c
                .stops
                .iter()
                .filter_map(|s| station_of(s, &network.atco_prefix))
                .collect();
            line_ids.extend(lines.iter().cloned());
            stop_ids.extend(stops.iter().cloned());
            TransitConsequence {
                condition: c.condition,
                severity: c.severity,
                all_lines: c.all_lines,
                line_ids: lines.into_iter().collect(),
                unmapped_line_refs: unmapped_refs.into_iter().collect(),
                stop_ids: stops.into_iter().collect(),
                advice: c.advice,
            }
        })
        .collect();
    let equipment = raw
        .reason
        .as_deref()
        .is_some_and(|r| r.starts_with("equipment:"));
    let scope = if equipment && !stop_ids.is_empty() {
        SituationScope::Stop
    } else if !line_ids.is_empty()
        || consequences
            .iter()
            .any(|c| c.all_lines && c.stop_ids.is_empty())
    {
        SituationScope::Line
    } else if !stop_ids.is_empty() {
        SituationScope::Stop
    } else {
        SituationScope::Network
    };
    Some(TransitSituation {
        situation_id: raw.number,
        version: raw.version,
        progress: raw.progress,
        planned: raw.planned,
        reason: raw.reason,
        summary: raw.summary,
        description: raw.description,
        info_url: raw.info_url,
        validity: raw.validity,
        publication: raw.publication,
        consequences,
        line_ids: line_ids.into_iter().collect(),
        stop_ids: stop_ids.into_iter().collect(),
        scope,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/sirisx-mini.xml");

    fn metrolink() -> NetworkConfig {
        NetworkConfig::load("metrolink").unwrap()
    }

    fn parse() -> Situations {
        parse_xml(Cursor::new(FIXTURE), &metrolink()).unwrap()
    }

    #[test]
    fn only_the_networks_tram_situations_survive() {
        let out = parse();
        assert_eq!(out.seen, 5);
        let ids: Vec<&str> = out
            .situations
            .iter()
            .map(|s| s.situation_id.as_str())
            .collect();
        // Not Supertram (another participant) nor a TfGM bus situation.
        assert_eq!(ids, ["tfgm-lift", "tfgm-lines", "tfgm-purple"]);
    }

    #[test]
    fn a_lift_failure_is_a_stop_notice_at_its_station() {
        let out = parse();
        let lift = &out.situations[0];
        assert_eq!(lift.reason.as_deref(), Some("equipment:liftFailure"));
        assert_eq!(lift.scope, SituationScope::Stop);
        // A platform code folds to its station.
        assert_eq!(lift.stop_ids, ["9400ZZMAHEA"]);
        assert!(lift.consequences[0].all_lines);
        assert_eq!(lift.validity.len(), 1);
        assert_eq!(lift.validity[0].end, None);
        assert_eq!(
            lift.info_url.as_deref(),
            Some("https://example.invalid/lift")
        );
        assert_eq!(lift.summary, "Heaton Park lift & stairs");
        assert_eq!(
            lift.description.as_deref(),
            Some("The lift is out of service.")
        );
    }

    #[test]
    fn line_refs_map_through_their_aliases_and_unmapped_ones_are_kept() {
        let out = parse();
        let lines = &out.situations[1];
        assert_eq!(lines.scope, SituationScope::Line);
        assert_eq!(lines.line_ids, ["blue", "green", "navy"]);
        assert_eq!(lines.consequences[0].unmapped_line_refs, ["ORANGE"]);
        assert_eq!(out.unmapped_line_refs.get("ORANGE"), Some(&1));
        assert_eq!(
            lines.consequences[0].condition.as_deref(),
            Some("suspended")
        );
        assert_eq!(
            lines.consequences[0].advice.as_deref(),
            Some("Use the buses.")
        );
        assert_eq!(out.situations[2].line_ids, ["purple"]);
        assert!(out.situations[2].planned);
    }

    #[test]
    fn the_output_is_stable_and_an_archive_parses() {
        let a = parse().situations;
        let b = parse().situations;
        assert_eq!(a, b);
        let mut zip_bytes = Vec::new();
        {
            use std::io::Write;
            let mut w = zip::ZipWriter::new(Cursor::new(&mut zip_bytes));
            w.start_file("sirisx.xml", zip::write::FileOptions::<'_, ()>::default())
                .unwrap();
            w.write_all(FIXTURE.as_bytes()).unwrap();
            w.finish().unwrap();
        }
        let out = parse_archive(Cursor::new(zip_bytes), &metrolink()).unwrap();
        assert_eq!(out.situations, a);
    }

    #[test]
    fn stations_fold() {
        assert_eq!(
            station_of("9400ZZMAPIC1", "9400ZZMA").as_deref(),
            Some("9400ZZMAPIC")
        );
        assert_eq!(
            station_of("9400ZZMAPIC", "9400ZZMA").as_deref(),
            Some("9400ZZMAPIC")
        );
        assert_eq!(station_of("9400ZZSYCAT", "9400ZZMA"), None);
        assert_eq!(clean("  a\n  b "), "a b");
    }

    /// A downloaded BODS bulk archive (`TRANSIT_REAL_SIRISX`, never
    /// committed): what survives the filter.
    #[test]
    #[ignore = "needs TRANSIT_REAL_SIRISX, a downloaded BODS SIRI-SX bulk archive"]
    fn the_real_archive() {
        let Ok(path) = std::env::var("TRANSIT_REAL_SIRISX") else {
            return;
        };
        let file = std::fs::File::open(path).unwrap();
        let out = parse_archive(std::io::BufReader::new(file), &metrolink()).unwrap();
        let scopes: Vec<_> = out.situations.iter().map(|s| s.scope).collect();
        eprintln!(
            "{} situations, {} kept, scopes {scopes:?}, unmapped {:?}",
            out.seen,
            out.situations.len(),
            out.unmapped_line_refs
        );
        assert!(out.seen > 0);
    }
}
