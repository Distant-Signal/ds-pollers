//! `TfL` Unified API `GET /Line/Mode/{modes}/Status` JSON, and its mapping to
//! this app's `common::LineStatusReport`.
//!
//! Field names are transcribed from a live response captured on 2026-08-22
//! (see `TRAM_STATUS_JSON` in the tests), not from documentation. Three
//! facts about that payload drive the shape here:
//!
//! - A line carries a **list** of `lineStatuses`, and several can be live
//!   at once (a planned closure plus a live disruption). All of them are
//!   kept; `common::LineStatus` is already a per-status type and
//!   `line_status.statuses` is already an array.
//! - `LineStatus.created` is serialised as `"0001-01-01T00:00:00"` — no
//!   timezone — which `chrono`'s serde impl will not parse into a
//!   `DateTime<Utc>`. It is deliberately not modelled. The line-level
//!   `created`/`modified` and the validity periods are proper RFC 3339
//!   with a `Z`, but are read leniently all the same (no offset is read
//!   as UTC): one entry that fails to parse no longer updates its line.
//! - `TfL`'s `statusSeverity` is its own 0–20 scale, which diverges from this
//!   app's `Severity` above 14 (`TfL` 20 is "Service Closed"; ours is the NR
//!   extension "Recovering"). Every code goes through
//!   `common::severity_from_tfl_code`; nothing is passed through raw.
//!
//! Everything stop-level (`affectedStops`, `affectedRoutes`) is dropped:
//! `TfL` identifies stops by Naptan id (`940GZZLUABC`) and this app's station
//! columns are `CHAR(3)` CRS codes. That is v1's scope line, not an
//! oversight.

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{
    DataQuality, Disruption, LineStatus, LineStatusReport, Severity, TFL_LINE_ID_PREFIX,
    TFL_OPERATOR, ValidityPeriod, severity_from_tfl_code,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TflLine {
    pub id: String,
    pub name: String,
    pub mode_name: String,
    /// When `TfL` last touched this line's record. Used as the `from_date`
    /// for a status that carries no validity period of its own — a stable
    /// timestamp, unlike `Utc::now()`, so an unchanged line does not
    /// produce a fresh `line_status_history` row every 300s. Lenient: an
    /// unreadable one is `None` (the fallback is then `now`), not a reason
    /// to drop the line.
    #[serde(default, deserialize_with = "lenient_time")]
    pub modified: Option<DateTime<Utc>>,
    #[serde(default)]
    pub line_statuses: Vec<TflLineStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TflLineStatus {
    pub status_severity: u8,
    #[serde(default)]
    pub status_severity_description: String,
    /// Absent on a healthy line — `TfL` sends no prose for Good Service.
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub validity_periods: Vec<TflValidityPeriod>,
    #[serde(default)]
    pub disruption: Option<TflDisruption>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TflValidityPeriod {
    #[serde(deserialize_with = "tfl_time")]
    pub from_date: DateTime<Utc>,
    #[serde(default, deserialize_with = "optional_tfl_time")]
    pub to_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub is_now: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TflDisruption {
    /// `"RealTime"` | `"PlannedWork"` | `"Information"` in every observed
    /// response; `Option` only so a missing one cannot fail the whole poll.
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// A `TfL` timestamp: RFC 3339, or (as `TfL` already sends
/// `LineStatus.created`) a date and time with no offset, read as UTC (the
/// .NET `DateTime` `TfL` serialises without its kind is UTC everywhere it
/// does carry a `Z`).
fn parse_tfl_time(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(time) = DateTime::parse_from_rfc3339(text) {
        return Some(time.with_timezone(&Utc));
    }
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|naive| naive.and_utc())
}

fn tfl_time<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    parse_tfl_time(&text)
        .ok_or_else(|| serde::de::Error::custom(format!("not a TfL timestamp: {text:?}")))
}

fn optional_tfl_time<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|text| {
            parse_tfl_time(&text)
                .ok_or_else(|| serde::de::Error::custom(format!("not a TfL timestamp: {text:?}")))
        })
        .transpose()
}

/// [`optional_tfl_time`], with anything unreadable as `None`.
fn lenient_time<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(value
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .and_then(parse_tfl_time))
}

/// A parsed `/Line/Mode/{modes}/Status` response: the lines that parsed,
/// and the entries that did not.
#[derive(Debug)]
pub(crate) struct ParsedStatus {
    pub reports: Vec<LineStatusReport>,
    /// Each malformed entry's line id (prefixed, as a report's), `None`
    /// when even that could not be read.
    pub skipped: Vec<Option<String>>,
}

/// What [`complete_snapshot`] did with the malformed entries.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Completed {
    /// Lines whose last good report stood in for a malformed entry.
    pub carried_forward: Vec<String>,
}

/// The whole snapshot to publish from `parsed`, with `last` (each line's
/// last good report) updated from it.
///
/// The writer prunes every `TfL` line missing from the snapshot it applies,
/// so a malformed entry simply left out would delete that line's live
/// status every cycle. Instead its line's last good report stands in for it
/// (the line keeps the status it last had, and every other line still
/// updates). When there is none (a fresh pod, or an entry with no readable
/// id) the snapshot is refused: `Err` names the entries, and nothing is
/// published, so nothing is pruned.
pub(crate) fn complete_snapshot(
    parsed: ParsedStatus,
    last: &mut std::collections::HashMap<String, LineStatusReport>,
) -> Result<(Vec<LineStatusReport>, Completed)> {
    let ParsedStatus {
        mut reports,
        skipped,
    } = parsed;
    let mut carried_forward = Vec::new();
    let mut missing = Vec::new();
    for id in skipped {
        // A malformed repeat of a line that did parse leaves nothing out.
        if id
            .as_ref()
            .is_some_and(|id| reports.iter().any(|r| &r.id == id))
        {
            continue;
        }
        match id.as_ref().and_then(|id| last.get(id)) {
            Some(report) => {
                carried_forward.push(report.id.clone());
                reports.push(report.clone());
            }
            None => missing.push(id.unwrap_or_else(|| "(no id)".to_owned())),
        }
    }
    if !missing.is_empty() {
        anyhow::bail!(
            "malformed TfL line status entries with no earlier report to stand in for them: {}; \
             refusing a snapshot that would delete their lines' status",
            missing.join(", ")
        );
    }
    for report in &reports {
        last.insert(report.id.clone(), report.clone());
    }
    Ok((reports, Completed { carried_forward }))
}

/// Parses a whole `/Line/Mode/{modes}/Status` response body.
///
/// `now` is a parameter rather than a `Utc::now()` call so the mapping is
/// deterministic under test — the same convention
/// `common::ingest::duration_until_next_poll` and the aggregator's
/// `next_rail_day_boundary` use.
///
/// Deliberately does NOT deserialize the whole response as one
/// strongly-typed `Vec<TflLine>` in a single `serde_json::from_str` call:
/// one malformed line entry anywhere in the array (an unexpected type for
/// `statusSeverity`, a missing required field, ...) would fail that whole
/// call, silently stopping every OTHER line in the response from updating
/// too. Instead the array is first parsed loosely as `Vec<serde_json::Value>`
/// and each element is deserialized into `TflLine` independently, skipping
/// (and logging) just the malformed ones -- mirroring the per-station
/// isolation `poller-ldbws` already does for its own batch of stations. A
/// genuinely invalid JSON document (not just one bad element) still fails
/// outright at the outer `Vec<serde_json::Value>` parse -- that failure is
/// not recoverable per-element.
///
/// The skipped entries are returned too: the snapshot is whole (the writer
/// prunes lines missing from it), so [`complete_snapshot`] must account for
/// them before anything is published.
pub(crate) fn parse_line_status(json: &str, now: DateTime<Utc>) -> Result<ParsedStatus> {
    let raw_lines: Vec<serde_json::Value> = serde_json::from_str(json)?;
    let mut reports = Vec::with_capacity(raw_lines.len());
    let mut skipped = Vec::new();
    for value in raw_lines {
        match TflLine::deserialize(&value) {
            Ok(line) => reports.push(to_report(&line, now)),
            Err(err) => {
                let id = value
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(|id| format!("{TFL_LINE_ID_PREFIX}{id}"));
                tracing::warn!(
                    error = %err,
                    line = ?value,
                    "malformed TfL line status entry; the other lines still update"
                );
                skipped.push(id);
            }
        }
    }
    Ok(ParsedStatus { reports, skipped })
}

fn to_report(line: &TflLine, now: DateTime<Utc>) -> LineStatusReport {
    let id = format!("{TFL_LINE_ID_PREFIX}{}", line.id);
    let fallback = line.modified.unwrap_or(now);
    LineStatusReport {
        id: id.clone(),
        name: line.name.clone(),
        mode_name: line.mode_name.clone(),
        // Hardcoded, not derived: TfL has no per-line operator code, and
        // every mode in scope is run by the same body.
        operators: vec![TFL_OPERATOR.to_string()],
        statuses: line
            .line_statuses
            .iter()
            .map(|status| map_status(status, now, fallback, &id))
            .collect(),
    }
}

fn map_status(
    status: &TflLineStatus,
    now: DateTime<Utc>,
    fallback: DateTime<Utc>,
    line_id: &str,
) -> LineStatus {
    let severity = severity_from_tfl_code(status.status_severity).unwrap_or_else(|| {
        tracing::warn!(
            line_id,
            code = status.status_severity,
            description = %status.status_severity_description,
            "unknown TfL statusSeverity code; recording it as Information rather than \
             guessing a severity or dropping the status"
        );
        Severity::Information
    });

    LineStatus {
        severity,
        reason: reason_text(status),
        validity: select_validity(&status.validity_periods, now, fallback),
        disruption: status.disruption.as_ref().map(|disruption| Disruption {
            category: disruption.category.clone().unwrap_or_default(),
            description: disruption.description.clone().unwrap_or_default(),
            // Naptan ids and TfL route objects have nowhere to go in a
            // CRS-shaped schema — see this module's doc comment.
            affected_stops: vec![],
            affected_routes: vec![],
            source: Some(format!("tfl-line-status-{line_id}")),
            impact_type: None, // TfL never runs the enricher's extraction pipeline
        }),
        data_quality: DataQuality::Tfl,
        // LDBWS-derived delay/cancellation counts. There is no TfL
        // equivalent and v1 does not sample TfL arrivals.
        sample_stats: None,
        sample_availability: common::SampleAvailability::NoCoverage,
        // No full-coverage (TRUST-vs-schedule) producer targets TfL modes
        // -- out of scope per
        // docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md's
        // own scoping (Decision 5: "Case 1 [TfL-quality lines] is
        // completely outside this document's scope").
        full_coverage_stats: None,
        full_coverage_availability: common::FullCoverageAvailability::NotEnabled,
    }
}

/// `TfL` omits `reason` entirely on a healthy line, and for a severity code
/// this app has never seen the description is the only human-readable
/// signal there is — so the description is the fallback, and the result is
/// never an empty string.
fn reason_text(status: &TflLineStatus) -> String {
    match status.reason.as_deref().map(str::trim) {
        Some(reason) if !reason.is_empty() => reason.to_string(),
        _ => status.status_severity_description.clone(),
    }
}

fn period_covers_now(period: &TflValidityPeriod, now: DateTime<Utc>) -> bool {
    period.from_date <= now && period.to_date.is_none_or(|to| to >= now)
}

/// Collapses `TfL`'s `validityPeriods[]` to the single `ValidityPeriod` that
/// `common::LineStatus` stores, preferring the period that is actually in
/// force: `TfL`'s own `isNow`, else one whose window contains `now`, else the
/// earliest on record. With no periods at all it synthesises one starting
/// at `fallback` (the line's `modified` timestamp) and open-ended.
///
/// The returned `is_now` is recomputed rather than copied when the dates
/// say otherwise, so a status that is in force cannot arrive at the
/// frontend flagged `isNow: false` — the exact bug that made the National
/// Rail issue list bucket in-progress works as neither Active nor Upcoming.
pub(crate) fn select_validity(
    periods: &[TflValidityPeriod],
    now: DateTime<Utc>,
    fallback: DateTime<Utc>,
) -> ValidityPeriod {
    let chosen = periods
        .iter()
        .find(|period| period.is_now)
        .or_else(|| periods.iter().find(|period| period_covers_now(period, now)))
        .or_else(|| periods.iter().min_by_key(|period| period.from_date));

    match chosen {
        Some(period) => ValidityPeriod {
            from_date: period.from_date,
            to_date: period.to_date,
            is_now: period.is_now || period_covers_now(period, now),
        },
        None => ValidityPeriod {
            from_date: fallback,
            to_date: None,
            is_now: true,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `GET /Line/Mode/tram/Status` response, captured 2026-08-22
    /// and trimmed of its `routeSections`/`serviceTypes`/`crowding` tails.
    ///
    /// Note `"created": "0001-01-01T00:00:00"` on the status object: no
    /// timezone, so it is not modelled — a `DateTime<Utc>` field there
    /// would fail the parse of every response `TfL` sends. The `$type`
    /// members are `TfL`'s .NET type tags and are ignored the same way.
    const TRAM_STATUS_JSON: &str = r#"[
      {
        "$type": "Tfl.Api.Presentation.Entities.Line, Tfl.Api.Presentation.Entities",
        "id": "tram",
        "name": "Tram",
        "modeName": "tram",
        "disruptions": [],
        "created": "2026-08-17T17:06:09.323Z",
        "modified": "2026-08-17T17:06:09.323Z",
        "lineStatuses": [
          {
            "$type": "Tfl.Api.Presentation.Entities.LineStatus, Tfl.Api.Presentation.Entities",
            "id": 0,
            "lineId": "tram",
            "statusSeverity": 20,
            "statusSeverityDescription": "Service Closed",
            "reason": "London Tramlink: Service will resume later this morning.",
            "created": "0001-01-01T00:00:00",
            "validityPeriods": [
              {
                "$type": "Tfl.Api.Presentation.Entities.ValidityPeriod, Tfl.Api.Presentation.Entities",
                "fromDate": "2026-08-22T01:46:28Z",
                "toDate": "2026-08-22T05:05:09Z",
                "isNow": true
              }
            ],
            "disruption": {
              "$type": "Tfl.Api.Presentation.Entities.Disruption, Tfl.Api.Presentation.Entities",
              "category": "RealTime",
              "categoryDescription": "RealTime",
              "description": "London Tramlink: Service will resume later this morning.",
              "affectedRoutes": [],
              "affectedStops": [],
              "closureText": "serviceClosed"
            }
          }
        ]
      }
    ]"#;

    fn now() -> DateTime<Utc> {
        "2026-08-22T03:00:00Z".parse().unwrap()
    }

    #[test]
    fn parses_a_real_response_and_maps_every_field() {
        let reports = parse_line_status(TRAM_STATUS_JSON, now())
            .expect("live capture should parse")
            .reports;
        assert_eq!(reports.len(), 1);
        let report = &reports[0];

        // Namespaced, because TfL's tube line id `northern` is also the id
        // of lines/northern.toml and line_status.line_id is a primary key.
        assert_eq!(report.id, "tfl-tram");
        assert_eq!(report.name, "Tram");
        assert_eq!(report.mode_name, "tram");
        assert_eq!(report.operators, vec!["TfL".to_string()]);
        assert_eq!(report.statuses.len(), 1);

        let status = &report.statuses[0];
        // TfL 20 is "Service Closed"; OUR 20 is the NR extension
        // `Recovering`. This assertion is the regression guard for that.
        assert_eq!(status.severity, Severity::ServiceClosed);
        assert_eq!(
            status.reason,
            "London Tramlink: Service will resume later this morning."
        );
        assert_eq!(
            status.validity.from_date,
            "2026-08-22T01:46:28Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            status.validity.to_date,
            Some("2026-08-22T05:05:09Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert!(status.validity.is_now);
        assert!(matches!(status.data_quality, DataQuality::Tfl));
        assert!(status.sample_stats.is_none());
        assert!(matches!(
            status.sample_availability,
            common::SampleAvailability::NoCoverage
        ));

        let disruption = status
            .disruption
            .as_ref()
            .expect("disruption should be carried through");
        assert_eq!(disruption.category, "RealTime");
        assert_eq!(
            disruption.description,
            "London Tramlink: Service will resume later this morning."
        );
        // v1 is line-status only: TfL's affectedStops are Naptan ids, which
        // this app's CHAR(3) CRS columns cannot hold.
        assert_eq!(disruption.affected_stops, [] as [String; 0]);
        assert!(disruption.affected_routes.is_empty());
        assert_eq!(
            disruption.source.as_deref(),
            Some("tfl-line-status-tfl-tram")
        );
        assert_eq!(disruption.impact_type, None);
    }

    #[test]
    fn keeps_every_simultaneous_status_on_a_line() {
        // TfL routinely reports a planned closure and a live disruption on
        // one line at the same time. Collapsing to "the worst" here would
        // throw away the other one before the frontend's issue list ever
        // sees it.
        let json = r#"[
          {
            "id": "victoria",
            "name": "Victoria",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              {
                "statusSeverity": 4,
                "statusSeverityDescription": "Planned Closure",
                "reason": "No service between Seven Sisters and Walthamstow Central",
                "validityPeriods": [
                  { "fromDate": "2026-08-22T00:00:00Z", "toDate": "2026-08-23T00:00:00Z", "isNow": true }
                ]
              },
              {
                "statusSeverity": 6,
                "statusSeverityDescription": "Severe Delays",
                "reason": "Signal failure at Oxford Circus",
                "validityPeriods": [
                  { "fromDate": "2026-08-22T02:30:00Z", "toDate": null, "isNow": true }
                ]
              }
            ]
          }
        ]"#;

        let reports = parse_line_status(json, now())
            .expect("should parse")
            .reports;
        assert_eq!(reports[0].id, "tfl-victoria");
        assert_eq!(reports[0].statuses.len(), 2);
        assert_eq!(reports[0].statuses[0].severity, Severity::PlannedClosure);
        assert_eq!(reports[0].statuses[1].severity, Severity::SevereDelays);
    }

    #[test]
    fn an_unknown_severity_code_is_recorded_as_information_not_dropped() {
        // Dropping it would leave the line with zero statuses, which the
        // frontend renders as Good Service — a fault reported as "fine" is
        // the worst possible failure mode here. Guessing a severity for a
        // code we have never seen is the second worst.
        let json = r#"[
          {
            "id": "dlr",
            "name": "DLR",
            "modeName": "dlr",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              { "statusSeverity": 99, "statusSeverityDescription": "Partly Marvellous", "validityPeriods": [] }
            ]
          }
        ]"#;

        let reports = parse_line_status(json, now())
            .expect("should parse")
            .reports;
        let status = &reports[0].statuses[0];
        assert_eq!(status.severity, Severity::Information);
        assert_eq!(status.reason, "Partly Marvellous");
    }

    #[test]
    fn a_status_with_no_validity_period_falls_back_to_the_lines_modified_time() {
        // NOT to `now`: a fresh timestamp every cycle would make the
        // statuses JSON differ on every poll, and the api's
        // `tfl_statuses_changed` would then append a history row every
        // 300s for a line that never changed.
        let json = r#"[
          {
            "id": "central",
            "name": "Central",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              { "statusSeverity": 10, "statusSeverityDescription": "Good Service", "validityPeriods": [] }
            ]
          }
        ]"#;

        let reports = parse_line_status(json, now())
            .expect("should parse")
            .reports;
        let validity = &reports[0].statuses[0].validity;
        assert_eq!(
            validity.from_date,
            "2026-08-22T02:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(validity.to_date, None);
        assert!(validity.is_now);
        // With no `reason`, TfL's own description is the only prose there is.
        assert_eq!(reports[0].statuses[0].reason, "Good Service");
    }

    #[test]
    fn select_validity_prefers_the_period_covering_now_over_one_that_has_ended() {
        let ended = TflValidityPeriod {
            from_date: "2026-08-21T00:00:00Z".parse().unwrap(),
            to_date: Some("2026-08-21T06:00:00Z".parse().unwrap()),
            is_now: false,
        };
        let current = TflValidityPeriod {
            from_date: "2026-08-22T02:00:00Z".parse().unwrap(),
            to_date: Some("2026-08-22T06:00:00Z".parse().unwrap()),
            is_now: false,
        };
        let chosen = select_validity(&[ended, current], now(), now());
        assert_eq!(
            chosen.from_date,
            "2026-08-22T02:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        // `isNow` was false on the wire but the window contains `now`, so
        // the stored flag says what it means — the same correction the
        // aggregator's `validity_for_output` makes for incidents.
        assert!(chosen.is_now);
    }

    #[test]
    fn one_malformed_line_is_skipped_not_the_whole_batch() {
        // The real bug: deserializing the whole response as one
        // `Vec<TflLine>` in a single call meant one malformed line entry
        // (here, `statusSeverity` sent as a string instead of a number)
        // failed the ENTIRE batch, silently stopping every OTHER line in
        // the response from updating too.
        let json = r#"[
          {
            "id": "victoria",
            "name": "Victoria",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              { "statusSeverity": 10, "statusSeverityDescription": "Good Service", "validityPeriods": [] }
            ]
          },
          {
            "id": "central",
            "name": "Central",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              { "statusSeverity": "not-a-number", "statusSeverityDescription": "Malformed", "validityPeriods": [] }
            ]
          },
          {
            "id": "circle",
            "name": "Circle",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00Z",
            "lineStatuses": [
              { "statusSeverity": 10, "statusSeverityDescription": "Good Service", "validityPeriods": [] }
            ]
          }
        ]"#;

        let parsed = parse_line_status(json, now())
            .expect("one malformed line entry must not fail the whole batch parse");
        assert_eq!(
            parsed.reports.len(),
            2,
            "both well-formed lines must survive; only the malformed one is skipped"
        );
        let ids: Vec<&str> = parsed.reports.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["tfl-victoria", "tfl-circle"]);
        assert_eq!(parsed.skipped, vec![Some("tfl-central".to_owned())]);

        // The snapshot is whole (the writer prunes what it lacks): with no
        // earlier Central report it is refused rather than published
        // without Central, which would delete Central's live status.
        let mut last = std::collections::HashMap::new();
        let refused = complete_snapshot(parse_line_status(json, now()).unwrap(), &mut last);
        assert!(refused.unwrap_err().to_string().contains("tfl-central"));
        assert!(
            last.is_empty(),
            "nothing remembered from a refused snapshot"
        );

        // With one, Central keeps its last status and the others update.
        let good = json.replace(r#""not-a-number""#, "6");
        let (first, _) =
            complete_snapshot(parse_line_status(&good, now()).unwrap(), &mut last).unwrap();
        assert_eq!(first.len(), 3);
        let (snapshot, completed) =
            complete_snapshot(parse_line_status(json, now()).unwrap(), &mut last).unwrap();
        assert_eq!(completed.carried_forward, vec!["tfl-central".to_owned()]);
        let mut ids: Vec<&str> = snapshot.iter().map(|r| r.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["tfl-central", "tfl-circle", "tfl-victoria"]);
        let central = snapshot.iter().find(|r| r.id == "tfl-central").unwrap();
        assert_eq!(central.statuses[0].severity, Severity::SevereDelays);
    }

    #[test]
    fn an_entry_with_no_readable_id_refuses_the_snapshot() {
        let json = r#"[
          { "id": "victoria", "name": "Victoria", "modeName": "tube", "lineStatuses": [] },
          { "name": "Mystery", "modeName": "tube", "lineStatuses": [] }
        ]"#;
        let parsed = parse_line_status(json, now()).unwrap();
        assert_eq!(parsed.skipped, vec![None]);
        let mut last = std::collections::HashMap::new();
        assert!(complete_snapshot(parsed, &mut last).is_err());
    }

    #[test]
    fn a_malformed_repeat_of_a_parsed_line_leaves_nothing_out() {
        let json = r#"[
          { "id": "victoria", "name": "Victoria", "modeName": "tube", "lineStatuses": [] },
          { "id": "victoria", "name": "Victoria", "lineStatuses": [] }
        ]"#;
        let mut last = std::collections::HashMap::new();
        let (snapshot, completed) =
            complete_snapshot(parse_line_status(json, now()).unwrap(), &mut last).unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(completed.carried_forward, Vec::<String>::new());
    }

    /// `TfL` already sends `LineStatus.created` without an offset; a
    /// validity period or `modified` sent the same way must not lose the
    /// line (which the writer's prune would turn into deleting it).
    #[test]
    fn timestamps_without_an_offset_are_read_as_utc() {
        let json = r#"[
          {
            "id": "jubilee",
            "name": "Jubilee",
            "modeName": "tube",
            "modified": "2026-08-22T02:00:00.123",
            "lineStatuses": [
              {
                "statusSeverity": 9,
                "statusSeverityDescription": "Minor Delays",
                "validityPeriods": [
                  { "fromDate": "2026-08-22T01:00:00", "toDate": "2026-08-22T09:00:00.5", "isNow": true }
                ]
              },
              { "statusSeverity": 10, "statusSeverityDescription": "Good Service", "validityPeriods": [] }
            ]
          }
        ]"#;
        let parsed = parse_line_status(json, now()).unwrap();
        assert_eq!(parsed.skipped, Vec::<Option<String>>::new());
        let report = &parsed.reports[0];
        let validity = &report.statuses[0].validity;
        assert_eq!(
            validity.from_date,
            "2026-08-22T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            validity.to_date,
            Some("2026-08-22T09:00:00.5Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(
            report.statuses[1].validity.from_date,
            "2026-08-22T02:00:00.123Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn an_unreadable_modified_time_falls_back_to_now_not_a_skip() {
        let json = r#"[
          {
            "id": "central",
            "name": "Central",
            "modeName": "tube",
            "modified": "yesterday-ish",
            "lineStatuses": [
              { "statusSeverity": 10, "statusSeverityDescription": "Good Service", "validityPeriods": [] }
            ]
          }
        ]"#;
        let parsed = parse_line_status(json, now()).unwrap();
        assert_eq!(parsed.skipped, Vec::<Option<String>>::new());
        assert_eq!(parsed.reports[0].statuses[0].validity.from_date, now());
    }

    #[test]
    fn an_unreadable_validity_date_is_still_malformed() {
        assert_eq!(parse_tfl_time("not a date"), None);
        let json = r#"[
          {
            "id": "central",
            "name": "Central",
            "modeName": "tube",
            "lineStatuses": [
              { "statusSeverity": 6, "validityPeriods": [ { "fromDate": "soon" } ] }
            ]
          }
        ]"#;
        let parsed = parse_line_status(json, now()).unwrap();
        assert_eq!(parsed.skipped, vec![Some("tfl-central".to_owned())]);
    }

    #[test]
    fn genuinely_invalid_json_still_fails_the_whole_parse() {
        assert!(parse_line_status("not json at all", now()).is_err());
    }

    #[test]
    fn select_validity_falls_back_to_the_earliest_future_period() {
        let later = TflValidityPeriod {
            from_date: "2026-09-01T00:00:00Z".parse().unwrap(),
            to_date: None,
            is_now: false,
        };
        let sooner = TflValidityPeriod {
            from_date: "2026-08-25T00:00:00Z".parse().unwrap(),
            to_date: None,
            is_now: false,
        };
        let chosen = select_validity(&[later, sooner], now(), now());
        assert_eq!(
            chosen.from_date,
            "2026-08-25T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert!(!chosen.is_now);
    }
}
