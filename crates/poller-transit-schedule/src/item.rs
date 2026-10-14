//! A timetable as one stream item (transit spec §7.2): every
//! `transit-timetable/1` part, kind by kind, each at most
//! `ingest_stream::budget::TRANSIT_PART_BYTES`, then the
//! `transit-timetable-commit/1` manifest. They are added in order as one
//! latest-snapshot item, so the commit always follows its parts.
//!
//! `feed_version` is `<source feed_version>-<first 12 hex of the content
//! SHA-256>`; the hash covers every row sent, so an unchanged feed has the
//! same version and the writer only records its freshness.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::transit::{
    NetworkId, TimetableCommit, TimetablePartKind, TimetableSource, TransitNetworkRow,
    TransitRailLink, schema,
};
use ingest_stream::budget::TRANSIT_PART_BYTES;
use ingest_stream::snapshot::Snapshot;
use ingest_stream::{Envelope, SchemaId};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::feed_guard;
use crate::model::TransitModel;
use crate::network::NetworkConfig;

/// Trips per part to start with (spec §7.2); halved until a part fits.
const TRIPS_PER_PART: usize = 1500;
/// Rows per part of every other kind, to start with.
const ROWS_PER_PART: usize = 1000;

/// A built item and its manifest.
pub(crate) struct Item {
    pub snapshot: Snapshot,
    pub commit: TimetableCommit,
}

/// A part's body, borrowing its rows: the wire shape of
/// `common::transit::TimetablePart`.
#[derive(Serialize)]
struct PartBody<'a, T> {
    network: &'a NetworkId,
    feed_version: &'a str,
    kind: TimetablePartKind,
    rows: &'a [T],
}

/// What the content hash covers, in a fixed order.
#[derive(Serialize)]
struct Content<'a> {
    network: &'a TransitNetworkRow,
    model_lines: &'a [common::transit::TransitLine],
    stops: &'a [common::transit::TransitStop],
    patterns: &'a [common::transit::TransitPattern],
    trips: &'a [common::transit::TransitTrip],
    service_days: &'a [common::transit::TransitServiceDays],
    rail_links: &'a [TransitRailLink],
}

/// The parts of one item, as they are added.
struct Parts<'a> {
    snapshot: Snapshot,
    counts: BTreeMap<TimetablePartKind, u32>,
    stream: &'a str,
    schema: &'a SchemaId,
    producer: &'a str,
    produced_at: DateTime<Utc>,
    network: &'a NetworkId,
    version: &'a str,
}

impl Parts<'_> {
    /// Adds `rows` as `kind` parts (none if empty), batch
    /// `<produced_at>/<kind>`, and counts them.
    fn add<T: Serialize>(
        &mut self,
        kind: TimetablePartKind,
        rows: &[T],
        rows_per_part: usize,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let before = self.snapshot.parts().len();
        let (network, version) = (self.network, self.version);
        self.snapshot
            .add_capped(
                self.stream,
                self.schema,
                self.producer,
                self.produced_at,
                kind.as_str(),
                rows,
                rows_per_part,
                TRANSIT_PART_BYTES,
                |chunk| {
                    serde_json::value::to_raw_value(&PartBody {
                        network,
                        feed_version: version,
                        kind,
                        rows: chunk,
                    })
                },
            )
            .with_context(|| format!("encoding the {kind} parts"))?;
        let added = u32::try_from(self.snapshot.parts().len() - before)?;
        self.counts.insert(kind, added);
        Ok(())
    }
}

/// The network's own row, from its file.
pub(crate) fn network_row(network: &NetworkConfig) -> TransitNetworkRow {
    TransitNetworkRow {
        name: network.name.clone(),
        mode: network.mode.clone(),
        timezone: network.timezone.clone(),
        attribution: network.attribution.clone(),
    }
}

/// Lower-case hex of the SHA-256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// `<source version>-<hash12>`, the source part reduced to
/// `[A-Za-z0-9_.-]` and 40 characters.
fn feed_version(source: Option<&str>, sha: &str) -> String {
    let source: String = source
        .unwrap_or("unversioned")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        .take(40)
        .collect();
    let source = if source.is_empty() {
        "unversioned".to_owned()
    } else {
        source
    };
    format!("{source}-{}", &sha[..12])
}

/// Builds the item for `model` with `rail_links`, produced at
/// `produced_at` by `producer` for `stream`.
pub(crate) fn build(
    network: &NetworkConfig,
    model: &TransitModel,
    rail_links: &[TransitRailLink],
    stream: &str,
    producer: &str,
    produced_at: DateTime<Utc>,
    url_kind: &str,
) -> Result<Item> {
    let network_id = network.network_id();
    let row = network_row(network);
    let content = serde_json::to_vec(&Content {
        network: &row,
        model_lines: &model.lines,
        stops: &model.stops,
        patterns: &model.patterns,
        trips: &model.trips,
        service_days: &model.service_days,
        rail_links,
    })?;
    let content_sha256 = sha256_hex(&content);
    let version = feed_version(model.source_feed_version.as_deref(), &content_sha256);
    let schema = SchemaId::new(schema::TIMETABLE, 1)?;

    let mut parts = Parts {
        snapshot: Snapshot::new(),
        counts: BTreeMap::new(),
        stream,
        schema: &schema,
        producer,
        produced_at,
        network: &network_id,
        version: &version,
    };
    parts.add(TimetablePartKind::Network, std::slice::from_ref(&row), 1)?;
    parts.add(TimetablePartKind::Lines, &model.lines, ROWS_PER_PART)?;
    parts.add(TimetablePartKind::Stops, &model.stops, ROWS_PER_PART)?;
    parts.add(TimetablePartKind::Patterns, &model.patterns, ROWS_PER_PART)?;
    parts.add(TimetablePartKind::Trips, &model.trips, TRIPS_PER_PART)?;
    parts.add(
        TimetablePartKind::ServiceDays,
        &model.service_days,
        ROWS_PER_PART,
    )?;
    parts.add(TimetablePartKind::RailLinks, rail_links, ROWS_PER_PART)?;
    let Parts {
        mut snapshot,
        counts: parts,
        ..
    } = parts;

    let commit = TimetableCommit {
        network: network_id.clone(),
        feed_version: version.clone(),
        content_sha256,
        parts,
        counts: feed_guard::counts(model),
        horizon: model.horizon,
        source: TimetableSource {
            url_kind: url_kind.to_owned(),
            feed_version: model.source_feed_version.clone(),
            published_at: None,
        },
    };
    let commit_schema = SchemaId::new(schema::TIMETABLE_COMMIT, 1)?;
    let key = format!(
        "{}:{}:1/1",
        schema::TIMETABLE_COMMIT,
        ingest_stream::envelope::format_produced_at(produced_at)
    );
    let entry = Envelope::new(commit_schema, producer, key, produced_at, &commit)?.encode()?;
    snapshot.push(entry, 1);
    Ok(Item { snapshot, commit })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use chrono::NaiveDate;
    use common::transit::{TimetablePart, TimetableRows};

    use super::*;
    use crate::adapter::gtfs::{self, GtfsOptions};

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/bods-nw-metl-mini.zip");

    fn model() -> TransitModel {
        gtfs::parse(
            Cursor::new(FIXTURE),
            &NetworkConfig::load("metrolink").unwrap(),
            &GtfsOptions {
                today: NaiveDate::from_ymd_opt(2026, 10, 19).unwrap(),
                horizon_days: 60,
                max_entry_bytes: gtfs::MAX_ENTRY_BYTES,
            },
        )
        .unwrap()
    }

    fn build_at(model: &TransitModel, at: &str) -> Item {
        build(
            &NetworkConfig::load("metrolink").unwrap(),
            model,
            &crate::rail_links::rail_links("metrolink").unwrap(),
            "ds:ingest:transit-metrolink",
            "poller-transit-schedule/test",
            at.parse().unwrap(),
            "bods-gtfs",
        )
        .unwrap()
    }

    #[test]
    fn the_item_is_capped_parts_then_the_commit() {
        let model = model();
        let item = build_at(&model, "2026-10-19T03:00:00Z");
        let entries = item.snapshot.parts();
        assert!(entries.iter().all(|e| e.body_len <= TRANSIT_PART_BYTES));
        let last = Envelope::decode(&entries.last().unwrap().fields).unwrap();
        assert_eq!(last.schema.name(), "transit-timetable-commit");
        let commit: TimetableCommit = last.payload_as().unwrap();
        assert_eq!(commit, item.commit);
        assert!(commit.feed_version.starts_with("20261014_030249-"));
        assert_eq!(commit.feed_version.len(), "20261014_030249-".len() + 12);
        assert_eq!(commit.counts.trips, 32);
        // Every part decodes as a TimetablePart of this version, and the
        // manifest counts them per kind; their keys are distinct.
        let mut per_kind: BTreeMap<TimetablePartKind, u32> = BTreeMap::new();
        let mut keys = std::collections::BTreeSet::new();
        for entry in &entries[..entries.len() - 1] {
            let envelope = Envelope::decode(&entry.fields).unwrap();
            assert!(keys.insert(envelope.key.clone()), "{}", envelope.key);
            let part: TimetablePart = envelope.payload_as().unwrap();
            assert_eq!(part.feed_version, commit.feed_version);
            *per_kind.entry(part.rows.kind()).or_default() += 1;
            if let TimetableRows::RailLinks(links) = &part.rows {
                assert_eq!(links.len(), 10);
            }
        }
        assert_eq!(per_kind, commit.parts);
        assert_eq!(commit.parts[&TimetablePartKind::Network], 1);
    }

    #[test]
    fn the_same_content_has_the_same_version() {
        let model = model();
        let a = build_at(&model, "2026-10-19T03:00:00Z");
        let b = build_at(&model, "2026-10-20T03:00:00Z");
        assert_eq!(a.commit.feed_version, b.commit.feed_version);
        let mut changed = model.clone();
        changed.trips[0].start_s += 60;
        let c = build_at(&changed, "2026-10-19T03:00:00Z");
        assert_ne!(a.commit.content_sha256, c.commit.content_sha256);
    }

    #[test]
    fn feed_versions_are_clean_and_short() {
        let sha = "0123456789abcdef".repeat(4);
        assert_eq!(
            feed_version(Some("2026 10/14"), &sha),
            "20261014-0123456789ab"
        );
        assert_eq!(feed_version(None, &sha), "unversioned-0123456789ab");
        assert_eq!(feed_version(Some("!!"), &sha), "unversioned-0123456789ab");
        assert!(feed_version(Some(&"x".repeat(100)), &sha).len() <= 64);
    }
}
