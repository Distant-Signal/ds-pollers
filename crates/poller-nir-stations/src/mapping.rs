//! Parses OpenDataNI's two Translink CSVs ("Northern Ireland Railways
//! Stations" and "...Halts") into
//! `common::island_of_ireland::{IslandOfIrelandStation, IslandOfIrelandLineDefinition}`,
//! all tagged `NorthernIreland`. Tier A of
//! docs/superpowers/specs/2026-09-05-nir-tier-a-implementation-design.md;
//! see docs/superpowers/plans/2026-09-05-nir-tier-a-implementation-plan.md
//! Task 2.
//!
//! Both real CSVs share one column schema, confirmed directly this
//! session (both files fetched fresh via `curl -sL -A <User-Agent>` from
//! the exact URLs `config.rs` defaults to):
//! `OID_,NAME,TYPE,EASTING,NORTHING,Comment,Lat,Long` -- this crate only
//! needs `NAME`, `Comment`, `Lat`, `Long` (see design spec §2.1: `Lat`/
//! `Long` are already WGS84 decimal degrees, no Irish Grid conversion
//! needed; `OID_` is explicitly NOT used as a station id, see Global
//! Constraints; `TYPE`/`EASTING`/`NORTHING` are unused).
//!
//! **Real finding, not a guess**: both CSVs begin with a UTF-8 BOM
//! (confirmed this session: `od -An -tx1` on the first bytes of both
//! fetched files shows `ef bb bf` immediately before `4f 49 44 5f`, i.e.
//! `OID_`). The `csv` crate does not strip a BOM automatically, so a
//! header-based deserialize target that referenced a field literally named
//! `OID_` would fail to match (the real header cell is `"\u{FEFF}OID_"`).
//! This does not affect `RawRow` below, since it has no `OID_` field at
//! all -- flagged here so a future change that adds one doesn't get bitten
//! silently.

use std::collections::HashSet;

use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
};

#[derive(Debug, serde::Deserialize)]
struct RawRow {
    #[serde(rename = "NAME")]
    name: String,
    #[serde(rename = "Comment")]
    comment: Option<String>,
    #[serde(rename = "Lat")]
    lat: f64,
    #[serde(rename = "Long")]
    long: f64,
}

fn parse_rows(csv_bytes: &[u8]) -> anyhow::Result<Vec<RawRow>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_bytes);
    reader
        .deserialize::<RawRow>()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| anyhow::anyhow!("failed to parse OpenDataNI CSV: {err}"))
}

fn is_disused(comment: &Option<String>) -> bool {
    comment
        .as_deref()
        .map(|c| c.to_ascii_lowercase().contains("disused"))
        .unwrap_or(false)
}

/// Border/Enterprise-corridor stations already sourced from Iarnród
/// Éireann's GTFS feed (`RepublicOfIreland`), per the combined spec's §4
/// single-authoritative-source policy. `LISBURN`/`LURGAN`/`PORTADOWN`/
/// `NEWRY` are unambiguous; `BELFAST - EUROPA/GVS` is this plan's own
/// Decision 1 (see the plan's own header section for the full citation --
/// GTFS's single real `Belfast` stop, `7020IR2162` @ `(54.594684,
/// -5.939831)`, sits ~240m from this row vs. 830m-2,220m from NIR's other
/// three Belfast rows).
const EXCLUDED_STATION_NAMES: &[&str] = &[
    "LISBURN RAIL STATION",
    "LURGAN RAIL STATION",
    "PORTADOWN RAIL STATION",
    "NEWRY RAIL STATION",
    "BELFAST - EUROPA/GVS",
];

/// Strips a trailing `RAIL STATION`/`RAIL HALT` type-suffix (case-sensitive
/// on the real CSVs' own consistent ALL-CAPS formatting) and trims
/// whitespace -- used both for the dedup comparison below and for slug
/// generation, so `POYNTZPASS RAIL HALT` (Stations dataset, real quirk:
/// its own NAME still says "RAIL HALT" despite living in the Stations
/// file -- design spec §2.1) and `POYNTZPASS RAIL HALT` (Halts dataset)
/// compare equal.
fn bare_name(name: &str) -> &str {
    name.strip_suffix("RAIL STATION")
        .or_else(|| name.strip_suffix("RAIL HALT"))
        .unwrap_or(name)
        .trim()
}

/// `nir-` + lowercased, non-alphanumeric-run-collapsed `bare_name`.
/// Verified against the design spec's own two worked examples (§3.3 point
/// 4): `slugify("LURGAN RAIL STATION") == "nir-lurgan"`,
/// `slugify("BELFAST - EUROPA/GVS") == "nir-belfast-europa-gvs"` (both
/// asserted in this module's own tests below).
fn slugify(name: &str) -> String {
    let mut slug = String::from("nir-");
    let mut last_was_dash = true; // suppresses a leading dash
    for ch in bare_name(name).chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            slug.push('-');
            last_was_dash = true;
        }
    }
    if slug.ends_with('-') {
        slug.pop();
    }
    slug
}

/// Parses both real OpenDataNI CSVs (raw bytes, as fetched over HTTP) into
/// the filtered, deduped, `NorthernIreland`-tagged station catalogue.
/// Order of operations matters: Stations rows are processed (and their
/// bare names recorded) BEFORE Halts rows, so the Poyntzpass dedup always
/// keeps the Stations-dataset row, per the design spec's own §3.3 point 2
/// rule.
pub fn map_stations(
    stations_csv: &[u8],
    halts_csv: &[u8],
) -> anyhow::Result<Vec<IslandOfIrelandStation>> {
    let station_rows = parse_rows(stations_csv)?;
    let halt_rows = parse_rows(halts_csv)?;

    let mut seen_bare_names: HashSet<String> = HashSet::new();
    let mut stations = Vec::new();

    for row in station_rows {
        if is_disused(&row.comment) || EXCLUDED_STATION_NAMES.contains(&row.name.as_str()) {
            continue;
        }
        seen_bare_names.insert(bare_name(&row.name).to_string());
        stations.push(IslandOfIrelandStation {
            id: slugify(&row.name),
            name: row.name,
            network: IslandOfIrelandNetwork::NorthernIreland,
            latitude: Some(row.lat),
            longitude: Some(row.long),
        });
    }

    for row in halt_rows {
        if is_disused(&row.comment) || EXCLUDED_STATION_NAMES.contains(&row.name.as_str()) {
            continue;
        }
        let bare = bare_name(&row.name).to_string();
        if seen_bare_names.contains(&bare) {
            continue;
        }
        seen_bare_names.insert(bare);
        stations.push(IslandOfIrelandStation {
            id: slugify(&row.name),
            name: row.name,
            network: IslandOfIrelandNetwork::NorthernIreland,
            latitude: Some(row.lat),
            longitude: Some(row.long),
        });
    }

    Ok(stations)
}

/// Hand-curated, NOT CSV-parsed -- OpenDataNI publishes no per-line
/// stopping-pattern dataset for NIR at all (design spec §2.3: the only
/// "lines" data is track-engineering geometry with no rider-line tag).
/// Same posture this app already takes for GB's `lines/*.toml` catalogue
/// (hand-curated because no feed publishes this shape of data either).
///
/// Station id lists below are built from Translink's own current official
/// network map, fetched fresh this session:
/// <https://www.translink.co.uk/getmedia/bd00b3e0-0309-429c-ae33-59ebb14d0b60/NIR-schematic-map-portrait-Grand-Central-(6).pdf>
/// (its own KEY lists six lines: Dublin Line, Derry/Londonderry Line,
/// Portadown/Newry Line, Bangor Line, Portrush Line, Larne Line -- Dublin
/// Line is deliberately NOT reproduced here, it's Iarnród Éireann's own
/// GTFS-sourced line, see the combined spec's §4), cross-referenced
/// against the real, fetched OpenDataNI CSV `NAME` values so every id here
/// is `slugify`'d from a name that genuinely exists in `map_stations`'
/// own output.
///
/// **Two real, upstream data gaps, not bugs in this function**: the
/// map shows "Cullybackey" and plain "Coleraine" (the mainline interchange
/// station, not just its "Coleraine University" halt) on the
/// Derry~Londonderry Line, but NEITHER appears in either OpenDataNI CSV at
/// all (confirmed by grepping both fetched files this session) --
/// Cullybackey reopened in December 2024, after these 2023-vintage CSVs
/// were captured; plain "Coleraine" appears to be a genuine omission from
/// Translink's own 2023 survey. Both are skipped below rather than
/// invented -- no real `island_of_ireland_stations.id` exists for either
/// today.
pub fn map_lines() -> Vec<IslandOfIrelandLineDefinition> {
    vec![
        IslandOfIrelandLineDefinition {
            id: "nir-bangor-line".to_string(),
            name: "Bangor Line".to_string(),
            network: IslandOfIrelandNetwork::NorthernIreland,
            stations: [
                "BELFAST - CENTRAL RAIL STATION",
                "BELFAST - BRIDGE END RAIL HALT",
                "BELFAST - SYDENHAM RAIL HALT",
                "HOLLYWOOD RAIL HALT",
                "MARINO RAIL HALT",
                "CULTRA RAIL HALT",
                "SEAHILL RAIL HALT",
                "HELEN'S BAY RAIL HALT",
                "CARNALEA RAIL HALT",
                "BANGOR WEST RAIL HALT",
                "BANGOR RAIL STATION",
            ]
            .iter()
            .map(|n| slugify(n))
            .collect(),
        },
        IslandOfIrelandLineDefinition {
            id: "nir-larne-line".to_string(),
            name: "Larne Line".to_string(),
            network: IslandOfIrelandNetwork::NorthernIreland,
            stations: [
                "BELFAST - YORKGATE RAIL STATION",
                "WHITEABBEY RAIL HALT",
                "JORDANSTOWN RAIL STATION",
                "GREENISLAND RAIL STATION",
                "TROOPERSLANE RAIL HALT",
                "CLIPPERSTOWN RAIL HALT",
                "CARRICKFERGUS RAIL STATION",
                "DOWNSHIRE RAIL HALT",
                "WHITEHEAD RAIL HALT",
                "BALLYCARRY RAIL HALT",
                "MAGHERAMORNE RAIL HALT",
                "GLYNN RAIL HALT",
                "LARNE RAIL STATION",
                "LARNE HARBOUR RAIL STATION",
            ]
            .iter()
            .map(|n| slugify(n))
            .collect(),
        },
        // Shares its first four stops (Yorkgate through Greenisland) with
        // the Larne Line above -- a real, shared trunk, not a data error
        // (design spec §2.3's own ELR grouping: both lines' segments
        // originate from the same Belfast-side junction cluster).
        // Cullybackey and plain Coleraine are real gaps, not included --
        // see this function's own doc comment above.
        IslandOfIrelandLineDefinition {
            id: "nir-londonderry-line".to_string(),
            name: "Derry~Londonderry Line".to_string(),
            network: IslandOfIrelandNetwork::NorthernIreland,
            stations: [
                "BELFAST - YORKGATE RAIL STATION",
                "WHITEABBEY RAIL HALT",
                "JORDANSTOWN RAIL STATION",
                "GREENISLAND RAIL STATION",
                "MOSSLEY WEST RAIL HALT",
                "ANTRIM RAIL STATION",
                "BALLYMENA RAIL STATION",
                "BALLYMONEY RAIL STATION",
                // Portrush branch, flattened into this same ordered list --
                // same "one flat representative stopping pattern per line"
                // simplification poller-irish-rail-gtfs::mapping::map_lines
                // already makes for GTFS routes with multiple real
                // variants (crates/poller-irish-rail-gtfs/src/mapping.rs:45-55),
                // not a new posture. Branches off the real network at
                // Coleraine, which is itself one of this function's two
                // documented gaps.
                "COLERAINE UNIVERSITY RAIL HALT",
                "PORTRUSH DHU VARREN RAIL HALT",
                "PORTRUSH RAIL STATION",
                // Back onto the Derry-bound continuation.
                "CASTLEROCK RAIL HALT",
                "BELLARENA RAIL HALT",
                "L'DERRY RAIL STATION",
            ]
            .iter()
            .map(|n| slugify(n))
            .collect(),
        },
        // Decision 2 (see this plan's own header section): a genuinely
        // distinct NIR-only local/stopping line, confirmed via Translink's
        // own current official route map, NOT the same line as Iarnród
        // Éireann's GTFS-sourced Dublin Line/Enterprise -- despite sharing
        // the same physical BCJ corridor for most of its length. Endpoint
        // stations Lisburn/Lurgan/Portadown/Newry are excluded (GTFS-sourced
        // instead, per the border-overlap policy); the local halts between
        // them are real NIR-only stops with no GTFS counterpart and stay.
        IslandOfIrelandLineDefinition {
            id: "nir-portadown-newry-line".to_string(),
            name: "Portadown/Newry Line".to_string(),
            network: IslandOfIrelandNetwork::NorthernIreland,
            stations: [
                "BELFAST - CENTRAL RAIL STATION",
                "BELFAST - BOTANIC RAIL STATION",
                "BELFAST - CITY HOSPITAL RAIL HALT",
                "BELFAST - ADELAIDE RAIL HALT",
                "BELFAST - BALMORAL RAIL HALT",
                "FINAGHY RAIL HALT",
                "DUNMURRY RAIL HALT",
                "DERRIAGHY RAIL HALT",
                "LAMBEG RAIL HALT",
                "HILDEN RAIL HALT",
                // Lisburn excluded -- GTFS-sourced.
                "MOIRA RAIL HALT",
                // Lurgan, Portadown excluded -- GTFS-sourced.
                "SCARVA RAIL HALT",
                "POYNTZPASS RAIL HALT",
                // Newry excluded -- GTFS-sourced.
            ]
            .iter()
            .map(|n| slugify(n))
            .collect(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATIONS_CSV_HEADER: &str = "OID_,NAME,TYPE,EASTING,NORTHING,Comment,Lat,Long\n";

    /// A small, real-shaped fixture -- every row's `NAME`/`Comment`/`Lat`/
    /// `Long` values are copied verbatim from this session's own fetch of
    /// the real CSVs (not invented), covering every filtering rule this
    /// module implements: a border exclusion (Lisburn), the Decision-1
    /// Belfast exclusion (Europa/GVS) alongside a kept Belfast row
    /// (Central), and an ordinary kept row (Bangor).
    fn stations_fixture() -> Vec<u8> {
        format!(
            "\u{FEFF}{STATIONS_CSV_HEADER}\
             1,BELFAST - EUROPA/GVS,RAIL STATION,333444,373777,,54.594613570000000,-5.936183220000000\n\
             3,BELFAST - CENTRAL RAIL STATION,RAIL STATION,334663,373896,Remnamed,54.595358900000001,-5.917282820000000\n\
             10,LISBURN RAIL STATION,RAIL STATION,326581,364591,,54.513905760000000,-6.046240710000000\n\
             13,POYNTZPASS RAIL HALT,RAIL STATION,306049,339455,,54.292897179999997,-6.372081310000000\n\
             20,BANGOR RAIL STATION,RAIL STATION,350361,381476,,54.658980000000000,-5.669660000000000\n"
        )
        .into_bytes()
    }

    /// Covers: a disused halt (Knockmore), the cross-dataset Poyntzpass
    /// duplicate (must be skipped in favour of the Stations row above),
    /// and an ordinary kept halt (Moira).
    fn halts_fixture() -> Vec<u8> {
        format!(
            "\u{FEFF}{STATIONS_CSV_HEADER}\
             26,KNOCKMORE RAIL HALT,HALT,325198,364265,Disused,54.511321969999997,-6.067719940000000\n\
             27,MOIRA RAIL HALT,HALT,315819,361885,,54.492179270000001,-6.213381050000000\n\
             37,POYNTZPASS RAIL HALT,HALT,306049,339455,,54.292897000000004,-6.372081000000000\n"
        )
        .into_bytes()
    }

    #[test]
    fn slugify_matches_the_design_specs_own_worked_examples() {
        assert_eq!(slugify("LURGAN RAIL STATION"), "nir-lurgan");
        assert_eq!(slugify("BELFAST - EUROPA/GVS"), "nir-belfast-europa-gvs");
    }

    #[test]
    fn map_stations_excludes_border_and_decision_1_belfast_rows() {
        let stations = map_stations(&stations_fixture(), &halts_fixture()).unwrap();
        assert!(!stations.iter().any(|s| s.id == "nir-belfast-europa-gvs"));
        assert!(!stations.iter().any(|s| s.name == "LISBURN RAIL STATION"));
        assert!(stations.iter().any(|s| s.id == "nir-belfast-central"));
        assert!(stations.iter().any(|s| s.id == "nir-bangor"));
    }

    #[test]
    fn map_stations_filters_disused_halts() {
        let stations = map_stations(&stations_fixture(), &halts_fixture()).unwrap();
        assert!(!stations.iter().any(|s| s.name.contains("KNOCKMORE")));
        assert!(stations.iter().any(|s| s.id == "nir-moira"));
    }

    #[test]
    fn map_stations_dedups_poyntzpass_preferring_the_stations_dataset_row() {
        let stations = map_stations(&stations_fixture(), &halts_fixture()).unwrap();
        let poyntzpass: Vec<_> = stations
            .iter()
            .filter(|s| s.id == "nir-poyntzpass")
            .collect();
        assert_eq!(poyntzpass.len(), 1, "must appear exactly once");
        // The Stations-dataset row's own Lat carries an extra trailing
        // digit vs. the Halts row (54.292897179999997 vs.
        // 54.292897000000004) -- asserting on the exact value confirms
        // which row won, not just that dedup happened at all.
        assert_eq!(poyntzpass[0].latitude, Some(54.292_897_18));
    }

    #[test]
    fn map_stations_tags_every_row_northern_ireland() {
        let stations = map_stations(&stations_fixture(), &halts_fixture()).unwrap();
        assert!(!stations.is_empty());
        assert!(
            stations
                .iter()
                .all(|s| s.network == IslandOfIrelandNetwork::NorthernIreland)
        );
    }

    #[test]
    fn map_lines_returns_four_lines_with_non_empty_station_lists() {
        let lines = map_lines();
        assert_eq!(lines.len(), 4);
        for line in &lines {
            assert!(
                !line.stations.is_empty(),
                "{} must have a non-empty station list",
                line.id
            );
            assert!(
                line.stations.iter().all(|id| id.starts_with("nir-")),
                "{} must only reference nir- station ids",
                line.id
            );
        }
    }

    #[test]
    fn map_lines_portadown_newry_line_excludes_gtfs_sourced_endpoints() {
        let lines = map_lines();
        let line = lines
            .iter()
            .find(|l| l.id == "nir-portadown-newry-line")
            .unwrap();
        for excluded in ["nir-lisburn", "nir-lurgan", "nir-portadown", "nir-newry"] {
            assert!(
                !line.stations.iter().any(|id| id == excluded),
                "{excluded} must not appear -- it's GTFS-sourced"
            );
        }
        assert_eq!(line.stations.len(), 13);
    }
}
