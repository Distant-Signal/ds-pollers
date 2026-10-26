//! The links between a network's stations and National Rail stations
//! (transit spec §6.3): a checked-in CSV per network,
//! `reference-data/transit/<network>-rail-links.csv`, compiled in and sent
//! with every timetable (the writer replaces `transit_rail_links` with it
//! at each activation).
//!
//! Columns `stop_id,crs,tiploc,kind,note`: the station's `NaPTAN` `MET`
//! code, the National Rail station's CRS and TIPLOC (a pair in
//! `reference-data/crs-tiploc.csv`), `interchange` (the same complex) or
//! `nearby` (a short walk; no minutes, Q10).

use anyhow::{Context, Result, bail, ensure};
use common::transit::{RailLinkKind, TransitRailLink, is_stop_id};

const FILES: &[(&str, &str)] = &[
    (
        "metrolink",
        include_str!("../../../reference-data/transit/metrolink-rail-links.csv"),
    ),
    (
        "net",
        include_str!("../../../reference-data/transit/net-rail-links.csv"),
    ),
];

/// The network's rail links (none if it has no file).
pub(crate) fn rail_links(network: &str) -> Result<Vec<TransitRailLink>> {
    let Some((_, text)) = FILES.iter().find(|(name, _)| *name == network) else {
        return Ok(Vec::new());
    };
    parse(text).with_context(|| format!("reference-data/transit/{network}-rail-links.csv"))
}

fn parse(text: &str) -> Result<Vec<TransitRailLink>> {
    let mut reader = csv::Reader::from_reader(text.as_bytes());
    let headers = reader.headers()?.clone();
    ensure!(
        headers.iter().collect::<Vec<_>>() == ["stop_id", "crs", "tiploc", "kind", "note"],
        "columns {headers:?}"
    );
    let mut links = Vec::new();
    for record in reader.records() {
        let record = record?;
        let get = |i: usize| record.get(i).unwrap_or("").trim();
        let kind = match get(3) {
            "interchange" => RailLinkKind::Interchange,
            "nearby" => RailLinkKind::Nearby,
            other => bail!("kind {other:?}"),
        };
        let link = TransitRailLink {
            stop_id: get(0).to_owned(),
            crs: get(1).to_owned(),
            tiploc: get(2).to_owned(),
            kind,
            note: Some(get(4).to_owned()).filter(|n| !n.is_empty()),
        };
        ensure!(is_stop_id(&link.stop_id), "stop id {:?}", link.stop_id);
        ensure!(
            common::corpus_inference::is_crs(&link.crs),
            "CRS {:?}",
            link.crs
        );
        ensure!(
            common::corpus_inference::is_tiploc(&link.tiploc),
            "TIPLOC {:?}",
            link.tiploc
        );
        links.push(link);
    }
    Ok(links)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use common::corpus_inference::Exclusion;

    use super::*;

    /// `reference-data/crs-tiploc.csv`: (crs, tiploc) -> name.
    fn crs_tiploc() -> Vec<(String, String, String)> {
        let path = common::manifest_dir!().join("../../reference-data/crs-tiploc.csv");
        let mut reader = csv::Reader::from_path(path).unwrap();
        reader
            .records()
            .map(|r| {
                let r = r.unwrap();
                (r[0].to_owned(), r[1].to_owned(), r[2].to_owned())
            })
            .collect()
    }

    /// `(ATCOCode, StopType)` of the trimmed `NaPTAN` fixture.
    fn naptan() -> BTreeSet<(String, String)> {
        let path = common::manifest_dir!().join("tests/fixtures/naptan-940-mini.csv");
        let mut reader = csv::Reader::from_path(path).unwrap();
        let headers = reader.headers().unwrap().clone();
        let at = |name: &str| headers.iter().position(|h| h == name).unwrap();
        let (atco, kind) = (at("ATCOCode"), at("StopType"));
        reader
            .records()
            .map(|r| {
                let r = r.unwrap();
                (r[atco].to_owned(), r[kind].to_owned())
            })
            .collect()
    }

    /// Every link of `network` is a real CRS/TIPLOC pair, a real station
    /// (not a CORPUS pseudo or tram-stop code) and a `NaPTAN` `MET` row,
    /// and names each CRS once.
    fn check_real(network: &str, links: &[TransitRailLink]) {
        let pairs = crs_tiploc();
        let met = naptan();
        let mut crs_seen = BTreeSet::new();
        for link in links {
            let row = pairs
                .iter()
                .find(|(crs, tiploc, _)| *crs == link.crs && *tiploc == link.tiploc);
            let Some((_, _, name)) = row else {
                panic!(
                    "{network}: {} {} is not in crs-tiploc.csv",
                    link.crs, link.tiploc
                );
            };
            assert_ne!(
                Exclusion::of(&link.crs),
                Exclusion::PseudoCode,
                "{}",
                link.crs
            );
            assert_eq!(name, &name.to_uppercase(), "{}: {name}", link.crs);
            assert!(
                met.contains(&(link.stop_id.clone(), "MET".to_owned())),
                "{} is not a NaPTAN MET row",
                link.stop_id
            );
            assert!(crs_seen.insert(link.crs.clone()), "{} twice", link.crs);
        }
    }

    #[test]
    fn nets_three_interchanges_are_real_station_pairs() {
        let links = rail_links("net").unwrap();
        check_real("net", &links);
        let crs: BTreeSet<&str> = links.iter().map(|l| l.crs.as_str()).collect();
        assert_eq!(crs, BTreeSet::from(["BLW", "HKN", "NOT"]));
        assert!(links.iter().all(|l| l.kind == RailLinkKind::Interchange));
        assert!(links.iter().all(|l| l.stop_id.starts_with("9400ZZNO")));
    }

    #[test]
    fn metrolinks_ten_interchanges_are_real_station_pairs() {
        let links = rail_links("metrolink").unwrap();
        assert_eq!(links.len(), 10);
        check_real("metrolink", &links);
        let pairs = crs_tiploc();
        for link in &links {
            // Not one of CORPUS's Metrolink-stop codes (spec §3.4: AMM
            // "Abraham Moss Mtlk" and friends are NotAStation).
            let (_, _, name) = pairs
                .iter()
                .find(|(crs, tiploc, _)| *crs == link.crs && *tiploc == link.tiploc)
                .unwrap();
            assert!(
                !name.ends_with("Mtlk"),
                "{} is a Metrolink pseudo-CRS",
                link.crs
            );
        }
        let crs: BTreeSet<&str> = links.iter().map(|l| l.crs.as_str()).collect();
        assert_eq!(
            crs,
            BTreeSet::from([
                "AHN", "ALT", "DGT", "ECC", "EDY", "MAN", "MCV", "MIA", "NVR", "RCD"
            ])
        );
    }

    #[test]
    fn bad_rows_are_refused() {
        let header = "stop_id,crs,tiploc,kind,note\n";
        for bad in [
            "9400ZZMAPIC,MAN,MNCRPIC,walk,\n",
            "9400zzmapic,MAN,MNCRPIC,nearby,\n",
            "9400ZZMAPIC,MANC,MNCRPIC,nearby,\n",
        ] {
            assert!(parse(&format!("{header}{bad}")).is_err(), "{bad}");
        }
        assert!(parse("stop,crs\n").is_err());
        assert!(rail_links("supertram").unwrap().is_empty());
    }
}
