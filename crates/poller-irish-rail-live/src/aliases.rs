//! Which of `getAllStationsXML`'s codes get a board fetch (DS-373).
//!
//! A few stations appear in the live list two or three times, under codes
//! with the same name and position (Park West and Cherry Orchard: `CHORC`
//! and `PWESF`; Kishoge, Clondalkin, Adamstown and Hazelhatch likewise),
//! and every code in such a group returns the same board. Fetching each
//! one triples the upstream calls for those stations and stores the same
//! calls under every code. ds-core's
//! [`common::island_of_ireland::live_codes::OVERRIDES`] already pins each
//! of those catalogue stations to one code, the code the pages read; this
//! drops the other codes in its group.
//!
//! Deliberately narrow, since a missing board is worse than a duplicate
//! one: a code is skipped only when another listed code with the same
//! name and position is an override's code, and an override's code is
//! never skipped. A group with no override is fetched in full, as is
//! every code without a twin, so every board the pages read is still
//! fetched. The station list itself is still sent whole: the writer
//! matches the catalogue against it.

use common::island_of_ireland::IslandOfIrelandLiveStation;
use common::island_of_ireland::live_codes::{OVERRIDES, normalise};

/// How far apart, in degrees of latitude and of longitude, two codes may
/// be and still count as one station's aliases: about 110 m north-south
/// and 65 m east-west at Irish latitudes. The aliases seen so far share
/// their position exactly; this only tolerates rounding.
const SAME_POSITION_DEGREES: f64 = 0.001;

/// The override codes: the code each pinned catalogue station reads.
fn pinned_codes() -> impl Iterator<Item = &'static str> {
    OVERRIDES.iter().filter_map(|(_, code)| *code)
}

/// Whether `a` and `b` are the same station under two codes: the same
/// normalised name and both positions known and within
/// [`SAME_POSITION_DEGREES`].
fn same_station(a: &IslandOfIrelandLiveStation, b: &IslandOfIrelandLiveStation) -> bool {
    let name = normalise(&a.name);
    let near = |x: Option<f64>, y: Option<f64>| {
        x.zip(y)
            .is_some_and(|(x, y)| (x - y).abs() <= SAME_POSITION_DEGREES)
    };
    !name.is_empty()
        && name == normalise(&b.name)
        && near(a.latitude, b.latitude)
        && near(a.longitude, b.longitude)
}

/// The codes to fetch a board for, in list order, and the alias codes
/// skipped. See the [module docs](self).
pub(crate) fn board_codes(stations: &[IslandOfIrelandLiveStation]) -> (Vec<String>, Vec<String>) {
    board_codes_with(stations, &pinned_codes().collect::<Vec<_>>())
}

/// [`board_codes`] with the caller's pinned codes (tests).
fn board_codes_with(
    stations: &[IslandOfIrelandLiveStation],
    pinned: &[&str],
) -> (Vec<String>, Vec<String>) {
    let is_pinned = |code: &str| pinned.contains(&code);
    let pinned_twin = |station: &IslandOfIrelandLiveStation| {
        stations.iter().any(|other| {
            other.code != station.code && is_pinned(&other.code) && same_station(station, other)
        })
    };
    let mut fetch = Vec::with_capacity(stations.len());
    let mut skipped = Vec::new();
    for station in stations {
        if !is_pinned(&station.code) && pinned_twin(station) {
            skipped.push(station.code.clone());
        } else {
            fetch.push(station.code.clone());
        }
    }
    (fetch, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(code: &str, name: &str, at: Option<(f64, f64)>) -> IslandOfIrelandLiveStation {
        IslandOfIrelandLiveStation {
            code: code.into(),
            name: name.into(),
            alias: None,
            latitude: at.map(|(lat, _)| lat),
            longitude: at.map(|(_, lon)| lon),
        }
    }

    /// The five aliased stations as the live list carries them (ds-core's
    /// `live_codes` test fixture), plus neighbours that must stay.
    fn list() -> Vec<IslandOfIrelandLiveStation> {
        let park_west = Some((53.3343, -6.3790));
        let kishoge = Some((53.3315, -6.4205));
        let clondalkin = Some((53.3349, -6.4064));
        let adamstown = Some((53.3353, -6.4523));
        let hazelhatch = Some((53.3226, -6.5232));
        vec![
            live("ADAMF", "Adamstown", adamstown),
            live("ADMTN", "Adamstown", adamstown),
            live("CHORC", "Park West and Cherry Orchard", park_west),
            live("CLDKN", "Clondalkin", clondalkin),
            live("CLONF", "Clondalkin", clondalkin),
            live("CLONS", "Clondalkin", clondalkin),
            live("CNLLY", "Dublin Connolly", Some((53.3531, -6.24591))),
            live("HAZEF", "Hazelhatch", hazelhatch),
            live("HAZES", "Hazelhatch", hazelhatch),
            live("HZLCH", "Hazelhatch", hazelhatch),
            live("KISHF", "Kishoge", kishoge),
            live("KISHO", "Kishoge", kishoge),
            live("KISHS", "Kishoge", kishoge),
            live("PWESF", "Park West and Cherry Orchard", park_west),
        ]
    }

    #[test]
    fn only_the_pinned_code_of_each_alias_group_is_fetched() {
        let (fetch, skipped) = board_codes(&list());
        assert_eq!(
            fetch,
            ["ADMTN", "CHORC", "CLDKN", "CNLLY", "HZLCH", "KISHO"]
        );
        assert_eq!(
            skipped,
            [
                "ADAMF", "CLONF", "CLONS", "HAZEF", "HAZES", "KISHF", "KISHS", "PWESF"
            ]
        );
    }

    #[test]
    fn every_aliased_station_is_pinned_by_ds_core() {
        // The groups above collapse only because ds-core pins them; if an
        // override goes, its group is fetched in full again, never dropped.
        let pinned: Vec<_> = pinned_codes().collect();
        for code in ["CHORC", "KISHO", "CLDKN", "ADMTN", "HZLCH"] {
            assert!(pinned.contains(&code), "{code}");
        }
    }

    #[test]
    fn a_group_without_a_pinned_code_is_fetched_in_full() {
        let (fetch, skipped) = board_codes_with(&list(), &["CHORC"]);
        assert_eq!(skipped, ["PWESF"]);
        assert_eq!(fetch.len(), list().len() - 1);
        let (fetch, skipped) = board_codes_with(&list(), &[]);
        assert_eq!(skipped, Vec::<String>::new());
        assert_eq!(fetch.len(), list().len());
    }

    #[test]
    fn a_pinned_code_missing_from_the_list_drops_nothing() {
        let without: Vec<_> = list().into_iter().filter(|s| s.code != "HZLCH").collect();
        let (fetch, _) = board_codes(&without);
        assert!(fetch.contains(&"HAZEF".to_owned()));
        assert!(fetch.contains(&"HAZES".to_owned()));
    }

    #[test]
    fn a_different_name_or_a_distant_or_unknown_position_is_not_an_alias() {
        let at = Some((53.3226, -6.5232));
        let stations = [
            live("HZLCH", "Hazelhatch", at),
            // Same place, another name.
            live("HAZEX", "Hazelhatch Sidings", at),
            // Same name, about 1 km away.
            live("HAZEY", "Hazelhatch", Some((53.3316, -6.5232))),
            // Same name, no position.
            live("HAZEZ", "Hazelhatch", None),
        ];
        let (fetch, skipped) = board_codes(&stations);
        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(fetch, ["HZLCH", "HAZEX", "HAZEY", "HAZEZ"]);
    }

    #[test]
    fn names_compare_normalised_and_positions_tolerate_rounding() {
        let stations = [
            live(
                "CHORC",
                "Park West & Cherry Orchard",
                Some((53.3343, -6.3790)),
            ),
            live(
                "PWESF",
                "park west and cherry orchard",
                Some((53.33435, -6.37905)),
            ),
        ];
        let (fetch, skipped) = board_codes(&stations);
        assert_eq!(fetch, ["CHORC"]);
        assert_eq!(skipped, ["PWESF"]);
    }
}
