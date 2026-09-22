//! Reconstructs a "planned vs actual" platform distinction that RDM's
//! `GetDepBoardWithDetails` feed does not itself provide -- see
//! `schema.rs::RdmServiceItem::platform`'s own doc comment. Darwin's live
//! board only ever reports the CURRENT best-known platform, merging any
//! late alteration in place and never separately exposing what was
//! originally published.
//!
//! `PlatformHistory` owns this poller process's own memory of the earliest
//! platform it has seen for each `(station CRS, service_id)` pair across
//! successive polls, and fills `StationDeparture.planned_platform` in from
//! that memory each cycle (`apply`, below). A poller restart resets this
//! memory to nothing, which conservatively means "no known change yet" for
//! the first poll after a restart -- graceful degradation, not a
//! correctness bug, given Darwin itself has no durable record of the
//! originally-published value either.
//!
//! Lives entirely in this crate's own process memory, owned by `main()` and
//! threaded into `poll_once` by mutable reference across poll cycles --
//! same "cycle-to-cycle mutable state" shape as `poller-tfl`'s own
//! `dlr::inference::DlrMatchState`
//! (`crates/common/src/poller_loop.rs`'s own doc comment names this exact
//! pattern). Deliberately NOT persisted to `station_samples`: that table is
//! explicitly "no history -- wholesale-replaced per poll"
//! (`crates/api/src/data/queries.rs::upsert_station_samples`'s own doc
//! comment), so this reconstruction has to live upstream of it instead of
//! trying to read history back out of a table designed to have none.

use std::collections::{HashMap, HashSet};

use common::StationDeparture;

/// `(station CRS, Darwin `serviceId`) -> earliest platform observed for
/// that pairing this process has seen`. Keyed by CRS as well as
/// `service_id` (not `service_id` alone) purely for hygiene -- nothing in
/// this codebase claims Darwin's `serviceId` tokens are globally unique
/// across every station's own board, and scoping by station costs nothing.
#[derive(Debug, Default)]
pub struct PlatformHistory {
    seen: HashMap<(String, String), String>,
}

impl PlatformHistory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fills `planned_platform` on every departure in `departures` (all
    /// freshly polled for station `crs`) from this process's own
    /// first-seen-platform memory, and prunes any remembered service for
    /// `crs` that isn't present in `departures` this cycle -- it has since
    /// departed or fallen off the board's near-term window, so remembering
    /// it forever would leak memory across a long-running process.
    ///
    /// A departure with `platform: None` this cycle (still unallocated, or
    /// genuinely unknown) is left with `planned_platform: None` too and is
    /// NOT remembered yet -- there is nothing to plan against until a real
    /// platform value is first seen.
    pub fn apply(&mut self, crs: &str, departures: &mut [StationDeparture]) {
        let mut current_service_ids: HashSet<String> = HashSet::new();

        for departure in departures.iter_mut() {
            current_service_ids.insert(departure.service_id.clone());

            let Some(platform) = departure.platform.clone() else {
                continue;
            };
            let key = (crs.to_string(), departure.service_id.clone());
            let planned = self.seen.entry(key).or_insert(platform);
            departure.planned_platform = Some(planned.clone());
        }

        self.seen.retain(|(k_crs, k_service_id), _| {
            k_crs != crs || current_service_ids.contains(k_service_id)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn departure(service_id: &str, platform: Option<&str>) -> StationDeparture {
        StationDeparture {
            service_id: service_id.to_string(),
            operator: "GW".to_string(),
            destination_crs: "RDG".to_string(),
            scheduled: "10:00".to_string(),
            estimated: "10:00".to_string(),
            is_cancelled: false,
            delay_minutes: 0,
            cancel_reason: None,
            delay_reason: None,
            headcode: None,
            skipped_stations: vec![],
            platform: platform.map(str::to_string),
            planned_platform: None,
        }
    }

    #[test]
    fn first_sighting_becomes_the_planned_platform() {
        let mut history = PlatformHistory::new();
        let mut departures = vec![departure("svc1", Some("6"))];

        history.apply("PAD", &mut departures);

        assert_eq!(departures[0].platform, Some("6".to_string()));
        assert_eq!(departures[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn a_later_poll_keeps_the_first_seen_planned_platform_even_after_a_change() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut first_poll);

        let mut second_poll = vec![departure("svc1", Some("9"))];
        history.apply("PAD", &mut second_poll);

        assert_eq!(second_poll[0].platform, Some("9".to_string()));
        assert_eq!(second_poll[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn an_unchanged_platform_across_polls_reports_the_same_planned_and_current_value() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut first_poll);

        let mut second_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut second_poll);

        assert_eq!(second_poll[0].platform, Some("6".to_string()));
        assert_eq!(second_poll[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn no_platform_yet_is_never_remembered_or_planned() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", None)];
        history.apply("PAD", &mut first_poll);
        assert_eq!(first_poll[0].planned_platform, None);

        // A later poll that DOES report a platform starts the memory from
        // there -- the first "unallocated" poll left nothing to plan
        // against.
        let mut second_poll = vec![departure("svc1", Some("4"))];
        history.apply("PAD", &mut second_poll);
        assert_eq!(second_poll[0].planned_platform, Some("4".to_string()));
    }

    #[test]
    fn different_stations_are_tracked_independently_even_with_the_same_service_id() {
        // Defensive: nothing guarantees Darwin's serviceId tokens are
        // globally unique across stations, so two different stations
        // reporting the same token must not cross-contaminate each other's
        // planned platform.
        let mut history = PlatformHistory::new();
        let mut pad = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut pad);

        let mut rdg = vec![departure("svc1", Some("2"))];
        history.apply("RDG", &mut rdg);

        assert_eq!(rdg[0].planned_platform, Some("2".to_string()));
    }

    #[test]
    fn a_service_that_drops_off_the_board_is_forgotten_and_starts_fresh_if_it_reappears() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut first_poll);

        // svc1 has departed / fallen off the near-term board this cycle.
        history.apply("PAD", &mut []);
        assert_eq!(history.seen.len(), 0);

        // A day later, a different service happens to reuse the same
        // token (or the same service genuinely reappears) -- it must not
        // inherit yesterday's planned platform.
        let mut later_poll = vec![departure("svc1", Some("3"))];
        history.apply("PAD", &mut later_poll);
        assert_eq!(later_poll[0].planned_platform, Some("3".to_string()));
    }
}
