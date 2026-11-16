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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::StationDeparture;

/// How long a remembered service outlives its last sighting on any board
/// polled for its station. Forgotten by age, not by absence from the latest
/// board: every board this poller fetches is a partial view of the station
/// (the rotation's first `numRows` rows, fewer after a `numRows` fallback,
/// and the tracked-stop lane's `timeOffset`/`timeWindow` slice around one
/// train), so a service missing from one board has not necessarily gone.
/// Six hours is well past the tracked-stop lane's two-hour horizon plus a
/// long delay, and well short of the day it takes a `serviceID` token to
/// come round again.
pub(crate) const FORGET_AFTER: Duration = Duration::from_secs(6 * 3600);

/// The earliest platform seen for one service at one station, and when the
/// service was last on a board there.
#[derive(Debug)]
struct Remembered {
    planned: String,
    last_seen: Instant,
}

/// `(station CRS, Darwin `serviceId`) -> earliest platform observed for
/// that pairing this process has seen`. Keyed by CRS as well as
/// `service_id` (not `service_id` alone) purely for hygiene -- nothing in
/// this codebase claims Darwin's `serviceId` tokens are globally unique
/// across every station's own board, and scoping by station costs nothing.
#[derive(Debug, Default)]
pub(crate) struct PlatformHistory {
    seen: HashMap<(String, String), Remembered>,
}

impl PlatformHistory {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fills `planned_platform` on every departure in `departures` (all
    /// freshly polled for station `crs` at `now`) from this process's own
    /// first-seen-platform memory, and marks every remembered one as seen
    /// at `now`. Services absent from `departures` are left alone (the
    /// board may be a window of the station's departures, see
    /// [`FORGET_AFTER`]); [`PlatformHistory::prune`] forgets them by age.
    ///
    /// A departure with `platform: None` this cycle (still unallocated, or
    /// genuinely unknown) is left with `planned_platform: None` too and is
    /// NOT remembered yet -- there is nothing to plan against until a real
    /// platform value is first seen.
    pub(crate) fn apply(&mut self, crs: &str, departures: &mut [StationDeparture], now: Instant) {
        for departure in departures.iter_mut() {
            let key = (crs.to_string(), departure.service_id.clone());
            let Some(platform) = departure.platform.clone() else {
                if let Some(remembered) = self.seen.get_mut(&key) {
                    remembered.last_seen = now;
                }
                continue;
            };
            let remembered = self.seen.entry(key).or_insert(Remembered {
                planned: platform,
                last_seen: now,
            });
            remembered.last_seen = now;
            departure.planned_platform = Some(remembered.planned.clone());
        }
    }

    /// Forgets every service no board has shown for [`FORGET_AFTER`]: it
    /// has departed, and remembering it forever would leak memory across a
    /// long-running process. Called once a cycle.
    pub(crate) fn prune(&mut self, now: Instant) {
        self.seen.retain(|_, remembered| {
            now.saturating_duration_since(remembered.last_seen) < FORGET_AFTER
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
            rsid: None,
            calling_points: Vec::new(),
        }
    }

    #[test]
    fn first_sighting_becomes_the_planned_platform() {
        let mut history = PlatformHistory::new();
        let mut departures = vec![departure("svc1", Some("6"))];

        history.apply("PAD", &mut departures, Instant::now());

        assert_eq!(departures[0].platform, Some("6".to_string()));
        assert_eq!(departures[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn a_later_poll_keeps_the_first_seen_planned_platform_even_after_a_change() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut first_poll, Instant::now());

        let mut second_poll = vec![departure("svc1", Some("9"))];
        history.apply("PAD", &mut second_poll, Instant::now());

        assert_eq!(second_poll[0].platform, Some("9".to_string()));
        assert_eq!(second_poll[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn an_unchanged_platform_across_polls_reports_the_same_planned_and_current_value() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut first_poll, Instant::now());

        let mut second_poll = vec![departure("svc1", Some("6"))];
        history.apply("PAD", &mut second_poll, Instant::now());

        assert_eq!(second_poll[0].platform, Some("6".to_string()));
        assert_eq!(second_poll[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn no_platform_yet_is_never_remembered_or_planned() {
        let mut history = PlatformHistory::new();
        let mut first_poll = vec![departure("svc1", None)];
        history.apply("PAD", &mut first_poll, Instant::now());
        assert_eq!(first_poll[0].planned_platform, None);
        assert!(history.seen.is_empty());

        // A later poll that DOES report a platform starts the memory from
        // there -- the first "unallocated" poll left nothing to plan
        // against.
        let mut second_poll = vec![departure("svc1", Some("4"))];
        history.apply("PAD", &mut second_poll, Instant::now());
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
        history.apply("PAD", &mut pad, Instant::now());

        let mut rdg = vec![departure("svc1", Some("2"))];
        history.apply("RDG", &mut rdg, Instant::now());

        assert_eq!(rdg[0].planned_platform, Some("2".to_string()));
    }

    #[test]
    fn a_service_missing_from_a_windowed_board_keeps_its_planned_platform() {
        // 1A23 is first seen at PAD on platform 6. The tracked-stop lane
        // then polls PAD for another train 40 minutes out (a window 1A23
        // is not in), and a numRows fallback board misses it too. When it
        // moves to platform 8, the change is still reported.
        let mut history = PlatformHistory::new();
        let start = Instant::now();
        history.apply("PAD", &mut [departure("1A23", Some("6"))], start);
        history.apply(
            "PAD",
            &mut [departure("2B45", Some("1"))],
            start + Duration::from_secs(60),
        );
        history.apply("PAD", &mut [], start + Duration::from_secs(120));
        history.prune(start + Duration::from_secs(180));

        let mut moved = vec![departure("1A23", Some("8"))];
        history.apply("PAD", &mut moved, start + Duration::from_secs(240));
        assert_eq!(moved[0].platform, Some("8".to_string()));
        assert_eq!(moved[0].planned_platform, Some("6".to_string()));
    }

    #[test]
    fn a_service_unseen_for_the_forget_age_is_forgotten_and_starts_fresh_if_it_reappears() {
        let mut history = PlatformHistory::new();
        let start = Instant::now();
        history.apply("PAD", &mut [departure("svc1", Some("6"))], start);

        // Seen again an hour later (its platform blanked): kept from then.
        let seen_again = start + Duration::from_secs(3600);
        history.apply("PAD", &mut [departure("svc1", None)], seen_again);
        history.prune(start + FORGET_AFTER);
        assert_eq!(history.seen.len(), 1, "seen within the forget age");

        // Departed: on no board for the forget age.
        history.prune(seen_again + FORGET_AFTER);
        assert!(history.seen.is_empty());

        // A day later, a different service happens to reuse the same
        // token (or the same service genuinely reappears) -- it must not
        // inherit yesterday's planned platform.
        let mut later_poll = vec![departure("svc1", Some("3"))];
        history.apply("PAD", &mut later_poll, seen_again + FORGET_AFTER);
        assert_eq!(later_poll[0].planned_platform, Some("3".to_string()));
    }
}
