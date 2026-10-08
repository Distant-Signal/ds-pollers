//! Which stations a cycle samples first (SVC-04).
//!
//! Each cycle samples stations one at a time until the 45 s
//! `CYCLE_TIME_BUDGET` runs out, which in production is about 350-380 of
//! the 560 sample stations (2026-09-27; ~255-290 by 2026-10-01). The list from `api` is sorted, so every cycle
//! used to start at "AAP" and stop around "RMD": S-Z were never sampled.
//!
//! Now each cycle starts where the previous one stopped (the first station
//! it did not get to), wrapping round the end of the list. The number of
//! requests per cycle is unchanged -- still whatever fits the budget -- so
//! this adds no LDBWS request volume (LEG-18). A station is
//! sampled at least once every `ceil(stations / per-cycle capacity)` cycles.
//!
//! A budget-cut cycle is therefore normal and only logged at debug.
//! Instead, `main.rs` reports each full pass over the list (counted by
//! [`Rotation::record_progress`]) with one info line and
//! `ldbws_full_rotation_seconds`/`_cycles`, and warns only when a pass is
//! slower than the aggregator's sample-age limit.
//!
//! The position survives list changes (it is kept as a CRS, not an index)
//! but not restarts. A restart starts from a position derived from the wall
//! clock instead, so a crash loop does not keep resampling the start of the
//! alphabet: `(unix_time / poll_interval) * TYPICAL_CYCLE_CAPACITY`, modulo
//! the station count -- the offset the rotation would roughly have reached
//! had it run since the epoch.
//!
//! Stations LDBWS rejects as an invalid CRS (a catalogue typo such as
//! "ANV" for Andover's "ADV", 2026-09-28) are a permanent error, not a
//! staleness one: they can never produce a sample, so counting them in
//! [`Rotation::stalest_age`] made `ldbws_stalest_station_age_seconds` equal
//! the pod's uptime and fired `DistantSignalLdbwsStationStale` a couple of
//! hours after every restart. They are tracked separately
//! ([`Rotation::mark_invalid`]), excluded from the staleness age, and only
//! re-probed once every [`INVALID_CRS_REPROBE`] (so a fixed catalogue, or a
//! one-off upstream misfire, heals on its own) instead of every cycle.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Roughly how many stations one cycle gets through in production (logs,
/// 2026-09-27: 355-382). Only used to spread restart offsets; nothing
/// depends on it being exact.
const TYPICAL_CYCLE_CAPACITY: u64 = 367;

/// How long a station LDBWS rejected as an invalid CRS is left out of the
/// cycle before it is tried again: one request an hour per such station,
/// instead of one a cycle.
pub(crate) const INVALID_CRS_REPROBE: Duration = Duration::from_secs(3600);

/// The rotation's state for the life of the process.
#[derive(Debug)]
pub(crate) struct Rotation {
    /// The first station the next cycle samples. `None` until the first
    /// cycle has run, when the clock-derived offset is used instead.
    next_start: Option<String>,
    /// When each station last produced a sample (success only).
    last_sampled: HashMap<String, Instant>,
    /// Stations never sampled by this process count as stale since this.
    started: Instant,
    /// Stations LDBWS answered "Invalid crs code supplied" for, and when it
    /// last did. Excluded from [`Rotation::stalest_age`], and from the cycle
    /// until [`INVALID_CRS_REPROBE`] has passed.
    invalid: HashMap<String, Instant>,
    /// Progress through the current full rotation: stations completed
    /// since it began, over how many cycles, and when it began (the end of
    /// the previous full rotation, or process start).
    lap_completed: usize,
    lap_cycles: u32,
    lap_began: Instant,
    /// Whether the stalest station was last reported older than the
    /// aggregator's limit, so only changes are logged.
    stale: bool,
}

/// One full pass over the station list, as reported by
/// [`Rotation::record_progress`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FullRotation {
    /// Cycles the pass took (including the one that finished it).
    pub cycles: u32,
    /// From the end of the previous full pass (or process start) to the
    /// end of the cycle that finished this one: roughly how long a station
    /// waits between samples.
    pub duration: Duration,
}

impl Rotation {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            next_start: None,
            last_sampled: HashMap::new(),
            started: now,
            invalid: HashMap::new(),
            lap_completed: 0,
            lap_cycles: 0,
            lap_began: now,
            stale: false,
        }
    }

    /// Records whether the stalest station is currently too old for the
    /// aggregator, returning the new state only when it changed.
    pub(crate) fn note_stale(&mut self, stale: bool) -> Option<bool> {
        (std::mem::replace(&mut self.stale, stale) != stale).then_some(stale)
    }

    /// Counts one cycle that completed `completed` of the `total` stations
    /// it was given. Returns the finished pass once the stations completed
    /// since the last one add up to the whole list; any overshoot counts
    /// towards the next pass.
    pub(crate) fn record_progress(
        &mut self,
        total: usize,
        completed: usize,
        now: Instant,
    ) -> Option<FullRotation> {
        if total == 0 {
            return None;
        }
        self.lap_completed += completed;
        self.lap_cycles += 1;
        if self.lap_completed < total {
            return None;
        }
        let full = FullRotation {
            cycles: self.lap_cycles,
            duration: now.saturating_duration_since(self.lap_began),
        };
        // `completed <= total`, so at most one pass finishes per cycle.
        self.lap_completed = (self.lap_completed - total).min(total - 1);
        self.lap_cycles = 0;
        self.lap_began = now;
        Some(full)
    }

    /// `stations` (sorted and deduplicated here, so the order does not
    /// depend on the caller) rotated to start at this cycle's offset.
    pub(crate) fn order(
        &self,
        stations: &[String],
        unix_secs: u64,
        poll_interval_secs: u64,
    ) -> Vec<String> {
        let mut sorted = stations.to_vec();
        sorted.sort();
        sorted.dedup();
        if sorted.is_empty() {
            return sorted;
        }
        let start = match &self.next_start {
            // The first station at or after the remembered one, so a
            // station removed from the list does not reset the rotation.
            Some(next) => sorted.partition_point(|crs| crs < next) % sorted.len(),
            None => clock_offset(unix_secs, poll_interval_secs, sorted.len()),
        };
        sorted.rotate_left(start);
        sorted
    }

    /// `ordered` minus the stations known to be invalid whose re-probe is
    /// not yet due, keeping the rotation order. This is the list a cycle
    /// actually polls.
    pub(crate) fn pollable(&self, ordered: &[String], now: Instant) -> Vec<String> {
        ordered
            .iter()
            .filter(|crs| match self.invalid.get(*crs) {
                Some(rejected) => now.saturating_duration_since(*rejected) >= INVALID_CRS_REPROBE,
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Records that LDBWS rejected `crs` as an invalid CRS code. Returns
    /// `true` only the first time (until it recovers or leaves the list),
    /// so the caller logs and flags it once rather than every re-probe.
    pub(crate) fn mark_invalid(&mut self, crs: &str, now: Instant) -> bool {
        self.invalid.insert(crs.to_string(), now).is_none()
    }

    /// Forgets invalid stations no longer in `stations` (the catalogue was
    /// fixed and api dropped them), returning them so their metric can be
    /// cleared.
    pub(crate) fn prune_invalid(&mut self, stations: &[String]) -> Vec<String> {
        let mut gone: Vec<String> = self
            .invalid
            .keys()
            .filter(|crs| !stations.contains(crs))
            .cloned()
            .collect();
        gone.sort();
        for crs in &gone {
            self.invalid.remove(crs);
        }
        gone
    }

    /// The stations currently known to be invalid, sorted.
    #[cfg(test)]
    pub(crate) fn invalid_stations(&self) -> Vec<&str> {
        let mut v: Vec<&str> = self.invalid.keys().map(String::as_str).collect();
        v.sort_unstable();
        v
    }

    /// Records one cycle: `ordered` is the list the cycle polled (what
    /// [`Rotation::pollable`] returned), `completed` how many of them were
    /// attempted to completion (success or failure) before the budget ran
    /// out, and `sampled` the stations that produced a sample. The next
    /// cycle starts at the first station not completed. Returns the
    /// sampled stations that had been marked invalid (they recovered).
    /// `main.rs` calls [`Rotation::advance`] and [`Rotation::note_sampled`]
    /// itself (it advances only once api took the samples); the tests
    /// drive whole cycles through this.
    #[cfg(test)]
    pub(crate) fn finish_cycle<'a>(
        &mut self,
        ordered: &[String],
        completed: usize,
        sampled: impl IntoIterator<Item = &'a str>,
        now: Instant,
    ) -> Vec<String> {
        self.advance(ordered, completed);
        sampled
            .into_iter()
            .filter(|crs| self.note_sampled(crs, now))
            .map(str::to_string)
            .collect()
    }

    /// Moves the next cycle's start past the first `completed` stations of
    /// `ordered` (the list the cycle polled). `main.rs` calls this only once
    /// the cycle's samples were delivered to `api`; see [`Rotation::hold`].
    pub(crate) fn advance(&mut self, ordered: &[String], completed: usize) {
        if !ordered.is_empty() {
            self.next_start = Some(ordered[completed % ordered.len()].clone());
        }
    }

    /// Keeps the next cycle's start where this cycle's was, for a cycle
    /// whose samples `api` did not take: the next cycle samples the same
    /// stations again, so no station is skipped. `ordered` is what
    /// [`Rotation::order`] returned this cycle; this pins its first station
    /// when the start was still the clock-derived one, which would
    /// otherwise move on with the clock.
    pub(crate) fn hold(&mut self, ordered: &[String]) {
        if self.next_start.is_none() {
            self.next_start = ordered.first().cloned();
        }
    }

    /// Records that `crs` produced a sample, taken at `at`, that reached
    /// `api`. Returns `true` if it had been marked invalid (it recovered).
    pub(crate) fn note_sampled(&mut self, crs: &str, at: Instant) -> bool {
        let last = self.last_sampled.entry(crs.to_string()).or_insert(at);
        *last = (*last).max(at);
        self.invalid.remove(crs).is_some()
    }

    /// How long ago the least recently sampled station in `stations` was
    /// sampled; a station never sampled by this process counts from
    /// process start. Stations known to be invalid are left out: they are
    /// a catalogue error with their own metric and alert, not staleness.
    /// `Duration::ZERO` for an empty list.
    pub(crate) fn stalest_age(&self, stations: &[String], now: Instant) -> Duration {
        stations
            .iter()
            .filter(|crs| !self.invalid.contains_key(*crs))
            .map(|crs| {
                let since = self.last_sampled.get(crs).copied().unwrap_or(self.started);
                now.saturating_duration_since(since)
            })
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

/// The start offset for a process's first cycle -- see the module docs.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the result is reduced modulo len, so it fits in usize"
)]
fn clock_offset(unix_secs: u64, poll_interval_secs: u64, len: usize) -> usize {
    let cycle_number = unix_secs / poll_interval_secs.max(1);
    (cycle_number.wrapping_mul(TYPICAL_CYCLE_CAPACITY) % len as u64) as usize
}

#[cfg(test)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "test code: casts of small known test values"
)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn stations(n: usize) -> Vec<String> {
        // Three-letter, sorted, distinct -- like real CRS codes.
        (0..n)
            .map(|i| {
                let a = (b'A' + (i / 676) as u8) as char;
                let b = (b'A' + ((i / 26) % 26) as u8) as char;
                let c = (b'A' + (i % 26) as u8) as char;
                format!("{a}{b}{c}")
            })
            .collect()
    }

    /// Runs `cycles` cycles in which the budget allows exactly `capacity`
    /// stations each, returning the stations each cycle attempted.
    fn simulate(
        all: &[String],
        capacity: usize,
        cycles: usize,
        unix_secs: u64,
    ) -> Vec<Vec<String>> {
        let mut rotation = Rotation::new(Instant::now());
        let mut attempted = Vec::new();
        for cycle in 0..cycles {
            let ordered = rotation.order(all, unix_secs + 60 * cycle as u64, 60);
            let completed = capacity.min(ordered.len());
            let this_cycle: Vec<String> = ordered[..completed].to_vec();
            rotation.finish_cycle(
                &ordered,
                completed,
                this_cycle.iter().map(String::as_str),
                Instant::now(),
            );
            attempted.push(this_cycle);
        }
        attempted
    }

    /// SVC-04's acceptance test: with 560 stations and any per-cycle
    /// capacity, every station is attempted within
    /// `ceil(560 / capacity)` consecutive cycles, and each cycle attempts
    /// exactly as many stations as it would have without rotation.
    #[test]
    fn every_station_is_covered_within_ceil_n_over_capacity_cycles() {
        let all = stations(560);
        for capacity in [1, 7, 100, 355, 367, 382, 559, 560, 600] {
            let cycles = 560usize.div_ceil(capacity);
            for unix_secs in [0, 1_790_000_000, 1_790_000_123] {
                let attempted = simulate(&all, capacity, cycles, unix_secs);
                let covered: HashSet<&String> = attempted.iter().flatten().collect();
                assert_eq!(
                    covered.len(),
                    560,
                    "capacity {capacity}: {cycles} cycles must cover every station"
                );
                for cycle in &attempted {
                    assert_eq!(
                        cycle.len(),
                        capacity.min(560),
                        "the per-cycle request count is unchanged by rotation"
                    );
                }
            }
        }
    }

    /// Each cycle picks up exactly where the previous one stopped.
    #[test]
    fn consecutive_cycles_continue_where_the_previous_one_stopped() {
        let all = stations(10);
        let attempted = simulate(&all, 4, 3, 0);
        assert_eq!(attempted[1][0], all[4]);
        assert_eq!(attempted[2][0], all[8]);
        assert_eq!(attempted[2][2], all[0], "wraps round the end of the list");
    }

    /// A timed-out station (not completed) is the first one next cycle.
    #[test]
    fn the_station_cut_off_by_the_budget_starts_the_next_cycle() {
        let all = stations(5);
        let mut rotation = Rotation::new(Instant::now());
        let ordered = rotation.order(&all, 0, 60);
        rotation.finish_cycle(&ordered, 2, [], Instant::now());
        assert_eq!(rotation.order(&all, 60, 60)[0], ordered[2]);
    }

    /// A cycle whose samples were not delivered leaves the start where it
    /// was, including a fresh process's clock-derived start, which would
    /// otherwise move on with the clock.
    #[test]
    fn a_held_cycle_starts_the_next_one_at_the_same_station() {
        let all = stations(560);
        let mut rotation = Rotation::new(Instant::now());
        let first = rotation.order(&all, 1_790_000_000, 60);
        rotation.hold(&first);
        assert_eq!(rotation.order(&all, 1_790_000_060, 60)[0], first[0]);

        rotation.advance(&first, 2);
        let second = rotation.order(&all, 1_790_000_120, 60);
        assert_eq!(second[0], first[2]);
        rotation.hold(&second);
        assert_eq!(rotation.order(&all, 1_790_000_180, 60)[0], first[2]);
    }

    /// A restart does not start at "A" every time: the first cycle's offset
    /// comes from the clock and moves on by about a cycle's worth of
    /// stations per poll interval.
    #[test]
    fn a_restart_starts_at_a_clock_derived_offset() {
        let all = stations(560);
        let fresh = Rotation::new(Instant::now());
        let a = fresh.order(&all, 1_790_000_000, 60);
        let b = fresh.order(&all, 1_790_000_060, 60);
        assert_ne!(a[0], all[0]);
        let pos = |crs: &String| all.iter().position(|s| s == crs).unwrap();
        assert_eq!(
            (pos(&b[0]) + 560 - pos(&a[0])) % 560,
            TYPICAL_CYCLE_CAPACITY as usize % 560
        );
    }

    /// A change in the station list keeps the rotation where it was rather
    /// than resetting it.
    #[test]
    fn the_rotation_survives_a_station_being_removed() {
        let mut all = stations(10);
        let mut rotation = Rotation::new(Instant::now());
        let ordered = rotation.order(&all, 0, 60);
        rotation.finish_cycle(&ordered, 3, [], Instant::now());
        let next = ordered[3].clone();
        all.retain(|crs| crs != &next);
        let reordered = rotation.order(&all, 60, 60);
        assert!(reordered[0] > next || reordered[0] == all[0]);
        assert_eq!(reordered.len(), 9);
    }

    #[test]
    fn stalest_age_counts_never_sampled_stations_from_process_start() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        let all = stations(3);
        rotation.finish_cycle(
            &all,
            2,
            [all[0].as_str(), all[1].as_str()],
            started + Duration::from_secs(50),
        );
        let now = started + Duration::from_secs(100);
        assert_eq!(rotation.stalest_age(&all, now), Duration::from_secs(100));
        rotation.finish_cycle(
            &all,
            1,
            [all[2].as_str()],
            started + Duration::from_secs(90),
        );
        assert_eq!(rotation.stalest_age(&all, now), Duration::from_secs(50));
        assert_eq!(rotation.stalest_age(&[], now), Duration::ZERO);
    }

    /// The prod incident (2026-09-28): one station LDBWS always rejects
    /// used to hold the stalest age at the process's uptime forever. Once
    /// marked invalid it no longer counts, and the rest of the list's real
    /// staleness shows through.
    #[test]
    fn a_permanently_invalid_station_does_not_count_as_stale() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        let all = stations(4);
        let bad = all[1].clone();
        let mut now = started;
        for _ in 0..200 {
            now += Duration::from_secs(60);
            let polled = rotation.pollable(&all, now);
            let sampled: Vec<&str> = polled
                .iter()
                .filter(|crs| **crs != bad)
                .map(String::as_str)
                .collect();
            if polled.contains(&bad) {
                rotation.mark_invalid(&bad, now);
            }
            rotation.finish_cycle(&polled, polled.len(), sampled, now);
        }
        // 200 minutes after start (well past the 7200s alert threshold),
        // every valid station was sampled this cycle.
        assert_eq!(rotation.stalest_age(&all, now), Duration::ZERO);
        assert_eq!(rotation.invalid_stations(), vec![bad.as_str()]);
    }

    /// Only the first rejection is "new"; re-probes are hourly, not every
    /// cycle; and a successful re-probe clears it.
    #[test]
    fn an_invalid_station_is_reprobed_hourly_and_recovers() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        let all = stations(3);
        let bad = all[2].clone();

        assert!(rotation.mark_invalid(&bad, started));
        assert!(!rotation.mark_invalid(&bad, started), "logged once only");

        let soon = started + Duration::from_secs(60);
        assert_eq!(rotation.pollable(&all, soon), all[..2].to_vec());
        let due = started + INVALID_CRS_REPROBE;
        assert_eq!(rotation.pollable(&all, due), all);

        let recovered = rotation.finish_cycle(&all, 3, all.iter().map(String::as_str), due);
        assert_eq!(recovered, vec![bad.clone()]);
        assert!(rotation.invalid_stations().is_empty());
        assert_eq!(rotation.pollable(&all, due), all);
    }

    /// Production's shape (2026-10-01): ~255 of 560 stations per 60 s
    /// cycle. A pass finishes every 3 cycles, then every 2 or 3 as the
    /// overshoot carries over, and its duration is the time since the last.
    #[test]
    fn full_rotations_are_reported_as_the_completed_counts_add_up() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        let at = |cycle: u64| started + Duration::from_secs(60 * cycle);
        let passes: Vec<(u64, FullRotation)> = (1..=7)
            .filter_map(|cycle| {
                rotation
                    .record_progress(560, 255, at(cycle))
                    .map(|full| (cycle, full))
            })
            .collect();
        let minute = |n: u64| Duration::from_secs(60 * n);
        assert_eq!(
            passes,
            vec![
                // 765 >= 560, 205 carried over
                (
                    3,
                    FullRotation {
                        cycles: 3,
                        duration: minute(3)
                    }
                ),
                // 205 + 510 = 715, 155 carried
                (
                    5,
                    FullRotation {
                        cycles: 2,
                        duration: minute(2)
                    }
                ),
                // 155 + 510 = 665
                (
                    7,
                    FullRotation {
                        cycles: 2,
                        duration: minute(2)
                    }
                ),
            ]
        );
    }

    #[test]
    fn an_empty_list_or_a_cycle_with_no_progress_finishes_nothing() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        assert_eq!(rotation.record_progress(0, 0, started), None);
        assert_eq!(rotation.record_progress(5, 0, started), None);
        let end = started + Duration::from_secs(120);
        assert_eq!(
            rotation.record_progress(5, 5, end),
            Some(FullRotation {
                cycles: 2,
                duration: Duration::from_secs(120)
            })
        );
    }

    #[test]
    fn staleness_is_only_reported_when_it_changes() {
        let mut rotation = Rotation::new(Instant::now());
        assert_eq!(rotation.note_stale(false), None);
        assert_eq!(rotation.note_stale(true), Some(true));
        assert_eq!(rotation.note_stale(true), None);
        assert_eq!(rotation.note_stale(false), Some(false));
    }

    /// Fixing the catalogue drops the station from api's list; the invalid
    /// entry goes with it (and is reported so its gauge can be zeroed).
    #[test]
    fn an_invalid_station_removed_from_the_list_is_forgotten() {
        let started = Instant::now();
        let mut rotation = Rotation::new(started);
        let mut all = stations(3);
        let bad = all[0].clone();
        rotation.mark_invalid(&bad, started);
        assert!(rotation.prune_invalid(&all).is_empty());
        all.remove(0);
        assert_eq!(rotation.prune_invalid(&all), vec![bad]);
        assert!(rotation.invalid_stations().is_empty());
    }
}
