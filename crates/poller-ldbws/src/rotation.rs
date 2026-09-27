//! Which stations a cycle samples first (SVC-04).
//!
//! Each cycle samples stations one at a time until the 45 s
//! `CYCLE_TIME_BUDGET` runs out, which in production is about 350-380 of
//! the 560 sample stations. The list from `api` is sorted, so every cycle
//! used to start at "AAP" and stop around "RMD": S-Z were never sampled.
//!
//! Now each cycle starts where the previous one stopped (the first station
//! it did not get to), wrapping round the end of the list. The number of
//! requests per cycle is unchanged -- still whatever fits the budget -- so
//! this adds no LDBWS request volume (LEG-18). A station is
//! sampled at least once every `ceil(stations / per-cycle capacity)` cycles.
//!
//! The position survives list changes (it is kept as a CRS, not an index)
//! but not restarts. A restart starts from a position derived from the wall
//! clock instead, so a crash loop does not keep resampling the start of the
//! alphabet: `(unix_time / poll_interval) * TYPICAL_CYCLE_CAPACITY`, modulo
//! the station count -- the offset the rotation would roughly have reached
//! had it run since the epoch.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Roughly how many stations one cycle gets through in production (logs,
/// 2026-09-27: 355-382). Only used to spread restart offsets; nothing
/// depends on it being exact.
const TYPICAL_CYCLE_CAPACITY: u64 = 367;

/// The rotation's state for the life of the process.
#[derive(Debug)]
pub struct Rotation {
    /// The first station the next cycle samples. `None` until the first
    /// cycle has run, when the clock-derived offset is used instead.
    next_start: Option<String>,
    /// When each station last produced a sample (success only).
    last_sampled: HashMap<String, Instant>,
    /// Stations never sampled by this process count as stale since this.
    started: Instant,
}

impl Rotation {
    pub fn new(now: Instant) -> Self {
        Self {
            next_start: None,
            last_sampled: HashMap::new(),
            started: now,
        }
    }

    /// `stations` (sorted and deduplicated here, so the order does not
    /// depend on the caller) rotated to start at this cycle's offset.
    pub fn order(
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

    /// Records one cycle: `ordered` is what [`Rotation::order`] returned,
    /// `completed` how many of them were attempted to completion (success
    /// or failure) before the budget ran out, and `sampled` the stations
    /// that produced a sample. The next cycle starts at the first station
    /// not completed.
    pub fn finish_cycle<'a>(
        &mut self,
        ordered: &[String],
        completed: usize,
        sampled: impl IntoIterator<Item = &'a str>,
        now: Instant,
    ) {
        if !ordered.is_empty() {
            self.next_start = Some(ordered[completed % ordered.len()].clone());
        }
        for crs in sampled {
            self.last_sampled.insert(crs.to_string(), now);
        }
    }

    /// How long ago the least recently sampled station in `stations` was
    /// sampled; a station never sampled by this process counts from
    /// process start. `Duration::ZERO` for an empty list.
    pub fn stalest_age(&self, stations: &[String], now: Instant) -> Duration {
        stations
            .iter()
            .map(|crs| {
                let since = self.last_sampled.get(crs).copied().unwrap_or(self.started);
                now.saturating_duration_since(since)
            })
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

/// The start offset for a process's first cycle -- see the module docs.
fn clock_offset(unix_secs: u64, poll_interval_secs: u64, len: usize) -> usize {
    let cycle_number = unix_secs / poll_interval_secs.max(1);
    (cycle_number.wrapping_mul(TYPICAL_CYCLE_CAPACITY) % len as u64) as usize
}

#[cfg(test)]
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
}
