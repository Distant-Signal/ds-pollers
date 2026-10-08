//! Station samples sampled but not yet delivered to `api`.
//!
//! **Why** (2026-10-08): every deploy restarts `api` (a `Recreate` rollout)
//! and the poller's POST retry budget is a quarter of its interval (15 s),
//! so each cycle that ran while `api` was down threw its whole batch away,
//! about three cycles' worth of samples per deploy. The rotation had
//! already moved past those stations too, so they waited a whole extra
//! rotation for their next sample.
//!
//! Now a cycle's samples join this set, and every POST sends the whole set.
//! It is emptied only by a successful POST, so a sample is either delivered
//! or still held. It keeps only the latest sample per station:
//! `station_samples` is keyed by CRS and holds only the latest sample per
//! station, so an older one would be overwritten anyway. Each held sample
//! keeps its original `polled_at`; `api` stores it as given (it does not
//! check its age), and the aggregator ignores one more than
//! [`common::STATION_SAMPLE_MAX_AGE_MINUTES`] old, as it would any stale
//! sample.
//!
//! **Bound.** While the POST fails, the rotation does not advance
//! (`main.rs`), so each cycle re-samples the same stations and the set stays
//! at about one cycle's stations (255-380 in production). It can never
//! hold more than one sample per station on the list, about 565. As a backstop
//! against a much longer list, [`MAX_PENDING_STATIONS`] caps it, evicting the
//! oldest samples first. Production's largest stored board is 4.8 KB of
//! JSON (2026-10-08, `station_samples`), so even a full set at the cap is a
//! few tens of MB in memory at most, inside the pod's 192 Mi limit (the
//! pod uses about 18 Mi).

use std::collections::HashMap;
use std::time::Instant;

use common::StationSample;

/// At most this many stations' samples are held. About 2.7 times the
/// current list (~565 stations); see the module docs.
pub(crate) const MAX_PENDING_STATIONS: usize = 1_500;

/// One held sample, and when (on this process's clock) it was taken.
#[derive(Debug)]
struct Held {
    sample: StationSample,
    sampled_at: Instant,
}

/// The samples not yet delivered, at most one per station. See the module
/// docs.
#[derive(Debug, Default)]
pub(crate) struct PendingSamples {
    by_crs: HashMap<String, Held>,
}

impl PendingSamples {
    pub(crate) fn len(&self) -> usize {
        self.by_crs.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.by_crs.is_empty()
    }

    /// Adds a cycle's samples, each replacing any older one held for the
    /// same station, then evicts the oldest samples beyond
    /// [`MAX_PENDING_STATIONS`]. Returns how many were evicted.
    pub(crate) fn add(&mut self, samples: Vec<StationSample>, sampled_at: Instant) -> usize {
        for sample in samples {
            match self.by_crs.get(&sample.crs) {
                Some(held) if held.sample.polled_at > sample.polled_at => {}
                _ => {
                    self.by_crs
                        .insert(sample.crs.clone(), Held { sample, sampled_at });
                }
            }
        }
        let excess = self.by_crs.len().saturating_sub(MAX_PENDING_STATIONS);
        if excess > 0 {
            let mut by_age: Vec<(chrono::DateTime<chrono::Utc>, String)> = self
                .by_crs
                .iter()
                .map(|(crs, held)| (held.sample.polled_at, crs.clone()))
                .collect();
            by_age.sort();
            for (_, crs) in by_age.into_iter().take(excess) {
                self.by_crs.remove(&crs);
            }
        }
        excess
    }

    /// Every held sample, sorted by CRS: the body of the next POST.
    pub(crate) fn batch(&self) -> Vec<&StationSample> {
        let mut batch: Vec<&StationSample> = self.by_crs.values().map(|h| &h.sample).collect();
        batch.sort_by(|a, b| a.crs.cmp(&b.crs));
        batch
    }

    /// Empties the set after a successful POST (or a rejected one, which
    /// would only be rejected again), returning each station delivered and
    /// when it was sampled.
    pub(crate) fn clear(&mut self) -> Vec<(String, Instant)> {
        let mut delivered: Vec<(String, Instant)> = self
            .by_crs
            .drain()
            .map(|(crs, held)| (crs, held.sampled_at))
            .collect();
        delivered.sort();
        delivered
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    use super::*;

    fn sample(crs: &str, minute: u32) -> StationSample {
        StationSample {
            crs: crs.to_string(),
            polled_at: Utc.with_ymd_and_hms(2026, 10, 8, 9, minute, 0).unwrap(),
            departures: Vec::new(),
        }
    }

    #[test]
    fn keeps_only_the_latest_sample_per_station() {
        let start = Instant::now();
        let mut pending = PendingSamples::default();
        pending.add(vec![sample("AAA", 0), sample("BBB", 0)], start);
        pending.add(
            vec![sample("AAA", 1), sample("CCC", 1)],
            start + Duration::from_secs(60),
        );
        // An older sample never replaces a newer one.
        pending.add(vec![sample("CCC", 0)], start);

        let batch = pending.batch();
        let got: Vec<(&str, u32)> = batch
            .iter()
            .map(|s| (s.crs.as_str(), chrono::Timelike::minute(&s.polled_at)))
            .collect();
        assert_eq!(got, vec![("AAA", 1), ("BBB", 0), ("CCC", 1)]);

        let delivered = pending.clear();
        assert_eq!(
            delivered,
            vec![
                ("AAA".to_string(), start + Duration::from_secs(60)),
                ("BBB".to_string(), start),
                ("CCC".to_string(), start + Duration::from_secs(60)),
            ]
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn evicts_the_oldest_samples_beyond_the_cap() {
        let mut pending = PendingSamples::default();
        let now = Instant::now();
        let old: Vec<StationSample> = (0..10).map(|i| sample(&format!("O{i:02}"), 0)).collect();
        pending.add(old, now);
        let fresh: Vec<StationSample> = (0..MAX_PENDING_STATIONS)
            .map(|i| sample(&format!("N{i:04}"), 5))
            .collect();

        let evicted = pending.add(fresh, now);

        assert_eq!(evicted, 10);
        assert_eq!(pending.len(), MAX_PENDING_STATIONS);
        assert!(pending.batch().iter().all(|s| s.crs.starts_with('N')));
    }
}
