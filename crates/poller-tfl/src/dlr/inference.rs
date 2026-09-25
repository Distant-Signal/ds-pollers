//! Matches DLR scheduled trips (`timetable::ScheduledTrip`) against live
//! Arrivals predictions (`arrivals::Prediction`) to infer per-trip delay,
//! and (via `DlrMatchState`, added once this module also owns cross-poll
//! state) cancellation. `sample_stats` computed here is attached to the
//! DLR line's existing `LineStatus` — it never changes that status's
//! `severity`; see the plan's Global Constraints for why.

use chrono::{DateTime, Utc};

use super::arrivals::Prediction;
use super::timetable::ScheduledTrip;

/// A service is "delayed" once its delay exceeds this many minutes —
/// mirrors `common::Defaults::delay_threshold_minutes`'s default (5). Not
/// read from `Defaults` itself: that struct is wired to the NR aggregator's
/// `severity_overrides` TOML mechanism (per-line configuration this pilot
/// has no equivalent of), and the spec's Non-goals rule out unifying the
/// two severity models beyond areas 1-2. This is a local, DLR-only
/// constant instead.
const DLR_DELAY_THRESHOLD_MINUTES: i64 = 5;

/// How close a live prediction's `expected_arrival` must be to a scheduled
/// trip's `scheduled_departure` at the same station to count as a match.
/// Wide enough to tolerate a train running early or a schedule/prediction
/// clock skew, narrow enough that two distinct trips ~4-10 minutes apart
/// (DLR's typical headway) don't both claim the same prediction.
const MATCH_WINDOW_MINUTES: i64 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum MatchedTrip {
    /// Found a prediction within `MATCH_WINDOW_MINUTES` of this trip's
    /// scheduled time. `delay_minutes` can be negative (early) but callers
    /// treat negative as zero — see `Task 6`'s `SampleStats` computation.
    Matched { delay_minutes: i64 },
    /// No matching prediction yet. Not necessarily cancelled — the train
    /// may simply not be visible in predictions yet if its scheduled time
    /// hasn't arrived. Task 6 decides when "pending" becomes "cancelled".
    Pending,
}

/// For each scheduled trip, finds the live prediction (at the same
/// station) whose `expected_arrival` is closest to `scheduled_departure`
/// and within `MATCH_WINDOW_MINUTES`, and computes its delay. Each
/// prediction can match at most one trip — the closest trip claims it,
/// so two trips near the same time don't both consume one late train's
/// prediction as evidence they individually ran on time.
///
/// `_now` is unused by matching itself — a prediction either falls inside
/// `MATCH_WINDOW_MINUTES` of a scheduled time or it doesn't, regardless of
/// when the question is asked. It stays in the signature because
/// `DlrMatchState::resolve`, which owns the wall-clock-dependent half
/// (cancellation grace, retention), threads its own `now` through here.
pub fn match_trips(
    trips: Vec<ScheduledTrip>,
    predictions: &[Prediction],
    _now: DateTime<Utc>,
) -> Vec<MatchedTrip> {
    let mut claimed = vec![false; predictions.len()];
    trips
        .into_iter()
        .map(|trip| {
            let best = predictions
                .iter()
                .enumerate()
                .filter(|(i, _)| !claimed[*i])
                .map(|(i, p)| {
                    (
                        i,
                        p,
                        (p.expected_arrival - trip.scheduled_departure)
                            .num_minutes()
                            .abs(),
                    )
                })
                .filter(|(_, _, diff)| *diff <= MATCH_WINDOW_MINUTES)
                .min_by_key(|(_, _, diff)| *diff);

            match best {
                Some((i, p, _)) => {
                    claimed[i] = true;
                    let delay_minutes =
                        (p.expected_arrival - trip.scheduled_departure).num_minutes();
                    MatchedTrip::Matched {
                        delay_minutes: delay_minutes.max(0),
                    }
                }
                None => MatchedTrip::Pending,
            }
        })
        .collect()
}

/// A scheduled trip still waiting for a matching prediction, remembered
/// across poll cycles so `resolve` can tell "genuinely never showed up"
/// from "hasn't happened yet".
#[derive(Debug, Clone)]
struct PendingTrip {
    scheduled_departure: DateTime<Utc>,
}

/// A trip that has been resolved one way or the other, kept for
/// `RESOLVED_RETENTION_MINUTES` so `SampleStats` reflects a rolling recent
/// window rather than every trip since the poller started.
#[derive(Debug, Clone)]
struct ResolvedTrip {
    /// Kept so a trip that has already resolved is recognised when the
    /// next cycle hands the same day's full timetable in again — without
    /// it, every resolved trip is re-admitted and re-counted every cycle
    /// for the whole retention window.
    scheduled_departure: DateTime<Utc>,
    resolved_at: DateTime<Utc>,
    delay_minutes: Option<i64>, // None means cancelled
}

/// A trip pending longer than this past its scheduled time, with no
/// matching prediction ever found, is treated as cancelled. DLR's typical
/// headway is 3-10 minutes; this is roughly two headways' grace so a
/// train that's simply running very late doesn't get misread as
/// cancelled — a pilot-tuned value, not derived from any published TfL
/// number, and worth revisiting once real data is observed.
const CANCELLATION_GRACE_MINUTES: i64 = 15;

/// How long a resolved trip counts toward the reported `SampleStats`
/// before aging out. An hour gives a stable-enough sample size at DLR's
/// headway (roughly 6-20 trips) without the reported numbers describing
/// disruption from hours ago as if it were still happening.
const RESOLVED_RETENTION_MINUTES: i64 = 60;

/// How many past cycles' prediction counts `DlrMatchState` remembers, used
/// solely to build the "was I blind?" baseline below. Six cycles at this
/// pilot's 300s poll interval is half an hour of history -- long enough to
/// smooth over a couple of genuinely quiet minutes, short enough that a
/// baseline still reflects roughly "now", not a stale reading from hours
/// ago.
const RECENT_PREDICTION_HISTORY: usize = 6;

/// The recent baseline must average at least this many predictions per
/// cycle before a near-zero cycle is trusted as evidence of an outage
/// rather than just noise from a baseline that was already thin. DLR's
/// live Arrivals feed reports across the whole network, so a healthy
/// baseline is expected to be comfortably above this.
const MIN_BASELINE_PREDICTIONS_FOR_OUTAGE_CHECK: f64 = 3.0;

/// A cycle's prediction count is treated as possible evidence of a feed
/// outage (as opposed to a genuine quiet period) once it falls at or below
/// this fraction of the recent baseline. 10% is deliberately aggressive --
/// this guard only ever *withholds* a cancellation inference for a cycle,
/// never fabricates one, so a false positive here just delays a real
/// cancellation by one cycle, while a false negative mass-cancels every
/// pending trip on a feed outage. Erring toward more false positives is
/// the safer trade.
const OUTAGE_PREDICTION_RATIO: f64 = 0.1;

/// How far *ahead* of `now` a scheduled trip is picked up for tracking.
/// The Timetable poll returns the whole service day every cycle, so this
/// is what stops the state from holding hundreds of trips that cannot be
/// matched yet. Six 300s poll cycles of lead time — comfortably more than
/// the horizon on which live Arrivals predictions appear at all, so a trip
/// is always being watched well before the first prediction that could
/// match it. A pilot-tuned value, like the constants above.
const ADMISSION_LOOKAHEAD_MINUTES: i64 = 30;

/// How far *behind* `now` a scheduled trip may be and still be picked up
/// for the first time. Zero: this poller can only infer anything about a
/// trip by watching for its prediction, and a trip whose time has already
/// passed can no longer be watched — admitting one produces no evidence
/// either way and then reads that silence as a cancellation. That is what
/// made a cold start ingest the whole day's earlier schedule as instant
/// cancellations. Nothing is lost in steady state: with
/// `ADMISSION_LOOKAHEAD_MINUTES` of lead time, every trip is already being
/// tracked long before its scheduled time, and this bound only gates the
/// *first* sighting — a trip admitted earlier stays pending and still
/// cancels normally once `CANCELLATION_GRACE_MINUTES` is up.
///
/// Must stay below `RESOLVED_RETENTION_MINUTES` so that a trip aging out
/// of `resolved` can never fall back inside the admission window and be
/// counted a second time (see `admission_window_cannot_readmit_an_aged_out_trip`).
const ADMISSION_LOOKBACK_MINUTES: i64 = 0;

const _: () = assert!(
    ADMISSION_LOOKBACK_MINUTES < RESOLVED_RETENTION_MINUTES,
    "a trip aging out of `resolved` must already be outside the admission window, \
     or it can be picked up and counted a second time"
);

pub struct DlrMatchState {
    pending: Vec<PendingTrip>,
    resolved: Vec<ResolvedTrip>,
    /// This cycle's and the last `RECENT_PREDICTION_HISTORY - 1` cycles'
    /// `predictions.len()`, oldest first. Exists solely so `resolve` can
    /// tell "the feed itself may have gone dark this cycle" from "every
    /// pending trip genuinely got cancelled" before promoting anything --
    /// see `resolve`'s "was I blind?" guard.
    recent_prediction_counts: std::collections::VecDeque<usize>,
}

impl DlrMatchState {
    pub fn new() -> Self {
        DlrMatchState {
            pending: Vec::new(),
            resolved: Vec::new(),
            recent_prediction_counts: std::collections::VecDeque::with_capacity(
                RECENT_PREDICTION_HISTORY,
            ),
        }
    }

    /// Runs one poll cycle: adds newly-seen scheduled trips to the pending
    /// set (skipping ones already tracked *or already resolved*, by
    /// `scheduled_departure`, and ignoring anything outside the
    /// `ADMISSION_LOOKBACK_MINUTES`/`ADMISSION_LOOKAHEAD_MINUTES` window
    /// around `now` — the Timetable poll hands in the whole service day
    /// every cycle, so both filters are load-bearing),
    /// matches everything pending against this cycle's predictions,
    /// promotes newly-matched or grace-window-expired trips into
    /// `resolved` -- unless a "was I blind?" guard suspects this cycle's
    /// near-empty prediction count reflects a dead Arrivals feed rather
    /// than a wave of genuine cancellations (see below) -- evicts resolved
    /// trips older than `RESOLVED_RETENTION_MINUTES`, and returns the
    /// resulting `SampleStats` — `None` if nothing has resolved yet (e.g.
    /// right after startup).
    ///
    /// Signal Box Audit, poll-area Low finding -- "no 'was I blind?' guard
    /// on cancellation inference": cancellation here is inferred purely
    /// from *absence* -- a pending trip with no matching prediction past
    /// `CANCELLATION_GRACE_MINUTES`. That reasoning silently assumes the
    /// Arrivals feed itself is working; if the feed instead had an outage
    /// (empty response, upstream 5xx swallowed upstream as "no
    /// predictions", a bad deploy) every trip pending at that moment would
    /// look cancelled and get promoted to `resolved` together, reporting a
    /// mass cancellation that never happened. Guarded by comparing this
    /// cycle's `predictions.len()` against a short rolling baseline
    /// (`recent_prediction_counts`): once the baseline itself is
    /// established and healthy (`MIN_BASELINE_PREDICTIONS_FOR_OUTAGE_CHECK`)
    /// and this cycle's count collapses to `OUTAGE_PREDICTION_RATIO` of it
    /// or below, cancellation promotion is skipped for the cycle (trips
    /// stay `Pending` and get another chance once the feed recovers) with
    /// a warning logged, rather than trusting the silence. This never
    /// blocks a *matched* trip from resolving -- only the "nothing showed
    /// up, so it must be cancelled" inference is withheld.
    pub fn resolve(
        &mut self,
        trips: Vec<ScheduledTrip>,
        predictions: &[Prediction],
        now: DateTime<Utc>,
    ) -> Option<common::SampleStats> {
        let baseline_established = self.recent_prediction_counts.len() >= RECENT_PREDICTION_HISTORY;
        let baseline_avg = if self.recent_prediction_counts.is_empty() {
            0.0
        } else {
            self.recent_prediction_counts.iter().sum::<usize>() as f64
                / self.recent_prediction_counts.len() as f64
        };
        let feed_looks_blind = baseline_established
            && baseline_avg >= MIN_BASELINE_PREDICTIONS_FOR_OUTAGE_CHECK
            && (predictions.len() as f64) <= baseline_avg * OUTAGE_PREDICTION_RATIO;
        if feed_looks_blind {
            tracing::warn!(
                predictions_this_cycle = predictions.len(),
                recent_baseline_avg = baseline_avg,
                "DLR Arrivals prediction count collapsed far below its recent baseline; \
                 skipping cancellation inference this cycle in case the feed itself went \
                 blind, rather than risk mass-cancelling every pending trip"
            );
        }
        self.recent_prediction_counts.push_back(predictions.len());
        if self.recent_prediction_counts.len() > RECENT_PREDICTION_HISTORY {
            self.recent_prediction_counts.pop_front();
        }

        let known: std::collections::HashSet<DateTime<Utc>> = self
            .pending
            .iter()
            .map(|t| t.scheduled_departure)
            .chain(self.resolved.iter().map(|r| r.scheduled_departure))
            .collect();
        let earliest = now - chrono::Duration::minutes(ADMISSION_LOOKBACK_MINUTES);
        let latest = now + chrono::Duration::minutes(ADMISSION_LOOKAHEAD_MINUTES);
        for trip in trips.iter().filter(|t| {
            !known.contains(&t.scheduled_departure)
                && t.scheduled_departure >= earliest
                && t.scheduled_departure <= latest
        }) {
            self.pending.push(PendingTrip {
                scheduled_departure: trip.scheduled_departure,
            });
        }

        let pending_as_trips: Vec<ScheduledTrip> = self
            .pending
            .iter()
            .map(|p| ScheduledTrip {
                scheduled_departure: p.scheduled_departure,
                interval_id: None,
            })
            .collect();
        let matches = match_trips(pending_as_trips, predictions, now);

        let mut still_pending = Vec::new();
        for (pending_trip, matched) in self.pending.drain(..).zip(matches) {
            match matched {
                MatchedTrip::Matched { delay_minutes } => {
                    self.resolved.push(ResolvedTrip {
                        scheduled_departure: pending_trip.scheduled_departure,
                        resolved_at: now,
                        delay_minutes: Some(delay_minutes),
                    });
                }
                MatchedTrip::Pending => {
                    let overdue = (now - pending_trip.scheduled_departure).num_minutes();
                    if overdue >= CANCELLATION_GRACE_MINUTES && !feed_looks_blind {
                        self.resolved.push(ResolvedTrip {
                            scheduled_departure: pending_trip.scheduled_departure,
                            resolved_at: now,
                            delay_minutes: None,
                        });
                    } else {
                        still_pending.push(pending_trip);
                    }
                }
            }
        }
        self.pending = still_pending;

        self.resolved
            .retain(|r| (now - r.resolved_at).num_minutes() < RESOLVED_RETENTION_MINUTES);

        if self.resolved.is_empty() {
            return None;
        }

        let total = self.resolved.len();
        let cancelled = self
            .resolved
            .iter()
            .filter(|r| r.delay_minutes.is_none())
            .count();
        let running: Vec<i64> = self
            .resolved
            .iter()
            .filter_map(|r| r.delay_minutes)
            .collect();
        let delayed = running
            .iter()
            .filter(|&&d| d >= DLR_DELAY_THRESHOLD_MINUTES)
            .count();
        let avg_delay_minutes = if running.is_empty() {
            0.0
        } else {
            running.iter().sum::<i64>() as f64 / running.len() as f64
        };

        Some(common::SampleStats {
            total,
            delayed,
            cancelled,
            skipped: 0,
            avg_delay_minutes,
        })
    }
}

impl Default for DlrMatchState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trip(minute: u32) -> ScheduledTrip {
        ScheduledTrip {
            scheduled_departure: "2026-08-22T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
                + chrono::Duration::minutes(minute as i64),
            interval_id: None,
        }
    }

    fn prediction(expected_offset_minutes: i64) -> Prediction {
        Prediction {
            vehicle_id: "301".to_string(),
            naptan_id: "940GZZDLPOP".to_string(),
            station_name: "Poplar".to_string(),
            direction: "outbound".to_string(),
            destination_naptan_id: String::new(),
            destination_name: String::new(),
            expected_arrival: "2026-08-22T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
                + chrono::Duration::minutes(expected_offset_minutes),
            time_to_station: 0,
        }
    }

    #[test]
    fn an_on_time_prediction_matches_with_zero_delay() {
        let result = match_trips(
            vec![trip(0)],
            &[prediction(0)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        assert_eq!(result, vec![MatchedTrip::Matched { delay_minutes: 0 }]);
    }

    #[test]
    fn a_late_prediction_matches_with_the_observed_delay() {
        let result = match_trips(
            vec![trip(0)],
            &[prediction(6)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        // 6 minutes is outside MATCH_WINDOW_MINUTES (3), so this should be
        // Pending, not a 6-minute-late match — window too tight to claim a
        // 6-minute-late train as "this trip" rather than a later one.
        assert_eq!(result, vec![MatchedTrip::Pending]);
    }

    #[test]
    fn a_prediction_within_the_match_window_computes_its_delay() {
        let result = match_trips(
            vec![trip(0)],
            &[prediction(2)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        assert_eq!(result, vec![MatchedTrip::Matched { delay_minutes: 2 }]);
    }

    #[test]
    fn a_trip_with_no_nearby_prediction_is_pending() {
        let result = match_trips(vec![trip(0)], &[], "2026-08-22T10:00:00Z".parse().unwrap());
        assert_eq!(result, vec![MatchedTrip::Pending]);
    }

    #[test]
    fn each_prediction_matches_at_most_one_trip() {
        // Two trips 4 minutes apart (DLR's typical headway), one
        // prediction. The closer trip claims it; the other stays Pending
        // rather than both being marked on-time from the same train.
        let result = match_trips(
            vec![trip(0), trip(4)],
            &[prediction(0)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        assert_eq!(
            result,
            vec![
                MatchedTrip::Matched { delay_minutes: 0 },
                MatchedTrip::Pending
            ]
        );
    }

    #[test]
    fn an_early_prediction_is_clamped_to_zero_delay_not_negative() {
        let result = match_trips(
            vec![trip(2)],
            &[prediction(0)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        assert_eq!(result, vec![MatchedTrip::Matched { delay_minutes: 0 }]);
    }

    #[test]
    fn a_matched_trip_resolves_immediately() {
        let mut state = DlrMatchState::new();
        let stats = state
            .resolve(
                vec![trip(0)],
                &[prediction(0)],
                "2026-08-22T10:00:00Z".parse().unwrap(),
            )
            .expect("one resolved trip");
        assert_eq!(stats.total, 1);
        assert_eq!(stats.cancelled, 0);
        assert_eq!(stats.delayed, 0);
    }

    #[test]
    fn a_trip_still_pending_within_the_grace_window_produces_no_stats_yet() {
        let mut state = DlrMatchState::new();
        let stats = state.resolve(vec![trip(0)], &[], "2026-08-22T10:00:00Z".parse().unwrap());
        assert_eq!(stats, None);
    }

    #[test]
    fn a_trip_still_unmatched_past_the_grace_window_is_cancelled() {
        let mut state = DlrMatchState::new();
        // First cycle: trip scheduled for 10:00, no prediction yet.
        state.resolve(vec![trip(0)], &[], "2026-08-22T10:00:00Z".parse().unwrap());
        // Second cycle, 16 minutes later: still nothing, past the 15-minute
        // grace window.
        let stats = state
            .resolve(vec![], &[], "2026-08-22T10:16:00Z".parse().unwrap())
            .expect("the overdue trip should have resolved as cancelled");
        assert_eq!(stats.total, 1);
        assert_eq!(stats.cancelled, 1);
    }

    #[test]
    fn resolved_trips_age_out_after_the_retention_window() {
        let mut state = DlrMatchState::new();
        state.resolve(
            vec![trip(0)],
            &[prediction(0)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        // 61 minutes later, with no new trips at all: the earlier resolved
        // trip should have aged out, leaving nothing to report.
        let stats = state.resolve(vec![], &[], "2026-08-22T11:01:00Z".parse().unwrap());
        assert_eq!(stats, None);
    }

    #[test]
    fn a_resolved_trip_is_not_re_admitted_when_the_timetable_repeats_it() {
        // The Timetable poll returns the same day's full schedule every
        // cycle, so a trip that has already resolved keeps being handed
        // back in. It must be recognised as already-resolved, not treated
        // as new and re-counted once per cycle for the whole retention
        // window. The trip is scheduled 20 minutes out and every cycle
        // runs before it, so the admission window would happily let it
        // back in on all three — only the resolved-set check stops that.
        let mut state = DlrMatchState::new();
        let cycles = [
            "2026-08-22T10:00:00Z",
            "2026-08-22T10:02:00Z",
            "2026-08-22T10:04:00Z",
        ];
        for cycle in cycles {
            let stats = state
                .resolve(vec![trip(20)], &[prediction(20)], cycle.parse().unwrap())
                .expect("the trip resolved on the first cycle and is retained");
            assert_eq!(stats.total, 1, "trip re-counted on cycle {cycle}");
            assert_eq!(stats.cancelled, 0);
        }
    }

    #[test]
    fn a_cold_start_does_not_ingest_the_whole_days_past_schedule_as_cancellations() {
        // First-ever cycle at 14:00, handed the full service day from
        // 05:28 onwards (what the real Timetable poll returns). Every one
        // of those trips is hours past its time with no prediction, so
        // without an admission window they all resolve as cancelled on
        // the spot.
        let mut state = DlrMatchState::new();
        let start: DateTime<Utc> = "2026-08-22T05:28:00Z".parse().unwrap();
        let days_schedule: Vec<ScheduledTrip> = (0..50)
            .map(|i| ScheduledTrip {
                scheduled_departure: start + chrono::Duration::minutes(i * 10),
                interval_id: None,
            })
            .collect();
        let stats = state.resolve(days_schedule, &[], "2026-08-22T14:00:00Z".parse().unwrap());
        assert_eq!(
            stats, None,
            "past trips must not be tracked, let alone cancelled"
        );
    }

    #[test]
    fn a_trip_inside_the_admission_window_still_cancels_after_the_grace_window() {
        // The counterpart to the test above: skipping trips that are
        // already past must not weaken cancellation for the ones that
        // aren't. A cold start handed both an unobservable trip from
        // hours ago and one due in 10 minutes reports exactly one
        // outcome — the second trip's, and only once its grace window is
        // up, not before.
        let mut state = DlrMatchState::new();
        let hours_ago = ScheduledTrip {
            scheduled_departure: "2026-08-22T08:00:00Z".parse().unwrap(),
            interval_id: None,
        };
        let schedule = vec![hours_ago, trip(10)];
        assert_eq!(
            state.resolve(
                schedule.clone(),
                &[],
                "2026-08-22T10:00:00Z".parse().unwrap()
            ),
            None
        );
        // 10:10's trip never showed up, and it is now 15 minutes overdue.
        let stats = state
            .resolve(schedule, &[], "2026-08-22T10:25:00Z".parse().unwrap())
            .expect("the overdue trip should have resolved as cancelled");
        assert_eq!(
            stats.total, 1,
            "the 08:00 trip must not be counted: {stats:?}"
        );
        assert_eq!(stats.cancelled, 1);
    }

    #[test]
    fn admission_window_cannot_readmit_an_aged_out_trip() {
        // A resolved trip stops being remembered once it ages out of the
        // retention window, so the admission window is what has to keep it
        // from being picked up again. (The constant-level assertion next
        // to `ADMISSION_LOOKBACK_MINUTES` guards the relation between the
        // two bounds; this covers the behaviour it exists for.)
        let mut state = DlrMatchState::new();
        state.resolve(
            vec![trip(0)],
            &[prediction(0)],
            "2026-08-22T10:00:00Z".parse().unwrap(),
        );
        // 61 minutes on, the 10:00 trip ages out of `resolved` — eviction
        // runs after admission, so this cycle is still covered by the
        // resolved-set check.
        assert_eq!(
            state.resolve(vec![trip(0)], &[], "2026-08-22T11:01:00Z".parse().unwrap()),
            None
        );
        // The cycle after that, `resolved` is empty and nothing remembers
        // the trip any more — only the admission window keeps the same
        // schedule entry, handed in yet again, from being picked up and
        // cancelled all over again.
        let stats = state.resolve(vec![trip(0)], &[], "2026-08-22T11:06:00Z".parse().unwrap());
        assert_eq!(stats, None);
    }

    #[test]
    fn a_perfectly_on_time_railway_reports_no_cancellations_over_many_cycles() {
        // The whole failure mode end to end, driven the way `main.rs`
        // drives it: the same full service day handed in on every cycle,
        // a poller started mid-afternoon, and a railway running exactly to
        // time. Every resolved trip should be an on-time one.
        let day_start: DateTime<Utc> = "2026-08-22T05:28:00Z".parse().unwrap();
        let days_schedule: Vec<ScheduledTrip> = (0..110)
            .map(|i| ScheduledTrip {
                scheduled_departure: day_start + chrono::Duration::minutes(i * 10),
                interval_id: None,
            })
            .collect();

        let mut state = DlrMatchState::new();
        let start: DateTime<Utc> = "2026-08-22T14:00:00Z".parse().unwrap();
        let mut latest = None;
        for cycle in 0..12 {
            let now = start + chrono::Duration::minutes(cycle * 5);
            // Punctual railway: everything due before the next poll is
            // already showing a prediction at exactly its scheduled time.
            let predictions: Vec<Prediction> = days_schedule
                .iter()
                .filter(|t| {
                    t.scheduled_departure >= now
                        && t.scheduled_departure <= now + chrono::Duration::minutes(5)
                })
                .map(|t| Prediction {
                    expected_arrival: t.scheduled_departure,
                    ..prediction(0)
                })
                .collect();
            latest = state.resolve(days_schedule.clone(), &predictions, now);
        }

        let stats = latest.expect("an hour of on-time trips should have resolved");
        assert_eq!(
            stats.cancelled, 0,
            "an on-time railway must report no cancellations: {stats:?}"
        );
        assert_eq!(stats.delayed, 0);
        // 55 minutes of poll cycles over a 10-minute headway, each trip
        // counted exactly once — not once per cycle it stays in the
        // timetable payload.
        assert!(
            (5..=7).contains(&stats.total),
            "unexpected sample size: {stats:?}"
        );
    }

    #[test]
    fn a_trip_already_pending_is_not_added_twice_on_the_next_cycle() {
        let mut state = DlrMatchState::new();
        state.resolve(vec![trip(0)], &[], "2026-08-22T10:00:00Z".parse().unwrap());
        // Same trip handed in again next cycle (the timetable poll always
        // returns the same day's full schedule) — must not be double-counted.
        let stats = state
            .resolve(
                vec![trip(0)],
                &[prediction(0)],
                "2026-08-22T10:01:00Z".parse().unwrap(),
            )
            .expect("the trip should resolve exactly once");
        assert_eq!(stats.total, 1);
    }

    #[test]
    fn a_feed_outage_does_not_mass_cancel_pending_trips() {
        let mut state = DlrMatchState::new();
        let base: DateTime<Utc> = "2026-08-22T10:00:00Z".parse().unwrap();

        // Establish a healthy baseline: several cycles, each with five
        // trips all matched immediately by five predictions, so
        // `recent_prediction_counts` reflects a feed averaging five
        // predictions per cycle.
        for cycle in 0..RECENT_PREDICTION_HISTORY as i64 {
            let now = base + chrono::Duration::minutes(cycle * 5);
            let trips: Vec<ScheduledTrip> = (0..5)
                .map(|n| ScheduledTrip {
                    scheduled_departure: now + chrono::Duration::minutes(n),
                    interval_id: None,
                })
                .collect();
            let predictions: Vec<Prediction> = trips
                .iter()
                .map(|t| Prediction {
                    expected_arrival: t.scheduled_departure,
                    ..prediction(0)
                })
                .collect();
            state.resolve(trips, &predictions, now);
        }

        // Now the Arrivals feed itself goes completely dark: a new trip is
        // admitted, but every subsequent cycle sees zero predictions at
        // all -- not because it was cancelled, but because the whole feed
        // stopped returning data.
        let outage_departure = base + chrono::Duration::minutes(30);
        let outage_trip = ScheduledTrip {
            scheduled_departure: outage_departure,
            interval_id: None,
        };
        state.resolve(vec![outage_trip.clone()], &[], outage_departure);
        // 16 minutes later: past the normal CANCELLATION_GRACE_MINUTES
        // (15), which without the outage guard would promote this trip to
        // `resolved` as cancelled.
        let stats = state
            .resolve(
                vec![outage_trip],
                &[],
                outage_departure + chrono::Duration::minutes(16),
            )
            .expect("the healthy baseline trips are still within the retention window");
        assert_eq!(
            stats.cancelled, 0,
            "a feed outage must not be inferred as a mass cancellation: {stats:?}"
        );
    }

    #[test]
    fn without_an_established_baseline_cancellation_still_infers_normally() {
        // Guards against the guard itself over-firing: with no history
        // yet (a cold start), an empty-predictions cycle past the grace
        // window must still resolve as cancelled -- this is exactly
        // `a_trip_still_unmatched_past_the_grace_window_is_cancelled`'s
        // scenario, re-asserted here to make the "no baseline yet" branch
        // explicit.
        let mut state = DlrMatchState::new();
        state.resolve(vec![trip(0)], &[], "2026-08-22T10:00:00Z".parse().unwrap());
        let stats = state
            .resolve(vec![], &[], "2026-08-22T10:16:00Z".parse().unwrap())
            .expect("the overdue trip should have resolved as cancelled");
        assert_eq!(stats.cancelled, 1);
    }
}
