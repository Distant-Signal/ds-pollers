//! The tracked-stop lane (pre-departure prediction phase 1, DS-127;
//! docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
//! section 6.2 item 3).
//!
//! The station rotation polls about 560 stations at 10 rows each, so a
//! tracked train's platform appears only once it is in its station's first
//! 10 departures, and never at a station outside the sample set. This lane
//! asks LDBWS for the board of each tracked train's own stops (boarding,
//! alighting and change stations with a booked departure; boards list
//! departures only), narrowed with `timeOffset`/`timeWindow` to a window
//! around that train's booked time there. The api's per-stop board match
//! reads these boards before the station's general sample.
//!
//! - **Scope**: [`ingest_tracked_stop_targets`] rows departing from
//!   [`BEHIND`] ago to [`HORIZON`] ahead.
//! - **Cadence**: each stop every 2 minutes, every minute once its train is
//!   within [`NEAR`] of departure ([`Cadence`]).
//! - **Budget**: its own, separate from the station rotation's: at most
//!   `TRACKED_STOP_MAX_REQUESTS_PER_CYCLE` requests and
//!   [`LANE_TIME_BUDGET`] a cycle, soonest departure first. Off unless
//!   `TRACKED_STOP_POLL_ENABLED`.
//! - **Output**: one `tracked-stop-boards/1` row per stop polled, in the
//!   same snapshot as the cycle's station samples.
//!
//! **Arrivals (DS-152).** Departure boards never list a call where the train
//! terminates or only sets down, which is usually the user's destination.
//! With `TRACKED_STOP_ARRIVALS_ENABLED`, the lane also asks the LDBWS
//! arrivals product (`GetArrBoardWithDetails`, its own RDM subscription:
//! [`arrivals_url`]) for each alighting stop with a booked arrival
//! ([`ingest_tracked_stop_arrival_targets`]), with the same horizon,
//! cadence and window (around the booked arrival), its own [`Cadence`],
//! its own cap (`TRACKED_STOP_ARRIVALS_MAX_REQUESTS_PER_CYCLE`) and
//! [`ARRIVALS_TIME_BUDGET`]. Output: `tracked-stop-arrival-boards/1` rows in
//! the same snapshot.
//!
//! [`ingest_tracked_stop_arrival_targets`]: ds_store::reads::tracked_stop_arrival_targets
//!
//! [`ingest_tracked_stop_targets`]: ds_store::reads::tracked_stop_targets

use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use ds_store::reads::tracked_stop_targets::TrackedStopTarget;

/// How far ahead a stop is polled.
pub(crate) const HORIZON: Duration = Duration::minutes(120);
/// A stop just past its booked departure is still polled (a late train is
/// still on the board).
pub(crate) const BEHIND: Duration = Duration::minutes(10);
/// Within this of departure, a stop is polled every cycle.
pub(crate) const NEAR: Duration = Duration::minutes(20);
/// The re-poll interval for a stop further out than [`NEAR`]: 2 minutes,
/// less a little so a 60 s cycle that runs a few seconds early still
/// polls it on its second cycle.
const FAR_INTERVAL: Duration = Duration::seconds(110);
/// ... and within [`NEAR`]: every 60 s cycle.
const NEAR_INTERVAL: Duration = Duration::seconds(50);
/// The board window opens this long before the booked time ...
const WINDOW_BEFORE_MINUTES: i64 = 5;
/// ... and lasts this long, so the train's row is in it even when the
/// station is busy.
const WINDOW_MINUTES: i64 = 15;
/// LDBWS's `timeOffset` range, in minutes.
const MAX_OFFSET: i64 = 119;
const MIN_OFFSET: i64 = -120;
/// The tracked-stop lanes' share of the cycle's time, both together: the
/// station rotation has 45 s of the 60 s interval, so the two lanes get 10
/// s and the cycle ends with 5 s to spare.
pub(crate) const LANE_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
/// The arrivals sub-lane's part of [`LANE_TIME_BUDGET`] (DS-152), taken
/// from the departure lane only while the sub-lane is on.
pub(crate) const ARRIVALS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(3);

/// The departure lane's time budget: all of [`LANE_TIME_BUDGET`], less
/// [`ARRIVALS_TIME_BUDGET`] while the arrivals sub-lane is on, so the two
/// lanes and the 45 s rotation never take the whole 60 s interval.
pub(crate) fn departures_time_budget(arrivals_enabled: bool) -> std::time::Duration {
    if arrivals_enabled {
        LANE_TIME_BUDGET.saturating_sub(ARRIVALS_TIME_BUDGET)
    } else {
        LANE_TIME_BUDGET
    }
}

/// An arrival target as a [`TrackedStopTarget`] whose `departs_at` is the
/// booked arrival, so the arrivals sub-lane can reuse [`Cadence`] and
/// [`window`] as they are (with its own `Cadence`).
pub(crate) fn arrival_as_target(
    target: &ds_store::reads::tracked_stop_arrival_targets::TrackedStopArrivalTarget,
) -> TrackedStopTarget {
    TrackedStopTarget {
        train_uid: target.train_uid.clone(),
        service_date: target.service_date,
        crs: target.crs.clone(),
        departs_at: target.arrives_at,
    }
}

/// The arrivals product's request URL for `crs`, from the Secret's `url`
/// (`LDBWS_ARRIVALS_URL`), without the query string:
///
/// - a `{crs}` placeholder is replaced by the station;
/// - else a URL whose last path segment starts with `Get` names the
///   operation already (`.../GetArrBoardWithDetails`): `/{crs}` is
///   appended;
/// - else it is the product's base (`.../LDBWS/api/20220120`):
///   `/GetArrBoardWithDetails/{crs}` is appended.
///
/// A trailing `/` is ignored. `crs` must already be a well-formed code (the
/// lane only polls those).
pub(crate) fn arrivals_url(base: &str, crs: &str) -> String {
    let base = base.trim();
    if base.contains("{crs}") {
        return base.replace("{crs}", crs);
    }
    let base = base.trim_end_matches('/');
    let last_segment = base.rsplit('/').next().unwrap_or_default();
    if last_segment.starts_with("Get") {
        format!("{base}/{crs}")
    } else {
        format!("{base}/GetArrBoardWithDetails/{crs}")
    }
}

type StopKey = (String, NaiveDate, String);

fn key(target: &TrackedStopTarget) -> StopKey {
    (
        target.train_uid.clone(),
        target.service_date,
        target.crs.clone(),
    )
}

/// When each stop was last polled, to pace re-polls. In memory: a restart
/// polls everything once more, which is what a restart should do.
#[derive(Debug, Default)]
pub(crate) struct Cadence {
    last_polled: HashMap<StopKey, DateTime<Utc>>,
}

impl Cadence {
    /// The targets due a poll at `now`, soonest departure first.
    pub(crate) fn due<'a>(
        &self,
        targets: &'a [TrackedStopTarget],
        now: DateTime<Utc>,
    ) -> Vec<&'a TrackedStopTarget> {
        let mut due: Vec<&TrackedStopTarget> = targets
            .iter()
            .filter(|target| well_formed_crs(&target.crs))
            .filter(|target| {
                let interval = if target.departs_at - now <= NEAR {
                    NEAR_INTERVAL
                } else {
                    FAR_INTERVAL
                };
                self.last_polled
                    .get(&key(target))
                    .is_none_or(|last| now - *last >= interval)
            })
            .collect();
        due.sort_by_key(|target| target.departs_at);
        due
    }

    /// Records a poll of `target` at `now`.
    pub(crate) fn polled(&mut self, target: &TrackedStopTarget, now: DateTime<Utc>) {
        self.last_polled.insert(key(target), now);
    }

    /// Forgets stops not polled for longer than the horizon.
    pub(crate) fn prune(&mut self, now: DateTime<Utc>) {
        self.last_polled.retain(|_, last| now - *last <= HORIZON);
    }
}

/// `timeOffset` and `timeWindow` (minutes) for a board around
/// `departs_at`, seen from `now`.
pub(crate) fn window(departs_at: DateTime<Utc>, now: DateTime<Utc>) -> (i64, i64) {
    let offset =
        ((departs_at - now).num_minutes() - WINDOW_BEFORE_MINUTES).clamp(MIN_OFFSET, MAX_OFFSET);
    (offset, WINDOW_MINUTES)
}

/// The same guard as the station list's (`well_formed_stations`): only a
/// three-letter code may be spliced into the request path.
fn well_formed_crs(crs: &str) -> bool {
    crs.len() == 3 && crs.bytes().all(|b| b.is_ascii_alphabetic())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(minutes: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2099-07-10T10:00:00Z")
            .expect("instant")
            .with_timezone(&Utc)
            + Duration::minutes(minutes)
    }

    fn target(uid: &str, crs: &str, departs_in: i64) -> TrackedStopTarget {
        TrackedStopTarget {
            train_uid: uid.to_string(),
            service_date: NaiveDate::from_ymd_opt(2099, 7, 10).expect("date"),
            crs: crs.to_string(),
            departs_at: at(departs_in),
        }
    }

    #[test]
    fn a_new_stop_is_due_and_soonest_comes_first() {
        let cadence = Cadence::default();
        let targets = [target("A", "YRK", 90), target("B", "LDS", 30)];
        let due: Vec<&str> = cadence
            .due(&targets, at(0))
            .iter()
            .map(|t| t.train_uid.as_str())
            .collect();
        assert_eq!(due, ["B", "A"]);
    }

    #[test]
    fn a_far_stop_is_polled_every_two_minutes_and_a_near_one_every_minute() {
        let mut cadence = Cadence::default();
        let far = [target("A", "YRK", 90)];
        let near = [target("B", "LDS", 15)];
        cadence.polled(&far[0], at(0));
        cadence.polled(&near[0], at(0));
        let one_cycle_later = at(0) + Duration::seconds(58);
        assert!(cadence.due(&far, one_cycle_later).is_empty());
        assert_eq!(cadence.due(&near, one_cycle_later).len(), 1);
        let two_cycles_later = at(0) + Duration::seconds(118);
        assert_eq!(cadence.due(&far, two_cycles_later).len(), 1);
    }

    #[test]
    fn a_malformed_code_is_never_polled() {
        let cadence = Cadence::default();
        assert!(cadence.due(&[target("A", "Y/K", 30)], at(0)).is_empty());
        assert!(cadence.due(&[target("A", "YORK", 30)], at(0)).is_empty());
    }

    #[test]
    fn prune_forgets_old_stops() {
        let mut cadence = Cadence::default();
        let stop = [target("A", "YRK", 30)];
        cadence.polled(&stop[0], at(0));
        cadence.prune(at(0) + HORIZON + Duration::minutes(1));
        assert_eq!(cadence.due(&stop, at(1)).len(), 1);
    }

    #[test]
    fn the_arrivals_url_follows_the_secret_s_shape() {
        let base = "https://arrivals.example.invalid/LDBWS/api/20220120";
        assert_eq!(
            arrivals_url(base, "LDS"),
            format!("{base}/GetArrBoardWithDetails/LDS")
        );
        assert_eq!(
            arrivals_url(&format!("{base}/"), "LDS"),
            format!("{base}/GetArrBoardWithDetails/LDS")
        );
        assert_eq!(
            arrivals_url(&format!("{base}/GetArrivalBoard"), "LDS"),
            format!("{base}/GetArrivalBoard/LDS")
        );
        assert_eq!(
            arrivals_url(&format!("{base}/GetArrBoardWithDetails/{{crs}}"), "LDS"),
            format!("{base}/GetArrBoardWithDetails/LDS")
        );
    }

    #[test]
    fn an_arrival_target_is_paced_by_its_booked_arrival() {
        let arrival = ds_store::reads::tracked_stop_arrival_targets::TrackedStopArrivalTarget {
            train_uid: "A".to_string(),
            service_date: NaiveDate::from_ymd_opt(2099, 7, 10).expect("date"),
            crs: "LDS".to_string(),
            arrives_at: at(45),
        };
        let target = arrival_as_target(&arrival);
        assert_eq!(target.departs_at, at(45));
        assert_eq!(Cadence::default().due(&[target], at(0)).len(), 1);
    }

    #[test]
    fn the_window_opens_five_minutes_before_the_booked_time() {
        assert_eq!(window(at(60), at(0)), (55, 15));
        assert_eq!(window(at(3), at(0)), (-2, 15));
        // Clamped to LDBWS's range.
        assert_eq!(window(at(200), at(0)), (119, 15));
        assert_eq!(window(at(-200), at(0)), (-120, 15));
    }

    #[test]
    fn the_two_lanes_share_one_ten_second_budget() {
        assert_eq!(
            departures_time_budget(false),
            std::time::Duration::from_secs(10)
        );
        assert_eq!(
            departures_time_budget(true) + ARRIVALS_TIME_BUDGET,
            LANE_TIME_BUDGET
        );
    }
}
