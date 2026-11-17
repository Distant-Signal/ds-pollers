//! The feed guard (transit spec §6.2 step 11, as poller-irish-rail-gtfs'):
//! a parsed timetable is refused, and the previous version stays active,
//! when it is empty, when neither today nor the next [`SERVICE_LOOKAHEAD_DAYS`]
//! days have service, or when its line, stop or trip count fell by more
//! than `FEED_MAX_DROP_FRACTION` since the last commit. The baseline is the
//! last `transit-timetable-commit/1` on the network's stream, so it
//! survives a restart.
//!
//! A feed with no service today but some in the next few days (Metrolink,
//! NET and the Tube run nothing on Christmas Day) is not refused: it is
//! held back ([`Verdict::NoServiceUntil`]) without an alert, because the
//! writer refuses a commit with no service on its day (ds-store's
//! consistency check), and the next daily cycle publishes it.

use chrono::NaiveDate;
use common::transit::TimetableCounts;

use crate::model::TransitModel;

/// How many days after today the guard looks for service before it calls
/// a feed with none today broken (Christmas Day, then Boxing Day and the
/// substitute bank holidays).
pub(crate) const SERVICE_LOOKAHEAD_DAYS: u64 = 3;

/// A feed the guard did not refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Publish it.
    Publish,
    /// No service today, but some on this later day: hold it back for a
    /// day (the writer would refuse it today), with no refusal counted.
    NoServiceUntil(NaiveDate),
}

/// Why the guard refused a feed: the `reason` of
/// `transit_feed_refused_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    Empty,
    /// No service from this day to [`SERVICE_LOOKAHEAD_DAYS`] after it.
    NoServiceToday(NaiveDate),
    SharpDrop {
        what: &'static str,
        was: u32,
        now: u32,
    },
}

impl Refusal {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::NoServiceToday(_) => "no_service_today",
            Self::SharpDrop { .. } => "sharp_drop",
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("the feed has no trips"),
            Self::NoServiceToday(day) => write!(
                f,
                "no service on {day} or the {SERVICE_LOOKAHEAD_DAYS} days after it"
            ),
            Self::SharpDrop { what, was, now } => {
                write!(f, "{what} fell from {was} to {now}, past the drop limit")
            }
        }
    }
}

/// The model's counts, as a commit carries them.
pub(crate) fn counts(model: &TransitModel) -> TimetableCounts {
    let n = |len: usize| u32::try_from(len).unwrap_or(u32::MAX);
    TimetableCounts {
        lines: n(model.lines.len()),
        stops: n(model.stops.len()),
        patterns: n(model.patterns.len()),
        trips: n(model.trips.len()),
        service_days: n(model.service_day_count()),
    }
}

/// The first day from `today` to [`SERVICE_LOOKAHEAD_DAYS`] after it with
/// service in `model`.
fn first_service_day(model: &TransitModel, today: NaiveDate) -> Option<NaiveDate> {
    (0..=SERVICE_LOOKAHEAD_DAYS)
        .filter_map(|n| today.checked_add_days(chrono::Days::new(n)))
        .find(|day| model.service_days.iter().any(|s| s.dates.contains(day)))
}

/// Checks `model` (with `new` its counts) against `baseline`, the last
/// commit's counts if there is one.
pub(crate) fn check(
    model: &TransitModel,
    new: TimetableCounts,
    baseline: Option<TimetableCounts>,
    today: NaiveDate,
    max_drop_fraction: f64,
) -> Result<Verdict, Refusal> {
    if new.trips == 0 || new.stops == 0 {
        return Err(Refusal::Empty);
    }
    let verdict = match first_service_day(model, today) {
        None => return Err(Refusal::NoServiceToday(today)),
        Some(day) if day == today => Verdict::Publish,
        Some(day) => Verdict::NoServiceUntil(day),
    };
    let Some(was) = baseline else {
        return Ok(verdict);
    };
    for (what, before, after) in [
        ("lines", was.lines, new.lines),
        ("stops", was.stops, new.stops),
        ("trips", was.trips, new.trips),
    ] {
        if dropped_too_far(before, after, max_drop_fraction) {
            return Err(Refusal::SharpDrop {
                what,
                was: before,
                now: after,
            });
        }
    }
    Ok(verdict)
}

/// Whether `after` is more than `fraction` below `before`.
fn dropped_too_far(before: u32, after: u32, fraction: f64) -> bool {
    if after >= before || before == 0 {
        return false;
    }
    f64::from(before - after) / f64::from(before) > fraction
}

#[cfg(test)]
mod tests {
    use common::transit::{Horizon, TransitServiceDays};

    use super::*;

    fn day(d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 10, d).unwrap()
    }

    fn model(dates: Vec<NaiveDate>) -> TransitModel {
        TransitModel {
            lines: vec![],
            stops: vec![],
            patterns: vec![],
            trips: vec![],
            service_days: vec![TransitServiceDays {
                service_id: "s".to_owned(),
                dates,
            }],
            horizon: Horizon {
                from: day(13),
                to: day(20),
            },
            source_feed_version: None,
        }
    }

    fn counts(trips: u32, stops: u32, lines: u32) -> TimetableCounts {
        TimetableCounts {
            lines,
            stops,
            patterns: 1,
            trips,
            service_days: 1,
        }
    }

    #[test]
    fn refusals() {
        let m = model(vec![day(14)]);
        assert_eq!(
            check(&m, counts(0, 10, 1), None, day(14), 0.3),
            Err(Refusal::Empty)
        );
        assert_eq!(
            check(&m, counts(10, 10, 1), None, day(15), 0.3)
                .unwrap_err()
                .reason(),
            "no_service_today"
        );
        assert_eq!(
            check(&m, counts(10, 10, 1), None, day(14), 0.3),
            Ok(Verdict::Publish)
        );
        // A 40% drop in trips is refused at 0.3, allowed at 1.0.
        let was = Some(counts(100, 10, 7));
        let refusal = check(&m, counts(60, 10, 7), was, day(14), 0.3).unwrap_err();
        assert_eq!(refusal.reason(), "sharp_drop");
        assert!(
            refusal.to_string().contains("trips fell from 100 to 60"),
            "{refusal}"
        );
        assert!(check(&m, counts(60, 10, 7), was, day(14), 1.0).is_ok());
        // A 20% drop, or growth, is fine.
        assert!(check(&m, counts(80, 10, 7), was, day(14), 0.3).is_ok());
        assert!(check(&m, counts(500, 50, 9), was, day(14), 0.3).is_ok());
        // Losing two of seven lines (29%) passes; three (43%) does not.
        assert!(check(&m, counts(100, 10, 5), was, day(14), 0.3).is_ok());
        assert!(check(&m, counts(100, 10, 4), was, day(14), 0.3).is_err());
    }

    #[test]
    fn a_day_without_service_before_some_is_held_back_not_refused() {
        // Christmas Day: nothing on the 25th, service again on the 26th.
        let dec = |d| NaiveDate::from_ymd_opt(2026, 12, d).unwrap();
        let m = model(vec![dec(24), dec(26), dec(27)]);
        assert_eq!(
            check(&m, counts(10, 10, 1), None, dec(25), 0.3),
            Ok(Verdict::NoServiceUntil(dec(26)))
        );
        // Service three days on still counts; four days on does not.
        let m = model(vec![dec(28)]);
        assert_eq!(
            check(&m, counts(10, 10, 1), None, dec(25), 0.3),
            Ok(Verdict::NoServiceUntil(dec(28)))
        );
        let m = model(vec![dec(29)]);
        let refusal = check(&m, counts(10, 10, 1), None, dec(25), 0.3).unwrap_err();
        assert_eq!(refusal, Refusal::NoServiceToday(dec(25)));
        assert_eq!(refusal.reason(), "no_service_today");
        // A held-back feed still meets the sharp-drop guard.
        let m = model(vec![dec(26)]);
        let was = Some(counts(100, 10, 7));
        assert_eq!(
            check(&m, counts(50, 10, 7), was, dec(25), 0.3)
                .unwrap_err()
                .reason(),
            "sharp_drop"
        );
    }
}
