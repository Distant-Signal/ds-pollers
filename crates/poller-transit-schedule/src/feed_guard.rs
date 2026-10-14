//! The feed guard (transit spec §6.2 step 11, as poller-irish-rail-gtfs'):
//! a parsed timetable is refused, and the previous version stays active,
//! when it is empty, when today has no service, or when its line, stop or
//! trip count fell by more than `FEED_MAX_DROP_FRACTION` since the last
//! commit. The baseline is the last `transit-timetable-commit/1` on the
//! network's stream, so it survives a restart.

use chrono::NaiveDate;
use common::transit::TimetableCounts;

use crate::model::TransitModel;

/// Why the guard refused a feed: the `reason` of
/// `transit_feed_refused_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    Empty,
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
            Self::NoServiceToday(day) => write!(f, "no service on {day}"),
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

/// Checks `model` (with `new` its counts) against `baseline`, the last
/// commit's counts if there is one.
pub(crate) fn check(
    model: &TransitModel,
    new: TimetableCounts,
    baseline: Option<TimetableCounts>,
    today: NaiveDate,
    max_drop_fraction: f64,
) -> Result<(), Refusal> {
    if new.trips == 0 || new.stops == 0 {
        return Err(Refusal::Empty);
    }
    if !model.service_days.iter().any(|s| s.dates.contains(&today)) {
        return Err(Refusal::NoServiceToday(today));
    }
    let Some(was) = baseline else {
        return Ok(());
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
    Ok(())
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
        assert!(check(&m, counts(10, 10, 1), None, day(14), 0.3).is_ok());
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
}
