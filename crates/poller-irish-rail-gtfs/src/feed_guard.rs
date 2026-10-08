//! Whole-feed sharp-drop guard: refuses to publish a feed whose station or
//! line count fell by more than a configured fraction since the last
//! successfully published feed, so a truncated or broken upstream export
//! never replaces a good catalogue.
//!
//! The last published counts live in memory only. This poller keeps no
//! persisted per-feed state of its own (its startup cursor, where there is
//! one, is just the last publish time), so after a restart the first feed
//! publishes as on a first run. A refusal returns an error the poll loop
//! records as a failed cycle (so `DistantSignalPollerFailing` covers it,
//! when enabled) and retries after the normal interval.

use std::sync::{Mutex, PoisonError};

/// Station and line counts of one mapped feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FeedCounts {
    pub stations: usize,
    pub lines: usize,
}

/// The counts of the last feed that was published, process-wide. A static
/// rather than a value threaded through `poll_once`, so the guard does not
/// change the poll loop's or the sink's signatures.
static LAST_PUBLISHED: Mutex<Option<FeedCounts>> = Mutex::new(None);

/// Why a feed was refused.
#[derive(Debug)]
pub(crate) struct FeedRefused(String);

impl std::fmt::Display for FeedRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FeedRefused {}

/// The fraction `new` dropped below `previous` (0 when it did not drop).
#[expect(
    clippy::cast_precision_loss,
    reason = "catalogue counts are far below 2^52"
)]
fn drop_fraction(previous: usize, new: usize) -> f64 {
    if previous == 0 || new >= previous {
        0.0
    } else {
        (previous - new) as f64 / previous as f64
    }
}

/// Decides whether `new` may be published given the `previous` published
/// counts. An empty feed (no stations or no lines) is always refused,
/// first run included: publishing it would empty the catalogue. Otherwise
/// the first run (no `previous`) is accepted, and later ones are refused
/// when either count dropped by more than `max_drop_fraction`.
pub(crate) fn check(
    previous: Option<FeedCounts>,
    new: FeedCounts,
    max_drop_fraction: f64,
) -> Result<(), FeedRefused> {
    if new.stations == 0 || new.lines == 0 {
        return Err(FeedRefused(format!(
            "GTFS feed mapped to {} stations and {} lines; refusing to publish an empty catalogue",
            new.stations, new.lines
        )));
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    for (what, before, after) in [
        ("stations", previous.stations, new.stations),
        ("lines", previous.lines, new.lines),
    ] {
        let dropped = drop_fraction(before, after);
        if dropped > max_drop_fraction {
            return Err(FeedRefused(format!(
                "GTFS feed's {what} fell from {before} to {after} ({:.0}% drop, over the {:.0}% \
                 limit); refusing to publish it and keeping the previous catalogue",
                dropped * 100.0,
                max_drop_fraction * 100.0
            )));
        }
    }
    Ok(())
}

/// [`check`] against the last published counts. A refusal is logged and
/// counted in `distant_signal_gtfs_feed_refused_total`, and comes back as
/// an error wrapping [`common::ingest::DataRejected`], so the poll loop
/// waits the normal interval instead of re-downloading the same feed.
pub(crate) fn check_against_last_published(
    new: FeedCounts,
    max_drop_fraction: f64,
) -> anyhow::Result<()> {
    let previous = *LAST_PUBLISHED
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    check(previous, new, max_drop_fraction).map_err(|refused| {
        metrics::counter!(common::metrics::metric_name("gtfs_feed_refused_total")).increment(1);
        tracing::error!(?previous, ?new, max_drop_fraction, "{refused}");
        anyhow::Error::new(common::ingest::DataRejected(refused.to_string()))
    })
}

/// Records `published` as the baseline for the next feed's check. Call
/// only after both catalogues were published.
pub(crate) fn record_published(published: FeedCounts) {
    *LAST_PUBLISHED
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(published);
    #[expect(
        clippy::cast_precision_loss,
        reason = "catalogue counts are far below 2^52"
    )]
    for (kind, count) in [
        ("stations", published.stations as f64),
        ("lines", published.lines as f64),
    ] {
        metrics::gauge!(
            common::metrics::metric_name("gtfs_feed_published_items"),
            "kind" => kind
        )
        .set(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: f64 = 0.3;

    fn counts(stations: usize, lines: usize) -> FeedCounts {
        FeedCounts { stations, lines }
    }

    #[test]
    fn the_first_run_publishes_normally() {
        assert!(check(None, counts(150, 12), LIMIT).is_ok());
        assert!(check(None, counts(1, 1), LIMIT).is_ok());
    }

    #[test]
    fn a_shrink_over_the_limit_is_refused() {
        // Stations: 100 -> 69 is a 31% drop.
        let err = check(Some(counts(100, 10)), counts(69, 10), LIMIT).unwrap_err();
        assert!(
            err.to_string().contains("stations fell from 100 to 69"),
            "{err}"
        );
        // Lines: 10 -> 6 is a 40% drop, with stations unchanged.
        let err = check(Some(counts(100, 10)), counts(100, 6), LIMIT).unwrap_err();
        assert!(err.to_string().contains("lines fell from 10 to 6"), "{err}");
    }

    #[test]
    fn a_shrink_within_the_limit_is_accepted() {
        // Exactly 30% and just under it on each count.
        assert!(check(Some(counts(100, 10)), counts(70, 7), LIMIT).is_ok());
        assert!(check(Some(counts(100, 10)), counts(99, 9), LIMIT).is_ok());
        // Growth is always fine.
        assert!(check(Some(counts(100, 10)), counts(500, 50), LIMIT).is_ok());
    }

    #[test]
    fn an_empty_feed_is_refused_even_on_the_first_run() {
        assert!(check(None, counts(0, 10), LIMIT).is_err());
        assert!(check(None, counts(100, 0), LIMIT).is_err());
        // A limit of 1.0 turns the drop check off but not this one.
        assert!(check(Some(counts(100, 10)), counts(0, 0), 1.0).is_err());
        assert!(check(Some(counts(100, 10)), counts(1, 1), 1.0).is_ok());
    }

    /// The process-wide baseline: no previous publish means the first run
    /// rule, a recorded publish becomes the baseline, and a refusal is a
    /// `DataRejected` error (the poll loop's "wait the full interval" class).
    /// The only test touching `LAST_PUBLISHED`, so tests running in
    /// parallel cannot race on it.
    #[test]
    fn the_baseline_is_the_last_published_feed() {
        assert!(check_against_last_published(counts(100, 10), LIMIT).is_ok());
        record_published(counts(100, 10));
        let err = check_against_last_published(counts(50, 10), LIMIT).unwrap_err();
        assert_eq!(
            common::ingest::classify_failure(&err),
            common::ingest::FailureClass::Rejected
        );
        // The refused feed did not move the baseline.
        assert!(check_against_last_published(counts(80, 10), LIMIT).is_ok());
        record_published(counts(80, 10));
        assert!(check_against_last_published(counts(60, 10), LIMIT).is_ok());
    }
}
