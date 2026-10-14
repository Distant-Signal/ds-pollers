//! The one-shot timetable refresh (`TRANSIT_TIMETABLE_REFRESH`, chart
//! `pollerTransitSchedule.networks.<n>.timetableRefresh`;
//! docs/transit-networks.md).
//!
//! The timetable loop normally waits until the newest commit on the stream
//! is a day old, so a restart does not refresh the timetable. A refresh
//! request is a token the operator sets (or changes). At start the producer
//! compares it with the `source.refresh` of the newest commit on the
//! stream:
//!
//! - **unanswered** (no commit carries it): the loop's first cycle runs at
//!   once, and it publishes the timetable even if unchanged, stamping the
//!   token into the commit;
//! - **answered** (the newest commit carries it): nothing special; the
//!   daily schedule resumes from that commit.
//!
//! So a token is answered once, by its own commit, and later restarts with
//! the same token change nothing: it never needs unsetting. The feed guard
//! still applies to a forced fetch: a refused feed publishes nothing, and
//! the request stays unanswered until the next start.

use crate::Context;

/// Sets `ctx.refresh_pending` from the configured token and the newest
/// commit on the stream. A stream it cannot read counts as unanswered (at
/// worst one extra publish).
pub(crate) async fn check_at_start(ctx: &Context) {
    let requested = &ctx.config.timetable_refresh;
    if requested.is_empty() {
        return;
    }
    let answered = crate::timetable::baseline(ctx)
        .await
        .and_then(|last| last.commit.source.refresh);
    let pending = pending(requested, answered.as_deref());
    tracing::info!(
        token = %requested,
        answered = ?answered,
        pending,
        "timetable refresh request"
    );
    ctx.refresh_pending
        .store(pending, std::sync::atomic::Ordering::Relaxed);
}

/// Whether `requested` (the configured token, empty for none) still needs
/// answering, given the token the newest commit on the stream answered.
pub(crate) fn pending(requested: &str, answered: Option<&str>) -> bool {
    !requested.is_empty() && answered != Some(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_answered_once() {
        // No request: never forced.
        assert!(!pending("", None));
        assert!(!pending("", Some("2026-10-15")));
        // A new request: forced, whatever the stream holds.
        assert!(pending("2026-10-15", None));
        assert!(pending("2026-10-15", Some("2026-10-01")));
        // Already answered by the newest commit: not again.
        assert!(!pending("2026-10-15", Some("2026-10-15")));
    }
}
