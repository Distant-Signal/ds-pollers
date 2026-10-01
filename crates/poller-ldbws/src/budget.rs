//! Optional hourly LDBWS request budget (an LEG-18 operator safety knob).
//!
//! Off by default (`HOURLY_REQUEST_BUDGET=0`): [`RequestBudget::try_acquire`]
//! then always succeeds and nothing is tracked, so the poller behaves
//! exactly as it did before the knob existed.
//!
//! When set, two limits apply to every `GetDepBoardWithDetails` request,
//! numRows fallback retries included:
//!
//! - **Per cycle** (the spreading limit): each cycle may make at most
//!   `ceil(budget * poll_interval_secs / 3600)` requests, its even share of
//!   the hour. Without this the poller would spend the whole hour's budget
//!   in the first few cycles and then go quiet until the window rolled.
//! - **Rolling hour** (the hard limit): no more than `budget` requests in
//!   any 60-minute window, whatever the individual cycles did. The window
//!   is held in memory, so a restarted pod starts with an empty one.
//!
//! A station the budget does not allow is skipped for the cycle, not
//! failed: the rotation (`rotation.rs`) starts the next cycle at the first
//! station not reached, so a budget thins out how often each station is
//! sampled rather than starving the end of the list.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

const HOUR: Duration = Duration::from_secs(3600);

/// Which limit refused a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BudgetLimit {
    /// This cycle's even share of the hourly budget is used up.
    Cycle,
    /// The rolling-hour total has reached the budget.
    Hour,
}

impl BudgetLimit {
    /// Metric label and log value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            BudgetLimit::Cycle => "cycle",
            BudgetLimit::Hour => "hour",
        }
    }
}

#[derive(Debug)]
pub(crate) struct RequestBudget {
    /// Requests allowed in any rolling hour. 0 means unlimited.
    hourly_limit: usize,
    /// Requests allowed per cycle (see the module docs). Unused when
    /// `hourly_limit` is 0.
    per_cycle_limit: usize,
    /// When each request in the last hour was made, oldest first.
    window: VecDeque<Instant>,
    used_this_cycle: usize,
}

impl RequestBudget {
    pub(crate) fn new(hourly_limit: u32, poll_interval_secs: u64) -> Self {
        let hourly_limit = hourly_limit as usize;
        let per_cycle_limit = if hourly_limit == 0 {
            0
        } else {
            let share = (hourly_limit as u128 * u128::from(poll_interval_secs)).div_ceil(3600);
            usize::try_from(share)
                .unwrap_or(usize::MAX)
                .clamp(1, hourly_limit)
        };
        Self {
            hourly_limit,
            per_cycle_limit,
            window: VecDeque::new(),
            used_this_cycle: 0,
        }
    }

    /// No budget: every request is allowed and nothing is recorded.
    #[cfg(test)]
    pub(crate) fn unlimited() -> Self {
        Self::new(0, 0)
    }

    pub(crate) fn is_unlimited(&self) -> bool {
        self.hourly_limit == 0
    }

    pub(crate) fn per_cycle_limit(&self) -> Option<usize> {
        (!self.is_unlimited()).then_some(self.per_cycle_limit)
    }

    /// Resets the per-cycle count. Called once at the start of each cycle.
    pub(crate) fn start_cycle(&mut self) {
        self.used_this_cycle = 0;
    }

    /// Whether one more request would be allowed at `now`, without using
    /// it up.
    pub(crate) fn check(&mut self, now: Instant) -> Result<(), BudgetLimit> {
        if self.is_unlimited() {
            return Ok(());
        }
        while self
            .window
            .front()
            .is_some_and(|made| now.saturating_duration_since(*made) >= HOUR)
        {
            self.window.pop_front();
        }
        if self.window.len() >= self.hourly_limit {
            return Err(BudgetLimit::Hour);
        }
        if self.used_this_cycle >= self.per_cycle_limit {
            return Err(BudgetLimit::Cycle);
        }
        Ok(())
    }

    /// Uses up one request at `now` if [`RequestBudget::check`] allows it.
    pub(crate) fn try_acquire(&mut self, now: Instant) -> Result<(), BudgetLimit> {
        self.check(now)?;
        if !self.is_unlimited() {
            self.window.push_back(now);
            self.used_this_cycle += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_budget_never_refuses_and_records_nothing() {
        let mut budget = RequestBudget::new(0, 60);
        assert!(budget.is_unlimited());
        assert_eq!(budget.per_cycle_limit(), None);
        let now = Instant::now();
        for _ in 0..100_000 {
            assert_eq!(budget.try_acquire(now), Ok(()));
        }
        assert!(budget.window.is_empty());
        assert_eq!(budget.used_this_cycle, 0);
    }

    #[test]
    fn the_per_cycle_share_is_the_hourly_budget_spread_over_the_hours_cycles() {
        // 5,000 an hour at 60 s cycles: 60 cycles an hour, ceil(83.3) each.
        assert_eq!(RequestBudget::new(5000, 60).per_cycle_limit(), Some(84));
        // Never below one request per cycle...
        assert_eq!(RequestBudget::new(1, 60).per_cycle_limit(), Some(1));
        // ...and never more than the hour's whole budget.
        assert_eq!(RequestBudget::new(10, 7200).per_cycle_limit(), Some(10));
    }

    #[test]
    fn a_cycle_stops_at_its_share_and_the_next_cycle_starts_afresh() {
        let mut budget = RequestBudget::new(120, 60); // 2 per cycle
        let now = Instant::now();
        assert_eq!(budget.try_acquire(now), Ok(()));
        assert_eq!(budget.try_acquire(now), Ok(()));
        assert_eq!(budget.try_acquire(now), Err(BudgetLimit::Cycle));
        budget.start_cycle();
        assert_eq!(budget.try_acquire(now), Ok(()));
    }

    #[test]
    fn the_rolling_hour_is_a_hard_cap_that_frees_up_as_requests_age_out() {
        // Per-cycle share is the whole budget here, so only the hour binds.
        let mut budget = RequestBudget::new(3, 3600);
        let start = Instant::now();
        for minute in 0..3 {
            budget.start_cycle();
            let at = start + Duration::from_secs(minute * 60);
            assert_eq!(budget.try_acquire(at), Ok(()));
        }
        budget.start_cycle();
        let later = start + Duration::from_secs(30 * 60);
        assert_eq!(budget.try_acquire(later), Err(BudgetLimit::Hour));
        // An hour after the first request, exactly one slot is free again.
        let after_first_expires = start + HOUR;
        assert_eq!(budget.try_acquire(after_first_expires), Ok(()));
        assert_eq!(
            budget.try_acquire(after_first_expires),
            Err(BudgetLimit::Hour)
        );
    }

    #[test]
    fn check_does_not_use_up_a_request() {
        let mut budget = RequestBudget::new(60, 60); // 1 per cycle
        let now = Instant::now();
        assert_eq!(budget.check(now), Ok(()));
        assert_eq!(budget.check(now), Ok(()));
        assert_eq!(budget.try_acquire(now), Ok(()));
        assert_eq!(budget.check(now), Err(BudgetLimit::Cycle));
    }
}
