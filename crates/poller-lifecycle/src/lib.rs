//! A long-running poller's pod lifecycle around its poll loop
//! (docs/redeploy-safety.md): when the pod counts as ready, and what it
//! does on SIGTERM.
//!
//! **Readiness.** `/healthz` answers 503 `"connecting"` until the pod's
//! first successful cycle (or [`READY_FALLBACK_ENV`] after it started,
//! whichever comes first), then 200 for the rest of its life. With the
//! chart's surge rollout (`RollingUpdate`, `maxUnavailable: 0`) the old pod
//! keeps polling until the new one has delivered data, so a new pod that
//! crashes, hangs on the schema gate or fails every cycle never replaces a
//! working one. The fallback keeps an upstream outage (where the old pod
//! fails too) from stalling a rollout for ever. Readiness stays true once
//! set: a later failure is the alerts' business, not a reason to route a
//! rollout around the pod.
//!
//! **SIGTERM** (a rollout, a drain). The loop is not cancelled mid-cycle: a
//! cycle in flight may finish for up to [`DRAIN_GRACE`], no new one starts,
//! then [`Lifecycle::run`] returns and the caller flushes its stream
//! producer ([`FLUSH_GRACE`]). Both fit Kubernetes' default 30 s
//! `terminationGracePeriodSeconds`. Before this every poller but
//! poller-transit-schedule died at once: a database writer lost that
//! snapshot's follow-up work, a stream producer its queued or half-sent
//! snapshot.
//!
//! **Once mode** (`RUN_ONCE`, a `CronJob`) is left alone: no SIGTERM handler
//! is installed, so an evicted run still dies at once and its Job retries
//! it. A handler there could exit 0 without the run's data.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ds_runtime::service_args::HealthArgs;
use health_http::{ConnectionState, Progress};
use tokio::sync::Notify;

/// Readiness turns true this many seconds after the pod started even
/// without a successful cycle. `0` is ready at once (the old behaviour).
pub const READY_FALLBACK_ENV: &str = "POLLER_READY_FALLBACK_SECS";

/// [`READY_FALLBACK_ENV`]'s default: past a 5-minute poller's first cycle
/// plus its startup, and under `DistantSignalIngestRolloutStuck`'s 10
/// minutes, so a rollout held back by an upstream outage finishes before it
/// alerts.
pub const DEFAULT_READY_FALLBACK: Duration = Duration::from_secs(480);

/// How long a cycle in flight at SIGTERM may run on.
pub const DRAIN_GRACE: Duration = Duration::from_secs(20);

/// How long the caller should then wait for its stream producer to flush.
/// [`DRAIN_GRACE`] plus this stays under the default 30 s grace period.
pub const FLUSH_GRACE: Duration = Duration::from_secs(5);

/// The readiness gauge, `distant_signal_poller_ready`.
const READY_GAUGE: &str = "poller_ready";

/// The pod's readiness and its in-flight cycle. Cheap to clone.
#[derive(Clone)]
pub struct Lifecycle {
    inner: Arc<Inner>,
}

struct Inner {
    ready: ConnectionState,
    /// A cycle is running.
    in_flight: AtomicBool,
    /// SIGTERM came: start no new cycle.
    stopping: AtomicBool,
    /// Notified whenever a cycle ends.
    cycle_ended: Notify,
}

impl Lifecycle {
    /// Over an existing readiness state (`health_http`'s `/healthz`), not
    /// ready yet.
    pub fn new(ready: ConnectionState) -> Self {
        ready.store(false, Ordering::Relaxed);
        Self {
            inner: Arc::new(Inner {
                ready,
                in_flight: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
                cycle_ended: Notify::new(),
            }),
        }
    }

    /// Starts the health listener (`/livez` and `/healthz`, as
    /// `health_http::spawn_liveness` did) with readiness as in the module
    /// docs, and the fallback timer from [`READY_FALLBACK_ENV`].
    ///
    /// # Errors
    ///
    /// [`READY_FALLBACK_ENV`] is not whole seconds.
    pub fn spawn(args: &HealthArgs) -> anyhow::Result<(Self, Progress)> {
        let fallback = ready_fallback(std::env::var(READY_FALLBACK_ENV).ok().as_deref())?;
        let (state, progress) = health_http::spawn_worker(args);
        let lifecycle = Self::new(state);
        lifecycle.ready_after(fallback);
        Ok((lifecycle, progress))
    }

    /// Whether `/healthz` answers ready.
    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Relaxed)
    }

    /// Marks the pod ready after `fallback` whatever its cycles do; at once
    /// for zero.
    pub fn ready_after(&self, fallback: Duration) {
        if fallback.is_zero() {
            self.mark_ready("POLLER_READY_FALLBACK_SECS is 0");
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(fallback).await;
            if !this.is_ready() {
                tracing::warn!(
                    fallback_secs = fallback.as_secs(),
                    "no successful cycle yet; ready anyway, so a rollout is not held for ever"
                );
            }
            this.mark_ready("readiness fallback elapsed");
        });
    }

    fn mark_ready(&self, why: &'static str) {
        if !self.inner.ready.swap(true, Ordering::Relaxed) {
            tracing::info!(why, "ready");
            health_http::set_connected(&self.inner.ready, READY_GAUGE, true);
        }
    }

    /// Runs one cycle: marks the pod ready when it succeeds, and lets
    /// [`Lifecycle::run`] wait for it at SIGTERM. After SIGTERM it never
    /// starts the cycle (it waits until the loop is dropped). Wrap every
    /// loop's cycle in it: `|| lifecycle.track(poll_once(..))`.
    pub async fn track<C>(&self, cycle: C) -> anyhow::Result<()>
    where
        C: Future<Output = anyhow::Result<()>>,
    {
        if self.inner.stopping.load(Ordering::Acquire) {
            return std::future::pending().await;
        }
        let result = {
            let _in_flight = InFlight::enter(&self.inner);
            cycle.await
        };
        if result.is_ok() {
            self.mark_ready("first successful cycle");
        }
        result
    }

    /// Drives `poll_loop` until it returns or, outside once mode, SIGTERM
    /// (or Ctrl-C) comes; then as [`Lifecycle::run_until`].
    pub async fn run<L>(&self, poll_loop: L, once: bool) -> anyhow::Result<()>
    where
        L: Future<Output = anyhow::Result<()>>,
    {
        if once {
            return poll_loop.await;
        }
        self.run_until(poll_loop, shutdown_signal(), DRAIN_GRACE)
            .await
    }

    /// Drives `poll_loop` until it returns (its result) or `signal`
    /// resolves. After the signal no new cycle starts; a cycle in flight
    /// runs on for up to `drain`; then the loop is dropped and this returns
    /// `Ok`, for the caller to flush its producer and exit.
    pub async fn run_until<L, S>(
        &self,
        poll_loop: L,
        signal: S,
        drain: Duration,
    ) -> anyhow::Result<()>
    where
        L: Future<Output = anyhow::Result<()>>,
        S: Future<Output = ()>,
    {
        tokio::pin!(poll_loop);
        tokio::select! {
            result = &mut poll_loop => return result,
            () = signal => {}
        }
        self.inner.stopping.store(true, Ordering::Release);
        if !self.inner.in_flight.load(Ordering::Acquire) {
            tracing::info!("terminating between cycles");
            return Ok(());
        }
        tracing::info!(
            drain_secs = drain.as_secs(),
            "terminating: finishing the cycle in flight"
        );
        let idle = async {
            loop {
                let ended = self.inner.cycle_ended.notified();
                tokio::pin!(ended);
                ended.as_mut().enable();
                if !self.inner.in_flight.load(Ordering::Acquire) {
                    return;
                }
                ended.await;
            }
        };
        tokio::select! {
            result = &mut poll_loop => result,
            () = idle => {
                tracing::info!("the cycle in flight finished; terminating");
                Ok(())
            }
            () = tokio::time::sleep(drain) => {
                tracing::warn!(
                    drain_secs = drain.as_secs(),
                    "the cycle in flight did not finish in time; terminating anyway"
                );
                Ok(())
            }
        }
    }
}

/// Clears `in_flight` and wakes [`Lifecycle::run_until`] when a cycle ends,
/// including one whose future is dropped half way.
struct InFlight<'a>(&'a Inner);

impl<'a> InFlight<'a> {
    fn enter(inner: &'a Inner) -> Self {
        inner.in_flight.store(true, Ordering::Release);
        Self(inner)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.store(false, Ordering::Release);
        self.0.cycle_ended.notify_waiters();
    }
}

/// `defaults` (a poller's `--print-config-defaults`) with
/// [`READY_FALLBACK_ENV`], which [`Lifecycle::spawn`] reads outside clap, so
/// the chart's config check knows the variable and its default.
#[must_use]
pub fn config_defaults(
    defaults: ds_runtime::service_args::ConfigDefaults,
) -> ds_runtime::service_args::ConfigDefaults {
    defaults.env(
        READY_FALLBACK_ENV,
        Some(&DEFAULT_READY_FALLBACK.as_secs().to_string()),
    )
}

/// [`READY_FALLBACK_ENV`]'s value: unset is [`DEFAULT_READY_FALLBACK`].
fn ready_fallback(value: Option<&str>) -> anyhow::Result<Duration> {
    match value.map(str::trim) {
        None | Some("") => Ok(DEFAULT_READY_FALLBACK),
        Some(text) => text.parse::<u64>().map(Duration::from_secs).map_err(|err| {
            anyhow::anyhow!("{READY_FALLBACK_ENV} must be whole seconds, not {text:?}: {err}")
        }),
    }
}

/// SIGTERM (Kubernetes) or Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(term) => term,
                Err(err) => {
                    tracing::warn!(error = %err, "no SIGTERM handler; Ctrl-C only");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    fn lifecycle() -> Lifecycle {
        Lifecycle::new(Arc::new(AtomicBool::new(true)))
    }

    #[test]
    fn a_new_lifecycle_is_not_ready_whatever_the_state_said() {
        assert!(!lifecycle().is_ready());
    }

    #[test]
    fn the_fallback_defaults_and_parses() {
        assert_eq!(ready_fallback(None).ok(), Some(DEFAULT_READY_FALLBACK));
        assert_eq!(ready_fallback(Some(" ")).ok(), Some(DEFAULT_READY_FALLBACK));
        assert_eq!(ready_fallback(Some("0")).ok(), Some(Duration::ZERO));
        assert_eq!(
            ready_fallback(Some("90")).ok(),
            Some(Duration::from_secs(90))
        );
        assert!(ready_fallback(Some("5m")).is_err());
    }

    #[test]
    fn config_defaults_declare_the_fallback() {
        let printed =
            config_defaults(ds_runtime::service_args::ConfigDefaults::new("poller-test")).to_json();
        assert_eq!(
            printed["env"][READY_FALLBACK_ENV]["default"].as_str(),
            Some("480")
        );
    }

    #[test]
    fn the_drain_and_flush_fit_the_default_grace_period() {
        assert!(DRAIN_GRACE + FLUSH_GRACE < Duration::from_secs(30));
    }

    #[tokio::test]
    async fn a_failed_cycle_leaves_it_unready_and_a_successful_one_makes_it_ready() {
        let lc = lifecycle();
        assert!(
            lc.track(async { Err(anyhow::anyhow!("upstream down")) })
                .await
                .is_err()
        );
        assert!(!lc.is_ready());
        assert!(lc.track(async { Ok(()) }).await.is_ok());
        assert!(lc.is_ready());
        // Sticky: a later failure does not take it back.
        assert!(
            lc.track(async { Err(anyhow::anyhow!("again")) })
                .await
                .is_err()
        );
        assert!(lc.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn the_fallback_makes_it_ready_without_a_successful_cycle() {
        let lc = lifecycle();
        lc.ready_after(Duration::from_secs(480));
        tokio::time::sleep(Duration::from_secs(479)).await;
        assert!(!lc.is_ready());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(lc.is_ready());
    }

    #[tokio::test]
    async fn a_zero_fallback_is_ready_at_once() {
        let lc = lifecycle();
        lc.ready_after(Duration::ZERO);
        assert!(lc.is_ready());
    }

    #[tokio::test]
    async fn a_loop_that_returns_returns_its_result() {
        let lc = lifecycle();
        let result = lc
            .run_until(
                async { Err(anyhow::anyhow!("fatal")) },
                std::future::pending(),
                DRAIN_GRACE,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn once_mode_never_waits_for_a_signal() {
        let lc = lifecycle();
        assert!(lc.run(async { Ok(()) }, true).await.is_ok());
    }

    /// A loop shaped like `common::poller_loop`'s: idle `period`, then one
    /// cycle of `work` through `track`, counting cycles started and
    /// finished.
    async fn poll_loop(
        lc: &Lifecycle,
        period: Duration,
        work: Duration,
        started: &AtomicU32,
        finished: &AtomicU32,
    ) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(period).await;
            let _ = lc
                .track(async {
                    started.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(work).await;
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_between_cycles_returns_at_once() {
        let lc = lifecycle();
        let (started, finished) = (AtomicU32::new(0), AtomicU32::new(0));
        let begun = tokio::time::Instant::now();
        let result = lc
            .run_until(
                poll_loop(
                    &lc,
                    Duration::from_secs(60),
                    Duration::from_secs(5),
                    &started,
                    &finished,
                ),
                // During the first idle wait.
                tokio::time::sleep(Duration::from_secs(10)),
                DRAIN_GRACE,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(begun.elapsed(), Duration::from_secs(10));
        assert_eq!(started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_mid_cycle_waits_for_the_cycle_and_starts_no_other() {
        let lc = lifecycle();
        let (started, finished) = (AtomicU32::new(0), AtomicU32::new(0));
        let begun = tokio::time::Instant::now();
        let result = lc
            .run_until(
                // Cycles start at 1 s and run 5 s: the signal at 3 s is
                // mid-cycle.
                poll_loop(
                    &lc,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                    &started,
                    &finished,
                ),
                tokio::time::sleep(Duration::from_secs(3)),
                DRAIN_GRACE,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(begun.elapsed(), Duration::from_secs(6));
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert!(lc.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn a_cycle_past_the_drain_is_cut_off_at_the_drain() {
        let lc = lifecycle();
        let (started, finished) = (AtomicU32::new(0), AtomicU32::new(0));
        let begun = tokio::time::Instant::now();
        let result = lc
            .run_until(
                poll_loop(
                    &lc,
                    Duration::from_secs(1),
                    Duration::from_secs(120),
                    &started,
                    &finished,
                ),
                tokio::time::sleep(Duration::from_secs(3)),
                DRAIN_GRACE,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(begun.elapsed(), Duration::from_secs(3) + DRAIN_GRACE);
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        assert!(!lc.is_ready());
    }
}
