//! A long-running poller's pod lifecycle around its poll loop
//! (docs/redeploy-safety.md): when the pod counts as ready, and what it
//! does on SIGTERM.
//!
//! **Readiness.** `/healthz` answers 503 `"connecting"` until the pod's
//! first successful cycle, or until [`READY_FALLBACK_ENV`] has passed since
//! its sink connected ([`Lifecycle::sink_connected`]: Postgres and the
//! schema gate for a database writer), whichever comes first. With the
//! chart's surge rollout (`RollingUpdate`, `maxUnavailable: 0`) the old pod
//! keeps polling until the new one has delivered data, so a new pod that
//! crashes, hangs on the schema gate or fails every cycle never replaces a
//! working one. The fallback keeps an upstream outage (where the old pod
//! fails too) from stalling a rollout for ever, and starts only once the
//! sink is connected, so a pod still waiting for Postgres is never ready.
//!
//! Readiness then follows the cycles: [`UNREADY_AFTER_FAILURES`]
//! consecutive failed cycles make the pod unready, and the next successful
//! one makes it ready again. (Readiness used to stay true once set.)
//! Liveness (`/livez`) is unaffected: it is loop progress, which a failed
//! cycle and a quiet wait both keep, so a pod whose upstream is down is
//! never restarted for it.
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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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

/// Consecutive failed cycles after which the pod is unready, until a
/// cycle succeeds. Three, so one bad poll (or two) is not a flap; a
/// failure streak this long is also what the poll loop backs off on.
pub const UNREADY_AFTER_FAILURES: u32 = 3;

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
    /// [`READY_FALLBACK_ENV`], started by [`Lifecycle::sink_connected`].
    fallback: Duration,
    /// [`Lifecycle::sink_connected`] was called (the fallback is running).
    sink_connected: AtomicBool,
    /// A cycle has succeeded: the fallback no longer applies.
    succeeded: AtomicBool,
    /// Failed cycles since the last success (or the fallback).
    failures: AtomicU32,
    /// A cycle is running.
    in_flight: AtomicBool,
    /// SIGTERM came: start no new cycle.
    stopping: AtomicBool,
    /// Notified whenever a cycle ends.
    cycle_ended: Notify,
}

impl Lifecycle {
    /// Over an existing readiness state (`health_http`'s `/healthz`), not
    /// ready yet, with `fallback` from [`Lifecycle::sink_connected`].
    pub fn new(ready: ConnectionState, fallback: Duration) -> Self {
        ready.store(false, Ordering::Relaxed);
        Self {
            inner: Arc::new(Inner {
                ready,
                fallback,
                sink_connected: AtomicBool::new(false),
                succeeded: AtomicBool::new(false),
                failures: AtomicU32::new(0),
                in_flight: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
                cycle_ended: Notify::new(),
            }),
        }
    }

    /// Starts the health listener (`/livez` and `/healthz`, as
    /// `health_http::spawn_liveness` did) with readiness as in the module
    /// docs, reading [`READY_FALLBACK_ENV`]. The fallback timer starts at
    /// [`Lifecycle::sink_connected`].
    ///
    /// # Errors
    ///
    /// [`READY_FALLBACK_ENV`] is not whole seconds.
    pub fn spawn(args: &HealthArgs) -> anyhow::Result<(Self, Progress)> {
        let fallback = ready_fallback(std::env::var(READY_FALLBACK_ENV).ok().as_deref())?;
        let (state, progress) = health_http::spawn_worker(args);
        Ok((Self::new(state, fallback), progress))
    }

    /// Whether `/healthz` answers ready.
    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Relaxed)
    }

    /// The poller's sink is connected (a database writer: Postgres and its
    /// schema gate; a stream producer, whose Redis client is lazy: once it
    /// is built), so the pod could deliver data. Starts the readiness
    /// fallback: if no cycle has succeeded within it, the pod is ready
    /// anyway (at once for a zero fallback). Call it once, before the
    /// loop; later calls do nothing.
    pub fn sink_connected(&self) {
        if self.inner.sink_connected.swap(true, Ordering::AcqRel) {
            return;
        }
        let fallback = self.inner.fallback;
        if fallback.is_zero() {
            self.fallback_elapsed("POLLER_READY_FALLBACK_SECS is 0");
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(fallback).await;
            if !this.inner.succeeded.load(Ordering::Acquire) {
                tracing::warn!(
                    fallback_secs = fallback.as_secs(),
                    "no successful cycle yet; ready anyway, so a rollout is not held for ever"
                );
            }
            this.fallback_elapsed("readiness fallback elapsed");
        });
    }

    /// The fallback: ready, with the failure streak started afresh, unless
    /// a cycle has already succeeded (readiness then follows the cycles).
    fn fallback_elapsed(&self, why: &'static str) {
        if self.inner.succeeded.load(Ordering::Acquire) {
            return;
        }
        self.inner.failures.store(0, Ordering::Release);
        self.set_ready(true, why);
    }

    fn set_ready(&self, ready: bool, why: &'static str) {
        if self.inner.ready.swap(ready, Ordering::Relaxed) != ready {
            if ready {
                tracing::info!(why, "ready");
            } else {
                tracing::warn!(why, "not ready");
            }
            health_http::set_connected(&self.inner.ready, READY_GAUGE, ready);
        }
    }

    /// A cycle's outcome: a success makes the pod ready and ends any
    /// failure streak; [`UNREADY_AFTER_FAILURES`] failures in a row make it
    /// unready.
    fn cycle_ended(&self, ok: bool) {
        if ok {
            self.inner.succeeded.store(true, Ordering::Release);
            self.inner.failures.store(0, Ordering::Release);
            self.set_ready(true, "successful cycle");
            return;
        }
        let failures = self
            .inner
            .failures
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if failures >= UNREADY_AFTER_FAILURES {
            self.set_ready(false, "consecutive failed cycles");
        }
    }

    /// Runs one cycle: marks the pod ready when it succeeds, counts it
    /// towards [`UNREADY_AFTER_FAILURES`] when it fails, and lets
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
        self.cycle_ended(result.is_ok());
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
        Lifecycle::new(Arc::new(AtomicBool::new(true)), DEFAULT_READY_FALLBACK)
    }

    fn lifecycle_with_fallback(fallback: Duration) -> Lifecycle {
        Lifecycle::new(Arc::new(AtomicBool::new(true)), fallback)
    }

    async fn fail(lc: &Lifecycle) {
        assert!(
            lc.track(async { Err(anyhow::anyhow!("upstream down")) })
                .await
                .is_err()
        );
    }

    async fn succeed(lc: &Lifecycle) {
        assert!(lc.track(async { Ok(()) }).await.is_ok());
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
        fail(&lc).await;
        assert!(!lc.is_ready());
        succeed(&lc).await;
        assert!(lc.is_ready());
        // One failure is not enough to take it back.
        fail(&lc).await;
        assert!(lc.is_ready());
    }

    /// Readiness is honest after the first success: a failure streak makes
    /// the pod unready, and the next success makes it ready again.
    #[tokio::test]
    async fn consecutive_failures_make_it_unready_until_a_success() {
        let lc = lifecycle();
        succeed(&lc).await;
        for _ in 1..UNREADY_AFTER_FAILURES {
            fail(&lc).await;
            assert!(lc.is_ready(), "not yet");
        }
        fail(&lc).await;
        assert!(!lc.is_ready());
        fail(&lc).await;
        assert!(!lc.is_ready());
        succeed(&lc).await;
        assert!(lc.is_ready());
        // The streak started again.
        fail(&lc).await;
        assert!(lc.is_ready());
    }

    /// The fallback timer starts when the sink connects, not when the pod
    /// starts: a pod still waiting for Postgres (or its schema gate) is
    /// never ready, however long it waits.
    #[tokio::test(start_paused = true)]
    async fn the_fallback_starts_only_once_the_sink_is_connected() {
        let lc = lifecycle();
        tokio::time::sleep(DEFAULT_READY_FALLBACK * 3).await;
        assert!(!lc.is_ready(), "the sink never connected");
        lc.sink_connected();
        tokio::time::sleep(Duration::from_secs(479)).await;
        assert!(!lc.is_ready());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(lc.is_ready());
    }

    /// An upstream outage at startup: the fallback makes the pod ready
    /// (so a rollout is not held), and the failure streak then starts
    /// afresh, so it goes unready only after that many more failures.
    #[tokio::test(start_paused = true)]
    async fn a_pod_ready_on_the_fallback_goes_unready_after_a_new_streak() {
        let lc = lifecycle_with_fallback(Duration::from_secs(60));
        lc.sink_connected();
        for _ in 0..UNREADY_AFTER_FAILURES + 2 {
            fail(&lc).await;
        }
        assert!(!lc.is_ready());
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert!(lc.is_ready(), "the fallback");
        for _ in 1..UNREADY_AFTER_FAILURES {
            fail(&lc).await;
        }
        assert!(lc.is_ready());
        fail(&lc).await;
        assert!(!lc.is_ready());
    }

    /// Once a cycle has succeeded the fallback no longer applies: it does
    /// not make a pod that has since gone unready ready again.
    #[tokio::test(start_paused = true)]
    async fn the_fallback_does_not_override_a_failure_streak_after_a_success() {
        let lc = lifecycle_with_fallback(Duration::from_secs(60));
        lc.sink_connected();
        succeed(&lc).await;
        for _ in 0..UNREADY_AFTER_FAILURES {
            fail(&lc).await;
        }
        assert!(!lc.is_ready());
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert!(!lc.is_ready());
    }

    #[tokio::test]
    async fn a_zero_fallback_is_ready_once_the_sink_connects() {
        let lc = lifecycle_with_fallback(Duration::ZERO);
        assert!(!lc.is_ready());
        lc.sink_connected();
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
