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
//! **Several loops in one process** (poller-tfl's status poll and daily
//! jobs). Each loop wraps its cycles in its own [`Tracker`]
//! ([`Lifecycle::tracker`]), so each has its own failure streak: the pod is
//! unready while any loop has failed [`UNREADY_AFTER_FAILURES`] cycles in a
//! row, and one loop's successes no longer hide another's failures. The
//! cycles in flight are counted, so SIGTERM waits for every loop's cycle,
//! not only the one that started last. Each loop also beats its own
//! [`Progress`] ([`LoopProgress`]), so `/livez` follows the slowest loop:
//! one loop's idle beats no longer hide another loop hanging.
//!
//! **A sink that can go away** (a stream producer's Redis): with
//! [`Lifecycle::watch_sink`] the pod is also unready while the sink says it
//! cannot write, as poller-transit-schedule's readiness does.
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
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

/// How often [`Lifecycle::watch_sink`] asks the sink whether it can write.
pub const SINK_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How often [`LoopProgress`] folds the loops' progress into `/livez`'s.
const LOOP_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

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
    /// What readiness is computed from.
    cycles: Mutex<Cycles>,
    /// Cycles running (at most one per loop).
    in_flight: AtomicUsize,
    /// SIGTERM came: start no new cycle.
    stopping: AtomicBool,
    /// Notified whenever a cycle ends.
    cycle_ended: Notify,
}

/// The cycles' outcomes, as readiness sees them.
struct Cycles {
    /// A cycle has succeeded: the fallback no longer applies.
    succeeded: bool,
    /// A cycle has succeeded or the fallback elapsed: the pod may be ready.
    may_be_ready: bool,
    /// Per loop ([`Tracker`]; index 0 is [`Lifecycle::track`]'s): failed
    /// cycles since its last success (or the fallback).
    failures: Vec<u32>,
    /// [`Lifecycle::watch_sink`]'s last answer (true without one).
    sink_available: bool,
}

impl Cycles {
    /// Ready: a success (or the fallback), no loop on a failure streak, and
    /// a sink that can write.
    fn ready(&self) -> bool {
        self.may_be_ready
            && self.sink_available
            && self.failures.iter().all(|&f| f < UNREADY_AFTER_FAILURES)
    }
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
                cycles: Mutex::new(Cycles {
                    succeeded: false,
                    may_be_ready: false,
                    failures: vec![0],
                    sink_available: true,
                }),
                in_flight: AtomicUsize::new(0),
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
            let succeeded = this.cycles().succeeded;
            if !succeeded {
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
        let mut cycles = self.cycles();
        if cycles.succeeded {
            return;
        }
        cycles.may_be_ready = true;
        cycles.failures.iter_mut().for_each(|f| *f = 0);
        self.set_ready(cycles.ready(), why);
    }

    /// The readiness state. Never held across an await.
    fn cycles(&self) -> MutexGuard<'_, Cycles> {
        self.inner
            .cycles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    /// Loop `index`'s cycle ended: a success makes the pod ready (unless
    /// another loop is on a failure streak, or the sink cannot write) and
    /// ends that loop's streak; [`UNREADY_AFTER_FAILURES`] failures in a row
    /// make it unready.
    fn cycle_ended(&self, index: usize, ok: bool) {
        let mut cycles = self.cycles();
        if ok {
            cycles.succeeded = true;
            cycles.may_be_ready = true;
            cycles.failures[index] = 0;
            self.set_ready(cycles.ready(), "successful cycle");
            return;
        }
        cycles.failures[index] = cycles.failures[index].saturating_add(1);
        if cycles.failures[index] >= UNREADY_AFTER_FAILURES {
            self.set_ready(false, "consecutive failed cycles");
        }
    }

    /// Runs one cycle: marks the pod ready when it succeeds, counts it
    /// towards [`UNREADY_AFTER_FAILURES`] when it fails, and lets
    /// [`Lifecycle::run`] wait for it at SIGTERM. After SIGTERM it never
    /// starts the cycle (it waits until the loop is dropped). Wrap every
    /// loop's cycle in it: `|| lifecycle.track(poll_once(..))`. A process
    /// with several loops gives each other loop its own
    /// [`Lifecycle::tracker`].
    pub async fn track<C>(&self, cycle: C) -> anyhow::Result<()>
    where
        C: Future<Output = anyhow::Result<()>>,
    {
        self.track_as(0, cycle).await
    }

    async fn track_as<C>(&self, index: usize, cycle: C) -> anyhow::Result<()>
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
        self.cycle_ended(index, result.is_ok());
        result
    }

    /// A [`Tracker`] for another loop of this process, with its own failure
    /// streak (see the module docs).
    pub fn tracker(&self) -> Tracker {
        let mut cycles = self.cycles();
        cycles.failures.push(0);
        Tracker {
            lifecycle: self.clone(),
            index: cycles.failures.len() - 1,
        }
    }

    /// Keeps the pod unready while `available` says the sink cannot write
    /// (a stream producer's `is_available`: its last XADD failed), asking
    /// every [`SINK_CHECK_INTERVAL`]. Never returns: drive it alongside the
    /// loops (it borrows the producer, so it is not spawned).
    pub async fn watch_sink<F>(&self, available: F) -> std::convert::Infallible
    where
        F: Fn() -> bool,
    {
        let mut tick = tokio::time::interval(SINK_CHECK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let now = available();
            let mut cycles = self.cycles();
            if cycles.sink_available != now {
                cycles.sink_available = now;
                let why = if now {
                    "the stream can be written again"
                } else {
                    "the stream cannot be written"
                };
                self.set_ready(cycles.ready(), why);
            }
        }
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
        if self.inner.in_flight.load(Ordering::Acquire) == 0 {
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
                if self.inner.in_flight.load(Ordering::Acquire) == 0 {
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

/// One loop's cycles of a [`Lifecycle`] that runs several loops: its own
/// failure streak, the same readiness and drain. Cheap to clone.
#[derive(Clone)]
pub struct Tracker {
    lifecycle: Lifecycle,
    index: usize,
}

impl Tracker {
    /// [`Lifecycle::track`] for this loop.
    pub async fn track<C>(&self, cycle: C) -> anyhow::Result<()>
    where
        C: Future<Output = anyhow::Result<()>>,
    {
        self.lifecycle.track_as(self.index, cycle).await
    }
}

/// Counts a cycle in `in_flight` and, when it ends (including one whose
/// future is dropped half way), uncounts it and wakes
/// [`Lifecycle::run_until`].
struct InFlight<'a>(&'a Inner);

impl<'a> InFlight<'a> {
    fn enter(inner: &'a Inner) -> Self {
        inner.in_flight.fetch_add(1, Ordering::AcqRel);
        Self(inner)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.cycle_ended.notify_waiters();
    }
}

/// Per-loop liveness for a process that runs several loops over one
/// `/livez` [`Progress`]. Each loop beats its own [`LoopProgress::add`]ed
/// `Progress`; `/livez`'s is beaten only once every loop has beaten since
/// it last was, so it goes stalled when the slowest loop does (give or
/// take one of the other loops' beats: [`Progress::idle`] beats every
/// 10 s), and one loop's idle beats cannot hide another loop hanging. With
/// no loop added it beats nothing (whatever beats `/livez`'s progress
/// itself, such as a startup wait, still does).
#[derive(Clone)]
pub struct LoopProgress {
    loops: Arc<Mutex<Vec<Progress>>>,
    stall_after: Duration,
}

impl LoopProgress {
    /// Starts folding the loops' progress into `livez`'s (a task, every
    /// second). Needs a Tokio runtime.
    pub fn spawn(livez: &Progress) -> Self {
        let this = Self {
            loops: Arc::new(Mutex::new(Vec::new())),
            stall_after: livez.stall_after(),
        };
        let watcher = this.clone();
        let livez = livez.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(LOOP_PROGRESS_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                watcher.fold_into(&livez);
            }
        });
        this
    }

    /// A new loop's progress, "just beaten". Add one for each loop the
    /// process actually runs (a loop that is off never beats, so it would
    /// read as a stall), and beat it in place of `/livez`'s.
    pub fn add(&self) -> Progress {
        let progress = Progress::new(self.stall_after);
        self.lock().push(progress.clone());
        progress
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Progress>> {
        self.loops
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Beats `livez` if every loop has beaten since `livez` last was.
    fn fold_into(&self, livez: &Progress) {
        let loops = self.lock();
        if loops.is_empty() {
            return;
        }
        let since = livez.since_last_beat();
        if loops
            .iter()
            .all(|progress| progress.since_last_beat() < since)
        {
            livez.beat();
        }
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

    /// poller-tfl's shape: a short status cycle ending while a long daily
    /// cycle runs must not read as "nothing in flight" at SIGTERM.
    #[tokio::test(start_paused = true)]
    async fn a_signal_waits_for_every_loops_cycle_not_only_the_last_to_start() {
        let lc = lifecycle();
        let daily = lc.tracker();
        let daily_done = AtomicBool::new(false);
        let loops = async {
            let status = async {
                // Starts after the daily cycle, ends before the signal.
                tokio::time::sleep(Duration::from_secs(1)).await;
                let _ = lc
                    .track(async {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Ok(())
                    })
                    .await;
                std::future::pending::<()>().await;
            };
            let routes = async {
                let _ = daily
                    .track(async {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        daily_done.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await;
                std::future::pending::<()>().await;
            };
            tokio::join!(status, routes);
            Ok(())
        };
        let begun = tokio::time::Instant::now();
        let result = lc
            .run_until(
                loops,
                tokio::time::sleep(Duration::from_secs(3)),
                DRAIN_GRACE,
            )
            .await;
        assert!(result.is_ok());
        assert!(daily_done.load(Ordering::SeqCst), "the daily cycle drained");
        assert_eq!(begun.elapsed(), Duration::from_secs(10));
    }

    /// One loop's successes do not hide another loop's failure streak.
    #[tokio::test]
    async fn each_loop_has_its_own_failure_streak() {
        let lc = lifecycle();
        let daily = lc.tracker();
        succeed(&lc).await;
        for _ in 0..UNREADY_AFTER_FAILURES {
            assert!(
                daily
                    .track(async { Err(anyhow::anyhow!("routes failed")) })
                    .await
                    .is_err()
            );
            succeed(&lc).await;
        }
        assert!(!lc.is_ready(), "the daily loop is on a streak");
        succeed(&lc).await;
        assert!(!lc.is_ready(), "status successes do not end it");
        assert!(daily.track(async { Ok(()) }).await.is_ok());
        assert!(lc.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn the_fallback_ends_every_loops_streak() {
        let lc = lifecycle_with_fallback(Duration::from_secs(60));
        let daily = lc.tracker();
        lc.sink_connected();
        for _ in 0..UNREADY_AFTER_FAILURES {
            fail(&lc).await;
            let _ = daily.track(async { Err(anyhow::anyhow!("down")) }).await;
        }
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert!(lc.is_ready());
        let _ = daily.track(async { Err(anyhow::anyhow!("down")) }).await;
        assert!(lc.is_ready(), "the streak started afresh");
    }

    /// A stream producer whose Redis is gone is unready (its cycles still
    /// succeed: the snapshot is queued), and ready again once it writes.
    #[tokio::test(start_paused = true)]
    async fn an_unavailable_sink_makes_it_unready() {
        let lc = lifecycle();
        succeed(&lc).await;
        let available = AtomicBool::new(true);
        let watch = lc.watch_sink(|| available.load(Ordering::SeqCst));
        tokio::pin!(watch);
        let check = Duration::from_secs(SINK_CHECK_INTERVAL.as_secs() + 1);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut watch)
                .await
                .is_err()
        );
        assert!(lc.is_ready());
        available.store(false, Ordering::SeqCst);
        assert!(tokio::time::timeout(check, &mut watch).await.is_err());
        assert!(!lc.is_ready());
        succeed(&lc).await;
        assert!(!lc.is_ready(), "a successful cycle does not override it");
        available.store(true, Ordering::SeqCst);
        assert!(tokio::time::timeout(check, &mut watch).await.is_err());
        assert!(lc.is_ready());
    }

    /// `/livez` follows the slowest loop: one loop idling (beating every
    /// 10 s) does not hide another wedged mid-cycle.
    #[tokio::test(start_paused = true)]
    async fn livez_stalls_when_any_loop_stalls() {
        let stall = Duration::from_secs(120);
        let livez = Progress::new(stall);
        let loops = LoopProgress::spawn(&livez);
        let idle = loops.add();
        let wedged = loops.add();
        let idle_loop = idle.idle(tokio::time::sleep(Duration::from_secs(3600)));
        let check = async {
            // Both beat for a while: never stalled.
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                wedged.beat();
                assert!(!livez.is_stalled());
            }
            // Then one wedges: stalled within the window plus an idle beat.
            tokio::time::sleep(stall + Duration::from_secs(12)).await;
            assert!(livez.is_stalled());
            // And recovers once it beats again.
            wedged.beat();
            tokio::time::sleep(Duration::from_secs(12)).await;
            assert!(!livez.is_stalled());
        };
        tokio::select! {
            () = idle_loop => panic!("the idle wait ended first"),
            () = check => {}
        }
    }

    #[tokio::test(start_paused = true)]
    async fn loop_progress_with_no_loops_leaves_livez_alone() {
        let livez = Progress::new(Duration::from_secs(60));
        let _loops = LoopProgress::spawn(&livez);
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert!(livez.is_stalled());
    }
}
