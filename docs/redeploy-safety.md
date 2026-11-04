# Pollers across redeploys: what can be lost

How each poller behaves when its pod is replaced (a chart upgrade, a new
image, a node drain, a crash) or a scheduled run is missed, and what was
changed because of it. The rule throughout: a duplicate is harmless, a gap
is not. Nothing here trades a gap for fewer duplicates.

Reviewed 2026-11-04 against ds-pollers `4420d2c`, ds-core `v0.1.0`,
ds-deploy `da861f2` and ds-ingest `main`. Production's settings are from
Ranma-Config's `distant-signal-config` (read only).

## How every poller restarts

- **The loop** is ds-core's `common::poller_loop`. At startup it reads the
  poller's cursor (its `ingest_freshness` row for a database writer, its
  newest stream entry for a stream producer) and waits out what is left of
  one interval. A restart therefore keeps the old schedule: it costs a poll
  only when the pod is down for longer than the time left to the next one.
  An empty or unreadable cursor means "poll now".
- **No source has a catch-up.** Every upstream is a current-state feed
  (RDM Knowledgebase, LDBWS, TfL's Unified API, Irish Rail's realtime XML,
  BODS SIRI-SX) or a whole reference file. What a poller misses while it is
  down is not recoverable. For reference data that is staleness, healed by
  the next run. For incidents, TfL line status, station samples and
  disruptions it is a hole in the history: an incident that opened and
  cleared while no pod was polling was never seen.
- **SIGTERM.** Only `poller-transit-schedule` handled it. Every other
  poller died at once, mid-cycle if a cycle was running (see the table).
- **Stream producers** (`ingest_stream::snapshot`) queue a snapshot and
  XADD it in the background, part by part. A cycle counts as a success
  once the snapshot is queued, but the restart cursor only moves when an
  entry reaches Redis, so a snapshot lost in the queue is re-fetched at
  once by the next pod. A snapshot cut off after some of its parts moves
  the cursor anyway.
- **Rollout strategy.** Every poller Deployment was `Recreate`, and
  `/healthz` answered ready as soon as the process started. A new pod that
  crashed, hung on the schema gate or failed every cycle therefore left no
  pod polling until someone rolled back.

## Per poller

"Prod" is how production runs it (Ranma-Config, 2026-11-04).

| Poller | Prod | Output | Kind of data | Gap on a restart | SIGTERM mid-cycle (before this change) | Freshness alerts |
| --- | --- | --- | --- | --- | --- | --- |
| incidents | Deployment, 5 min | Postgres: `incidents`, `incident_history`, removal inference; XADD `incident-text-changed` | Current-state feed. Opens, text changes and closes between two polls are lost | Downtime past the next due poll. No catch-up | Committed chunks stay and the rest roll back. The text-changed XADD (enricher's hourly sweep is the backstop, so up to 1 h late) and that snapshot's removal inference are skipped. The next poll heals both | Source `incidents` 900 s; `IncidentSnapshotsMissing`; `PollerStale` (30 min min.); `IngestRolloutStuck` |
| ldbws | Deployment, 60 s | `ds:ingest:station-samples`, 6 parts | Rolling samples; the aggregator drops any older than 15 min | About the downtime. One missed cycle is invisible | A half-sent snapshot leaves its other stations a cycle older. The cursor moves, so the next pod waits at most 60 s | `station-samples` 1800 s; `LdbwsStationStale` 900 s |
| tfl (status) | Deployment, 5 min | `ds:ingest:tfl`, 1 part | Line status. `line_status_history` is only what was written at the time; TfL keeps no history | Downtime past the next due poll. No catch-up | The queued snapshot is lost. The cursor does not move, so the next pod polls at once | `tfl` 900 s; `PollerStale` |
| tfl (stops, routes) | CronJob 05:40 | `ds:ingest:tfl-stops`, 1 part each, whole-table replace | Daily reference | The next day's run | A killed Job's pod is replaced (`DisruptionTarget` is ignored). Each job has its own cursor | `tfl-stops`, `tfl-line-routes` 2 days; scheduled-job alerts |
| stations | CronJob 04:20 | Postgres: one transaction with its freshness row | Daily reference | The next day's run | The transaction rolls back and the retry fetches again | `stations` 2 days; scheduled-job alerts |
| tocs | CronJob 04:40 | `ds:ingest:reference`, 1 part | Daily reference | The next day's run | As tfl status; a retried Job fetches again | `tocs` 2 days; scheduled-job alerts |
| transit-schedule (metrolink) | CronJob timetable + Deployment situations, 5 min | `ds:ingest:transit-metrolink`: timetable parts then a commit; situations | Timetable reference (60-day horizon); situations are current-state | Situations: the downtime (the cursor is "poll now"). Timetable: none, because the horizon covers missed days | Handled: it stops the loops and flushes for 5 s. A timetable without its commit is ignored, and the cursor reads commits only | `transit-metrolink-*`; `TransitFeedRefused`; scheduled-job alerts |
| transit-schedule (tfl) | CronJob | `ds:ingest:transit-tfl`, 61 parts + commit | Timetable reference | None (horizon) | As above | `transit-tfl-timetable` 36 h |
| irish-rail-gtfs | off | `ds:ingest:ioi-gtfs`: stations, then lines | Daily reference | The next day's run | **Stations sent, lines not: the cursor (stations only) says fresh, so lines miss a whole interval (a day)** | `island_of_ireland_*_gtfs` 2 days |
| nir-stations | off | `ds:ingest:ioi-nir`: stations, then lines | Daily reference | As gtfs | **As gtfs** | `island_of_ireland_*_nir` 2 days |
| irish-rail-live | off | `ds:ingest:ioi-live`, 1 part | Current-state samples, 5 min | About the downtime | As tfl status | None of its own (`PollerStale` only) |

The writer side makes overlap safe for the stream producers: every handler
is guarded on its observed time (an older snapshot changes nothing) and
`ingest_dedup` absorbs a re-sent key. Incidents was built for concurrent
writers too: chunk upserts are idempotent, and the removal inference locks
its baseline row `FOR UPDATE`, skipping a snapshot that comes within 120 s
of the last one.

## CronJob pollers

Built by `distant-signal.cronWorkerSpec`: `concurrencyPolicy: Forbid`,
`startingDeadlineSeconds: 3600`, `backoffLimit: 2`, a pod failure policy
(exit 3 and OOM fail the Job at once, `DisruptionTarget` is ignored), and
`activeDeadlineSeconds` per poller (15 to 45 minutes).

- **A Helm upgrade** changes the CronJob object, not a running Job, so a
  deploy during a run neither kills it nor skips it.
- **A missed start** (the controller or cluster down for over an hour at
  the scheduled time) skips that day. `Forbid` can only skip a start while
  a run is still going, and every run ends by its deadline, hours before
  the next day's. All of these are daily reference data or a 60-day
  timetable, so a skipped day is staleness, not a gap.
- **A run killed mid-way** (a drain or eviction) gets a replacement pod.
  stations is one transaction, and the others are single-part snapshots or
  a timetable behind a commit, so a retry never skips half-written data
  (`RUN_ONCE_SKIP_IF_FRESHER_THAN_SECS` reads the same complete-data
  cursor).
- **Noticed by** `DistantSignalScheduledJobFailed` (at once),
  `DistantSignalScheduledJobStale` (1.5 × the interval plus 10 min) and
  the sources' `DistantSignalIngestSourceStale` (2 days, or 36 h for
  timetables).

## Alerts: would a silent stop be noticed?

| Failure | Alert | About when |
| --- | --- | --- |
| Always-on poller's new pod never starts or never becomes Ready | `DistantSignalIngestRolloutStuck` (`poller-.+`) | 10 min |
| Pod running but writing nothing (incidents, tfl, ldbws, metrolink situations) | `DistantSignalIngestSourceStale` | 15 to 30 min, plus 10 min |
| Cycles failing | `DistantSignalPollerFailing` | 10 min after half of 15 min of cycles fail |
| No successful cycle | `DistantSignalPollerStale` | 2 × interval, at least 30 min, plus 10 min |
| CronJob run failed, skipped or never scheduled | `DistantSignalScheduledJobFailed` / `Stale` | At once / 36 h |

`DistantSignalPollerStale` reads a gauge that only exists while a pod is
scraped, so on its own it misses "no pod at all". The rollout and freshness
alerts cover that case. Coverage is adequate, so no alert was added.

## Ranked risks and what was done

1. **A bad deploy of an always-on poller leaves nothing polling.**
   (incidents, tfl status, ldbws, and irish-rail-live when enabled.) Under
   `Recreate` with ready-at-start, a new pod that crashes, waits on the
   schema gate or fails every cycle replaces a working one. The gap lasts
   until a rollback, at least 10 to 25 minutes after the first alert, and
   for incidents and TfL status it is permanent history loss. **Done:**
   - `/healthz` is now ready only after the pod's first successful cycle,
     or after `POLLER_READY_FALLBACK_SECS` (default 480) in any case, so
     an upstream outage cannot stall a rollout for ever (`poller-lifecycle`,
     #4);
   - the chart runs these Deployments with a surge (`pollers.<name>.rollout:
     surge`): RollingUpdate, `maxSurge: 1`, `maxUnavailable: 0`, with a
     readiness probe on `/healthz` (Distant-Signal/ds-deploy#24). The old
     pod keeps polling until the new one has delivered data. The cost is one
     duplicate poll per deploy (one extra LDBWS cycle for ldbws). A new pod
     that never succeeds keeps the old one running and raises
     `DistantSignalIngestRolloutStuck`. Each surge poller's Postgres role
     counts both pods in its connection limit.
2. **SIGTERM mid-cycle.** Incidents loses that snapshot's text-changed
   events (up to an hour late via the enricher sweep) and its removal
   inference. Stream producers lose a queued snapshot or send half of one.
   **Done:** every poller now finishes the cycle in flight (up to 20 s),
   flushes its stream producer (up to 5 s), and then exits (#4).
   Once mode (the CronJobs) keeps dying at once, so its Job retries.
3. **Two-schema island-of-Ireland pollers can leave lines a day stale.**
   gtfs and nir send stations, then lines, and read only the stations
   cursor. **Done:** the cursor is now the older of the two, so a restart
   re-sends both (#5).
4. **Nothing checked the pollers after a deploy.**
   `check-ingest-rollout.py` verified only that their Deployments rolled
   out. **Done:** it now also checks each always-on poller completed a
   successful cycle in a pod started by the deploy, and that no poller
   CronJob missed a scheduled start or failed a run since the baseline
   (Distant-Signal/ds-deploy#25).
5. **Not changed.**
   - A half-sent multi-part snapshot still moves a stream producer's
     cursor. The only multi-part snapshot left is ldbws's, whose cursor
     costs at most 60 s, and the drain in (2) makes it rare. A
     complete-snapshot cursor belongs in ds-core's `SnapshotStream` if a
     daily multi-part producer is ever added.
   - The CronJobs' `startingDeadlineSeconds` (1 h). A longer one would only
     let a daily reference refresh start late after a long outage.
   - transit-schedule stays `Recreate`: two timetable producers must never
     XADD at once (its chart template says why), and its situations
     already poll as soon as they start.
   - Daily pollers run as Deployments (when not CronJobs) stay `Recreate`:
     their first cycle may be a day away, so readiness could not gate a
     rollout, and their data heals on the next run anyway.

## Production questions

These need production data; none was gathered for this review.

- How long a poller pod takes from the old pod's deletion to its first
  cycle under `Recreate`: scheduling, image pull, Postgres or Redis
  connect, schema gate. Read from the pods' events and first-cycle logs
  around a recent deploy. This sizes how often a 5-minute poller loses a
  poll.
- Whether any upgrade so far left an always-on poller without a Ready pod,
  and for how long (`kube_deployment_status_replicas_available` for
  `poller-.+` over the past deploys).
- Whether the enricher's hourly sweep has ever picked up a text change the
  stream missed (its sweep metrics). That would show the SIGTERM window in
  (2) happening.
- The p99 of `distant_signal_poller_cycle_duration_seconds` per poller. It
  should stay well under the 20 s drain. ldbws's budget is 45 s, so a
  drain can still cut its cycle.
- How long Flux takes to apply a release with the surge rollout, against
  the HelmRelease's 20 m timeout.
