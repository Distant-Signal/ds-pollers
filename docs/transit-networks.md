# Transit networks: runbook

Transit networks are tram and light-rail networks that are neither National
Rail nor TfL. Manchester Metrolink is the first. Design:
[the transit networks spec](superpowers/specs/2026-10-09-transit-networks-design.md);
plan: [the transit networks plan](superpowers/plans/2026-10-09-transit-networks-plan.md).

One producer per network, `poller-transit-schedule` (Deployment
`<fullname>-poller-transit-schedule-<network>`, `Recreate`), downloads the
BODS GTFS timetable once a day and the BODS SIRI-SX disruptions every 5
minutes, and XADDs them to `ds:ingest:transit-<network>`. The
ingest-writer applies that stream to the `transit_*` tables and to
`line_status` (`source = 'transit'`, ids `<network>:<line>`). The api serves
`/public/transit/...` for the networks in `API_TRANSIT_NETWORKS`, and the
frontend shows its pages only while that list is not empty.

Everything ships off. These are the switches, all in Ranma-Config:

| Value | Default | What it does |
| --- | --- | --- |
| `redis.acl.clients.pollerTransitMetrolink` | `false` | Renders the producer's Redis user (an `optional` user, docs/redis-acl.md). Needs the `poller-transit-metrolink-password` key in the users Secret. |
| `ingestWriter.streams.transit-metrolink` | `off` | `apply` makes the writer consume the stream. Needs `postgresql.roles.perService.writer.connect`. |
| `pollerTransitSchedule.networks.metrolink.enabled` | `false` | Runs the producer. The chart refuses it unless the stream is `apply`. |
| `pollerTransitSchedule.networks.metrolink.situationsEnabled` | `false` | Runs the SIRI-SX loop (line status, disruptions, notifications). |
| `api.transitNetworks` | `[]` | The networks the api serves. Empty: every `/public/transit` route is 404 (`/transit/rail-links` is an empty list) and the frontend hides the pages, the "Trams" nav entry and the attribution lines. |

## Before the first rollout: the grants Job

The deploy that first carries the transit tables (and every later deploy
that adds a transit table or changes its grants) needs the chart fix that
runs the postgres roles and grants Job as a `pre-upgrade` hook after the
migrations. Without it the grants Job is `post-upgrade`: Helm waits for the
rollout first, while a `Recreate` worker whose schema gate checks its
grants (the ingest-writer's) never becomes ready without them. The upgrade
then deadlocks until it times out. The steps below assume that fix is in;
check the `postgres-roles` Job's `helm.sh/hook` annotation before starting.

## Rollout

Each step is its own Ranma-Config change. Wait for each to settle.

1. **The Redis user.** Add `poller-transit-metrolink-password` (letters and
   digits) to the Redis users SealedSecret and set
   `redis.acl.clients.pollerTransitMetrolink: true` in the same change.
   Redis restarts once (twice in quick succession if Reloader also rolls it
   for the Secret), about 4 s of AOF load with every consumer retrying.
   Check:

   ```sh
   REDIS="kubectl -n <namespace> exec deploy/<fullname>-redis -- redis-cli"
   $REDIS ACL USERS | grep poller-transit-metrolink
   ```

2. **The writer and the producer.** In one change:
   `ingestWriter.streams.transit-metrolink: apply` and
   `pollerTransitSchedule.networks.metrolink.enabled: true`. Leave
   `situationsEnabled` off for now. The ingest-writer restarts for its
   `INGEST_WRITER_STREAMS`; the producer starts and downloads the
   timetable (about 123 MB, a few minutes).

3. **Check the timetable, with nothing public.**
   - The stream: `$REDIS XLEN ds:ingest:transit-metrolink` and
     `$REDIS XREVRANGE ds:ingest:transit-metrolink + - COUNT 1` (the last
     entry is a `transit-timetable-commit/1`).
   - The writer: `ingest_stream_consumed_total{stream="ds:ingest:transit-metrolink"}`
     counts `applied` and no `dead_lettered`; `$REDIS XLEN ds:dlq:transit-metrolink`
     is 0.
   - The database (read-only):

     ```sql
     SELECT network, active_feed_version, activated_at FROM transit_networks;
     SELECT feed_version, state, counts, horizon_from, horizon_to
       FROM transit_feed_versions WHERE network = 'metrolink';
     SELECT source, fetched_at FROM ingest_freshness
      WHERE source LIKE 'transit-metrolink-%';
     ```

     Expect one `active` version whose `counts` show 7 lines, about 300
     stops (199 platforms and their stations), about 109 patterns and some
     15,000 trips (the spec's §3.2 figures for October 2026), a horizon of
     60 days from today, and a `transit-metrolink-timetable` freshness from
     the commit.
   - The producer's metrics: `transit_feed_fetch_total{outcome="ok"}`,
     no `transit_feed_refused_total`.

4. **The disruptions.** Set
   `pollerTransitSchedule.networks.metrolink.situationsEnabled: true`
   (the writer must have plan task 4.3, which every image built after batch
   4 has; an older writer dead-letters every snapshot as an unknown
   schema). Within 5 minutes `transit_situations` has rows,
   `line_status` has a `metrolink:<line>` row per line
   (`source = 'transit'`) and `transit-metrolink-situations` is fresh.
   `DistantSignalTransitUnmappedLineRef` means a SIRI `LineRef` has no
   alias (see "Adding a SIRI line alias").

5. **Public.** After a day with the timetable applied (one daily refresh
   seen), set `api.transitNetworks: [metrolink]`. The api restarts; the
   "Trams" nav entry, `/transit`, the footer's Open Government Licence
   line, the Metrolink section of `/attribution` and the "Change for
   trams" links on National Rail station pages appear.

### Rollback

1. `api.transitNetworks: []`: every page and link disappears at once.
   Pins on tram lines are kept but not shown.
2. `situationsEnabled: false`, then `enabled: false` and
   `ingestWriter.streams.transit-metrolink: off` in one change (the chart
   refuses a producer without `apply`). Transit rows in `line_status` stop
   updating; nothing reads them while the api switch is off.
3. Optionally the Redis client off (one more Redis restart), then the
   Secret key removed.
4. To remove the data too, see "Turning a network off for good".

## Inspecting the stream and its dead letters

The stream's cap is `MAXLEN ~ 320` (`crates/ingest-stream/src/budget.rs`).

```sh
$REDIS XINFO GROUPS ds:ingest:transit-metrolink
$REDIS XRANGE ds:dlq:transit-metrolink - + COUNT 5
```

Dead-letter reasons particular to transit (prefixes of the record's
`error`):

- `incomplete_feed_version`: a commit arrived but a part of its version
  had not (a part dead-lettered or trimmed). The active version keeps
  serving; the next daily feed retries. Re-inject the missing part and
  then the commit, in that order, only if it is the same day; otherwise
  delete the records and let tomorrow's feed land.
- `inconsistent_feed_version`: the staged rows don't hang together (a
  trip without its pattern, no service today). A producer or feed bug;
  keep the records as evidence.
- `network_not_for_stream`: an entry naming another network than its
  stream's. A producer bug or a misconfiguration; never re-inject.
- `invalid_transit_row`: an id outside its grammar. A producer bug.

Re-inject with the script in
[docs/ingest-streams-deadletter.md](ingest-streams-deadletter.md), with
`320` as the MAXLEN:

```sh
$REDIS EVAL "$(cat ingest-reinject.lua)" 2 ds:dlq:transit-metrolink ds:ingest:transit-metrolink <first> <last> 320 '*'
```

A re-injected situations snapshot older than the newest applied changes
nothing (the writer's ordering guard), which is right: the next snapshot
is at most 5 minutes away.

## Forcing a timetable refresh

The producer fetches the timetable when its newest commit on the stream is
`timetableIntervalSecs` (a day) old, so **a restart alone does not
refresh it**: the new pod reads that commit and waits out the rest of the
day (seen in the 2026-10-14 dry run). To pick up a new BODS file sooner,
set `pollerTransitSchedule.networks.metrolink.timetableIntervalSecs` to
`600` for one release (the pod restarts and fetches within a minute), then
put it back to `86400`.

A fetched feed whose content is unchanged since the last commit is not
re-sent ("the timetable is unchanged since the last commit; not re-sending
it yet" in the log); the daily re-send moves the timetable freshness.

## Adding a SIRI line alias

`DistantSignalTransitUnmappedLineRef` names the `ref` (also
`transit_situation_unmapped_line_refs_total{network,ref}`). Situations
with that ref still apply, but not to a line's status. Add the ref to the
line's `siri_line_refs` in
`crates/poller-transit-schedule/networks/metrolink.toml` (compiled in) and
release. Unit tests check the file at build time.

## Switching to TfGM's own GTFS (the fallback)

Only if BODS stops publishing Metrolink. TfGM publishes its own GTFS
under the ODbL (spec §4). Never use `apiary.tfgm.com` (its API portal):
only the open data download.

1. Set `pollerTransitSchedule.networks.metrolink.gtfsUrl` to TfGM's GTFS
   zip, and check the network file's `nocs` still match its
   `agency_noc` (the producer filters by them).
2. The attribution changes: the network file's `[attribution]` text and
   `frontend/components/OpenDataAttribution.tsx` take the ODbL notice
   TfGM's licence page asks for, worded as that page says at the time.
   This is a code change and a release, before the URL change.
3. Watch the first commit as in rollout step 3: a different source can
   change counts enough to trip the sharp-drop guard
   (`feedMaxDropFraction`; `transit_feed_refused_total{reason="sharp_drop"}`).

## Adding a network

Each network is compiled in and has its own stream, user and switches.
For `<n>` (`[a-z0-9-]`):

1. **Network file:** `crates/poller-transit-schedule/networks/<n>.toml`
   (sources, NOCs, ATCO prefix, SIRI filters and line aliases,
   attribution), added to `FILES` in `src/network.rs`.
2. **The id:** add it to `common::transit::NETWORKS`. The writer's
   network pinning reads it.
3. **The stream:** `ingest_stream::streams` gains `TRANSIT_<N>`
   (`streams::transit("<n>")`), with a `StreamDecl` in `budget.rs` sized
   from its feed, and the writer's `stream::STREAMS` gains a
   `transit-<n>` spec with the three transit schemas.
   `INGEST_WRITER_STREAMS` gains `transit-<n>:off`: update the CI
   expectations that list it (`.github/workflows/ci.yml`,
   `scripts/check-ingest-phase0-chart.py`) and
   `crates/ingest-stream/tests/chart_values.rs`.
4. **Redis:** an `optional` line `poller-transit-<n>` in
   `charts/distant-signal/files/redis-users.acl.tpl`, the
   `pollerTransit<N>` client value, `scripts/render-redis-acl.py` and
   `crates/common/tests/redis_acl.rs`, docs/redis-acl.md.
5. **Values:** `pollerTransitSchedule.networks.<n>`,
   `ingestWriter.streams.transit-<n>`, the per-stream backlog and stall
   maps, `ingestSourceStale` entries for `transit-<n>-timetable` (36 h)
   and `transit-<n>-situations` (30 min), and the chart README rows
   (`scripts/chart-values-doc.py check`).
6. **Grants:** the two freshness sources join the writer's
   `ingest_freshness` row policy in `db-grants.yaml`; regenerate
   `postgres-grants.sql`. `line_status` needs nothing (one `transit`
   source for every network).
7. **Attribution:** the network's credit in `OpenDataAttribution.tsx`
   (`TRANSIT_DATA_SOURCES`), with the licence basis.
8. Roll out as above, with the new names.

## Turning a network off for good

After the rollback, as the database owner, in one transaction:

```sql
BEGIN;
DELETE FROM line_status_history WHERE starts_with(line_id, 'metrolink:');
DELETE FROM line_status WHERE source = 'transit' AND starts_with(line_id, 'metrolink:');
DELETE FROM transit_situations WHERE network = 'metrolink';
DELETE FROM transit_rail_links WHERE network = 'metrolink';
DELETE FROM transit_networks WHERE network = 'metrolink';
-- Every versioned table cascades from its feed version.
DELETE FROM transit_feed_versions WHERE network = 'metrolink';
DELETE FROM ingest_freshness
 WHERE source IN ('transit-metrolink-timetable', 'transit-metrolink-situations');
-- Optional: users' pins on its lines (hidden anyway while it is off).
DELETE FROM pinned_lines WHERE starts_with(line_id, 'metrolink:');
COMMIT;
```

Then `$REDIS DEL ds:ingest:transit-metrolink ds:dlq:transit-metrolink`
once the writer no longer consumes it.

## Alerts

- [DistantSignalTransitFeedRefused](alerts.md#distantsignaltransitfeedrefused)
- [DistantSignalTransitUnmappedLineRef](alerts.md#distantsignaltransitunmappedlineref)
- [DistantSignalIngestSourceStale](alerts.md#distantsignalingestsourcestale)
  for `transit-metrolink-timetable` (36 h) and
  `transit-metrolink-situations` (30 min), only while the stream applies.
- [DistantSignalIngestStreamBacklog](alerts.md#distantsignalingeststreambacklog),
  [DistantSignalIngestStreamStalled](alerts.md#distantsignalingeststreamstalled)
  and [DistantSignalIngestDeadLetters](alerts.md#distantsignalingestdeadletters)
  for `ds:ingest:transit-metrolink`.

Thirty minutes after the last situations snapshot the pages show "Status
unavailable" instead of line statuses, before the stale alert fires.
