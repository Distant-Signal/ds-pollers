# Transit networks: runbook

Transit networks are tram, light-rail and metro networks with their own
timetable pages under `/transit`: Manchester Metrolink (`metrolink`),
Nottingham Express Transit (`net`) and TfL's Underground, DLR and London
Trams (`tfl`, see [TfL](#tfl-underground-dlr-and-london-trams)). Design:
[the transit networks spec](superpowers/specs/2026-10-09-transit-networks-design.md)
and [the NET addendum](superpowers/specs/2026-10-24-nottingham-net-design.md);
plan: [the transit networks plan](superpowers/plans/2026-10-09-transit-networks-plan.md).
This runbook is written for Metrolink; NET's differences, and its own
rollout, are in [Nottingham Express Transit](#nottingham-express-transit-net).

One producer per network, `poller-transit-schedule` (Deployment
`<fullname>-poller-transit-schedule-<network>`, `Recreate`), downloads the
BODS GTFS timetable once a day and the BODS SIRI-SX disruptions every 5
minutes, and XADDs them to `ds:ingest:transit-<network>`. The
ingest-writer applies that stream to the `transit_*` tables and to
`line_status` (`source = 'transit'`, ids `<network>:<line>`). The api serves
`/public/transit/...` for the networks in `API_TRANSIT_NETWORKS`, and the
frontend shows its pages only while that list is not empty.

Everything ships off. These are Metrolink's switches, all in Ranma-Config
(NET's are [below](#nottingham-express-transit-net)):

| Value | Default | What it does |
| --- | --- | --- |
| `redis.acl.clients.pollerTransitMetrolink` | `false` | Renders the producer's Redis user (an `optional` user, docs/redis-acl.md). Needs the `poller-transit-metrolink-password` key in the users Secret. |
| `ingestWriter.streams.transit-metrolink` | `off` | `apply` makes the writer consume the stream. Needs `postgresql.roles.perService.writer.connect`. |
| `pollerTransitSchedule.networks.metrolink.enabled` | `false` | Runs the producer. The chart refuses it unless the stream is `apply`. |
| `pollerTransitSchedule.networks.metrolink.situationsEnabled` | `false` | Runs the SIRI-SX loop (line status, disruptions, notifications). |
| `pollerTransitSchedule.networks.metrolink.timetableRefresh` | `""` | A one-shot timetable refresh: a new token makes the producer fetch and publish at start (see "Forcing a timetable refresh"). |
| `api.transitNetworks` | `[]` | The networks the api serves. Empty: every `/public/transit` route is 404 (`/transit/rail-links` is an empty list) and the frontend hides the pages, the "Trams" nav entry and the attribution lines. |

## Before the first rollout: the grants Job

The deploy that first carries the transit tables (and every later deploy
that adds a transit table or changes its grants, such as a new network's
freshness sources) relies on the chart running the postgres roles setup
Job as a `pre-upgrade` hook after the migrate Job (chart fix 41bfe34a,
2026-10-14; docs/postgres-app-role.md, "Hook order"). Before that fix the
Job was `post-upgrade`: Helm waited for the rollout first, while the
ingest-writer (`Recreate`, its schema gate checking its grants on the new
tables) never became ready without them, and the upgrade deadlocked until
its timeout. The steps below assume the fix is deployed and
`migrate.job.enabled` is on (without it the Job stays post-upgrade). If an
upgrade ever stalls with the writer NotReady on `missing: ... on
transit_*`, that is this deadlock: run the setup Job by hand, as on
2026-10-14.

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

## Trip planning

`/Trips/plan` can ride a network's trams anywhere in a journey (rail into
Piccadilly, the tram to Victoria, rail on). Design:
[the transit trip planning spec](superpowers/specs/2026-10-24-transit-trip-planning-design.md);
code: `crates/api/src/data/transit_planning.rs`. Off by default:

| Value | Default | What it does |
| --- | --- | --- |
| `api.tripPlanTransitNetworks` (`API_TRIP_PLAN_TRANSIT_NETWORKS`) | `[]` | The networks whose trams join the planner's day graph. Empty: the graph, its cache key and every response are as before. Only `metrolink` (the chart and the api refuse anything else). |
| `api.tripPlanTransitChangeMinutes` (`TRIP_PLAN_TRANSIT_CHANGE_MINUTES`) | `2` | Extra minutes on the tram side of a rail-to-tram or tram-to-rail change (0-15). |

With the list set, a request plans with trams only when it sends
`transit=true` or names a tram stop (`9400ZZMAPIC`) as its origin or
destination; any other request gets a byte-identical response. The
frontend sends `transit=true` from phase M3 on, and offers Metrolink stops
as ends only for networks `/public/transit/networks` marks `tripPlanning`,
so `api.transitNetworks` must list the network too.

Deploy notes: no migration, no grant change (the api role already reads
`transit_*`, `stations` and `line_status`). The api restarts for the new
values. Each cached date grows by the network's day (about 6 MB for a
Metrolink weekday); the cache keeps 2.

### Measuring before the rollout (phase M4)

Run on a copy of production, never on production: a local Postgres
restored from a recent backup, or a fresh clone. Build the api in release
mode (`cargo build --release -p api`) and, with the same environment both
times except the switch:

```sh
# 1. Off.
API_TRIP_PLAN_TRANSIT_NETWORKS= ./target/release/api &
uv run scripts/measure-transit-trip-planning.py measure \
  --api http://127.0.0.1:8080 --date <a weekday within the feed's horizon> \
  --label off --out off.json --pid $!
kill %1

# 2. On (a fresh process, so the RSS is comparable).
API_TRIP_PLAN_TRANSIT_NETWORKS=metrolink ./target/release/api &
uv run scripts/measure-transit-trip-planning.py measure \
  --api http://127.0.0.1:8080 --date <the same weekday> \
  --label on --transit --out on.json --pid $!
kill %1

# 3. The budget (spec §10): RSS at most +25 MB, searches at most +10%,
#    and every response without transit=true byte-identical.
uv run scripts/measure-transit-trip-planning.py compare off.json on.json
```

Also look at `api_trip_plan_transit_connections{network="metrolink"}` on
the api's metrics port (about 27,600 on a weekday, spec §3.1) and the
`options` worst case against `api.tripPlanMaxOptionsSearchSize`. Put only
the figures in the PR or the card, never feed rows or responses. The
in-repo benchmark on hand-made data is
`DS_RUN_BUDGET_BENCH=1 cargo test --release -p api --test transit_planning_budget -- --ignored --nocapture --test-threads=1`.

### Rollout (Metrolink)

1. Merge M1 and M2 (dark). The list is empty, so nothing changes; the
   golden test pins that.
2. With the measurement above within budget, set
   `api.tripPlanTransitNetworks: [metrolink]` in Ranma-Config (and keep
   `api.transitNetworks: [metrolink]`). The api restarts. No client opts in
   yet, so nothing visible changes.
3. Check, read-only:
   - `api_trip_plan_graph_cache_total{result="miss"}`: once per date, and
     once more after a new tram feed activates (the cache key moves);
   - `api_trip_plan_transit_connections{network="metrolink"}` about 27k on
     a weekday;
   - `api_trip_plan_transit_networks_skipped_total` flat on dates inside
     the horizon (`reason` `outside_horizon` is expected past 60 days,
     `no_service_day` on a feed gap, `error` never);
   - the api's RSS within the budget.

   Then make one request by hand with `transit=true` (rail into
   Piccadilly, the tram to Victoria):
   `/Trips/plan?origin=SPT&destination=LDS&date=<today>&departAfter=08:00&transit=true`.
4. Merge M3 (the frontend sends `transit=true`). After a day of use with
   no errors, Metrolink trams are in public plans.
5. **Rollback:** `api.tripPlanTransitNetworks: []`. The api restarts with
   the old graph and every response is as before. Saved untracked tram
   legs stay valid (plain untracked legs).

## Nottingham Express Transit (net)

NET is in no BODS feed (measured 2026-10-24, NET design §2), so its
timetable comes from the **Traveline National Dataset (TNDS)** East
Midlands region in TransXChange (TXC), over FTP with a Traveline login,
filtered to NOC `NEXT`. The producer's TXC adapter (`TRANSIT_ADAPTER=txc`)
turns it into the same tables as Metrolink's. No source publishes NET's
disruptions (BODS SIRI-SX has no Nottingham participant), so NET runs the
timetable loop only: its lines show no status, and the producer refuses
to start with `situationsEnabled` on. Rail links: Nottingham, Bulwell and
Hucknall (`reference-data/transit/net-rail-links.csv`).

| Value | Default | What it does |
| --- | --- | --- |
| `redis.acl.clients.pollerTransitNet` | `false` | Renders the producer's Redis user `poller-transit-net` (`optional`). Needs the `poller-transit-net-password` key in the users Secret. |
| `ingestWriter.streams.transit-net` | `off` | `apply` makes the writer consume `ds:ingest:transit-net`. Needs `postgresql.roles.perService.writer.connect`. |
| `pollerTransitSchedule.networks.net.enabled` | `false` | Runs the producer. The chart refuses it unless the stream is `apply`, and, with the default `ftp://` `txcUrl`, unless `txcCredentials.existingSecret` is set. |
| `pollerTransitSchedule.networks.net.txcCredentials.existingSecret` | `""` | The Secret with the TNDS login (keys `username`, `password`). |
| `pollerTransitSchedule.networks.net.txcUrl` | `ftp://ftp.tnds.co.uk/EM.zip` | The TXC zip. |
| `api.transitNetworks` | `[]` | List `net` to make it public. |

### Before NET's rollout: the TNDS login

TNDS is free and under the Open Government Licence, but its FTP needs a
login Traveline issues on request:

1. **The user registers** with the form on Traveline's TNDS page
   (`https://www.travelinedata.org.uk/traveline-open-data/traveline-national-dataset/`),
   as a named contact. Traveline emails the FTP login after a few working
   days.
2. **Ranma seals a Secret** in the DS namespace, say `distant-signal-tnds`,
   with keys `username` and `password`, each byte-exact from a file
   (`kubectl create secret generic distant-signal-tnds
   --from-file=username=./u --from-file=password=./p --dry-run=client -o yaml
   | kubeseal ...`; check the lengths, no trailing newline).
3. **Ranma adds `poller-transit-net-password`** (letters and digits) to the
   Redis users SealedSecret.

The login crosses the network in clear (TNDS offers plain FTP). It is a
free registration that guards nothing else; do not reuse a password.

**NetworkPolicy.** Passive FTP opens its data connection to a port the
server picks. With the chart's default (no `networkPolicy.egress.internetPorts`)
nothing is needed. If production limits `internetPorts`, also set
`networkPolicy.components.poller-transit-schedule.internetPorts: []` (every
port, still minus the private ranges) before step 2 below.

### NET's rollout

Each step its own Ranma-Config change, after the login above exists.

1. **The Redis user.** `redis.acl.clients.pollerTransitNet: true` (with
   the Secret key from step 3 above in the same change). Redis restarts
   once. Check `$REDIS ACL USERS | grep poller-transit-net`.
2. **The writer and the producer**, in one change:

   ```yaml
   ingestWriter:
     streams:
       transit-net: apply
   pollerTransitSchedule:
     networks:
       net:
         enabled: true
         txcCredentials:
           existingSecret: distant-signal-tnds
   ```

   Leave `situationsEnabled` off (the producer refuses it for NET). The
   producer logs in to TNDS and downloads `EM.zip` (the whole East
   Midlands region; its size was not measured, the cap is 400 MiB).
3. **Check the timetable, with nothing public**, as Metrolink's step 3
   with `transit-net` and `network = 'net'`:
   - `$REDIS XREVRANGE ds:ingest:transit-net + - COUNT 1` is a
     `transit-timetable-commit/1`; `$REDIS XLEN ds:dlq:transit-net` is 0.
   - `transit_feed_versions` for `net` has one `active` version: 2 lines,
     about 50 stations and 100 platforms, a few thousand trips, a 60-day
     horizon; `transit-net-timetable` is fresh.
   - **`transit_feed_refused_total{network="net", reason="unmapped_route"}`**:
     the network file's `txc_line_names` are the likely spellings of NET's
     two line names, not observed ones (TNDS was never downloaded while
     writing it). The producer's log names the real `LineName`; add it to
     the line's `txc_line_names` in
     `crates/poller-transit-schedule/networks/net.toml` and release.
   - `reason="malformed"` saying "no document with an operator of NOC
     NEXT": TNDS's file did not name NET's NOC; check the NOC in TNDS's
     NET document and the network file's `nocs`.
   - Check a few departures against NET's published timetable (one
     weekday, one Sunday, one past midnight): the adapter's TXC reading
     was tested on hand-made documents only.
4. **Public.** After a day with the timetable applied:
   `api.transitNetworks: [metrolink, net]` (or `[net]`). The footer's Open
   Government Licence line and NET's `/attribution` section (Traveline
   National Dataset and NaPTAN) appear with it.

Rollback, refresh and "Turning a network off for good" are Metrolink's
with `net` and `transit-net` in place of `metrolink` and
`transit-metrolink`. NET's stream is budgeted for its timetable only
(`MAXLEN` 24): turning its situations on one day (a source would have to
appear first) means resizing `crates/ingest-stream/src/budget.rs` and the
alert maps first (NET design §11).

## TfL: Underground, DLR and London Trams (tfl)

Design: [the TfL transit design](superpowers/specs/2026-10-30-tfl-transit-design.md).
The timetable is TfL's weekly Journey Planner export,
`https://tfl.gov.uk/tfl/syndication/feeds/journey-planner-timetables.zip`
(about 141 MB, anonymous, no login), a zip of zips: the network file's
`inner_zip_prefix` (`LULDLRTRAMRIVERCABLE`) names the inner zip read
(8.8 MB, 33 TransXChange documents on 2026-10-26). The operators are
`LUL`, `DLR` and `TCL`, so the river buses and the cable car are left out.
What differs from Metrolink and NET:

- **Overrides.** TfL publishes engineering-work and bank holiday timetables
  as whole files over the base file's dates. The network file's
  `override_rule = "narrowest-period"` runs, for each line and date, only
  the active file with the shortest period (the design's §3). A short
  file is the whole line for its days: a part closure shows as fewer
  trains on fewer stations, as TfL's status says.
- **Status is TfL's own.** No situations loop (the producer refuses
  `situationsEnabled`): the pages read poller-tfl's `tfl-<line>` rows
  (`common::transit::status_from_tfl`), and a line's pin is TfL's line
  (`tfl-victoria`). Pins, notifications and rollups are TfL's as before.
- **Stops.** Platforms are a station code and one digit (`9400ZZLUHR41`
  is Heathrow Terminal 4's, station `9400ZZLUHR4`); the stations of the
  Northern line's extension are `9400ZZBPSUST` and `9400ZZNEUGST`. TfL's
  stop pages (`/tfl/stops/{id}`) use the stop-area spelling, `940G…`.
- **No rail-links file.** TfL's stops already link their National Rail
  stations (`tfl_stop_crs`), so `/transit/rail-links` lists none for
  `tfl` and station pages keep their TfL links.
- **Sizes** (TfL's 2026-10-26 export): 13 lines, about 1,140 stops, 7,849
  patterns with 216,766 calls, 58,383 trips; a weekday has about 8,800
  Underground, 1,584 DLR and 707 tram trips. The parse takes about 11 s and
  332 MiB peak in a release build; the item is 61 parts. Its stream is
  `MAXLEN` 128 (two items a day), about 30 MB worst case with its dead
  letters, for which `ingest_stream::budget::BUDGET_BYTES` rose to 576 MiB
  (production's Redis `maxmemory` is 2048mb).

| Value | Default | What it does |
| --- | --- | --- |
| `redis.acl.clients.pollerTransitTfl` | `false` | Renders the producer's Redis user `poller-transit-tfl` (`optional`). Needs the `poller-transit-tfl-password` key in the users Secret. |
| `ingestWriter.streams.transit-tfl` | `off` | `apply` makes the writer consume `ds:ingest:transit-tfl`. Needs `postgresql.roles.perService.writer.connect`. |
| `pollerTransitSchedule.networks.tfl.enabled` | `false` | Runs the producer. The chart refuses it unless the stream is `apply`. |
| `pollerTransitSchedule.networks.tfl.txcUrl` | TfL's export | The zip. |
| `api.transitNetworks` | `[]` | List `tfl` to make it public. |

### TfL's rollout

Each step its own Ranma-Config change; no secret but the Redis password.

1. **The Redis user.** Add `poller-transit-tfl-password` (letters and
   digits) to the Redis users SealedSecret and set
   `redis.acl.clients.pollerTransitTfl: true` in the same change. Redis
   restarts once. Check `$REDIS ACL USERS | grep poller-transit-tfl`.
2. **The writer and the producer**, in one change:
   `ingestWriter.streams.transit-tfl: apply` and
   `pollerTransitSchedule.networks.tfl.enabled: true`. The migration that
   adds `transit_lines.mode` must already be deployed (it ships with the
   code).
3. **Check the timetable, with nothing public**, as Metrolink's step 3
   with `transit-tfl` and `network = 'tfl'`: one `active` version with 13
   lines, about 7,800 patterns and 58,000 trips; `transit-tfl-timetable`
   fresh; `transit_feed_refused_total{network="tfl"}` flat. Compare one
   line's trips on a day TfL's status reports planned work with TfL's
   own journey planner.
4. **Public.** After a day with the timetable applied:
   `api.transitNetworks: [metrolink, tfl]`. "Trams and metros" lists
   "London (TfL)"; `/lines/tfl-<line>` links each Tube, DLR and tram
   line's timetable; `/attribution` gains the TfL timetables.

Rollback is `api.transitNetworks` without `tfl`, then the producer and
the stream off, as Metrolink's.

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
day (seen in the 2026-10-14 dry run). Within that day, an unchanged feed
is not re-sent either ("the timetable is unchanged since the last commit;
not re-sending it yet").

To refresh now, set a **new** refresh token in one Ranma-Config change:

```yaml
pollerTransitSchedule:
  networks:
    metrolink:
      timetableRefresh: "2026-10-15"   # any new value: a date, or 1, 2, 3...
```

The env change restarts the producer. At start it compares the token with
the `source.refresh` of the newest commit on the stream; a token no commit
has answered makes the first cycle run at once and publish the timetable
even if unchanged, with the token in its commit. Check:

```sh
$REDIS XREVRANGE ds:ingest:transit-metrolink + - COUNT 50 | grep -o '"refresh":"[^"]*"'
```

and the log line "timetable queued for the stream" with `refresh`. An
unchanged feed then only moves the timetable freshness (the writer sees the
same content); a changed one activates. The feed guard still applies: a
refused feed publishes nothing, and the request stays unanswered until the
next start (after the cause is fixed, restart the pod or set another
token).

**Leave the token set.** It is one-shot by design: the commit records
which token it answered, so later restarts (a node drain, a rollout) with
the same token change nothing. To refresh again, set a different token.
Unsetting it (`""`) is harmless but restarts the pod for nothing. This was
chosen over a plain on/off switch, which would refresh on every restart
while on and so would have to be turned off again after each use (two
changes, and a forgotten "on" means a full download on every restart).
The token costs one optional field in the commit's `source`, which the
writer stores with an activated version's source and otherwise ignores.

## Adding a SIRI line alias

`DistantSignalTransitUnmappedLineRef` names the `ref` (also
`transit_situation_unmapped_line_refs_total{network,ref}`). Situations
with that ref still apply, but not to a line's status. Add the ref to the
line's `siri_line_refs` in
`crates/poller-transit-schedule/networks/<network>.toml` (compiled in) and
release. Unit tests check the file at build time. NET's aliases
(`1`, `L1`, `NET1`, `Line_1` and the same for line 2) are guesses: no
NET situation has been published to observe.

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
   (its `adapter`, `gtfs` or `txc`, with the matching `gtfs_route_names`
   or `txc_line_names`; NOCs, ATCO prefix or prefixes, name clean-ups,
   SIRI filters and line aliases, attribution; for a mixed network each
   line's `mode`; for a TXC zip of zips `inner_zip_prefix`; for overlapping
   override files `override_rule`), added to `FILES` in `src/network.rs`;
   rail links in `reference-data/transit/<n>-rail-links.csv`, added to
   `src/rail_links.rs` (none for a network whose stops link stations
   another way, as `tfl`'s do).
   A network whose line status is TfL's goes in
   `common::transit::TFL_STATUS_NETWORKS` instead of having a situations
   loop.
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
5. **Values:** `pollerTransitSchedule.networks.<n>` (a `txc` network
   from TNDS also needs `txcCredentials`),
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
