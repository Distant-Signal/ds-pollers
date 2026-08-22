# poller-tfl test fixtures

Live captures for the DLR arrivals-diffing pilot (see
`.superpowers/sdd/2026-08-22-dlr-arrivals-diffing-pilot/`).

- `dlr_arrivals.json` — `GET /Line/dlr/Arrivals`, captured 2026-08-22.
- `dlr_timetable_poplar.json` — `GET /Line/dlr/Timetable/940GZZDLPOP?direction=outbound`,
  captured 2026-08-22.

## Capture conditions

Both captures were made **unauthenticated** — no `Ocp-Apim-Subscription-Key`
header was sent. This sandbox has no `TFL_APP_KEY` configured anywhere
(checked environment variables and any `.env` file; neither exists), but
TfL's public API answered every request with HTTP 200 regardless. This is a
sandbox-only limitation of the capture step: production `poller-tfl` still
sends `TFL_APP_KEY` per its existing config, and this file does not change
that.

Poplar's Naptan id, `940GZZDLPOP`, was confirmed via
`GET /StopPoint/Search/Poplar`, which returns
`{"id":"940GZZDLPOP","name":"Poplar DLR Station"}` among its matches.

## Findings vs. the plan's assumed schema

Overall the plan's assumed `Prediction`, `Timetable`, `Schedule`, and
`StationInterval`/`Interval` shapes matched the live response closely. Three
differences were found, plus one additional observation about the real data
that isn't a schema mismatch but matters for parsing:

1. **`vehicleId` is always an empty string in practice.** Every one of the
   273 predictions in `dlr_arrivals.json` has `"vehicleId": ""`. All other
   required fields (`id`, `naptanId`, `stationName`, `lineId`,
   `platformName`, `destinationNaptanId`, `destinationName`, `timestamp`,
   `timeToStation`, `expectedArrival`, `modeName`) are present and correctly
   named, matching the plan's assumption. This does not require a schema
   change — Task 3's `Prediction.vehicle_id: String` field still parses an
   empty string fine — but nothing downstream should rely on `vehicleId` for
   tracking a specific train, since TfL is not populating it for DLR.

2. **The bare `GET /Line/dlr/Timetable/940GZZDLPOP` (no `direction` query
   param) does not return a timetable.** It returns a disambiguation
   response instead: `{"$type": "...Disambiguation, ...", "disambiguation":
   {"disambiguationOptions": [...]}}`. Poplar sits on a junction served by
   multiple DLR route pairs (e.g. Tower Gateway↔Beckton, Bank↔Woolwich
   Arsenal, Lewisham↔Stratford, Stratford↔Canary Wharf), so TfL needs a
   `direction` to resolve which routes to return. Passing
   `?direction=outbound` resolves it and returns an actual timetable (note:
   `direction=outbound` at Poplar still groups more than one route pair —
   the captured response contains 2 routes under `timetable.routes[]`. The
   requested direction is echoed once, as a top-level `"direction":
   "outbound"` on the response; the route objects themselves carry only
   `$type`, `schedules` and `stationIntervals`, with no per-route
   `direction`). This pilot fixed on `direction=outbound`
   as a single, arbitrary-but-consistent choice, consistent with the plan's
   single-station scope — it is not an attempt to capture the full DLR
   network.

3. **`knownJourneys[].intervalId` is a JSON integer in the live response**
   (values `0` and `1` were observed in this capture), not a string. The
   plan's assumed `KnownJourney` struct (`interval_id: Option<String>`)
   will fail to deserialize against this real data. Task 4 needs to change
   that field's type to a numeric type (e.g. `Option<u32>` or similar).

4. **(Observation, not a mismatch — Task 4's problem, not fixed here.)**
   Each route's `schedules[]` array has one entry per day-type, not one
   combined schedule. In this capture the names were `"Monday - Friday"`
   (243 `knownJourneys`), `"Sunday"` (203 `knownJourneys`), and `"Saturdays
   and Public Holidays"` (233 `knownJourneys`) — each carrying its own full
   `knownJourneys[]` list for that day-type only. A parser that flattens
   every schedule together regardless of the current date would incorrectly
   combine weekday and weekend departures into one arrivals set.

Also observed, not flagged as an issue: `knownJourneys[].hour` and
`.minute` are JSON strings (e.g. `"5"`, `"28"`), and
`stationIntervals[].intervals[].timeToArrival` is a JSON number (e.g.
`1.0`), both consistent with what the plan assumed.

Tasks 3 and 4 should update this note if they had to adjust field names or
types beyond what's recorded above.
