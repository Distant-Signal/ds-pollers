# Test fixtures

## `bods-nw-metl-mini.zip`

A trimmed extract of the Bus Open Data Service (BODS) North West GTFS file,
`https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/north_west/`,
downloaded on 2026-10-14 (`feed_info.txt` `feed_version=20261014_030249`).
Contains public sector information licensed under the Open Government
Licence v3.0 (Department for Transport).

The rows are copied unchanged from the source files; only rows were
dropped:

- `agency.txt`: Metrolink (`OP288`, NOC `METL`) and one other operator
  (`OP10`), to prove the NOC filter;
- `routes.txt`: one route each of `Blue Line`, `Green Line` and
  `Replacement Bus`, and the other operator's route `T8`;
- `trips.txt`: 14 trips of each Metrolink line route, 4 replacement-bus
  trips and 2 of the other operator's (34);
- `stop_times.txt`, `stops.txt` (with each platform's parent station),
  `calendar.txt` and `calendar_dates.txt`: the rows those trips use;
- `feed_info.txt` whole; no `shapes.txt` (the adapter never opens it).

The extraction kept the first trips of each route by `trip_id`. Their
services start between 2026-10-14 and 2026-10-19, so the tests pass the
date they parse as of rather than reading the clock.

## `naptan-940-mini.csv`

A trimmed extract of NaPTAN's access nodes for area 940 (tram, metro and
underground), `https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes=940&dataFormat=csv`,
downloaded on 2026-10-09. Contains public sector information licensed
under the Open Government Licence v3.0 (Department for Transport).

Rows unchanged, all columns kept: the `MET` station and `PLT` platform
rows of 14 Metrolink stops (the ten rail interchanges of
`reference-data/transit/metrolink-rail-links.csv`, plus Heaton Park,
Besses o'th' Barn, Market Street and Timperley); 47 rows.

## `sirisx-mini.xml`

Hand-written in the shape of the BODS SIRI-SX bulk archive's
`sirisx.xml` (`https://data.bus-data.dft.gov.uk/disruptions/download/bulk_archive`,
inspected on 2026-10-09 and 2026-10-14): the element structure, the
`LineRef` spellings (`BLUE`, `PURP`, `Green_Line`, `Navy_Line`) and the stop
codes are the real ones; the situation ids, texts and URLs are invented,
so the tests can name them. Five situations: a TfGM lift failure at a
platform, a TfGM line suspension with an unmapped `LineRef`, a planned
Purple Line change named only by its `OperatorRef`, a South Yorkshire
tram situation (another participant) and a TfGM bus situation.
