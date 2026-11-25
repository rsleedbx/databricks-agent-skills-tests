---
name: lfc-log-silver
description: "Silver from a change log: a typed silver table per source table, kept current from an lfc-change-source change log (a Lakeflow Connect gateway's files, Debezium through Zerobus, a CockroachDB changefeed, landed into one log), each table by its route's method: a group of small tables in one table with one MERGE a round, a view (no work a round), a MERGE every round, a MERGE only when it changed, or AUTO CDC flows in a continuous pipeline. Use when many tables share a change log and silver must stay cheap for the small, slow ones and fast for the big, busy ones."
---

# Silver from a change log

A change log holds every table's events in one place. Silver is a table per source table, typed, current. A table's
code path follows two things only, how its silver is kept and where its snapshot is:

| | `shared` (a group) | `view` (at read) | `merge` (each round) | `auto_cdc` (continuous) |
|---|---|---|---|---|
| snapshot in the log | 0 | 1 | 2 | 3 |
| snapshot read in place | - | 4 | 5 | 6 |

Every method reads a table's current rows one way, the source's `current_sql` (the snapshot wherever it is, then each
key's last change after it), so every cell gives the same table. lfc-change-source `routing.py`'s classes pick the
cell: `small_slow` and `small_fast` 0, `big_slow` 5 (merged only in a round that changed it), `big_fast` 6; `routing.plan`
takes a small table out of its group (to 1 or 2) when it needs its own table, carries much of its group's changes, or
is read often. A small table costs per statement (a MERGE has a fixed floor of seconds whatever it merges), a big one
per byte; transport and how many sources feed a table don't change the cell.

Code: [scripts/lfc_log_silver/silver.py](scripts/lfc_log_silver/silver.py), installed as the package `lfc-log-silver`
(`pip install <archive>#subdirectory=skills/lfc-log-silver/scripts`), with lfc-change-source beside it. The AUTO CDC
pipeline source: [scripts/pipelines/auto_cdc_from_log.py](scripts/pipelines/auto_cdc_from_log.py).

FlowThrough, the four layers (transport, a table's silver, many sources into one, gold), each in one table: [docs/design.md](../../docs/design.md).

## Use

```python
from lfc_change_source import routing
from lfc_log_silver import silver

# a Table: its change source, its method, and (snapshot read in place) its snapshot_table and position
tables = {t: silver.from_route(src[t], routing.log_for(t, size, rate, policy)) for t, (size, rate) in stats.items()}
silver.run(session, tables, "`main`.`silver`", to, first=True)    # views created, merged tables built
silver.run(session, tables, "`main`.`silver`", to)                # a round
lists = silver.auto_cdc_tables({t: x for t, x in tables.items() if x.method == "auto_cdc"})
```

- `shared`: a group's tables (one log and transport, keys landed: lfc-change-source `land.gateway_keys`) keep their
  current rows in `<schema>._shared_<group>` (destination, key, the event's row as a VARIANT): one scan of the log and
  one MERGE a round for the group, with nothing a table in its SQL, and a typed view a table (the source's own typing,
  `stored_values`). A table moved into a group has its own table dropped for the view; a group with unkeyed events is
  refused (`land.backfill_keys` fills them in).
- `build` makes each merged table from its current rows and keeps the run's position in `<silver schema>._log_silver`;
  `apply` runs one MERGE a table of each key's last change after it, `threads` at once; `only_changed` skips a table
  with no event past its position (one grouped query finds them).
- A newer snapshot of a table (a gateway tags each snapshot's rows) replaces the older one: views and builds read
  only the newest snapshot and the changes after it, a MERGE table whose snapshot is newer than the one it was built
  from is rebuilt from the log, and so is a shared group when a member's is (the state keeps each one's snapshot).
- A schema change (a table's columns, by name and type, differ from its source's now): its view is recreated, its
  MERGE table rebuilt from the log, a shared member's view recreated. Never a read of the source. AUTO CDC tables
  don't do either yet: a new snapshot or a type change there needs that table restarted from the log.
- `auto_cdc_tables` writes the lists for `pipelines/auto_cdc_from_log.py`, at most 999 tables a pipeline (a pipeline
  takes up to 1,000 tables, and its setup grows with its flows). A snapshot read in place adds a second flow that
  streams its snapshot table (an AUTO CDC flow reads its source as a stream, so a batch view is refused), sequenced by
  the snapshot's position in the change flow's type (`seq_type`).
- A snapshot read in place is a Delta table with the source's column names and types (`snapshot_table`): whatever
  took it, lfc-change-source `intake` or `qbc` made it one, so no cell reads files.
- FlowThrough's runner (`flowthrough.step`) measures each table's rate from the log, routes it and moves it when its
  route changes. Before the plan is pinned, `flowthrough.observe` keeps every table a view while its snapshot is
  taken: a view over the log alone at first, recreated over its snapshot table once that is ready (a metadata change),
  and the route the plan would give it kept as `proposed` in `<schema>._flowthrough`. `step` with the pinned policy
  then moves each view to its route, from its snapshot and the log.
- `flowthrough.checkpoint(session, schema, sources, to, into)`: an internal resnapshot of each table at `to`, from silver
  (`silver.checkpoint`), recorded so later rounds start there, then the log's changes up to `to` trimmed (lfc-change-
  source `land.trim`): a rebuild or a view no longer replays every change since the first snapshot.
- The dashboard [dashboard/flowthrough.lvdash.json](dashboard/flowthrough.lvdash.json) shows that schema: change events a
  minute by table and the newest one's age (its `cdc_log`, a view over the log), snapshots waiting and ready, each
  table's route now and proposed, and the stored tables' positions (`_log_silver`). Each table's row starts with
  links to its raw CDC (the Raw CDC page, filtered to it), raw snapshot and silver (CDC + snapshot, Catalog Explorer's
  sample data). `python3 -I scripts/lfc_log_silver/dashboard.py --profile <p> --warehouse <id> --catalog <c> --schema
  <s> [--host https://<your workspace address>]` fills in the address, the workspace id and its own id, then creates or
  updates it and publishes it. Its formats, read back from what the dashboard editor saves (the documented
  `link.templatedURL` and `displayAs: link` are ignored): a column's link is
  `"actions": {"click": {"type": "link", "urlTemplate": "{{ <url column> }}"}}`, the URL column only in the widget's
  query fields; a filter preset in a link is `/published/pages/<page>?o=<workspace id>&f_<page>%7E<filter>.s=<value>`.
- A table read across logs (a cut-over: lfc-change-source `routing.history_fq`) is a source like any other.

## Checks

`python3 -m unittest skills/lfc-log-silver/tests/test_log_silver.py` checks, offline (fake session): each of the six
cells reads the snapshot and the changes, the MERGE SQL (deletes first, a null-safe key match), each table merged from
its own kept position, `only_changed`, a round by method (views on the first round only) and the AUTO CDC lists.
