---
name: lfc-log-silver
description: "Silver from a change log: a typed silver table per source table, kept current from an lfc-change-source change log (a Lakeflow Connect gateway's files, Debezium through Zerobus, a CockroachDB changefeed, landed into one log), each table by its route's method: a view (no work a round), a MERGE every round, a MERGE only when it changed, or AUTO CDC flows in a continuous pipeline. Use when many tables share a change log and silver must stay cheap for the small, slow ones and fast for the big, busy ones."
---

# Silver from a change log

A change log holds every table's events in one place. Silver is a table per source table, typed, current. A table's
code path follows two things only, how its silver is kept and where its snapshot is:

| | `view` (at read) | `merge` (each round) | `auto_cdc` (continuous) |
|---|---|---|---|
| snapshot in the log | 1 | 2 | 3 |
| snapshot read in place | 4 | 5 | 6 |

Every method reads a table's current rows one way, the source's `current_sql` (the snapshot wherever it is, then each
key's last change after it), so every cell gives the same table. lfc-change-source `routing.py`'s classes pick the
cell: `small_slow` 1, `small_fast` 2, `big_slow` 5 (merged only in a round that changed it), `big_fast` 6. A small table
costs per object (a MERGE has a fixed floor of seconds whatever it merges), a big one per byte; transport and how many
sources feed a table don't change the cell.

Code: [scripts/lfc_log_silver/silver.py](scripts/lfc_log_silver/silver.py), installed as the package `lfc-log-silver`
(`pip install <archive>#subdirectory=skills/lfc-log-silver/scripts`), with lfc-change-source beside it. The AUTO CDC
pipeline source: [scripts/pipelines/auto_cdc_from_log.py](scripts/pipelines/auto_cdc_from_log.py).

The four layers, transport, a table's silver, many sources into one, and gold, each in one table: [docs/design.md](../../docs/design.md).

## Use

```python
from lfc_change_source import routing
from lfc_log_silver import silver

# a Table: its change source, its method, and (snapshot read in place) that snapshot's SQL, position and files
tables = {t: silver.from_route(src[t], routing.log_for(t, size, rate, policy)) for t, (size, rate) in stats.items()}
silver.run(session, tables, "`main`.`silver`", to, first=True)    # views created, merged tables built
silver.run(session, tables, "`main`.`silver`", to)                # a round
lists = silver.auto_cdc_tables({t: x for t, x in tables.items() if x.method == "auto_cdc"})
```

- `build` makes each merged table from its current rows and keeps the run's position in `<silver schema>._log_silver`;
  `apply` runs one MERGE a table of each key's last change after it, `threads` at once; `only_changed` skips a table
  with no event past its position (one grouped query finds them).
- `auto_cdc_tables` writes the lists for `pipelines/auto_cdc_from_log.py`, at most 999 tables a pipeline (a pipeline
  takes up to 1,000 tables, and its setup grows with its flows). A snapshot read in place adds a second flow that
  streams its parquet files (an AUTO CDC flow reads its source as a stream, so a batch view is refused), sequenced by
  the snapshot's position in the change flow's type (`seq_type`).
- A table read across logs (a cut-over: lfc-change-source `routing.history_fq`) is a source like any other.

## Checks

`python3 -m unittest skills/lfc-log-silver/tests/test_log_silver.py` checks, offline (fake session): each of the six
cells reads the snapshot and the changes, the MERGE SQL (deletes first, a null-safe key match), each table merged from
its own kept position, `only_changed`, a round by method (views on the first round only) and the AUTO CDC lists.
