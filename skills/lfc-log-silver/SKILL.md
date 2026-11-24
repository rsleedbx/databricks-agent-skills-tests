---
name: lfc-log-silver
description: "Silver from a change log: a typed silver table per source table, kept current from an lfc-change-source change log (a Lakeflow Connect gateway's files, Debezium through Zerobus, a CockroachDB changefeed, landed into one log), each table by its route's method: a view (no work a round), a MERGE every round, a MERGE only when it changed, or AUTO CDC flows in a continuous pipeline. Use when many tables share a change log and silver must stay cheap for the small, slow ones and fast for the big, busy ones."
---

# Silver from a change log

A change log holds every table's events in one place. Silver is a table per source table, typed, current. How each
table's silver is kept follows its route (lfc-change-source `routing.py`), because a small table costs per object and a
big one per byte:

| Route's silver | What a round does | For |
|---|---|---|
| `view` | nothing: a view over the table's current rows from the log (created once) | `small_slow`: most tables, almost no data |
| `merge_round` | one MERGE of each key's last change since the table's kept position | `small_fast` |
| `merge_changed` | the same MERGE, only for tables with events past their kept position (one grouped query finds them) | `big_slow` |
| `auto_cdc` | nothing here: flows in a continuous pipeline, its setup paid once | `big_fast` |

A MERGE has a fixed floor of seconds whatever it merges (planning, a small write, a commit), and running many at once
raises throughput but slows each, so skipping the tables that didn't change matters more than merging faster.

Code: [scripts/lfc_log_silver/silver.py](scripts/lfc_log_silver/silver.py), installed as the package `lfc-log-silver`
(`pip install <archive>#subdirectory=skills/lfc-log-silver/scripts`), with lfc-change-source beside it. The AUTO CDC
pipeline source: [scripts/pipelines/auto_cdc_from_log.py](scripts/pipelines/auto_cdc_from_log.py).

## Use

```python
from lfc_change_source import routing
from lfc_log_silver import silver

# sources: {table: an lfc_change_source source (cdc_log, gateway_log, changefeed_log)}; to: the run's position
routes = {t: routing.log_for(t, size, rate, policy) for t, (size, rate) in stats.items()}
silver.run(session, sources, routes, "`main`.`silver`", to, first=True)    # build the merged tables, create the views
silver.run(session, sources, routes, "`main`.`silver`", to)                # a round
lists = silver.auto_cdc_tables({t: s for t, s in sources.items() if routes[t].silver == "auto_cdc"})
```

- `build` makes each merged table from its source's current rows and keeps the position it's current to, in
  `<silver schema>._log_silver`. A table whose snapshot is read in place (routing's `direct` path) is built from that
  snapshot and kept at the snapshot's position (`at=`), so its first MERGE adds every change after it.
- `apply` runs a MERGE per table, `threads` at once. `apply_staged` first copies the run's changes from the log once
  into a batch table; it didn't help when measured (the log read was about 70 KB a MERGE), and stays for logs where the
  read does cost.
- `auto_cdc_tables` writes the table lists for `pipelines/auto_cdc_from_log.py`, at most 999 tables a pipeline (a
  pipeline takes up to 1,000 tables, and its setup grows with its flows). Each entry carries each column's SQL from an
  event and the position's SQL, so the pipeline reads any change source.
- A table read across logs (a cut-over: lfc-change-source `routing.history_fq`) is a source like any other.

## Checks

`python3 -m unittest skills/lfc-log-silver/tests/test_log_silver.py` checks, offline (fake session): the build and
MERGE SQL (deletes first, a null-safe key match), each table read from its own kept position, the staged apply reading
the log once, a round by method (views on the first round only, `merge_changed` only for changed tables), the AUTO CDC
lists, and a table built from a snapshot read in place kept at the snapshot's position.
