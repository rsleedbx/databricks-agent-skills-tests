# The MERGE optimizer: which engine, how many MERGEs at once

A table on the merge route gets its changed sources' rows in one MERGE of a UNION ALL of their change feeds. That
statement plans each source's change feed (its log and version range) one after another, so it grows with the sources:
the same work at 10 sources and at 2,500 isn't the same plan. `paths.py` picks, per table and run, a path as a SQL
planner picks a plan by cardinality:

| Choice | Options |
|---|---|
| engine | `spark`: the merge job's own session; `warehouse`: a SQL warehouse through the Statement Execution API (`engine.Warehouse`, the same `sql().collect()` the code uses) |
| k | the n changed sources in k groups, one MERGE each, the k at once on threads: k = 1 is one MERGE of everything, k = n a MERGE a source |

A path takes the longer of two times (a roofline):

- **latency**: `fixed + per_source × ⌈n/k⌉ + per_commit × k`, one MERGE alone: it plans and reads its sources' change
  feeds one after another, and the k MERGEs of one table commit to it in turn.
- **throughput**: `(w_source × n + w_statement × k + w_row × rows) × at_once / units`, the engine's work for every
  MERGE in flight (`at_once` tables, k MERGEs each) over its capacity. `units` is a warehouse's maximum clusters times
  its size (2X-Small = 1, each size up doubles); the job's session is 1.

A stopped warehouse's start is added on its first use. With few sources one MERGE is best, and latency binds. With
many, the planner splits until throughput binds; more units move that point. The inputs come before any statement:
the changed sources and their change rows from the ingestion pipelines' event logs (`changed_since`), and the
warehouse's state and size.

Each engine's line starts from what was measured and is refit, by least squares, from the paths log
(`<merge log>_paths`: each table's MERGE, its engine, k, sources, change rows, tables at once, units, predicted and
taken seconds, retries, and the other paths' predictions). Each run is fitted to the side that binds it, so every run
is a measurement. An engine never measured is taken only when forced.

`merge_route(..., warehouse=<id>, path="auto")`, or the task's `--warehouse` and `--path`: `auto` by cost, or a forced
path to measure one (`spark`, `spark:1`, `warehouse`, `warehouse:8`). lfc-medallion-pack renders the bundle's
`warehouse_id` and the module's `merge_path` (default `auto`).

Measured 2026-11-22 (TPC-B, `scale1`, 3 merge-route tables at once, 20 rows a source):

| Path | 10 sources | 62 | 124 |
|---|---|---|---|
| spark, k = 1 (the merge job) | 14 s | 78 s | 141 s |
| warehouse, k = 1 | 6.6 s | 17.6 s | 31.1 s (33 to 35 three tables at once) |
| warehouse, k = n (a MERGE a source, 16 threads, target partitioned by source: no conflicts) | 9.1 s | 41.6 s | 70.3 s |
| one stage table for every source's changes, then one MERGE (dropped) | | | 303 s: each source a commit to that table, in turn |

Measured 2026-11-22 at 124 LOBs, the same changes each time (a batch of 20 rows into every table), 3 tables at once.
The warehouse was a serverless 2X-Small with 1 cluster:

| Path | Merge task | Warehouse phases a MERGE (query history) |
|---|---|---|
| spark, k = 1 | 132 s | |
| spark, k = 18 (chosen by the old model's guessed commit cost) | 188 s | |
| warehouse, k = 1 | 105 s | compile 32 s, read 67 s |
| warehouse, k = 7 | 67 s | compile 6 s, read 22 s, queued 16 s |
| warehouse, k = 16 | 74 s | compile 3 s, read 12 s, queued 12 s |

At k = 1 latency binds. At k = 7 and 16 one cluster's throughput binds: every MERGE runs slower and queues. The
defaults in `paths.py` fit these runs. At 4 units they predict warehouse, k ≈ 14, about 21 s. Still to measure:
- more units;
- the warehouse's start;
- rows large enough to matter;
- the built-in alternative, an AUTO CDC flow per source into one streaming table (`flows.py`). Its first update of
  372 flows took 1,544 s, with the same result as the merge route; still to measure: triggered after a batch, and
  continuous.
