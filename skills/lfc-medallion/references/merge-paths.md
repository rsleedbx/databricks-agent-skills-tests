# The MERGE optimizer: which engine, how many MERGEs at once

A table on the merge route gets its changed sources' rows in one MERGE of a UNION ALL of their change feeds. That
statement plans each source's change feed (its log and version range) one after another, so it grows with the sources:
the same work at 10 sources and at 2,500 isn't the same plan. `paths.py` picks, per table and run, a path as a SQL
planner picks a plan by cardinality:

| Choice | Options |
|---|---|
| engine | `spark`: the merge job's own session; `warehouse`: a SQL warehouse through the Statement Execution API (`engine.Warehouse`, the same `sql().collect()` the code uses) |
| k | the n changed sources in k groups, one MERGE each, the k at once on threads: k = 1 is one MERGE of everything, k = n a MERGE a source |

A path costs `fixed + per_source × ⌈n/k⌉ + per_commit × k + per_row × rows` seconds, plus a stopped warehouse's start
on its first use: planning shrinks with k (the k MERGEs plan at once), commits grow with it (each commits to the same
target in turn). The best k is about √(per_source × n / per_commit). The inputs come before any statement: the changed
sources and their change rows from the ingestion pipelines' event logs (`changed_since`), and the warehouse's state.

Each engine's line starts from what was measured and is refit, by least squares, from the paths log
(`<merge log>_paths`: each table's MERGE, its engine, k, sources, change rows, predicted and taken seconds, retries,
and the other paths' predictions), so every run is a measurement. An engine never measured is taken only when forced.

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

The model's prediction at 124 with those lines: warehouse, k = 7, about 12 s; at 2,500 sources, k = 32, about 40 s.
Still to measure: k between 1 and n on both engines, the warehouse's start, rows large enough to matter, and the
built-in alternative (an AUTO CDC flow per source into one streaming table, triggered and continuous).
