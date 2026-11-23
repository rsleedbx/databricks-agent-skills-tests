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

Every run so far is in the per-object regime: the bytes were far too few to cost time, so the time is per LOB, per
statement and per commit. Splitting rescans target files, which costs once change rows are large; bytes and objects
first: [performance reasoning](performance-reasoning.md).

Each engine's line starts from what was measured and is refit, by least squares, from the paths log
(`<merge log>_paths`: each table's MERGE, its engine, k, sources, change rows, tables at once, units, predicted and
taken seconds, retries, and the other paths' predictions). Each run is fitted to the side that binds it, so every run
is a measurement. An engine never measured is taken only when forced.

`merge_route(..., warehouse=<id>, path="auto")`, or the task's `--warehouse` and `--path`: `auto` by cost, or a forced
path to measure one (`spark`, `spark:1`, `warehouse`, `warehouse:8`). lfc-medallion-pack renders the bundle's
`warehouse_id` and the module's `merge_path` (default `auto`).

## Measuring a path

Run the same changes through each path: a batch, the ingestion only, a copy of the merge state, then per path the
state restored and the merge task alone, with `--path` forced (`spark:1`, `warehouse:1`, `warehouse:7`, ...) and once
`auto`. Each MERGE's engine, k, sources, rows, tables at once, units, predicted and taken seconds land in the paths
log. For a warehouse, its phases per MERGE (compile, execute, waiting at capacity), bytes and files are in
`system.query.history`, and its clusters over time in `system.compute.warehouse_events`. Measured results are kept
with their configuration in internal notes.

What the runs show:

- **One MERGE of every changed source is latency-bound.** It plans and reads the sources one after another, and a
  warehouse plans each faster than the job's session.
- **Splitting is throughput-bound on one cluster.** The MERGEs queue.
- **Clusters set the capacity.** Each runs about 10 MERGEs at once. A larger size runs each a little faster but admits
  no more.
- **Autoscaling is too slow for a minute's burst.** It adds a cluster after queries have queued a while. Set the
  minimum clusters for the merge, not only the maximum.
- **With no queue left, the table's commits bind.** Each of k MERGEs commits in turn, so past a point more MERGEs cost
  more than they save.
- **At thousands of changed sources a table is throughput-bound** however it's split. It needs more clusters, or fewer
  objects per source (reading the gateway's staging instead of each LOB's change feed).
- **A stage table for every source's changes, then one MERGE, was slower.** Each source is a commit to that table, in
  turn.
- **On the job's session, splitting doesn't help.**

Still to measure: the warehouse's start; rows large enough to matter; the built-in alternative, an AUTO CDC flow per
source into one streaming table (`flows.py`). Its first update gave the same result as the merge route; still to
measure: triggered after a batch, and continuous.
