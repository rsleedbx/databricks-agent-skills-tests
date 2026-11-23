# Predicting a run: bytes and objects first

Capacity, latency and throughput are one quantity seen three ways: the work to do, how much of it runs at once, and
how fast one stream of it goes. The work is the bytes processed, so predict those before a run:

```text
bytes = rows × average bytes per row        (data: change rows read, target rows scanned, rows written)
      + objects × bytes per object          (metadata: a Delta log per table and version, a file listing, a statement)
time  ≈ objects × cost per object + bytes ÷ rate
```

- **Throughput** is the rate at capacity.
- **Latency** is the bytes on one serial chain divided by one stream's rate.
- **Capacity** is the number of streams times the rate.

An optimization is efficient when it removes bytes or objects. Running the same bytes on more streams cuts latency only
until capacity binds. Moving bytes to another place, without removing them, helps nothing.

**Bytes per row:**
- **Stored or processed.** Stored bytes are compressed Parquet and can be a few bytes a row for repetitive data; the
  bytes an engine processes are the decoded row, its columns' widths.
- **How to measure.** Take the average from the table (`DESCRIBE DETAIL` sizeInBytes ÷ rows), or from the pipelines'
  `flow_progress` (`num_output_bytes` ÷ rows), and the change rows from the event logs or the gateway's counts.
- **Before data exists,** the columns and their types predict bytes per row (strings and decimals weigh most). A table's
  schema is enough to estimate it.
- **Bytes per row alone isn't enough, even at gigabytes.** In large pipeline updates (anonymized, held-out fits) the
  bytes predicted little of the executor time. The rows (a cost per row), the column count and the type mix explained
  a good part of it, with nested types (struct, array, map, binary, variant) the heaviest. So a cost model needs rows,
  columns and types, and bytes per row only as one input.

## The method decides the bytes

Before the rate, predict the method: each processes different bytes, so its time follows a different input. For
materialized views, every refresh logs the technique it chose, why, and what its sources held and changed
(`planning_information`). `methods.py` reads that from any event log, or from the pipeline events system table, and
fits each technique's time against each input.

| Method | Processes | Time follows | Docs |
|---|---|---|---|
| Skip (NO_OP) | nothing: no source changed | per object only (planning) | the refresh detects changes first |
| Append only | the new rows | new rows; nearly flat in table size | only new data, when the query and sources allow it |
| Incremental (row based, group or generic aggregate, window) | the changes | change bytes, changed rows and files, plus planning | "only process new or changed data"; some operations "require row tracking" |
| Complete recompute | every source row | the sources' full size; the changes don't matter | "will do a full recompute of the data if needed"; always on classic compute |
| MERGE / AUTO CDC into Delta | the files the changed keys touch | files rewritten × file size; with deletion vectors, rows changed | "modifying a single row requires rewriting the entire Parquet file"; deletion vectors "mark rows as modified in metadata" |
| A pipeline update | its graph | a cost a flow and a cluster start, before any data | an update "starts a cluster", then sets up the graph |

- **Planning is a per-object cost every method pays.** For incremental methods it's often the largest single input:
  the planner reads each source's metadata (sizes, changed files) before any data moves. That's the same kind of cost
  as the n:1 merge planning each LOB's change feed.
- **The choice is cost-based, by thresholds and history.** The decision path names a small-changes threshold, the
  refresh history, or a technique that can't apply. That's the same design as the merge optimizer's choice of engine
  and split ([merge paths](merge-paths.md)).
- **To predict a refresh:**
  1. Predict its method, from the change share, the size, and whether the sources have row tracking and deletion
     vectors (the docs recommend both on every source).
  2. Then predict its time from that method's input.

  Checking an earlier run's method costs a query on its event log, not a new test.

## What the runs so far show

All of the merge and ingestion runs so far are in the per-object regime: their data bytes were far too few to cost
time.

| Observation | So |
|---|---|
| One table's MERGE of many LOBs' changes reads few bytes and takes long, most of it compile and read per LOB | the cost is planning each change feed and reading its log, not data |
| Splitting it into k MERGEs reads several times the bytes (each rescans target files) and takes less time | the per-LOB work ran on more streams; at large batches the rescans will cost |
| A Lakeflow Connect ingestion update | a cost a flow, data or not: the applier pays per object |
| The gateway staging a change | a small fraction of the time to bronze: not the bottleneck |
| A larger warehouse size | barely faster: more compute per stream doesn't shorten a serial chain of per-object steps |
| More clusters of the small size | faster, then nothing queued: more streams |
| With nothing queued | the table's commits bind, in turn |

The gains so far all removed objects rather than bytes, or ran per-object work at once:

| Change | What it removed |
|---|---|
| `changed_since`: skip sources with no change rows | every unchanged source's log read |
| The merge log's rows from the target's change feed for that commit only | a second read of every source's change feed |
| batch-once: DML in a few round trips | a Spark job a statement |
| One `remote_query` per table over every LOB (checksum) | one query per LOB |
| `read_files` leaving out files read before (capture) | rereading the staging volume |
| Reverted: one stage table for every LOB's changes | nothing removed: it added a commit, a log entry, per LOB |

## Fitting what explains a phase

Regress each warehouse MERGE's phases on what it processed. The phases are compile, execute and waiting at capacity,
from `system.query.history`. Match each MERGE to its paths-log record by time for its sources (query history redacts
the statement text), and take the clusters running from `system.compute.warehouse_events`. Compare R² by kind of
input:
- bytes and rows read;
- files read;
- sources in the MERGE;
- statements in flight per cluster;
- the cluster size.

In the runs so far:
- Bytes explained almost none of any phase.
- The sources explained most of compile.
- Sources and load explained much of execute.
- Queueing fit best as a threshold, the statements in flight beyond about 10 a cluster. That matches the docs' rule,
  though they state it for classic and pro; serverless admission is noisy.

## How to use it

- **Before a run** (the run gate in lfc-medallion-pack's SKILL.md), count its objects (sources, flows, statements,
  commits) and its bytes (change rows × bytes a row). If objects × cost per object dominates, rows won't change the
  result: a bigger batch measures nothing new until its bytes ÷ rate approaches that.
- **Which regime a run is in decides what to optimize:** remove objects in the per-object regime, bytes in the
  per-byte one.
- **Splitting moves between them.** It parallelizes per-object work, but each part rescans its share of the target. As
  change rows grow, the rescanned bytes start to count, and the best k comes down (the optimizer's `w_row` and the
  target's bytes, [merge paths](merge-paths.md)).
- **Record bytes with the time.** `system.query.history` has read_bytes, read_files, read_rows and written_bytes per
  statement. The event logs have rows and output bytes per flow.
