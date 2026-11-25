# FlowThrough: from source changes to gold in four layers

FlowThrough is intelligent, low latency, high throughput, scales to 10,000s of tables and supports n:1 fan-in. Best of
all, there is never a full table refresh, by design.

- **Intelligent:** it inspects table relationships (keys, foreign keys) and database statistics (row counts, sizes,
  inserts, updates and deletes) to decide whether a table is a dimension (small), a fact (big) or realtime (big and
  busy), and gives each the snapshot and silver that fit (cdc-table-plan `classify.py`, lfc-change-source `routing`).
  Busy is measured, not asked: the source's own counters before the change capture starts (PostgreSQL's and MySQL's
  statistics, SQL Server's change tracking by hour, lfc-source-statschema), then the change log's own rate every round,
  which moves a table when it turns busy or quiet. How fresh a table must be for its readers is the one thing the data
  can't say: a quiet table someone needs within seconds is chosen as realtime by name.
- **Latency:** Zerobus writes each change into the log, or a gateway's files are copied in once each; the busiest
  tables are kept by AUTO CDC in a continuous pipeline.
- **Throughput:** one log per transport, clustered by table; a table busy enough to slow the others gets its own.
- **Scale:** a group of small tables keeps its changes in one table, one MERGE a round for the group and a view
  per table over it and the table's snapshot; big ones get a MERGE of their own or AUTO CDC, picked by each table's size and change rate.
- **Fan-in:** each source (a line of business, a tenant, a region) into one table each, whatever transport or silver
  method it came through.
- **No full refresh:** the snapshot is taken once, into a table of its own, and each silver table catches up from
  its last position in the log. A new snapshot is a new table swapped in; a silver table that must change shape (a
  type change) is rebuilt from its snapshot table and the log, never from the source.

Tested so far: every transport landed and rebuilt equal to the source; one gateway from 100 to 5,000 tables; shaped
rounds on 500 tables with each silver method; n:1 merges in lfc-medallion; a table recovered by a new snapshot
appended to the log (before the split below); PostgreSQL through Debezium and Zerobus with a QBC snapshot table, CDC
first, equal to the source in every round, and its tables in one shared group built from their snapshot tables and
the log, equal again. 10,000s of tables and many-source fan-in through one log are the design's targets, not yet run.

FlowThrough is the technique this page describes: changes flow through four layers, each with its own short table.
It is not a package. Its parts are the skills each layer names (lfc-change-source, lfc-log-silver, lfc-medallion).
The fan-in bundle ([lfc-fanin](../skills/lfc-fanin/SKILL.md)) deploys an ingestion; its Debezium route builds silver
with zerobus-debezium-test's AUTO CDC pipeline today, not with FlowThrough's routing below.

A layer never depends on the choices of another:
- **Layer 1** turns every transport into one shape.
- **Snapshot and CDC** takes each table's snapshot by a method of its own, apart from its transport, into one
  shape too (a Delta table).
- **Layer 2** keeps one table's silver.
- **Layer 3** combines many sources' tables into one (n:1).
- **Layer 4** builds gold from the n:1 tables.

So the cases add up across the layers instead of multiplying: a new transport is one row in layer 1; a new snapshot
method one row in Snapshot and CDC; a new kind of table is a cell in layer 2. Each layer also says what limits it; measured values for those limits are kept with their
configuration in internal notes, since they change with the warehouse, the gateway and the source.

## Layer 1: transport, how changes arrive

Each transport ends in the same shape: change events in a change log (`destination`, `operation`, `value`), and a
snapshot either in that log (as `read` events) or left where the producer wrote it. Nothing after this layer knows
which transport a table came from. Each adapter passes one conformance scenario (lfc-change-source).

| Transport | Changes arrive as | Snapshot | Position | Into the log by | Next | Adapter |
|---|---|---|---|---|---|---|
| Lakeflow Connect ingestion pipeline | the pipeline's own streaming tables (SCD type 1 or 2) | the pipeline's | Delta version or commit time | none: the pipeline keeps its tables | layer 3 (its tables are a source's silver) | `lfc_bronze` (only to rebuild them in layer 2) |
| Lakeflow Connect gateway, staging read in place | JSON change files in the gateway's staging volume, every table mixed | parquet, a folder a table, in staging | the gateway's cursor | not landed: read where it is | layer 2 | `gateway_staging` |
| Lakeflow Connect gateway, staging landed | the same files | into a table (`intake.gateway`; today still landed as `read` events, being moved) | the gateway's cursor | `COPY INTO` (polled) or Auto Loader (continuous) | layer 2 | `gateway_log` (`land.gateway`, `autoloader`) |
| Debezium Server through Zerobus | rows Zerobus writes into the log | a table, by a [snapshot method](#snapshot-and-cdc) (QBC) | the source's own (LSN, binlog position, SCN, ...) | Zerobus: no landing compute | layer 2 | `cdc_log` |
| CockroachDB changefeed | JSON files in cloud storage or a Volume, a folder a table | the initial scan: landed from its JSON files, or (parquet) read in place, decimals cast to text ([finding](findings.md#crdb-export-parquet-decimal)) | the HLC | `COPY INTO` (`land.changefeed`) | layer 2 | `changefeed_log` |

The two gateway rows read the same staging; they differ only in whether its files are read where the gateway wrote
them or copied into the change log first. The ingestion pipeline row is Lakeflow Connect as shipped: its tables are
already one source's silver, so it goes straight to layer 3.

The log a table's events go to (the shared one, or its own: a big, fast table, or a cut-over between logs at a
position) is a landing choice here; readers take the log as a parameter (`routing.history_fq`).

What limits it: a gateway's tables (adding tables slows as it holds more; its snapshots run at a steady rate per
table), its change capture's cadence (rounds spread over its file writes), landing (one `COPY INTO` per gateway shares
the change files; a continuous stream adds seconds per file), Zerobus's documented per-stream and per-table rates.

## Snapshot and CDC

A table's silver is its snapshot plus every change after it. The two come apart: layer 1's transport carries the
changes (CDC), and a snapshot method takes the snapshot, chosen a table at a time like its size class (lfc-change-source
`routing.snapshot_method`, the route's `snapshot_by`). So a gateway doing the CDC can take QBC's snapshot, its task set
to take none of its own; Debezium through Zerobus carries a CockroachDB table's changes while a changefeed exports its
10 TB snapshot; another method later is one more entry.

**The order is fixed.** The change capture runs first and a change of its own lands in the log (the harness writes a
no-op UPDATE and waits for it); only then does the snapshot start. The log then holds every change from before the
snapshot on, and since it carries whole rows, a change the snapshot already holds replays to the same row. A table
without a key needs no writes in between.

**The contract: a snapshot is a Delta table** (a streaming table too) with the source's column names and types. Views
and MERGE read it in batch, AUTO CDC streams it, each with the log's changes over it (lfc-log-silver
`Table.snapshot_table`, `CdcLog.current_sql`). Files alone aren't a snapshot: lfc-change-source `intake` turns every
arrival into the contract's table (a table as it is, Parquet converted in place, other files copied once and typed, a
gateway's staging copied by name), so no silver cell knows a format, decodes text or lists folders.

**The log carries CDC only; a snapshot is never in it.** Each table's snapshot method by its size:

| Size | Snapshot by | Becomes the Delta table by |
|---|---|---|
| most tables | `qbc`: a Lakeflow Connect query-based pipeline through a UC connection, each table once (`SNAPSHOTS`) | itself: it writes a streaming table |
| big (past `snapshot_big_bytes`, 10 TB) | `files`: the customer's export into storage we're told of, e.g. a CockroachDB `CREATE CHANGEFEED ... WITH initial_scan = 'only', format = 'parquet'`; or the transport's own | `intake.parquet_in_place` (`CONVERT TO DELTA`, no copy; the files mustn't change), else `intake.files` (copied once) |
| a transport that takes its own (a gateway, a changefeed's initial scan) | the transport | `intake.gateway`, `intake.files`: into a table, not the log |

Keeping them apart is what makes each easy to see and to keep: raw CDC (the log), raw snapshot (its table) and the
two combined (silver) are three things to look at (the dashboard links each, lfc-log-silver `dashboard`); the log
holds no second copy of every row as a VARIANT, so it grows with changes only; and a new snapshot is a new table,
swapped in (lfc-log-silver `silver.renewed`: a table built from another snapshot table, or from the log alone before
its snapshot was ready, is rebuilt from the new one, or its view recreated), never a second set of rows every reader
must tell from the first.

A shared group (layer 2) keeps only its members' changes: each key's last change, a delete kept as a marker. Each
member's view is its rows in the group plus its snapshot table's rows for the keys the group hasn't changed or
deleted, matched by the key typed from the group's stored key (`stored_key_values`). So the group's table and its one
MERGE stay the same for every member, and a member's new snapshot only recreates its view.

A changefeed for CDC runs with `initial_scan = 'no'` (an initial scan would land every row as an update). The code
that kept snapshots in the log (`read` rows, the newest-snapshot rule, `land.gateway_snapshots`) was removed; it is
at the tag `snapshot-in-log-last` if it's ever needed again.

## Layer 2: one table's silver

Two things set a table's code path: how fresh its silver must be (slow, fast, realtime), and where its snapshot is
(read in place, its snapshot table: [Snapshot and CDC](#snapshot-and-cdc)). Every
cell reads the table's current rows one way (the snapshot wherever it is, then each key's last change after it), so
every cell gives the same table (lfc-log-silver).

| | slow | fast | realtime |
|---|---|---|---|
| **small** (most tables) | shared: a group's table of changes, a view a table over it and its snapshot table | shared (or a MERGE of its own) | AUTO CDC flows in a continuous pipeline, and a flow for the snapshot table |
| **big** (most of the bytes) | a MERGE only in a round that changed it (or a view) | a MERGE every round | AUTO CDC, and a flow for the snapshot table |

The four classes in lfc-change-source `routing.py` pick a cell from a table's snapshot size and change rate:
- `small_slow` is small and slow: most tables, almost no data;
- `small_fast` is small and fast;
- `big_slow` is big and slow;
- `big_fast` is big and realtime: few tables, most of the bytes.

A small table costs per statement (a MERGE has a fixed floor of seconds whatever it merges; a warehouse runs a few a
second), a big one per byte. So small tables share: a group keeps its members' changes in one table (the
row as a VARIANT, clustered by table), one MERGE a round for the group, and each table is a typed view over it and
its snapshot table.

`routing.plan` takes a small table out of its group, for a table of its own (a view if slow, a MERGE if fast), when:
- it needs what only its own table gives: streaming reads, its own change data feed, time travel (named in the
  policy);
- it carries more than a share of its group's changes (10% to start): its rows would slow every member's MERGE;
- it's read more than a number of times a day (from query history): each read casts its row, with no column
  statistics;
- it has a change log of its own (a busy table).

Any table's cell can be chosen by name. Moving a table into or out of a group builds that one table (or its rows in
the group) from the log, and swaps a view or a table; nothing else changes (`routing.transitions`).

What a group gives up for its one MERGE: per-table Delta features (time travel, change data feed, history are the
group's), streaming reads (a view can't be read as a stream), column statistics, and a failure holds back the whole
group that round. Its MERGE reads every member's changes since the last round, so it grows with their total.

What each cell can carry, and what sets its latency and throughput:

| Cell | Limited by | Tables per unit | Latency, change to silver | Throughput |
|---|---|---|---|---|
| shared | one MERGE a group a round, growing with the group's changed rows | tables per group (no SQL a table: the key is filled in when landing, the row kept as the event has it) | the log's latency + one MERGE | per MERGE: the group's changed rows |
| view | nothing a round; a read rebuilds the table | tables per schema (Unity Catalog: 10,000) | the log's latency; each read pays its rebuild | per read: the table's events in the log |
| MERGE every round | a fixed cost per MERGE; MERGEs at once per cluster | tables per cluster per round | the log's latency + tables / (clusters x MERGEs a second per cluster) | per MERGE: its changed rows (and the files they touch, for a big table) |
| MERGE when changed | the same, for changed tables only | changed tables per cluster per round | the same, over the changed tables | the same |
| AUTO CDC | flows per pipeline (a pipeline takes up to 1,000 tables); its setup grows with its flows, paid once when continuous | tables per pipeline | the log's latency + the pipeline's micro-batch | per flow; per byte for big tables |
| snapshot into the log, or read in place | a statement per table (into the log), nothing (in place) | per table, once | once, at the table's first snapshot | per byte (into the log) |

## Layer 3: many sources into one

Each source (a line of business, a tenant, a region) has its own silver tables from layer 2. Layer 3 combines them
into one table each, whatever cell each was built in (lfc-medallion).

| Shape | Reads | Writes | How | Code |
|---|---|---|---|---|
| one source | its silver tables | nothing more | layer 2's table is the answer | none |
| n:1 stack | each source's table | a view a table across sources, with a source column | `UNION ALL`, columns widened to one type (`coltypes`) | lfc-medallion `stack` |
| n:1 merge | each source's changes | a table a table across sources, with a source column | a MERGE a table of the changed sources; the fan-in planner picks the engine and how many MERGEs | lfc-medallion merge route (`paths.py`) |
| n:1 pipeline | each source's bronze | a streaming table a table | an AUTO CDC flow a source into one target | lfc-medallion `flows` |

What limits it: sources per table (a MERGE plans each changed source's changes in turn), tables per pipeline and
per schema, and the n:1 table's commits (its MERGEs commit one after another).

## Layer 4: gold

Gold reads the n:1 tables (or one source's silver) and builds marts for the questions asked of them.

| Shape | Reads | Writes | How | Code |
|---|---|---|---|---|
| SQL marts | the n:1 tables | tables | a job's SQL a run | lfc-medallion-pack |
| materialized views | the n:1 tables | materialized views | incremental refresh where the planner can, else a recompute | lfc-medallion-pack |

What limits it: how much of the source a refresh reads (incremental or a full recompute, the materialized-view
planner's choice), and how often it runs.

## Where each is checked

- **Layer 1:** each adapter's conformance scenario on a warehouse, and each transport landed and rebuilt equal to the
  source.
- **Layer 2:** every cell, offline (each reads its snapshot and its changes) and live (each class's tables equal to the
  source after shaped rounds).
- **Layer 3:** the n:1 routes on their own (lfc-medallion's tests and scale runs).
- **Layer 4:** the marts against the model (lfc-medallion-pack's checks).
