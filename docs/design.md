# From source changes to gold: four layers

Changes go through four layers, each with its own short table. A layer never depends on the choices of another:
- **Layer 1** turns every transport into one shape.
- **Layer 2** keeps one table's silver.
- **Layer 3** combines many sources' tables into one (n:1).
- **Layer 4** builds gold from the n:1 tables.

So the cases add up across the layers instead of multiplying: a new transport is one row in layer 1; a new kind of
table is a cell in layer 2. Each layer also says what limits it; measured values for those limits are kept with their
configuration in internal notes, since they change with the warehouse, the gateway and the source.

## Layer 1: transport, how changes arrive

Each transport ends in the same shape: change events in a change log (`destination`, `operation`, `value`), and a
snapshot either in that log (as `read` events) or left where the producer wrote it. Nothing after this layer knows
which transport a table came from. Each adapter passes one conformance scenario (lfc-change-source).

| Transport | Changes arrive as | Snapshot | Position | Into the log by | Next | Adapter |
|---|---|---|---|---|---|---|
| Lakeflow Connect ingestion pipeline | the pipeline's own streaming tables (SCD type 1 or 2) | the pipeline's | Delta version or commit time | none: the pipeline keeps its tables | layer 3 (its tables are a source's silver) | `lfc_bronze` (only to rebuild them in layer 2) |
| Lakeflow Connect gateway, staging read in place | JSON change files in the gateway's staging volume, every table mixed | parquet, a folder a table, in staging | the gateway's cursor | not landed: read where it is | layer 2 | `gateway_staging` |
| Lakeflow Connect gateway, staging landed | the same files | landed as `read` events, or left in staging | the gateway's cursor | `COPY INTO` (polled) or Auto Loader (continuous) | layer 2 | `gateway_log` (`land.gateway`, `autoloader`) |
| Debezium Server through Zerobus | rows Zerobus writes into the log | exported once into a Volume | the source's own (LSN, binlog position, SCN, ...) | Zerobus: no landing compute | layer 2 | `cdc_log` |
| CockroachDB changefeed | JSON files in cloud storage or a Volume, a folder a table | the initial scan: landed from its JSON files, or (parquet) read in place, decimals cast to text ([finding](findings.md#crdb-export-parquet-decimal)) | the HLC | `COPY INTO` (`land.changefeed`) | layer 2 | `changefeed_log` |

The two gateway rows read the same staging; they differ only in whether its files are read where the gateway wrote
them or copied into the change log first. The ingestion pipeline row is Lakeflow Connect as shipped: its tables are
already one source's silver, so it goes straight to layer 3.

The log a table's events go to (the shared one, or its own: a big, fast table, or a cut-over between logs at a
position) is a landing choice here; readers take the log as a parameter (`routing.history_fq`).

What limits it: a gateway's tables (adding tables slows as it holds more; its snapshots run at a steady rate per
table), its change capture's cadence (rounds spread over its file writes), landing (one `COPY INTO` per gateway shares
the change files; a continuous stream adds seconds per file), Zerobus's documented per-stream and per-table rates.

## Layer 2: one table's silver

Two things set a table's code path: how fresh its silver must be (slow, fast, realtime), and where its snapshot is
(in the log for a small table, read in place for a big one: a second copy of a big snapshot costs its bytes). Every
cell reads the table's current rows one way (the snapshot wherever it is, then each key's last change after it), so
every cell gives the same table (lfc-log-silver).

| | slow | fast | realtime |
|---|---|---|---|
| **small: snapshot in the log** | shared: a group's table, a view a table | shared (or a MERGE of its own) | AUTO CDC flows in a continuous pipeline |
| **big: snapshot read in place** | a MERGE only in a round that changed it (or a view) | a MERGE every round | AUTO CDC, and a flow for the snapshot |

The four classes in lfc-change-source `routing.py` pick a cell from a table's snapshot size and change rate:
- `small_slow` is small and slow: most tables, almost no data;
- `small_fast` is small and fast;
- `big_slow` is big and slow;
- `big_fast` is big and realtime: few tables, most of the bytes.

A small table costs per statement (a MERGE has a fixed floor of seconds whatever it merges; a warehouse runs a few a
second), a big one per byte. So small tables share: a group of up to 1,000 keeps its current rows in one table (the
row as a VARIANT, clustered by table), one MERGE a round for the group, and each table is a typed view over it.

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
| shared | one MERGE a group a round, growing with the group's changed rows | tables per group (up to 1,000: the MERGE has a branch a table) | the log's latency + one MERGE | per MERGE: the group's changed rows |
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
