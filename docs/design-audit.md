# FlowThrough: where the code meets the design

[design.md](design.md) and its pages say what FlowThrough is meant to be. This page says what the code does today:
what has been tested, and where it falls short. When a gap closes, its row goes and the test joins the tested list.

## Tested

| What | How |
|---|---|
| Every transport into the log, rebuilt equal to the source | each adapter landed live; the conformance scenario (`cdc_log`, `changefeed_log`, `gateway_log`, `gateway_staging`) on a warehouse after the snapshot/CDC split |
| One gateway, 100 to 5,000 tables | lfc_applier `scale` (internal) |
| Every silver cell | shaped rounds on 500 tables, each class's tables equal to the source |
| n:1 merges | lfc-medallion's tests and scale runs |
| PostgreSQL through Debezium and Zerobus, QBC snapshot after CDC | zerobus-debezium-test `SNAP=qbc`: the marker in the log 20 s after Debezium streamed, the QBC snapshot in 64 s, every table equal to the source in both rounds |
| Observe, then pin | on that run: 8 views over the log, recreated over the snapshot tables (6.6 s), two pinned to MERGE; every table equal to the run's silver |
| A shared group from snapshot tables | on that run: 8 tables, one group of 29 changed rows, every table equal |
| Checkpoint and log trim | on a copy of that run's log: 8 tables in 21 s, the log trimmed to empty, then 4 new changes through one round, every table equal to the untrimmed log over the original snapshot |
| The dashboard | rendered with a signed-in session: links to raw CDC, raw snapshot and silver; the Snapshot and Snapshot + log pages for a chosen table |

## Where the code falls short

| Design says | Today | Next |
|---|---|---|
| The fan-in builds silver by FlowThrough's routing | lfc-fanin `fanin_dbz_zbus` builds silver with zerobus-debezium-test's AUTO CDC pipeline, one per table | silver through `flowthrough.observe` then `step` |
| The snapshot comes from a snapshot method, inside Databricks | `SNAP=qbc` for PostgreSQL only; the other engines' test still exports on the VM, copies to a laptop and uploads to a Volume, then `intake.files` | QBC for each engine, then delete the export and upload, engine by engine |
| Configuration stays in Databricks | the test uploads `_sources.yaml`, `_schema/` and `_plan.yaml` from the laptop | the fan-in job writes them |
| Busy is measured before the change capture starts | source counters for PostgreSQL, MySQL and SQL Server only; other engines start slow until the log's rate moves them | collectors for Oracle, Db2, Informix, CockroachDB, YugabyteDB |
| The plan is pinned once snapshots are ready | `flowthrough.observe` takes which snapshots are ready from its caller | read it from QBC's completed update or intake's record |
| The dashboard shows each ingestion | deployed by `lfc_log_silver/dashboard.py`; the fan-in bundle doesn't deploy it or run observe | a bundle resource, and observe in the fan-in job |
| A new snapshot or a type change never needs the source | AUTO CDC tables don't handle either yet | restart that table's flows from its snapshot table and the log |
| A new snapshot is a new table, swapped in | the old snapshot table (and a checkpoint's predecessor) isn't dropped | drop it once nothing reads it |
| A checkpoint when changes have rewritten much of a table, decided by routing | `flowthrough.checkpoint` runs when called | a trigger in routing: changes since the snapshot past a share of its rows, or a schedule |
| A checkpoint's steps each in their layer | `silver.checkpoint` writes the snapshot table and switches silver over in one function | "from silver" as a snapshot source beside `intake`'s; silver's part is its `renewed` handling |
| A gateway doing the CDC can take QBC's snapshot | the gateway's task always asks for its own snapshot | a task option to take none |
| A big table's snapshot from the customer's files | `intake.parquet_in_place` and `intake.files` exist, not run on a CockroachDB `initial_scan = 'only'` export | that run |
| 10,000s of tables, many sources through one log | not run | a scale run |
| A schema change is an event at its position (add with a default, rename, type change, drop), never a refresh | a change found by comparing columns recreates a view or rebuilds a MERGE table from its snapshot table and the log, which gets `ADD COLUMN ... DEFAULT x` (NULL instead of x) and renames wrong; the decision is in silver, not routing | schema events in the log from each source's record (ddl_history, Debezium's schema events, an event trigger for PostgreSQL), and routing reading them |
| A refresh is partial: a column or row patch at a position, combined by silver | none: a suspect table needs a new whole snapshot (QBC) and a rebuild from it | patch tables (key and columns, or keys) from QBC's column selection or `intake`, silver's rule with the patch as one more input at its position, routing deciding from checks and drift |
| Routing is a layer of its own | its decision is lfc-change-source `routing.py` (the CDC package) and its runner lfc-log-silver `flowthrough.py` (the silver package) | one routing module both call |
| Silver combines snapshot and CDC | the combining query, `CdcLog.current_sql`, and the snapshot on a CDC source live in lfc-change-source; CDC should give only a table's changes after a position | move the combine into lfc-log-silver |
| One silver | zerobus-debezium-test's `silver-from-schema.py` and `merge-silver.py` combine snapshot and CDC on their own, and the fan-in uses them | the fan-in and the test on lfc-log-silver (the first row above) |
| Each layer in its own skill | the snapshot sources (`qbc`, `intake`) live in the CDC package | a snapshot skill of its own |
| The log without snapshots is faster | expected, not measured | the same round with and without snapshot rows in the log, bytes read and time |

## Removed

The code that kept snapshots in the change log (`read` rows, the newest-snapshot rule, `land.gateway_snapshots`, the
log/direct snapshot path) is at the tag `snapshot-in-log-last`, if it's ever needed again.

## Where each part is checked

- **Transport:** each adapter's conformance scenario on a warehouse, and each transport landed and rebuilt equal to
  the source.
- **Silver:** every cell, offline (each reads its snapshot table and its changes) and live (each class's tables equal
  to the source after shaped rounds).
- **n:1:** the n:1 routes on their own (lfc-medallion's tests and scale runs).
- **Gold:** the marts against the model (lfc-medallion-pack's checks).
