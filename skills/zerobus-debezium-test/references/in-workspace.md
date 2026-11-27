# Debezium inside a Databricks workspace

Two notebooks run Debezium on Databricks compute instead of on a VM, with nothing to host outside the workspace. Both start with `snapshot.mode=no_data`, keep their offsets in a Volume, and stop on a STOP file, an idle timeout, a maximum runtime, or a cancelled job run. The Debezium Server notebook takes `engine` (PostgreSQL, SQL Server, MySQL, Oracle, Db2); the pydbzengine one is PostgreSQL only.

| | [debezium-server-zerobus.py](../notebooks/debezium-server-zerobus.py) | [debezium-pydbzengine-append-log.py](../notebooks/debezium-pydbzengine-append-log.py) |
|---|---|---|
| What runs | Debezium Server 3.7.0.Final as a child process of the notebook | Debezium's embedded engine in a JVM inside the Python process ([pydbzengine](https://github.com/memiiso/pydbzengine), JPype) |
| Who writes the change log | Debezium's own Zerobus sink, over gRPC; Python only starts, watches and stops the process | Python: each batch is a Spark Delta append |
| Change log | `cdc_log`, Debezium's envelope in `VARIANT` columns (the same table as the VM route) | A Delta table of strings: `op`, `ts_ms`, `record_key`, `before_json`, `after_json`, `raw_value` |
| Java | 21: Debezium Server's core is built for 21. The notebook downloads a Temurin 21 runtime into the Volume once | 17, the runtime's own: the connectors and the embedded engine are built for 17 |
| Sign-in | A service principal's OAuth secret (the Zerobus sink takes only that) | The notebook's own identity |

## Five engines through Debezium Server → Zerobus

The package [scripts/zerobus_debezium](../scripts/zerobus_debezium) runs the rounds from a laptop: each engine's settings and capture setup (`engines`), the source database on a slot VM (`source`), the change log, the service principal and its grants (`destination`), and Debezium Server in the workspace as a producer (lfc-change-source's `WorkspaceDebeziumServer`). `python -m zerobus_debezium.run --help` lists its options; [tests/test_zerobus_debezium.py](../tests/test_zerobus_debezium.py) checks the settings offline.

Serverless jobs, Debezium Server 3.7.0.Final, one slot database per engine on an Azure VM, the four producers at once, into one `cdc_log` (2026-11-27). Each round's events are checked by operation and id, in the source's order (the `seq` expression per engine).

| Check | SQL Server 2022 | MySQL 8.4 | Oracle 23 Free | Db2 11.5.9 | PostgreSQL 16 |
|---|---|---|---|---|---|
| Streaming after submit | ✅ 46-77 s | ✅ 62 s | ✅ 46-62 s | ✅ 61-107 s | ✅ 62 s |
| First start: no row from before it | ✅ | ✅ | ✅ | ✅ | ✅ |
| Inserts, an update, a delete while streaming | ✅ | ✅ | ✅ | ✅ | ✅ |
| STOP file: offsets flushed, run `SUCCESS` | ✅ | ✅ | ✅ | ✅ | ✅ |
| Changes while stopped, then a restart | ✅ | ✅ | ✅ | ✅ | ✅ |
| A rerun of the same producer resumes from its offsets | ✅ | ✅ | ✅ | ✅ | ✅ (the earlier notebook) |
| Every change once, in order | ✅ | ✅ | ✅ | ✅ | ✅ |
| Snapshot after round 1, this run's rows as the source holds them | ✅ QBC 46 s | ✅ QBC 47 s | ✅ QBC 62 s | ✅ Spark jdbc 112-122 s | ✅ QBC 46 s |
| Silver (FlowThrough `flowthrough.step` over the log and the snapshot), after round 1 and round 3 | ✅ | ✅ | ✅ | ✅ | ✅ |
| The whole silver table equals a new snapshot of the source | ✅ 0 of 10 differ | ✅ 0 of 11 | ✅ 0 of 16 | ✅ 0 of 14 | ✅ 0 of 6 |

The snapshot (FlowThrough's layer 3, [docs/design.md](../../../docs/design.md)) is taken by the method lfc-routing's `snapshot_method` picks for the engine, from `lfc_change_source.SNAPSHOTS`: `qbc`, a query-based pipeline through a UC connection of the database's own type, where Lakeflow Connect has a query-based connector; else `jdbc`, Spark's jdbc read (`SELECT *`) in a serverless job through a TYPE JDBC connection. Both run after Debezium has landed round 1, so the log holds every change from before the snapshot on, and nothing leaves Databricks or the source as a file. The Oracle snapshot reads as LogMiner's common user through the CDB's service, with the PDB as `source_catalog`; Db2's server listens without TLS, so its JDBC URL can't ask for it.

What each engine needs (all in `engines`): SQL Server CDC on the database and the table (SQL Server Agent runs its jobs); MySQL's binary log in ROW format with full images; Oracle in ARCHIVELOG with supplemental logging and LogMiner's common user `c##dbzuser` (the setup restarts the instance once); Db2's ASN capture: Debezium's management scripts compiled in the container, the capture agent `asncap` started before `ASNCDC.ADDTABLE`, then a reinit. Db2's shell steps need a login on the database's host; the others are SQL only.

**Findings:**
- **The schema history can't live in a Volume.** MySQL writes several records at its first start, and Debezium's `FileSchemaHistory` reopens the file to append: `Unable to create writer for history file /Volumes/...: Illegal seek`, retried without end. One record (SQL Server, Oracle, Db2 here) went through. The notebook keeps the history on local disk, restores it from the Volume before a start, and copies it back whenever it changes. Offsets are rewritten whole, so they stay in the Volume.
- **Db2 is the slowest to deliver.** ASN capture commits its change tables on an interval (30 s by default), and Debezium polls them. A stop within that window leaves the last change for the next run: in one run, a delete arrived one run late. It was carried, not lost. The rounds wait 90 s for Db2, 30 s for the others.
- **A producer that ran before isn't a first start.** A row written while it was stopped is a change it must carry, and it does. The rounds check that (`resume`) instead of `no-snapshot` when the producer's folder has offsets.
- **Cancel is the fallback, the STOP file the stop.** Cancelling the serverless run ended it and Debezium in about 40 s (no binlog connection left on MySQL), but the notebook's cleanup didn't run: `status.json` kept `streaming`, and `application.properties` (with both secrets) stays on that compute's local disk until it's recycled. Offsets are flushed every second, so a restart replays at most that. The rounds cancel only to clean up after a failed round.
- **A connector that can't start retries without end.** Debezium Server stays up and logs `Failed with retriable exception ... attempt #N out of infinity`. The notebook gives up after `start_timeout_sec` (600 s) and writes Debezium's last errors to `status.json`.

## PostgreSQL in detail

Serverless notebook jobs in an Azure workspace that allows only serverless, PostgreSQL 16 with `pgoutput`, one table with a primary key and `REPLICA IDENTITY FULL` (2026-11-27).

| Check | Debezium Server → Zerobus | pydbzengine |
|---|---|---|
| A row inserted before the first start | ✅ Not in the log | ✅ Not in the log |
| Inserts, an update and a delete while running | ✅ One event each; the update with before and after, the delete with the before image | ✅ The same |
| Changes while stopped, then a restart | ✅ Picked up from the Volume's offsets | ✅ The same |
| Duplicates after the restart | ✅ None | ✅ None |
| `NUMERIC` values | ✅ Text, with `decimal.handling.mode=string` | ✅ Text, with the same setting; the default (`precise`) gives base64 bytes |
| Stop on the idle timeout | ✅ SIGTERM, offsets flushed, run `SUCCESS` | ✅ `engine.close()`, run `SUCCESS` |
| The slot's confirmed position on a quiet source | ✅ Past the last change with `heartbeat.interval.ms=10000`; short of it without | Not checked |

Silver is FlowThrough's (layer 4, lfc-log-silver): routing put the table in a shared group on every engine. It found one ordering gap: Oracle's LogMiner gives one transaction's changes the same SCN (here an insert and an update of one key), so the log's order for Oracle is now `scn_rs` in `lfc_change_source.cdc_log.SEQUENCES`: the SCN, then the redo record's address `rs_id`. A silver schema built on `scn` positions has to be rebuilt.

Not tested yet: classic compute, volume, schema changes; n:1 and gold (next: lfcddemo's databases through both a gateway and Debezium + Zerobus).

## What it took on serverless

- **Java 17 is there, Java 21 isn't.** `/usr/lib/jvm` has Zulu 8, 11 and 17, and `JAVA_HOME` points at 8, which JPype picks by default. The pydbzengine notebook passes the newest 17+ `libjvm.so` to the engine.
- **The notebook's `JAVA_OPTS` is for that Java 8** (`PrintGCDateStamps`, which Java 21 refuses). The Debezium Server notebook starts the process without `JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` and `_JAVA_OPTIONS`.
- **Downloads need a user agent of their own:** the default `Python-urllib` one got a 403.
- **A job can land on compute an earlier run used,** with its temp files: each run unpacks into a new folder, and makes the archives' folders readable.
- **Debezium Server reads `config/application.properties`.** The notebook writes it 0600 on local disk with the two secrets from a secret scope, and deletes it when the run ends.
- **`display()` can't show `VARIANT` on serverless:** cast to `STRING` first.

## Heartbeats

Without a heartbeat, a quiet source leaves the slot's confirmed position short of the last change, so PostgreSQL keeps that WAL. With `heartbeat.interval.ms`, the position moves, and the Zerobus sink writes each heartbeat into `cdc_log` as destination `__debezium-heartbeat.<prefix>`, operation `change`. Silver reads by `destination`, so they don't reach it. Dropping them before the sink with Kafka's `Filter` transform (`TopicNameMatches`) kept them out of `cdc_log`, but the slot's position stopped moving too, so the notebook keeps them.

## A source's own tables (`zerobus_debezium.stream`)

The rounds above use a test table the test creates. `python -m zerobus_debezium.stream` runs tables that already exist
the way a customer's would go: change capture on them (`engines.Engine.capture`, optional), Debezium Server streaming them,
a snapshot of each once its first change has landed (routing's method, through a UC connection that's there already),
at the table's newest event just before it, then FlowThrough silver rounds. A keyed table's changes during the
snapshot's read reapply harmlessly (whole rows); a keyless table's insert in that short window counts twice. The source keeps changing, so each silver table is checked against the snapshot and the
log up to the round's position (`combine.current_sql`), not against the source.

`capture()` only changes what capture needs and never restarts a database unless told it may (`restart=True`): Oracle
refuses a database that isn't in ARCHIVELOG, Db2's ASN install (archive logging, a backup and a restart) is skipped when
`ASNCDC` is there already. Informix is an engine too: its CDC database `syscdcv1`, and Debezium Server's `lib/` gets the
Informix JDBC driver and change-stream client (`Engine.jars`, downloaded by the notebook). On a database other clients
use, Db2's archive-logging backup can fail on their connections and leave the database in backup pending (SQL1116N),
refusing connections until an offline backup succeeds.

Each run takes its snapshot from scratch (a QBC full refresh) and builds its silver anew. A temporal column is decoded in Debezium's unit for 6 digits of fraction unless `--precision col=digits` says otherwise (milliseconds for 0-3, nanoseconds for 7-9). PostgreSQL capture gives a keyless table with the default replica identity `REPLICA IDENTITY FULL` before adding it to the publication, as PostgreSQL refuses updates and deletes on a published table without one.

Not run live yet: `stream.py` and the Informix engine. Capture has been set up with it on six long-running databases
(MySQL, PostgreSQL, SQL Server, Oracle, Db2, Informix).
