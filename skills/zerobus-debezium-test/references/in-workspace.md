# Debezium inside a Databricks workspace

Two notebooks run Debezium on Databricks compute instead of on a VM, with nothing to host outside the workspace. Both read a PostgreSQL slot with `snapshot.mode=no_data`, keep their offsets in a Volume, and stop on a STOP file, an idle timeout, a maximum runtime, or a cancelled job run.

| | [debezium-server-zerobus.py](../notebooks/debezium-server-zerobus.py) | [debezium-pydbzengine-append-log.py](../notebooks/debezium-pydbzengine-append-log.py) |
|---|---|---|
| What runs | Debezium Server 3.7.0.Final as a child process of the notebook | Debezium's embedded engine in a JVM inside the Python process ([pydbzengine](https://github.com/memiiso/pydbzengine), JPype) |
| Who writes the change log | Debezium's own Zerobus sink, over gRPC; Python only starts, watches and stops the process | Python: each batch is a Spark Delta append |
| Change log | `cdc_log`, Debezium's envelope in `VARIANT` columns (the same table as the VM route) | A Delta table of strings: `op`, `ts_ms`, `record_key`, `before_json`, `after_json`, `raw_value` |
| Java | 21: Debezium Server's core is built for 21. The notebook downloads a Temurin 21 runtime into the Volume once | 17, the runtime's own: the connectors and the embedded engine are built for 17 |
| Sign-in | A service principal's OAuth secret (the Zerobus sink takes only that) | The notebook's own identity |

## Tested

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

Not tested yet: the STOP file and a cancelled run (SIGTERM from the job), classic compute, other engines, volume.

## What it took on serverless

- **Java 17 is there, Java 21 isn't.** `/usr/lib/jvm` has Zulu 8, 11 and 17, and `JAVA_HOME` points at 8, which JPype picks by default. The pydbzengine notebook passes the newest 17+ `libjvm.so` to the engine.
- **The notebook's `JAVA_OPTS` is for that Java 8** (`PrintGCDateStamps`, which Java 21 refuses). The Debezium Server notebook starts the process without `JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` and `_JAVA_OPTIONS`.
- **Downloads need a user agent of their own:** the default `Python-urllib` one got a 403.
- **A job can land on compute an earlier run used,** with its temp files: each run unpacks into a new folder, and makes the archives' folders readable.
- **Debezium Server reads `config/application.properties`.** The notebook writes it 0600 on local disk with the two secrets from a secret scope, and deletes it when the run ends.
- **`display()` can't show `VARIANT` on serverless:** cast to `STRING` first.

## Heartbeats

Without a heartbeat, a quiet source leaves the slot's confirmed position short of the last change, so PostgreSQL keeps that WAL. With `heartbeat.interval.ms`, the position moves, and the Zerobus sink writes each heartbeat into `cdc_log` as destination `__debezium-heartbeat.<prefix>`, operation `change`. Silver reads by `destination`, so they don't reach it. Dropping them before the sink with Kafka's `Filter` transform (`TopicNameMatches`) kept them out of `cdc_log`, but the slot's position stopped moving too, so the notebook keeps them.
