# SQL Server: create an ingestion pipeline with Debezium through Zerobus

Debezium Server reads SQL Server's CDC change tables and its Zerobus sink writes each change into an append-only
change log in Unity Catalog. Silver is kept from that log. Back to [database connectors](../README.md).

Tested with SQL Server 2022 (16.0.4305) and Debezium Server 3.7, Zerobus sink, into a serverless Azure workspace:
inserts, updates and deletes reach silver equal to the source, and several sources into one set of tables
([lakeflow-connect-zerobus.md](../../lakeflow-connect-zerobus.md)).

## Requirements

- **SQL Server:** CDC on for the database and each table (`sys.sp_cdc_enable_db`, `sys.sp_cdc_enable_table`), the
  SQL Server Agent running (its capture job fills the change tables), and a login that can read them. The test's
  setup: [mssql-cdc.sql](../../../skills/zerobus-debezium-test/scripts/sources/mssql-cdc.sql).
- **Debezium Server** next to the database, with outbound 443 to the workspace's Zerobus endpoint.
- **Unity Catalog:** a catalog and schema for the change log, a service principal with `MODIFY` on it (Zerobus
  writes as it), and a Unity Catalog connection to the database for silver's schema reads.

## Create the ingestion pipeline

1. **The change log.** One Delta table for every source table: `destination STRING, operation STRING, key VARIANT,
   value VARIANT, ...`, `CLUSTER BY (destination)` (lfc-change-source `land.ensure_log` makes it).
2. **The snapshot.** Each table's rows as of the time Debezium starts from, into a Volume (Parquet), so the first load
   doesn't go through Zerobus.
3. **Debezium Server.** The SQL Server connector with the Zerobus sink, the tables to capture, starting after the
   snapshot. Its config: the test's [zerobus-debezium-test](../../../skills/zerobus-debezium-test/SKILL.md).
4. **Silver.** lfc-log-silver: each table's current rows from its snapshot and its changes in the log, kept by the
   method its route picks (a group's shared table for most small tables, a MERGE, or AUTO CDC for the busiest):
   [lfc-log-silver](../../../skills/lfc-log-silver/SKILL.md).

You run Debezium; Databricks only receives. The change log is the hand-off: a fan-in reader needs to know only its
table name.

## Deploy

**As a script** (what the tests run, end to end, with checks against the source: SQL Server in a container, Debezium,
Zerobus, silver, gold):

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> SOURCES=mssql MODELS=tpch NSRCS=1 PARALLEL=1 \
  bash skills/zerobus-debezium-test/scripts/matrix.sh
```

`NSRCS=2` merges two SQL Server sources into one set of tables. The workspace needs Zerobus first
([zerobus-workspace-setup](../../../skills/zerobus-workspace-setup/SKILL.md)).

**From a notebook or job:** with the change log already written by your Debezium Server, silver is a call per round:

```python
from lfc_change_source import routing
from lfc_log_silver import silver
tables = {t: silver.from_route(src[t], route) for t, route in routing.plan(stats, policy).items()}
silver.run(spark, tables, "`main`.`silver`", to, first=True)    # the first round builds; later rounds apply
```

Not covered yet: a bundle that deploys the Databricks side alone (schema, service principal grant, silver job).
