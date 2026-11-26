# SQL Server: create an ingestion pipeline with Debezium through Zerobus

You run Debezium Server next to SQL Server; its Zerobus sink writes every change into one append-only change log in
Unity Catalog. The fan-in bundle ([lfc-fanin](../../../skills/lfc-fanin/SKILL.md), `connector_type: fanin_dbz_zbus`)
builds silver from that log. Back to [database connectors](../README.md).

## Tested

2026-11-24, SQL Server 2022 (16.0.4305.5, Developer) and Debezium Server 3.7.0.Final (Zerobus sink) in containers on an Azure VM, into a serverless
Azure workspace. The source holds [TPC-H](../../tpc-models.md#tpc-h) at SF 0.001 (8 tables).

| Step | Result |
|---|---|
| Your side: snapshot, Debezium, Zerobus, the change log | every claim passed: each table's inserts, updates and deletes in the log within 1 s; the snapshot stays out of the log |
| The fan-in bundle: `bundle deploy`, `bundle run fanin_job` | created the view `cdc_log` over your log and the silver pipeline; one update |
| Silver against the source | all 8 tables equal (row count and a hash of every row), after two rounds of changes |

## Your side: SQL Server, Debezium, Zerobus

The skill [zerobus-debezium-test](../../../skills/zerobus-debezium-test/SKILL.md) sets it up and checks it: SQL Server
with CDC, the snapshot as of T in a Volume, Debezium Server with the Zerobus sink, the service principal and the
change log. What the fan-in needs from it is the log and the Volume, which `KEEP=1` leaves:

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> SOURCE=mssql MODEL=tpch NSRC=1 PHASE=2 KEEP=1 \
  bash skills/zerobus-debezium-test/scripts/debezium-zerobus.sh
# prints: schema <catalog>.<run schema>, the change log <catalog>.<run schema>.cdc_log,
#         the Volume /Volumes/<catalog>/<run schema>/snapshot
```

Your own Debezium works the same way when it writes that log and Volume layout (the skill's
[references](../../../skills/zerobus-debezium-test/references/) describe both).

## The fan-in: deploy

```bash
cd skills/lfc-fanin/bundle
export BUNDLE_VAR_pipeline_name=orders-fanin BUNDLE_VAR_connector_type=fanin_dbz_zbus BUNDLE_VAR_source_type=SQLSERVER \
  BUNDLE_VAR_dest_catalog=<catalog> BUNDLE_VAR_dest_schema=<fan-in schema> \
  BUNDLE_VAR_change_log=<catalog>.<run schema>.cdc_log BUNDLE_VAR_snapshot_volume=/Volumes/<catalog>/<run schema>/snapshot
databricks bundle deploy -p <cli-profile>
databricks bundle run fanin_job -p <cli-profile>      # each run brings silver up to date; -t scheduled: every 15 minutes
```

It creates, in `<catalog>.<fan-in schema>`: a view `cdc_log` over your log, the silver pipeline `<name>-silver`
(one table per source table, SCD type 1, and `<table>_history`, SCD type 2; `schema_drift` records values that didn't fit), and `_fanin` (what it made). Running it
again changes nothing it made, and runs one silver update.

From a notebook instead of a bundle: [the bundle's notebook](../../../skills/lfc-fanin/bundle/notebooks/fanin.py)
(the same `fanin.create` and `fanin.land` calls).
