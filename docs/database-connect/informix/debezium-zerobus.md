# Informix: create an ingestion pipeline with Debezium through Zerobus

You run Debezium Server next to Informix; its Zerobus sink writes every change into one append-only change log in
Unity Catalog. The fan-in bundle ([lfc-fanin](../../../skills/lfc-fanin/SKILL.md), `connector_type: fanin_dbz_zbus`)
builds silver from that log. Back to [database connectors](../README.md).

## Tested

2026-11-25: Informix 14.10 (docker.io/ibmcom/informix-developer-database:14.10.FC7W1DE), and Debezium Server (localhost/debezium-server-ifx:3.7.0-15.0.1.4-1.1.3, Zerobus sink), in containers on a cloud VM, into a serverless
workspace. The source holds [TPC-H](../../tpc-models.md#tpc-h) at SF 0.001 (8 tables).

| Step | Result |
|---|---|
| Your side: snapshot, Debezium, Zerobus, the change log | 23 checks passed, none failed (zerobus-debezium-test, its silver equal to the source after two rounds) |
| The fan-in bundle: `bundle deploy`, `bundle run fanin_job` | the view `cdc_log` over your log and the silver pipeline; one update |
| The fan-in's silver | all 16 tables (8 tables and their `_history`) equal to the skill's (row count and a hash of every row) |

Repeat the whole test, both sides and the check: [test-engine.sh](../../../skills/lfc-fanin/scripts/test-engine.sh)
(`SOURCE=informix SOURCE_TYPE=INFORMIX`); this page is written from its results by `fanin_page.py`.

## Your side: Informix, Debezium, Zerobus

The skill [zerobus-debezium-test](../../../skills/zerobus-debezium-test/SKILL.md) sets it up and checks it. The
fan-in needs the change log and the snapshot Volume it leaves with `KEEP=1`:

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> SOURCE=informix MODEL=tpch NSRC=1 PHASE=2 KEEP=1 \
  bash skills/zerobus-debezium-test/scripts/debezium-zerobus.sh
# prints the run's schema: the change log <catalog>.<run schema>.cdc_log, the Volume /Volumes/<catalog>/<run schema>/snapshot
```

## The fan-in: deploy

```bash
cd skills/lfc-fanin/bundle
export BUNDLE_VAR_pipeline_name=<name> BUNDLE_VAR_connector_type=fanin_dbz_zbus BUNDLE_VAR_source_type=INFORMIX \
  BUNDLE_VAR_dest_catalog=<catalog> BUNDLE_VAR_dest_schema=<fan-in schema> \
  BUNDLE_VAR_change_log=<catalog>.<run schema>.cdc_log BUNDLE_VAR_snapshot_volume=/Volumes/<catalog>/<run schema>/snapshot
databricks bundle deploy -p <cli-profile>
databricks bundle run fanin_job -p <cli-profile>      # each run brings silver up to date; -t scheduled: every 15 minutes
```

It creates, in `<catalog>.<fan-in schema>`: a view `cdc_log` over your log, the silver pipeline `<name>-silver` (a
table per source table, SCD type 1, and `<table>_history`, SCD type 2; `schema_drift` records values that didn't
fit), and `_fanin`. Running it again changes nothing it made, and runs one silver update.
