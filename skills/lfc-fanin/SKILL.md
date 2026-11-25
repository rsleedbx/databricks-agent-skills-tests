---
name: lfc-fanin
description: "Deploy a fan-in ingestion the way an integrated CDC pipeline is set up (name, catalog, schema, a connector type), as a Databricks bundle that runs a notebook: fanin_dbz_zbus builds silver from the append-only change log your Debezium Server writes through Zerobus and its snapshot Volume; fanin_crdb_cdf lands CockroachDB changefeed files from a Volume into a change log. Use when the source side (Debezium, Zerobus, a changefeed) already runs and silver tables are wanted from it."
---

# A fan-in ingestion as a bundle

You run the source side (Debezium Server through Zerobus, or a CockroachDB changefeed); this skill deploys the
Databricks side. It's set up like an [integrated CDC pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline):
a name, a destination catalog and schema, and an ingestion definition with a connector type.

| `connector_type` | You give | It creates in `<catalog>.<schema>` | Each run |
|---|---|---|---|
| `fanin_dbz_zbus` | `change_log` (the log Debezium writes), `snapshot_volume` (the snapshot as of T, `_sources.yaml`, `_schema/`), `source_type` | a view `cdc_log` over your log, the silver pipeline `<name>-silver`, `_fanin` | one silver update |
| `fanin_crdb_cdf` | `changefeed_path` (a folder a table), `tables` (`database.schema.table:key1\|key2`) | the change log `cdc_log`, `_fanin` | the new files into the log |

The source side, set up and checked end to end: [zerobus-debezium-test](../zerobus-debezium-test/SKILL.md) (`SOURCE=<engine>`,
`PHASE=2`, `KEEP=1` leaves the log and the Volume this skill reads).

## Deploy

```bash
cd skills/lfc-fanin/bundle
export BUNDLE_VAR_pipeline_name=orders-fanin BUNDLE_VAR_connector_type=fanin_dbz_zbus \
  BUNDLE_VAR_dest_catalog=<catalog> BUNDLE_VAR_dest_schema=<schema> BUNDLE_VAR_source_type=SQLSERVER \
  BUNDLE_VAR_change_log=<catalog>.<dbz schema>.cdc_log BUNDLE_VAR_snapshot_volume=/Volumes/<catalog>/<dbz schema>/snapshot
databricks bundle deploy -p <profile>
databricks bundle run fanin_job -p <profile>       # or -t scheduled: every 15 minutes
```

Variables go in as `BUNDLE_VAR_<name>`: the CLI splits a `--var` value on commas. Each run is safe to repeat: it
reuses what it made and brings silver up to date. Without a bundle, the notebook
([bundle/notebooks/fanin.py](bundle/notebooks/fanin.py)) runs the same two calls:

```python
from lfc_fanin import fanin
p = fanin.create(w, spark, name=..., catalog=..., schema=..., ingestion_definition={"connector_type": ..., ...})
fanin.land(w, spark, p)
```

Its `fanin_dbz_zbus` silver is zerobus-debezium-test's AUTO CDC pipeline, one per source table; it doesn't use
FlowThrough's routing ([docs/design.md](../../docs/design.md): a shared group, views, MERGE, AUTO CDC by each table's
size and rate) yet.

Code: [scripts/lfc_fanin/fanin.py](scripts/lfc_fanin/fanin.py), installed as the package `lfc-fanin`
(`pip install <archive>#subdirectory=skills/lfc-fanin/scripts`) next to `lfc-change-source`. The silver pipeline is
zerobus-debezium-test's `silver-from-schema.py`, fetched from the same archive.

## Test a database end to end

[scripts/test-engine.sh](scripts/test-engine.sh) runs both sides and the check: zerobus-debezium-test for your side,
this bundle on its log and Volume, then every fan-in silver table against the skill's (`compare.py`: count and a
hash of every row), and removes that run's containers from the VM. `fanin_page.py <results> <source>` writes the
database's page in docs/database-connect from those results, and refuses a test that didn't pass end to end.

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> WAREHOUSE=<warehouse id> SOURCE=pg SOURCE_TYPE=POSTGRESQL \
  bash skills/lfc-fanin/scripts/test-engine.sh
python3 -I skills/lfc-fanin/scripts/fanin_page.py ~/.lfc-test/results/fanin pg
```

## Checks

`python3 -m unittest skills/lfc-fanin/tests/test_fanin.py` (offline): names quoted part by part, what each
connector type refuses, which errors mean a folder isn't written yet, and an added connector type.

Tested end to end per database, with the result and how to repeat it: [docs/database-connect](../../docs/database-connect/README.md).
