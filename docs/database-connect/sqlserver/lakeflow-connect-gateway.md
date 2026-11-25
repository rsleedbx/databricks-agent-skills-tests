# SQL Server: create an ingestion pipeline with a Lakeflow Connect gateway

The ingestion gateway reads SQL Server (CDC or change tracking) and writes snapshots and changes into its staging
volume. lfc-change-source lands those files into the same append-only change log the Zerobus route writes, and silver
is kept from it. Back to [database connectors](../README.md).

## Requirements

What the Lakeflow Connect SQL Server connector needs: its
[setup](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline) (a login, CDC or change
tracking, the utility objects for schema changes), and a Unity Catalog connection to the database.

## Create the ingestion pipeline

1. **The gateway and its pipeline,** as the Lakeflow Connect docs create them: a gateway (`gateway_definition`:
   `connection_name`, `gateway_storage_catalog`, `gateway_storage_schema`) and an ingestion pipeline
   (`ingestion_definition`: `ingestion_gateway_id`, `objects`). Or a single
   [integrated CDC pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline)
   (`connector_type: CDC`).
2. **Land the staging into the change log.** Each round, `land.gateway` copies the new change files once each
   (`COPY INTO`), with each row's key filled in (`land.gateway_keys`), and `land.gateway_snapshots` lands the small
   tables' snapshots (a big one stays where the gateway wrote it and is read in place):
   [lfc-change-source](../../../skills/lfc-change-source/SKILL.md).
3. **Silver,** the same as the Zerobus route: [lfc-log-silver](../../../skills/lfc-log-silver/SKILL.md).

## Deploy

**As a bundle:** the Lakeflow Connect docs' bundle for the gateway and ingestion pipeline (or the integrated
pipeline), unchanged; then a job that runs the landing and silver rounds below on a schedule.

**From a notebook or job:**

```python
from lfc_change_source import gateway_staging as gs, land
root = "/Volumes/<catalog>/<staging schema>/<gateway staging volume>/<gateway id>"
entries = gs.schemas(spark, root)
land.ensure_log(spark, "`main`.`ctl`.`cdc_log`")
land.gateway_snapshots(spark, root, "`main`.`ctl`.`cdc_log`", "sales_gw", entries)
land.gateway(spark, root, "`main`.`ctl`.`cdc_log`", "sales_gw", keys=land.gateway_keys(entries))
```

Then silver as on the [Zerobus page](debezium-zerobus.md#deploy). A continuous alternative to `COPY INTO`: the
Auto Loader pipeline in lfc-change-source `pipelines/gateway_autoloader.py`.

Tested on a sandbox workspace: one gateway from 100 to 5,000 SQL Server tables landed and rebuilt equal to the
source; shaped rounds on 500 tables with each silver method ([numbers-from-the-tests.md](../../numbers-from-the-tests.md)
keeps the public summary).
