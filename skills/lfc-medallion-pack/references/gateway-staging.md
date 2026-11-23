# A gateway's staging volume: the files between the gateway and the applier

The Lakeflow Connect gateway (extractor) and the ingestion pipeline (applier) talk through files on the gateway's
staging volume: the applier asks for tables in a task file, the gateway writes the snapshot as parquet and the changes as
JSON lines, and the applier writes back how far it applied. Since the volume is readable, the same files could feed
another consumer: an append-only pipeline reading the parquet and the JSON as they land, into lfc-medallion.

Mapped 2026-11-22 on a PostgreSQL gateway (Lakebase, pgoutput, task-set protocol version 6), `scale1`: 62 LOBs, 248
tables, SCD type 1. These are observed formats, not a documented interface: check them on your own gateway's version
(`versioning/`) before building on them.

Root: `/Volumes/<gateway_storage_catalog>/<gateway_storage_schema>/__databricks_ingestion_gateway_staging_data-<gateway id>/<gateway id>/`
(`cdc.staging`). Names below: `<T>` is a table's uoid `T_<db id>_<schema OID>_<table OID>` (`T_HuC79qK_44229_44230`),
a column's uoid is `C_<T without T_>_<ordinal>` (`C_HuC79qK_44229_44230_1`).

## The task: what the applier asks for

| Path | Format | What it holds |
|---|---|---|
| `manager/taskSet/input/<applier id>_config.json` (+ `.md5`) | JSON | `create_or_update_extraction_process`: `source_configurations` (the source catalog, PostgreSQL `slot_config`: slot and publication names), `objects[]` (catalog, schema, `tables[]`: `name`, `snapshot_timestamp`, `snapshot_reason` such as `NEW_TABLE`, `scd_type`); `status` (`ACTIVE`), `version` (`major`, `gitHash`), `applier_id` |
| `manager/taskSet/versioned_input/<applier id>/config.<epoch ms>.json` | JSON | each version of that file |
| `versioning/supported_versions/{gateway_pipeline,ingestion_pipeline}/<kind>_<id>.json` | JSON | each side's supported protocol versions (`INGESTION_JOB_CONFIGURATION`, `TABLE_DATA_AND_METADATA`, `VERSIONED_TASK_SET_PHASE`, ...) |

## Table metadata

| Path | Format | What it holds |
|---|---|---|
| `metadata/table_schemas/<T>/target_schema.<schemaVersion>.json` | JSON | the table as written: `schemas[].tables[]` with `name`, `primaryKey` (name, columns), `columns[]` (`name`, `type` such as `INTEGER`, `DECIMAL(12, 2)`, `STRING`, `notNull`, `uoid`, `sourceDataType` such as `STRING(88)`). A change file's `schemaVersion` picks the file |
| `metadata/general_table_schemas/<uoid>/{source,target}_schema.json` | JSON | the same for the catalog tables the gateway reads to explore schemas (`pg_catalog.*`); source types (`INT8`, `TEXT`) beside target ones (`LONG`, `STRING`) |
| `metadata/snapshot/table_tokens/<T>.json` | JSON | a table's snapshot: `jobRequestTimestamp` (= the task's `snapshot_timestamp`), `schemaVersion`, `startCursor` (`sequence`, `timestamp`, `extractionTimestamp`: where its changes start), `state` (`FINISHED`), `rowCount` |
| `metadata/cdc/<snapshot job id>.yaml` | YAML | `post-snapshot-cdc-position` (`latest-cdc-log-segment-id`, `offset`, `start-timestamp`) and `tables:` (the uoids in the change stream) |
| `metadata/general_metadata.json` | JSON | `sourceType` (`POSTGRESQL`), `partialRowUpdateEnabled` |

## The data

**Snapshot:** `snapshot/<T>/<jobRequestTimestamp>/part_<epoch ms>.parquet`. Columns: the table's column uoids with
their types (`C_..._1 int`, `C_..._3 decimal(12,2)`, `C_..._4 string`), then `__sequence` (string, the cursor the
snapshot is consistent at, e.g. `00000000/8A44A650-0000000000000000000`) and `__operation` (`INSERT`).

**Changes:** `cdc/<segment>/<n>`: a segment folder per gateway run (its start, epoch ms: a restart begins a new one),
files numbered from `0`, JSON lines, one change each (about 200 rows, 118 KB, a file here). A record:

```json
{"d": 103, "operation": "UPDATE", "txnID": "116", "schemaVersion": 0,
 "table_id": {"d": 350, "catalog": "febar", "schema": "scale1_lob_001", "name": "account", "uoid": "T_HuC79qK_44229_44240"},
 "cursor": {"d": 172, "formatVersion": 0, "sequence": "00000000/8A4E4F40-0000000000000000001",
            "timestamp": 1795391230954, "timestampUtc": 1795391230954, "extractionTimestamp": 1795391230957},
 "data": {"C_HuC79qK_44229_44240_1": 1001, "C_HuC79qK_44229_44240_2": 1, "C_HuC79qK_44229_44240_3": "0.00", "C_HuC79qK_44229_44240_4": "..."}}
```

- `operation` with its code `d`: 101 `INSERT`, 102 `DELETE`, 103 `UPDATE`. `table_id.d` is 350 and `cursor.d` 172.
- `data` is the whole row by column uoid: an UPDATE's after image (no before image), a DELETE's row as the source
  logged it (all columns for a table with replica identity FULL, such as TPC-B's keyless `history`). Decimals come as
  strings, timestamps as `yyyy-MM-dd HH:mm:ss` text.
- `cursor.sequence` is `<LSN high>/<LSN low>-<ordinal within the commit>`: it orders the changes. `timestamp` is the
  source commit (ms), `extractionTimestamp` when the gateway read it: 7.7 ms later on average, 42 ms at most, here.
- `txnID` groups a transaction's changes (514 transactions in 95 files here).

To read a change as a row: pick `metadata/table_schemas/<uoid>/target_schema.<schemaVersion>.json`, map each
`data` key to its column name and type, and order by `cursor.sequence`.

## Progress: each side's position

| Path | Format | What it holds |
|---|---|---|
| `applier_metadata/<applier id>/update_time.json` | JSON | the applier, per bronze table (`table_name`, fully qualified): `snapshot_completion_timestamp`, `cdc_completion_timestamp` (ms, how far it has applied) |
| `applier_metadata/<applier id>/snapshot_progress.json` | JSON | per bronze table: `state` (`SNAPSHOT_APPLY_SUCCEEDED`), `update_state_ts`, `snapshot_timestamp`, `snapshot_reason` |
| `backup/<job id>/state_storage_<job id>_<job id>_<epoch ms>.sqlite` (+ `.md5`) | SQLite | the gateway's state, one per CDC or snapshot job: `cdc_extractor_metadata_*` (`cursor`, `committed_cursor` with `lsn`, `replicationSlot`, `publication`; `lag`), `cdc_recovery_metadata_*` (each table's cursor), `src_table_metadata_*` and `dst_table_metadata_*` (each table, `replication_state`: skipped or not), `replication_state_*` (`pg_txid_current`, `UNTRACKED_FAILED_TABLES`), `replication_recovery_metadata_*` (`is_cdc_started`, `snapshot_state`), `snapshot_recovery_metadata_*`, `resource_metadata_*` |
| `backup/manager/manager_backup_<epoch ms>.sqlite` | SQLite | `job`: the gateway's jobs (`id`, `type` `CDC` or `SNAPSHOT`, `status`, `exit_code`, `configuration`: the table filter) |
| `telemetry/<job id>_<epoch s>.json` | JSON arrays | events the event log doesn't carry ([gateway-timing.md](gateway-timing.md)): `CDC_EXTRACTION` watermarks, per-flow `REALTIME_PROGRESS` counts and windows, `RESOURCES` and `HEARTBEAT` (JVM memory) |

Read them from Databricks with `read_files(<path>, format => 'json' | 'parquet')` (a file's write time is
`_metadata.file_modification_time`) and the SQLite files with `sqlite3` on `/Volumes` (`cdc.committed` reads the
committed LSN). The gateway's change files and the applier's `update_time.json` together give both sides of the
handoff: a change written at the file's time, applied by `cdc_completion_timestamp`.
