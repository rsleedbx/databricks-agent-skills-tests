# Gateway or applier: where a change's time goes

A Lakeflow Connect change goes source commit → the gateway (the extractor: it reads the source's log and writes change
files to its staging volume) → the ingestion pipeline (the applier: it reads the staging volume and writes bronze) →
the n:1 layer → silver and gold. The pipelines' event logs show the applier well and the gateway only coarsely. The
gateway keeps much more on its staging volume than its event log has, so `cdc.capture` (run by the verify job) reads it
from there:

| What | Where on the staging volume | Fields used | Goes to |
|---|---|---|---|
| The gateway's telemetry | `telemetry/<id>_<epoch>.json` (JSON arrays) | `CDC_EXTRACTION`, about once a minute per gateway: `operation_details.cdc_extraction_report.upstream_commit_timestamp_watermark_ms` and `upstream_fetch_timestamp_watermark_ms`; `REALTIME_PROGRESS`, per table flow about once a minute: `statistics.insertCount`, `updateCount`, `deleteCount`, `startTimestamp`, `endTimestamp`, `duration_ms`, `schema_name`, `table_name` | `<control>.gateway_events` |
| Its change files | `cdc/<segment>/<n>` (JSON lines) | `table_id.schema`, `cursor.timestampUtc` (the source commit, ms) and the file's own `_metadata.file_modification_time` (when the gateway wrote it) | `<control>.staged_files`, one row per file and LOB |

The staging volume is `/Volumes/<gateway_storage_catalog>/<gateway_storage_schema>/__databricks_ingestion_gateway_staging_data-<gateway id>/<gateway id>/`
(`cdc.staging`). Each table gets one `INSERT ... SELECT FROM read_files(...)` per gateway, and a file already read is
left out (`NOT IN` its path), so the work is a few statements per gateway, not one per LOB or file. The volume also
has `backup/` (the SQLite state with the committed LSN: `cdc.committed`), `snapshot/`, `metadata/`, `manager/` and
`applier_metadata/` (every file and its format: [gateway-staging.md](gateway-staging.md)). The telemetry's `RESOURCES` and `HEARTBEAT` events (the gateway JVM's memory) aren't read yet.

`v_latency` then gives each batch, per LOB:

- `staged_at`: the first change file of the LOB whose newest source commit is at or after the batch, by its write
  time. That is exact for the file's newest row, so it is the capture time.
- `fetched_at`: the first extraction watermark past the batch. It is reported about once a minute and may already
  cover later commits, so it is an upper bound only.
- `bronze_at`, `merge_at`, `gold_at`: as before, from the pipelines' event logs, the merge log and the transform.

On the dashboard's Latency page these appear as fetched, staged, applier (bronze minus staged) and to bronze.

Read a batch's split from `v_latency`: staged minus the commit is the gateway's capture, bronze minus staged the
applier. In the triggered runs so far the capture was a small fraction of the time to bronze and the applier's
update (a cost a flow, data or not) the rest: the case for an applier of one's own fed by the gateway's staging, or
Debezium or Zerobus, once the table count is in the thousands. The numbers are kept with their configuration in
internal notes.
