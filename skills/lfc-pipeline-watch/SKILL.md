---
name: lfc-pipeline-watch
description: "Where a Lakeflow Connect gateway or ingestion pipeline stands, once or once a minute until it's ready, failed or stalled: the update's state, each table's snapshot and cdc flow, the errors since a start (the pipeline's own apart from each table's), and a gateway's staging files and bytes. Python on databricks-sdk only, so it runs from a laptop and in a notebook. Use while starting a gateway or pipeline, when one looks stuck, or before reading what it captured."
---

# Watch a pipeline

```python
from databricks.sdk import WorkspaceClient
from lfc_watch import check, watch      # scripts/ on sys.path
w = WorkspaceClient(profile="<p>")      # in a notebook: WorkspaceClient()
print(check(w, "<pipeline id>", since_ms=t0_ms, tables=["intpk", "dtix"]).line())
watch(w, "<pipeline id>", since_ms=t0_ms, tables=["intpk", "dtix"], every=60)   # one line a minute
```

A line: `update RUNNING; ready 1/2; waiting dtix; failed dtix: <its error> staging 187 files 3,412,123 B`.

| Signal | From | Means |
|---|---|---|
| update | `GET /api/2.0/pipelines/<id>/updates` (newest) | `RUNNING` or `COMPLETED`: the update runs |
| a table's snapshot, cdc | `flow_progress` events since `since_ms`, newest state per flow | ready: snapshot `COMPLETED` and cdc `RUNNING` |
| errors | `ERROR` events since `since_ms` | without a table: the pipeline's own (its update can't run: a missing connection parameter, a refused login); `watch` raises at once. With one: that table's (the gateway retries it), shown and returned |
| staging | a gateway's staging volume, through the Files API | files and bytes growing: it works before any flow says so (a gateway's flows carry no row counts) |

`watch` stops on: ready (`until="ready"`), the update running (`until="up"`), the pipeline's own error (`RuntimeError`), nothing changed for `stall_s` (no flow state, no staging growth: `TimeoutError`), or `max_s`. The events come from the REST answer, which has each event's `details` (the SDK's typed list drops them).

Flow names it reads: `<catalog>.<schema>.<table>_cdc_flow`, `extractor_<...>_<table>_snapshot_flow`, `[applier_]<catalog>_<schema>_<table>_{snapshot|cdc}_flow`. Pass `tables` for names with `_` in them (the longest table the flow ends with).

Seen 2026-11-26 on e2 (lfcddemo): a SQL Server gateway `ready 2/2` with 256 staging files; a PostgreSQL gateway with both tables failed, `CDC is not enabled on table 'postgres.lfcddemo.intpk'`; one check of three gateways took 47 s, mostly listing staging.

Rows per run of an ingestion pipeline: the public skill databricks-lakeflow-connect's `rows_per_run.sql`.
