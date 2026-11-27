"""lfc_watch: where a Lakeflow Connect pipeline (a gateway, an ingestion pipeline) stands, once, or once a minute until
it's ready, failed or stalled. REST through databricks-sdk only, so the same code runs on a laptop (a profile) and in a
notebook (its own login).

    from databricks.sdk import WorkspaceClient
    from lfc_watch import check, watch
    w = WorkspaceClient(profile="p")                 # in a notebook: WorkspaceClient()
    s = check(w, pipeline_id, since_ms=t0_ms, tables=["intpk", "dtix"])
    s = watch(w, pipeline_id, since_ms=t0_ms, tables=["intpk", "dtix"], every=15)   # one line a check

A check: the newest update's state; each table's snapshot and cdc flow state (the flow events since since_ms); the
errors since since_ms, the pipeline's own (no table: its update can't run) apart from each table's; for a gateway still
waiting on a table, the files and bytes on its staging volume (the Files API: progress before any flow says so, and a gateway's flows carry no
row counts).
"""
from .core import Status, check, flows, staging, watch  # noqa: F401
