"""qbc: a snapshot method, a Lakeflow Connect query-based pipeline through a UC connection reading each table once, for
any transport (docs/design.md, layer 1: the snapshot is chosen apart from how changes arrive). Debezium through
Zerobus uses it so nothing outside Databricks exports or uploads files; a gateway can use it too, its task set to take
no snapshot of its own.

The order is the caller's to keep: the change capture runs first and a change of its own has landed in the change
log, so the log holds every change from a position P on; then this snapshot runs. The snapshot is read in place
(snapshot_table, at position None: before every change in the log), and each key's last change after it wins: the log
carries whole rows, so a change the snapshot already holds replays to the same row, and the overlap between P and the
snapshot is harmless. A table without a key can't be told apart from a replay: it needs no writes between P and the
snapshot.

    from lfc_change_source import qbc
    got = qbc.snapshot(w, "orders-snap", "orders_conn", "main", "orders_snap", source_catalog="shop",
                       source_schema="public", tables={"orders": ["o_id"], "lines": ["o_id", "n"]})
    silver.Table(src, "merge", snapshot_table=qbc.snapshot_table("main", "orders_snap", "orders"))

    python3 -I qbc.py --profile <p> --name <pipeline> --connection <uc connection> --catalog <c> --schema <s> \\
        --source-catalog <database> --source-schema <schema> --schema-tsv <table, ordinal, column, type, in_pk ...>
"""
from __future__ import annotations

import json
import time


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def spec(name: str, connection: str, catalog: str, schema: str, source_catalog: str, source_schema: str,
         tables: dict) -> dict:
    """The pipeline: every table SCD type 1 on its key, no cursor (each update reads the whole table)."""
    def obj(t, keys):
        cfg = {"scd_type": "SCD_TYPE_1", **({"primary_keys": list(keys)} if keys else {})}
        return {"table": {"source_catalog": source_catalog, "source_schema": source_schema, "source_table": t,
                          "destination_catalog": catalog, "destination_schema": schema, "table_configuration": cfg}}
    return {"name": name, "catalog": catalog, "schema": schema, "serverless": True, "continuous": False,
            "ingestion_definition": {"connection_name": connection,
                                     "objects": [obj(t, k) for t, k in sorted(tables.items())]}}


def snapshot_table(catalog: str, schema: str, table: str) -> str:
    """The snapshot as silver takes it (the contract: a Delta table, lfc_log_silver Table.snapshot_table): the
    streaming table the pipeline wrote, quoted."""
    return f"{_q(catalog)}.{_q(schema)}.{_q(table)}"


def keys_from_tsv(path: str) -> dict:
    """{table: key columns in order} from a schema TSV (table, ordinal, column, type, in_pk, ...)."""
    out: dict = {}
    with open(path) as fh:
        lines = fh.read().splitlines()
    for line in lines:
        f = line.split("\t")
        if len(f) >= 5 and f[0]:
            out.setdefault(f[0], [])
            if f[4] == "1":
                out[f[0]].append((int(f[1]), f[2]))
    return {t: [c for _, c in sorted(v)] for t, v in out.items()}


def snapshot(w, name: str, connection: str, catalog: str, schema: str, source_catalog: str, source_schema: str,
             tables: dict, timeout: int = 3600) -> dict:
    """The snapshot pipeline (created, or its spec put when it exists) run once: {pipeline_id, state, seconds}."""
    from .pipeline import run_pipeline, upsert_pipeline
    pid = upsert_pipeline(w, spec(name, connection, catalog, schema, source_catalog, source_schema, tables))
    t0 = time.time()
    return {"pipeline_id": pid, "state": run_pipeline(w, pid, timeout), "seconds": round(time.time() - t0)}


def main(argv=None) -> None:
    import argparse
    ap = argparse.ArgumentParser(description="One QBC snapshot of a source's tables")
    for a in ("profile", "name", "connection", "catalog", "schema", "source-catalog", "source-schema", "schema-tsv"):
        ap.add_argument(f"--{a}", required=True)
    a = ap.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    got = snapshot(WorkspaceClient(profile=a.profile), a.name, a.connection, a.catalog, a.schema, a.source_catalog,
                   a.source_schema, keys_from_tsv(a.schema_tsv))
    print(json.dumps(got))
    raise SystemExit(0 if got["state"] == "COMPLETED" else 1)


if __name__ == "__main__":   # run as a file: import it as the package's module, so .pipeline resolves
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from lfc_change_source import (
        qbc as _qbc,  # noqa: PLW0406  the package's copy, not __main__
    )
    _qbc.main()
