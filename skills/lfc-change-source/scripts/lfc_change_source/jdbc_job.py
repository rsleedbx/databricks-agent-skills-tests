"""jdbc_job: a snapshot method beside qbc, for a source with no query-based connector (Db2, Informix) or a table read
directly: Spark's jdbc source (SELECT *, intake.jdbc) through a UC connection of TYPE JDBC, run as a serverless job of
lfc-change-source's notebooks/jdbc_snapshot.py. The same two calls as qbc: snapshot() takes it, snapshot_table() names
it. The order is qbc's: the change capture first, a change of its own landed, then the snapshot (no position needed).

    got = jdbc_job.snapshot(api, "me_db2_jdbc", ["DBZ.CUSTOMERS"], "main", "cdc_snap", "/Users/me/jdbc_snapshot",
                            package="/Volumes/main/cdc/v/wheels/lfc_change_source-0.1.0-py3-none-any.whl")
    jdbc_job.snapshot_table("main", "cdc_snap", "DBZ.CUSTOMERS")   # `main`.`cdc_snap`.`customers`

Run from a checkout: the notebook is read beside this package and uploaded to notebook_path.
"""
from __future__ import annotations

import os
import time

from . import jobs

NOTEBOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "notebooks", "jdbc_snapshot.py")


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def snapshot_table(catalog: str, schema: str, source_table: str) -> str:
    """The snapshot table for a source table (schema.table as the source names it): its table name, in lower case."""
    return f"{_q(catalog)}.{_q(schema)}.{_q(source_table.split('.')[-1].lower())}"


def snapshot(api, connection: str, tables: list, catalog: str, schema: str, notebook_path: str, package: str,
             position_kind: str = "", timeout: int = 3600) -> dict:
    """One run of the notebook over tables (as the source names them) into catalog.schema: {"state" (COMPLETED or
    the run's result), "seconds", "url", "error"}. package: what the notebook pip-installs for lfc_change_source."""
    with open(NOTEBOOK) as f:
        jobs.upload_notebook(api, notebook_path, f.read())
    t0 = time.time()
    rid = jobs.submit(api, f"{connection}-jdbc-snapshot", notebook_path,
                      {"package": package, "jdbc_connection": connection, "tables": ",".join(tables),
                       "target_schema": f"{catalog}.{schema}", "position_kind": position_kind})
    st = jobs.wait(api, rid, timeout)
    out = jobs.output(api, st["task_run_id"]) if st["task_run_id"] else {}
    return {"state": "COMPLETED" if st["result"] == "SUCCESS" else (st["result"] or st["life_cycle"]),
            "seconds": round(time.time() - t0), "url": st["url"], "error": out.get("error")}
