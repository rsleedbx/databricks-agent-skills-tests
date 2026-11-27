"""snapshot: the source's tables once, into the snapshot contract that silver reads, by whichever route the engine has:

  qbc   a Lakeflow Connect query-based pipeline through a UC connection of the database's own type
        (lfc_change_source.qbc): SQL Server, MySQL, Oracle, PostgreSQL
  jdbc  a serverless job with Spark's jdbc source (SELECT *) through a UC connection of TYPE JDBC (lfc-change-source's
        notebooks/jdbc_snapshot.py, lfc_change_source.intake.jdbc): Db2, any engine with no query-based connector

Nothing leaves Databricks or the source as a file, and no position T has to be lined up: Debezium streams first and a
change of its own has landed, then the snapshot runs; each key's later changes in the log win, as they carry whole rows
(qbc's module doc).

    got = snapshot.take(api, engines.get("sqlserver"), conn, "me-dbz-mssql", "main", "cdc_snap_ms", wh=wh)
    rows = snapshot.rows(wh, got["table"], range(100, 110))     # {id: (name, balance)}
"""
from __future__ import annotations

import os
import time

from lfc_change_source import jobs, qbc
from lfc_source_contract import federation
from lfc_source_contract import jdbc as jdbc_conn

from .destination import q

NOTEBOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "lfc-change-source", "notebooks",
                        "jdbc_snapshot.py")


def take(api, engine, conn: dict, connection: str, catalog: str, schema: str, pipeline: str | None = None,
         wh=None, notebook_path: str | None = None, package: str | None = None) -> dict:
    """The UC connection (created or updated) and one snapshot, by the engine's route (run from a checkout: the jdbc
    route reads lfc-change-source's notebook beside this skill). conn: the source's login
    {"host", "port", "user", "password", ...}. The jdbc route uploads lfc-change-source's notebook to notebook_path and
    pip-installs package there (lfc-change-source's wheel or the repo archive). {"how", "connection", "state",
    "seconds", "table", ...}."""
    if engine.qbc is not None:
        return _qbc(api, engine, conn, connection, catalog, schema, pipeline, wh)
    if engine.jdbc is not None:
        return _jdbc(api, engine, conn, connection, catalog, schema, notebook_path, package)
    raise ValueError(f"{engine.name}: no snapshot route (neither qbc nor jdbc)")


def _qbc(api, engine, conn, connection, catalog, schema, pipeline, wh) -> dict:
    n = engine.qbc(conn)
    federation.upsert(api, federation.body(connection, engine.name, conn["host"], conn["port"], n["login"],
                                           conn["password"], service=n.get("service"),
                                           comment="zerobus-debezium-test: the query-based snapshot's connection"))
    if wh is not None:
        wh.sql(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, schema)}")
    got = qbc.snapshot(api, pipeline or f"{connection}-snapshot", connection, catalog, schema, n["catalog"],
                       n["schema"], {n["table"]: [engine.key]})
    return {"how": "qbc", "connection": connection, **got, "table": qbc.snapshot_table(catalog, schema, n["table"]),
            "link": f"{api.host}/pipelines/{got['pipeline_id']}"}


def _jdbc(api, engine, conn, connection, catalog, schema, notebook_path, package) -> dict:
    if not notebook_path or not package:
        raise ValueError(f"{engine.name}: the jdbc snapshot needs notebook_path and package")
    n = engine.jdbc(conn)
    url = jdbc_conn.url(engine.name, conn["host"], conn["port"], n["database"], tls=n.get("tls", True))
    federation.upsert(api, jdbc_conn.connection_body(connection, engine.name, url, n["login"], conn["password"],
                                                     comment="zerobus-debezium-test: the JDBC snapshot's connection"))
    with open(NOTEBOOK) as f:
        jobs.upload_notebook(api, notebook_path, f.read())
    t0 = time.time()
    rid = jobs.submit(api, f"{connection}-jdbc-snapshot", notebook_path,
                      {"package": package, "jdbc_connection": connection, "tables": n["table"],
                       "target_schema": f"{catalog}.{schema}", "position_kind": ""})
    st = jobs.wait(api, rid)
    out = jobs.output(api, st["task_run_id"]) if st["task_run_id"] else {}
    table = n["table"].split(".")[-1].lower()
    return {"how": "jdbc", "connection": connection, "state": "COMPLETED" if st["result"] == "SUCCESS" else st["result"],
            "seconds": round(time.time() - t0), "table": q(catalog, schema, table), "link": st["url"], "error": out.get("error")}


def rows(wh, table: str, ids: range) -> dict:
    """{id: (name, balance as text)} of the snapshot's rows with those ids (column names fold: Oracle's are upper)."""
    got = wh.sql(f"SELECT CAST(id AS BIGINT), name, CAST(CAST(balance AS DECIMAL(12,2)) AS STRING) FROM {table} "
                 f"WHERE id BETWEEN {ids.start} AND {ids.stop - 1}").collect()
    return {r[0]: (r[1], r[2]) for r in got}
