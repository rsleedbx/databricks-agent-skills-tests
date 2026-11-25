"""intake: every way a snapshot arrives, turned into the snapshot contract (docs/design-no-full-refresh.md): a Delta table
with the source's column names and types, which every silver cell reads (lfc_log_silver Table.snapshot_table,
zerobus-debezium-test's silver through _sources.yaml snapshot_table). Silver then reads tables only: no format, no
text decoding, no folder listing. One function a kind of arrival; each returns {"table", "rows", "seconds", "how"}.

  table(name)                 a Delta table already: a QBC pipeline's streaming table (qbc.py), the customer's own
  parquet_in_place(path, ...) CONVERT TO DELTA over Parquet files whose names and types are the source's: no copy
                              (an external location the workspace writes the Delta log to; the files must not change)
  files(path, fmt, ...)       any files read_files reads (Parquet, CSV with its options, JSON) copied once into a
                              table, each column typed as the source's (Debezium's text and number encodings decoded,
                              lfc_change_source.debezium): an export in a Volume, a changefeed's initial scan
  gateway(staging, ...)       a gateway's snapshot in its staging (columns by id) copied once into a table by name,
                              with its position (the gateway's cursor at the snapshot: changes after it apply)

Every function takes a session with .sql() (a SQL warehouse: engine.Warehouse, or Spark) and runs once a snapshot.
A column is {"name", "type" (Databricks SQL), "kind" (a debezium.TEMPORAL kind or None), "precision"}.

    from lfc_change_source import intake
    got = intake.files(wh, "/Volumes/c/s/snapshot/orders/pg1", "csv", "`c`.`s_snap1`.`orders`", cols,
                       options={"header": "true"})
"""
from __future__ import annotations

import time

from . import debezium


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quoted(name: str) -> str:
    """catalog.schema.table quoted part by part (a name already in backticks is kept)."""
    return name if "`" in name else ".".join(_q(x) for x in name.split("."))


def _lit(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def value_sql(c: dict, have: set, binhex: bool = False) -> str:
    """A snapshot file's column as the source's type: its default (NULL) when the file lacks it, binary from hex
    text when binhex (a CSV export; Parquet holds binary as binary), temporal values decoded, the rest cast."""
    if c["name"] not in have:
        return f"CAST({c.get('default') or 'NULL'} AS {c['type']})"
    x = _q(c["name"])
    if c["type"].upper() == "BINARY" and binhex:
        return f"CASE WHEN typeof({x}) = 'binary' THEN CAST({x} AS BINARY) ELSE unhex(CAST({x} AS STRING)) END"
    if c.get("kind") in debezium.TEMPORAL:
        return debezium.decode(c["kind"], c["type"], c.get("precision"), f"CAST({x} AS STRING)")
    return f"CAST({x} AS {c['type']})"


def _rows(session, table: str) -> int:
    r = session.sql(f"SELECT count(*) FROM {table}").collect()
    return int(r[0][0]) if r else 0


def _done(session, table: str, t0: float, how: str) -> dict:
    return {"table": table, "rows": _rows(session, table), "seconds": round(time.time() - t0, 1), "how": how}


def table(session, name: str) -> dict:
    """A Delta table already (QBC's, the customer's): used as it is."""
    t0 = time.time()
    return _done(session, quoted(name), t0, "table")


def _reader(path: str, fmt: str, options: dict | None) -> str:
    opts = "".join(f", {k} => {_lit(str(v).lower() if isinstance(v, bool) else v)}" for k, v in (options or {}).items())
    return f"read_files({_lit(path)}, format => {_lit(fmt)}, recursiveFileLookup => 'true'{opts})"


def file_columns(session, path: str, fmt: str, options: dict | None = None) -> set:
    """The columns the files hold (DESCRIBE QUERY: no data read)."""
    return {r[0] for r in session.sql(f"DESCRIBE QUERY SELECT * FROM {_reader(path, fmt, options)}").collect()
            if r[0] and not str(r[0]).startswith("#")}


def files(session, path: str, fmt: str, target: str, columns: list, options: dict | None = None,
          binhex: bool = False) -> dict:
    """Files copied once into `target`, typed as the source's (module doc). A folder with no files: an empty table."""
    t0, target = time.time(), quoted(target)
    cols = ", ".join(f"{_q(c['name'])} {c['type']}" for c in columns)
    try:
        have = file_columns(session, path, fmt, options)
    except Exception as e:   # no files there: the table at T had no rows
        if "CANNOT_INFER_EMPTY_SCHEMA" not in str(e) and "PATH_NOT_FOUND" not in str(e) and "not exist" not in str(e):
            raise
        session.sql(f"CREATE OR REPLACE TABLE {target} ({cols})").collect()
        return _done(session, target, t0, f"no {fmt} files: empty")
    sel = ", ".join(f"{value_sql(c, have, binhex)} AS {_q(c['name'])}" for c in columns)
    session.sql(f"CREATE OR REPLACE TABLE {target} AS SELECT {sel} FROM {_reader(path, fmt, options)}").collect()
    return _done(session, target, t0, f"{fmt} files copied")


def parquet_in_place(session, path: str, target: str) -> dict:
    """Parquet with the source's names and types made a Delta table where it is (no copy), named `target`."""
    t0, target = time.time(), quoted(target)
    session.sql(f"CONVERT TO DELTA parquet.`{path}`").collect()
    session.sql(f"CREATE TABLE IF NOT EXISTS {target} USING DELTA LOCATION {_lit(path)}").collect()
    return _done(session, target, t0, "parquet converted in place")


def gateway(session, staging, target: str) -> dict:
    """A gateway's snapshot in its staging (gateway_staging.Staging: columns by id) copied once into `target`, by name."""
    t0, target = time.time(), quoted(target)
    cols = ", ".join(_q(c) for c in staging.columns)
    session.sql(f"CREATE OR REPLACE TABLE {target} AS SELECT {cols} FROM ({staging.snapshot_sql()})").collect()
    # its position: the gateway's cursor the snapshot was taken at; its changes are the log's after it
    return {**_done(session, target, t0, "gateway snapshot copied"), "position": staging.snapshot_position(session)}
