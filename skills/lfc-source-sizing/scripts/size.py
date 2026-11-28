#!/usr/bin/env python3
"""size.py <profile> <connection> <postgres|mysql|mariadb|sqlserver|oracle|cockroachdb|yugabytedb> [--database D | --service S] [--out DIR] [--top N]
  How big is the source behind a UC connection? Runs the engine's catalog query through the connection with
  remote_query (read-only, from serverless; no table scans, no count(*)), saves one row per table to DIR/tables.csv,
  and prints: the database total (for the infrastructure team), the data Lakeflow Connect moves (table data and
  large values, no indexes), rows and average row size, the largest tables, and tables whose row sources disagree.
  PostgreSQL, SQL Server, CockroachDB and YugabyteDB need --database; Oracle --service (the PDB). MySQL covers every user database at once.
  MariaDB uses the MySQL query (its connection is a MYSQL one; no PROCESS variant: MariaDB has no innodb_tablespaces).
  CockroachDB and YugabyteDB: a POSTGRESQL connection and the PostgreSQL query (--database, as for PostgreSQL).
  Db2 and Informix have no Unity Catalog connection type: run lfc-introspection's scripts/lfc_introspection/sql/size/db2.sql or informix.sql with the engine's client.

The query is size_query.remote_query (the same statement a notebook runs with spark.sql); it runs on a SQL warehouse
of the workspace through lfc_rest (lfc_change_source.engine.Warehouse: a running one first). The report is DuckDB's
(its CLI, `duckdb -box`, on this machine; in a notebook use the rows from size_query with spark.sql instead).
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))   # size_query.py beside this file (it finds lfc-introspection's statements)
from size_query import SizeQueryError, remote_query

try:   # installed (pip install the lfc-rest skill's scripts), else beside this skill in a checkout
    from lfc_rest import Rest
except ImportError:
    sys.path.insert(0, str(HERE.parents[1] / "lfc-rest" / "scripts"))
    from lfc_rest import Rest
try:
    from lfc_change_source.engine import (
        Warehouse,  # a SQL warehouse's statements (the Statement Execution API)
    )
except ImportError:
    sys.path.insert(0, str(HERE.parents[1] / "lfc-change-source" / "scripts"))
    from lfc_change_source.engine import Warehouse

NO_ROWS = "no rows"
USAGE = ("usage: size.py <profile> <connection> <postgres|mysql|mariadb|sqlserver|oracle|cockroachdb|yugabytedb> "
         "[--database D | --service S] [--out DIR] [--top N]")

# DuckDB's report over tables.csv (__CSV__ and __TOP__ filled in). Bytes and rows per table, the totals, the largest
# tables, and the tables whose row sources disagree by more than 1.5x or have none.
REPORT_SQL = """
CREATE TABLE t AS SELECT * FROM read_csv('__CSV__', header = true, all_varchar = true);
CREATE TABLE r AS SELECT table_schema AS s, table_name AS n,
  TRY_CAST(rows_est AS DOUBLE) AS est, TRY_CAST(rows_alt AS DOUBLE) AS alt, TRY_CAST(rows_size AS DOUBLE) AS sz, stats_time,
  coalesce(TRY_CAST(row_bytes AS DOUBLE), 0) AS rowb, coalesce(TRY_CAST(lob_bytes AS DOUBLE), 0) AS lobb,
  coalesce(TRY_CAST(index_bytes AS DOUBLE), 0) AS idxb,
  coalesce(TRY_CAST(total_bytes AS DOUBLE), coalesce(TRY_CAST(row_bytes AS DOUBLE), 0) + coalesce(TRY_CAST(lob_bytes AS DOUBLE), 0) + coalesce(TRY_CAST(index_bytes AS DOUBLE), 0)) AS totb,
  TRY_CAST(logical_row_bytes AS DOUBLE) AS lrb, TRY_CAST(database_bytes AS DOUBLE) AS dbb FROM t;
CREATE TABLE x AS SELECT *, coalesce(alt, est, sz) AS rows, rowb + lobb AS datab,
  greatest(coalesce(est, 0), coalesce(alt, 0), coalesce(sz, 0)) / nullif(least(coalesce(est, alt, sz), coalesce(alt, est, sz), coalesce(sz, alt, est)), 0) AS spread FROM r;
SELECT 'database total (files, incl. indexes, logs/WAL where reported)' AS what, round(max(dbb) / 1e9, 3) AS gb, round(max(dbb) / 1e6, 1) AS mb FROM x
UNION ALL SELECT 'tables + indexes + large values', round(sum(totb) / 1e9, 3), round(sum(totb) / 1e6, 1) FROM x
UNION ALL SELECT 'data Lakeflow Connect moves (table data + large values)', round(sum(datab) / 1e9, 3), round(sum(datab) / 1e6, 1) FROM x
UNION ALL SELECT '  of which large values (TOAST / LOB / overflow)', round(sum(lobb) / 1e9, 3), round(sum(lobb) / 1e6, 1) FROM x
UNION ALL SELECT 'indexes (not moved)', round(sum(idxb) / 1e9, 3), round(sum(idxb) / 1e6, 1) FROM x
UNION ALL SELECT 'logical data from row widths (rows x avg width)', round(sum(rows * lrb) / 1e9, 3), round(sum(rows * lrb) / 1e6, 1) FROM x;
SELECT count(*) AS tables, format('{:,}', CAST(sum(rows) AS BIGINT)) AS rows, CAST(sum(datab) / nullif(sum(rows), 0) AS BIGINT) AS avg_row_bytes_on_disk,
  CAST(sum(rows * lrb) / nullif(sum(rows), 0) AS BIGINT) AS avg_row_bytes_logical FROM x;
SELECT s || '.' || n AS largest_tables, format('{:,}', CAST(rows AS BIGINT)) AS rows, round(datab / 1e9, 3) AS data_gb, round(idxb / 1e9, 3) AS index_gb,
  CAST(datab / nullif(rows, 0) AS BIGINT) AS bytes_per_row FROM x ORDER BY datab DESC LIMIT __TOP__;
SELECT s || '.' || n AS check_rows, CAST(est AS BIGINT) AS from_stats, CAST(alt AS BIGINT) AS second_source, CAST(sz AS BIGINT) AS from_size, stats_time,
  CASE WHEN est IS NULL AND alt IS NULL THEN 'no statistics' ELSE 'sources differ ' || round(spread, 1) || 'x' END AS why
  FROM x WHERE (est IS NULL AND alt IS NULL) OR spread > 1.5 ORDER BY datab DESC LIMIT __TOP__;
"""


def cell(v) -> str:
    """A value as the CSV file has it: empty for NULL, ISO-like for a time, text otherwise."""
    if v is None:
        return ""
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    return str(v)


def query_to_csv(api, statement: str, path: str) -> str | None:
    """Runs the statement on a warehouse and writes its rows (with a header) to path. None when it worked, else the
    error's text (NO_ROWS when the answer is empty: nothing to write)."""
    try:
        rows = Warehouse.pick(api).sql(statement).collect()
    except Exception as e:  # noqa: BLE001  any failure is "query failed", as the CLI's was
        return str(e)
    if not rows:
        return NO_ROWS
    names = type(rows[0])._fields
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(names)
        for r in rows:
            w.writerow([cell(v) for v in r])
    return None


def sql_quote(s: str) -> str:
    return s.replace("'", "''")


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(USAGE, file=sys.stderr)
        return 2
    profile, conn, engine = argv[0], argv[1], argv[2]
    db, svc, out, top = None, None, ".", "15"
    rest = argv[3:]
    while rest:
        a = rest.pop(0)
        if a in ("--database", "--service", "--out", "--top") and rest:
            val = rest.pop(0)
            if a == "--database":
                db = val
            elif a == "--service":
                svc = val
            elif a == "--out":
                out = val
            else:
                top = val
        else:
            print(f"unknown {a}", file=sys.stderr)
            return 2
    os.makedirs(out, exist_ok=True)
    try:   # the engine and its --database / --service (the query says which is missing)
        remote_query(conn, engine, db, svc)
    except SizeQueryError as e:
        print(e, file=sys.stderr)
        return 2
    api = Rest.connect(profile)
    sql_path, csv_path = os.path.join(out, "size.sql"), os.path.join(out, "tables.csv")

    def run(file: str | None) -> str | None:
        """The engine's query (file: another one, mysql-process) to tables.csv, and the query to size.sql; None when
        it worked, else the error's text."""
        text = remote_query(conn, engine, db, svc, file)
        with open(sql_path, "w") as f:
            f.write(text + "\n")
        return query_to_csv(api, text, csv_path)

    err = run("mysql-process" if engine == "mysql" else None)
    if engine == "mysql" and err is None:
        print("(MySQL: with current tablespace file sizes)")
    elif engine == "mysql" and err and "PROCESS" in err:
        err = run(None)   # no PROCESS privilege: the information_schema estimates
        if err is None:
            print("(MySQL: no PROCESS privilege, so no current file sizes: rows and bytes are information_schema estimates; GRANT PROCESS ON *.* adds them)")
    if err == NO_ROWS:
        print(f"no tables in the catalog's answer for {db or svc or conn}: nothing to size")
        return 1
    if err is not None:
        print(f"query failed: {err[:400]}")
        return 1

    report = REPORT_SQL.replace("__CSV__", sql_quote(csv_path)).replace("__TOP__", str(int(top)))
    try:
        subprocess.run(["duckdb", "-box", "-c", report], check=False)
    except FileNotFoundError:
        print("duckdb isn't installed: the report needs its CLI (brew install duckdb)", file=sys.stderr)
    print(f"per-table detail: {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
