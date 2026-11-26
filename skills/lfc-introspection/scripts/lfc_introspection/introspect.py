"""introspect: routing's first box (docs/design-intelligent.md, "Two boxes"): a source database in, statschema's
schema.yaml and stats.yaml out, and nothing else. It reads only the catalog, through a Unity Catalog connection with
remote_query (no table scans, no row data): each base table's columns and types, its primary key and foreign keys
(sql/), and its rows and bytes (sql/size/<engine>.sql, which lfc-source-sizing runs too). The documents are statschema's
canonical format (statschema.tools.canonical.convert, the same conversion its collectors use).

    from lfc_introspection.introspect import introspect
    schema, stats = introspect(session, "lfcddemo-azurevm-pg", "postgres", database="lfcddemo", schema="lfcddemo")

session: anything with .sql(text).collect() returning rows (a Spark session, lfc_change_source.engine.Warehouse).
Engines: postgres, yugabytedb, cockroachdb, sqlserver (information_schema), mysql, mariadb. Oracle, Db2 and Informix
have no catalog query here yet.
"""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
try:   # installed (pip install the lfc-source-statschema skill's scripts), else beside this skill in a checkout
    from statschema.tools.canonical import convert
except ImportError:
    sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "lfc-source-statschema", "scripts"))
    from statschema.tools.canonical import convert  # noqa: E402

from .catalog import remote_query  # noqa: E402

SQL = os.path.join(HERE, "sql")
# engine -> (its columns query, its keys query (sql/<name>.sql), sqlglot's dialect for its type names)
PG = ("infoschema-columns", "pg-keys", "postgres")
ENGINES = {"postgres": PG, "yugabytedb": PG, "cockroachdb": PG,
           "sqlserver": ("infoschema-columns", "infoschema-keys", "tsql"),
           "mysql": ("mysql-columns", "mysql-keys", "mysql"), "mariadb": ("mysql-columns", "mysql-keys", "mysql")}
LENGTHS = ("char", "character", "varchar", "character varying", "nchar", "nvarchar", "binary", "varbinary")
DECIMALS = ("numeric", "decimal")


def native(engine: str, data_type: str, length, precision, scale, dt_precision) -> str:
    """The column's type as its own SQL spells it, from information_schema's pieces (MySQL's column_type is whole)."""
    t = str(data_type)
    if ENGINES[engine][2] == "mysql":
        return t
    low = t.lower()
    if low in LENGTHS and length not in (None, "", -1, "-1"):
        return f"{t}({int(length)})"
    if low in DECIMALS and precision not in (None, ""):
        return f"{t}({int(precision)},{int(scale or 0)})"
    if dt_precision not in (None, "") and (low.startswith(("timestamp", "time ", "datetime2", "datetimeoffset"))
                                         or low in ("time", "datetime2", "datetimeoffset")):
        word, _, rest = t.partition(" ")
        return f"{word}({int(dt_precision)}){(' ' + rest) if rest else ''}"
    return t


def raw(columns: list, keys: list, sizes: list, engine: str, schema: str) -> dict:
    """A collector's raw document (to-canonical.py's input) from the three queries' rows."""
    tables: dict = {}
    for r in sorted(columns, key=lambda r: (r[0], int(r[1]))):
        t = tables.setdefault(r[0], {"name": r[0], "columns": [], "primary_key": [], "foreign_keys": []})
        c = {"name": r[2], "data_type": native(engine, r[3], r[4], r[5], r[6], r[7]),
             "nullable": str(r[8]).upper() != "NO"}
        if r[9] is not None:
            c["default"] = str(r[9])
        t["columns"].append(c)
    fks: dict = {}
    for r in sorted(keys, key=lambda r: (r[1], r[2], int(r[4]))):
        t = tables.get(r[1])
        if t is None:
            continue
        if r[0] == "P":
            t["primary_key"].append(r[3])
        else:
            f = fks.setdefault((r[1], r[2]), {"columns": [], "ref_table": r[5], "ref_columns": [],
                                               "on_delete": r[7], "on_update": r[8]})
            f["columns"].append(r[3])
            f["ref_columns"].append(r[6])
    for (tn, _), f in sorted(fks.items()):
        tables[tn]["foreign_keys"].append(f)
    for r in sizes:
        d = {str(k).lower(): v for k, v in (r.asDict() if hasattr(r, "asDict") else dict(r)).items()}   # MySQL: TABLE_NAME
        t = tables.get(d.get("table_name"))
        if t is None or str(d.get("table_schema") or schema).lower() != schema.lower():
            continue
        # the first estimate that knows (an estimate of 0 rows next to another's count: the other)
        known = [int(d[k]) for k in ("rows_est", "rows_alt", "rows_size") if d.get(k) is not None]
        rows = next((n for n in known if n), known[0] if known else None)
        t["row_count"] = int(rows or 0)
        size = sum(int(d.get(k) or 0) for k in ("row_bytes", "lob_bytes"))
        if size:
            t["size_bytes"] = size
    return {"source_dialect": ENGINES[engine][2], "values": "none", "tables": [tables[n] for n in sorted(tables)]}


def introspect(session, connection: str, engine: str, database: str | None = None, schema: str = "public",
               service: str | None = None) -> tuple:
    """(schema, stats): statschema's two documents for `schema`'s base tables, read through `connection`."""
    if engine not in ENGINES:
        raise ValueError(f"introspect: engine {engine!r}, one of {', '.join(ENGINES)}")
    cols_q, keys_q, _ = ENGINES[engine]
    fill = {"__SCHEMA__": schema}
    q = lambda f: session.sql(remote_query(connection, engine, database, service, file=f,  # noqa: E731
                                           folder=SQL, fill=fill)).collect()
    sizes = session.sql(remote_query(connection, engine, database, service)).collect()
    return convert(raw(q(cols_q), q(keys_q), sizes, engine, schema))
