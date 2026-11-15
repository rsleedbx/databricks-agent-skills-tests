"""evidence: what each table is (fact, dimension, ...) decided from what the data shows, then the hints.

table_plan() builds one schema in lfc-source-statschema's canonical form (columns and types, primary and foreign keys,
row counts, inserts / updates / deletes) and runs roles.plan over it. The evidence comes from, in order:

1. the source itself, through the Unity Catalog connection Lakeflow Connect already reads it with (`connection`):
   declared primary and foreign keys and the database's own counters (PostgreSQL, through remote_query: columns from
   information_schema, keys from pg_catalog, which a SELECT-only login sees, counters from pg_stat_user_tables).
   Nothing is installed on the source.
2. bronze, when there's no connection: the primary key Lakeflow Connect records on each destination table (the table
   property spark.internal.streaming_table.apply_changes.keys: internal, not documented; SHOW TBLPROPERTIES may not show
   it, the Unity Catalog API does), column types, row counts,
   the inserts / updates / deletes of each MERGE in the table's history after the first (the snapshot), and foreign
   keys inferred from names (a column whose name, after its table's prefix, is another table's one-column key:
   l_suppkey -> supplier.s_suppkey), each marked as inferred in the table's `why`.
3. hints: {table: role} or {table: {role, silver}} from someone who knows the data, which win (decided_by: hint).

Each table also gets a route: where a pipeline builds it. Facts, logs and queues change all the time, so they stay in
the incremental pipeline; dimensions, lookups and bridges change rarely, so with many tables a MERGE job can keep them
current without putting each one in the pipeline's graph (ROUTE; `routes` overrides it per role). The measured
activity overrides the role: a table with at least `busy_changes` changes recorded, and at least `busy_per_row` per row,
goes to the pipeline whatever its role. A table with no relations falls to the rules' dimension default, yet can change
all the time (the demo VM's intpk: about 87,000 changes on 1,000 rows, seen through MySQL, SQL Server and PostgreSQL
connections 2026-11-15); a one-time bulk update that touches every row once isn't steady change (TPC-H's customer
relabelled once at setup: 7,500 updates on 7,500 rows, seen through the source's counters 2026-11-15).
"""
import json

from . import roles
from .stack import spark_string

ROUTE = {"fact": "pipeline", "log": "pipeline", "queue": "pipeline",
         "dimension": "merge", "lookup": "merge", "bridge": "merge"}
TYPES = {"decimal": "decimal", "double": "double", "float": "float", "real": "real", "numeric": "numeric",
         "timestamp": "timestamp", "timestamp_ntz": "timestamp", "timestamp with time zone": "timestamptz",
         "timestamp without time zone": "timestamp", "date": "date", "datetime": "datetime"}


def canonical_type(t: str) -> str:
    """A Spark or PostgreSQL type as the canonical schema names the ones the rules look at; any other as it is."""
    t = str(t).lower()
    return TYPES.get(t.split("(")[0].strip(), t)


def _rows(spark, query):
    return spark.sql(query).collect()


# --- 1. the source, through a Unity Catalog connection --------------------------------------------------------------
# Each engine: its own catalog queries, each returning the same columns. No ORDER BY inside them: remote_query runs a
# query as a subquery, and SQL Server refuses ORDER BY there; rows are sorted here by their `ord`. Activity is optional:
# a login without the privilege for the counters gets row counts only (`rows`).

def sql_string(s: str) -> str:
    """A string literal for PostgreSQL, SQL Server and MySQL ('' is a quote; MySQL also reads a backslash as an
    escape, so backslashes are doubled)."""
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"




def _engine_sql(s: str) -> dict:
    return {
        # PostgreSQL: keys from pg_catalog, which a SELECT-only login sees (information_schema.table_constraints
        # lists only tables the user owns or holds more than SELECT on: seen through Lakeflow Connect's login,
        # 2026-11-15); ordinary and partitioned tables, not partitions, views or foreign tables
        "POSTGRESQL": {
            "columns": "SELECT table_name, column_name, data_type, ordinal_position AS ord "
                       f"FROM information_schema.columns WHERE table_schema = {s} AND table_name IN (SELECT c.relname "
                       "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                       f"WHERE n.nspname = {s} AND c.relkind IN ('r', 'p') AND NOT c.relispartition)",
            "keys": "SELECT t.relname AS table_name, CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'FOREIGN KEY' "
                    "END AS constraint_type, c.conname AS constraint_name, a.attname AS column_name, "
                    "pt.relname AS parent_table, k.ord FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
                    "JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) "
                    "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum "
                    f"LEFT JOIN pg_class pt ON pt.oid = c.confrelid WHERE n.nspname = {s} AND c.contype IN ('p', 'f')",
            "activity": "SELECT relname AS table_name, n_live_tup AS live, n_tup_ins AS ins, n_tup_upd AS upd, "
                        f"n_tup_del AS del FROM pg_stat_user_tables WHERE schemaname = {s}",
        },
        # MySQL (and MariaDB): the database is the schema. Counters from performance_schema (needs SELECT on it, the
        # same source as lfc-source-statschema's collect-activity-mysql.sql); table_rows is an estimate
        "MYSQL": {
            "columns": "SELECT c.table_name AS table_name, c.column_name AS column_name, c.data_type AS data_type, "
                       "c.ordinal_position AS ord FROM information_schema.columns c JOIN information_schema.tables t "
                       "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
                       f"AND t.table_type = 'BASE TABLE' WHERE c.table_schema = {s}",
            "keys": "SELECT table_name AS table_name, CASE WHEN constraint_name = 'PRIMARY' THEN 'PRIMARY KEY' "
                    "ELSE 'FOREIGN KEY' END AS constraint_type, constraint_name AS constraint_name, "
                    "column_name AS column_name, referenced_table_name AS parent_table, ordinal_position AS ord "
                    f"FROM information_schema.key_column_usage WHERE table_schema = {s} "
                    "AND (constraint_name = 'PRIMARY' OR referenced_table_name IS NOT NULL)",
            "activity": "SELECT t.table_name AS table_name, t.table_rows AS live, w.count_insert AS ins, "
                        "w.count_update AS upd, w.count_delete AS del FROM information_schema.tables t "
                        "LEFT JOIN performance_schema.table_io_waits_summary_by_table w "
                        "ON w.object_type = 'TABLE' AND w.object_schema = t.table_schema "
                        "AND w.object_name = t.table_name "
                        f"WHERE t.table_schema = {s} AND t.table_type = 'BASE TABLE'",
            "rows": "SELECT table_name AS table_name, table_rows AS live FROM information_schema.tables "
                    f"WHERE table_schema = {s} AND table_type = 'BASE TABLE'",
        },
        # SQL Server: keys from the catalog views (a login with SELECT on a table sees its metadata); counters from
        # sys.dm_db_index_operational_stats need VIEW SERVER STATE, so without it only row counts (sys.partitions)
        "SQLSERVER": {
            "columns": "SELECT c.TABLE_NAME AS table_name, c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type, "
                       "c.ORDINAL_POSITION AS ord FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t "
                       "ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME "
                       f"AND t.TABLE_TYPE = 'BASE TABLE' WHERE c.TABLE_SCHEMA = {s}",
            "keys": "SELECT t.name AS table_name, 'PRIMARY KEY' AS constraint_type, i.name AS constraint_name, "
                    "col.name AS column_name, CAST(NULL AS NVARCHAR(128)) AS parent_table, ic.key_ordinal AS ord "
                    "FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id "
                    "JOIN sys.schemas sc ON sc.schema_id = t.schema_id "
                    "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
                    "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id "
                    f"WHERE i.is_primary_key = 1 AND sc.name = {s} "
                    "UNION ALL SELECT t.name, 'FOREIGN KEY', fk.name, col.name, pt.name, fkc.constraint_column_id "
                    "FROM sys.foreign_keys fk "
                    "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
                    "JOIN sys.tables t ON t.object_id = fk.parent_object_id "
                    "JOIN sys.schemas sc ON sc.schema_id = t.schema_id "
                    "JOIN sys.columns col ON col.object_id = fkc.parent_object_id "
                    "AND col.column_id = fkc.parent_column_id "
                    f"JOIN sys.tables pt ON pt.object_id = fk.referenced_object_id WHERE sc.name = {s}",
            # rows summed per table on their own; the counters over the heap or clustered index (index 0 or 1),
            # one row per partition, so a partitioned table's partitions aren't multiplied by each other
            "activity": "SELECT t.name AS table_name, r.live, SUM(o.leaf_insert_count) AS ins, "
                        "SUM(o.leaf_update_count) AS upd, SUM(o.leaf_delete_count) AS del FROM sys.tables t "
                        "JOIN sys.schemas sc ON sc.schema_id = t.schema_id "
                        "JOIN (SELECT object_id, SUM(rows) AS live FROM sys.partitions WHERE index_id IN (0, 1) "
                        "GROUP BY object_id) r ON r.object_id = t.object_id "
                        "LEFT JOIN sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) o "
                        "ON o.object_id = t.object_id AND o.index_id IN (0, 1) "
                        f"WHERE sc.name = {s} GROUP BY t.name, r.live",
            "rows": "SELECT t.name AS table_name, SUM(p.rows) AS live FROM sys.tables t "
                    "JOIN sys.schemas sc ON sc.schema_id = t.schema_id "
                    "JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1) "
                    f"WHERE sc.name = {s} GROUP BY t.name",
        },
    }


ENGINES = tuple(_engine_sql("''"))


def _remote(spark, connection, database, query):
    return _rows(spark, f"SELECT * FROM remote_query({spark_string(connection)}, database => {spark_string(database)}, "
                        f"query => {spark_string(query)})")


def connection_engine(connection: str, workspace=None) -> str:
    """The connection's type (POSTGRESQL, SQLSERVER, MYSQL, ...), from the Unity Catalog API."""
    if workspace is None:
        from databricks.sdk import WorkspaceClient
        workspace = WorkspaceClient()
    t = workspace.connections.get(connection).connection_type
    return str(getattr(t, "value", t)).upper()


def connection_schema(spark, connection: str, database: str, schema: str, engine: str = "POSTGRESQL") -> dict:
    """Columns, keys, foreign keys, row counts and activity of one schema, read through the connection with its
    engine's catalog queries (ENGINES)."""
    q = _engine_sql(sql_string(schema))[engine]
    tables = {}
    for r in sorted(_remote(spark, connection, database, q["columns"]), key=lambda r: (r.table_name, int(r.ord))):
        tables.setdefault(r.table_name, {"name": r.table_name, "columns": []})["columns"].append(
            {"name": r.column_name, "type": canonical_type(r.data_type)})
    fks = {}
    keys = _remote(spark, connection, database, q["keys"])
    for r in sorted(keys, key=lambda r: (r.table_name, r.constraint_name, int(r.ord))):
        t = tables.get(r.table_name)
        if not t:
            continue
        if r.constraint_type == "PRIMARY KEY":
            for c in t["columns"]:
                if c["name"] == r.column_name:
                    c["primary_key"] = True
        else:
            fks.setdefault((r.table_name, r.constraint_name), {"columns": [], "parent_table": r.parent_table})[
                "columns"].append(r.column_name)
    for (t, _), fk in fks.items():
        tables[t].setdefault("fk_constraints", []).append(fk)
    activity, note = {}, ""
    try:
        stats = _remote(spark, connection, database, q["activity"])
    except Exception as e:   # no privilege for the counters: row counts only, where the engine has a query for them
        if "rows" not in q:
            raise
        stats, note = _remote(spark, connection, database, q["rows"]), f" (no counters: {str(e).splitlines()[0][:80]})"
    for r in stats:
        if r.table_name not in tables:
            continue
        live = int(getattr(r, "live", 0) or 0)
        tables[r.table_name]["row_count"] = live
        ins, upd, dele = (int(getattr(r, k, 0) or 0) for k in ("ins", "upd", "del"))
        # the counters include the initial load: inserts alone, no more than the rows there now, can't tell a load
        # from appends, so they're no evidence; updates and deletes are
        if upd or dele or ins > live:
            activity[r.table_name] = {"inserts": ins, "updates": upd, "deletes": dele}
    return {"tables": list(tables.values()), "activity": activity,
            "evidence": f"connection {connection} ({engine}){note}"}


# --- 2. bronze, as Lakeflow Connect wrote it ------------------------------------------------------------------------

KEYS_PROPERTY = "spark.internal.streaming_table.apply_changes.keys"


RESETS = ("CREATE TABLE", "CREATE OR REPLACE TABLE", "REPLACE TABLE", "TRUNCATE")


def _write_counts(op: str, m: dict) -> tuple[int, int, int]:
    """Inserts, updates, deletes of one history entry, by its metrics. MERGE: what an SCD type 1 or 2 destination
    takes (Lakeflow Connect's database connectors, seen on TPC-H through PostgreSQL 2026-11-15); WRITE and STREAMING
    UPDATE: appends (an append-only destination); DELETE and UPDATE: their own counts."""
    def n(*keys):
        return next((int(m[k]) for k in keys if m.get(k) not in (None, "")), 0)
    if op == "MERGE":
        return (n("numTargetRowsInserted"), n("numTargetRowsMatchedUpdated", "numTargetRowsUpdated"),
                n("numTargetRowsMatchedDeleted", "numTargetRowsDeleted"))
    if op in ("WRITE", "STREAMING UPDATE"):
        return n("numOutputRows", "numAddedRows"), 0, 0
    if op == "DELETE":
        return 0, 0, n("numDeletedRows")
    if op == "UPDATE":
        return 0, n("numUpdatedRows"), 0
    return 0, 0, 0


def history_activity(spark, table: str) -> dict:
    """Inserts, updates and deletes in the table's writes after the snapshot. The snapshot: the writes of inserts only
    before the first that updates or deletes, from the start or the last time the table was replaced, truncated or
    overwritten (a full refresh starts a new one). A lower bound: inserts before the first update count as snapshot,
    and an append-only table never leaves it (its activity is no evidence; its structure decides)."""
    total, in_snapshot = {"inserts": 0, "updates": 0, "deletes": 0}, True
    history = (f"SELECT version, operation, operationParameters, operationMetrics FROM (DESCRIBE HISTORY {table}) "
               "ORDER BY version")
    for r in _rows(spark, history):
        params = r.operationParameters or {}
        params = json.loads(params) if isinstance(params, str) else params
        if r.operation in RESETS or r.operation.startswith("CREATE OR REPLACE") or \
                (r.operation == "WRITE" and str(params.get("mode", "")).lower() == "overwrite"):
            total, in_snapshot = {"inserts": 0, "updates": 0, "deletes": 0}, True
            continue
        m = r.operationMetrics or {}
        ins, upd, dele = _write_counts(r.operation, json.loads(m) if isinstance(m, str) else m)
        if in_snapshot and not upd and not dele:
            continue
        in_snapshot = False
        total["inserts"] += ins
        total["updates"] += upd
        total["deletes"] += dele
    return total


def _named_like(base: str) -> set[str]:
    """Table names a column <base>_id points at: customer -> customer, customers; category -> categories."""
    names = {base, base + "s", base + "es"}
    if base.endswith("y"):
        names.add(base[:-1] + "ies")
    return names


def infer_foreign_keys(tables: list[dict]) -> None:
    """Add a foreign key from names, the conventions that schemas follow without declaring the key:
    - a column with another table's one-column key's own name (orders.customer_id -> customer.customer_id);
    - after its table's own prefix, another table's key after that table's prefix (TPC-H: l_suppkey -> s_suppkey);
    - <name>_id to a table <name>, <name>s or <name>es keyed `id` (Rails and most ORMs: orders.customer_id ->
      customers.id);
    - columns for every column of another table's key of two or more (TPC-H: l_partkey, l_suppkey -> partsupp)."""
    def bare(col):
        return col.split("_", 1)[1] if "_" in col else col
    full, short, by_id = {}, {}, {}   # one-column keys: by own name, by name after the prefix; tables keyed `id`
    for t in tables:
        pk = [c["name"] for c in t["columns"] if c.get("primary_key")]
        if len(pk) == 1:
            full.setdefault(pk[0], []).append(t["name"])
            short.setdefault(bare(pk[0]), []).append(t["name"])
            if pk[0].lower() == "id":
                by_id[t["name"].lower()] = t["name"]
    def prefix(col):
        return col.split("_", 1)[0] if "_" in col else None
    for t in tables:
        declared = {c for fk in t.get("fk_constraints") or [] for c in fk["columns"]}
        pk = [c["name"] for c in t["columns"] if c.get("primary_key")]
        # the table's own column prefix (TPC: l_ for lineitem), when most of its columns carry one
        heads = [prefix(c["name"]) for c in t["columns"] if prefix(c["name"])]
        top = max(set(heads), key=heads.count) if heads else None
        own = top if top and heads.count(top) * 2 > len(t["columns"]) else None
        for c in t["columns"]:
            if c["name"] in declared or pk == [c["name"]]:   # its own one-column key isn't a reference to another
                continue
            by_short = [p for p in short.get(bare(c["name"]), []) if p != t["name"]]
            low = c["name"].lower()
            by_orm = [by_id[n] for n in sorted(_named_like(low[:-3])) if n in by_id and by_id[n] != t["name"]] \
                if low.endswith("_id") else []
            match = [p for p in full.get(c["name"], []) if p != t["name"]] or \
                (by_short if own and prefix(c["name"]) == own else []) or by_orm
            if len(set(match)) == 1:
                t.setdefault("fk_constraints", []).append({"columns": [c["name"]], "parent_table": match[0],
                                                           "inferred": True})
    multi = [(t["name"], [c["name"] for c in t["columns"] if c.get("primary_key")]) for t in tables]
    for t in tables:
        by_bare = {bare(c["name"]): c["name"] for c in t["columns"]}
        have = {tuple(fk["columns"]) for fk in t.get("fk_constraints") or []}
        for parent, pk in multi:
            cols = tuple(by_bare.get(bare(k)) for k in pk)
            if parent != t["name"] and len(pk) >= 2 and all(cols) and cols not in have:
                t.setdefault("fk_constraints", []).append({"columns": list(cols), "parent_table": parent,
                                                           "inferred": True})


def recorded_keys(spark, fq: str, full_name: str, workspace=None) -> list[str]:
    """The primary key Lakeflow Connect records on a destination table: SHOW TBLPROPERTIES when it shows the property,
    else the Unity Catalog API (`workspace`: a databricks.sdk WorkspaceClient, made here when not given), which does."""
    props = {r.key: r.value for r in _rows(spark, f"SHOW TBLPROPERTIES {fq}")}
    if KEYS_PROPERTY not in props:
        if workspace is None:
            from databricks.sdk import WorkspaceClient
            workspace = WorkspaceClient()
        props = workspace.tables.get(full_name).properties or {}
    return json.loads(props.get(KEYS_PROPERTY) or "[]")


SYSTEM_COLUMNS = ("_rescued_data",)   # plus anything starting with __ (SCD type 2's __START_AT and __END_AT)


def bronze_schema(spark, catalog: str, schema: str, workspace=None, count_rows: bool = True) -> dict:
    """Columns, keys, row counts and activity of one bronze schema; foreign keys inferred from names. count_rows=False
    skips the COUNT(*) per table (thousands of tables, or billions of rows): the relations still decide, and the size
    rules (a fact at least as large as its parents) then can't help."""
    tables, activity = [], {}
    listed = (f"SELECT table_name, table_type FROM `{catalog}`.information_schema.tables "
              f"WHERE table_schema = {spark_string(schema)}")
    kinds = {r.table_name: r.table_type for r in _rows(spark, listed)}
    # tables only: not views (no history), not a pipeline's own backing tables (__materialization_mat_<id>_<t>_1),
    # which information_schema lists and SHOW TABLES hides
    planned = [n for n, k in kinds.items() if k not in ("VIEW", "MATERIALIZED_VIEW") and not n.startswith("__")]
    for name in sorted(planned):
        fq = f"`{catalog}`.`{schema}`.`{name}`"
        pk = recorded_keys(spark, fq, f"{catalog}.{schema}.{name}", workspace)
        cols = []
        for r in _rows(spark, f"DESCRIBE TABLE {fq}"):   # the columns, up to the first blank or "# ..." section
            if not r.col_name or r.col_name.startswith("#"):
                break
            if r.col_name.startswith("__") or r.col_name in SYSTEM_COLUMNS:
                continue
            col = {"name": r.col_name, "type": canonical_type(r.data_type)}
            cols.append({**col, "primary_key": True} if r.col_name in pk else col)
        t = {"name": name, "columns": cols}
        if count_rows:
            t["row_count"] = int(_rows(spark, f"SELECT COUNT(*) AS n FROM {fq}")[0].n)
        tables.append(t)
        activity[name] = history_activity(spark, fq)
    infer_foreign_keys(tables)
    return {"tables": tables, "activity": activity, "evidence": f"bronze {catalog}.{schema}"}


# --- the plan --------------------------------------------------------------------------------------------------------

def apply_hints(p: dict, hints: dict | None) -> dict:
    """Hints win over the evidence: {table: role} or {table: {role, silver}}; decided_by: hint, the guess kept."""
    for n, h in (hints or {}).items():
        h = h if isinstance(h, dict) else {"role": h}
        t = p["tables"].get(n)
        if t is None:
            raise ValueError(f"hint for {n}: no such table")
        if h.get("role") not in roles.SILVER:
            raise ValueError(f"hint for {n}: role {h.get('role')!r} isn't one of {', '.join(roles.SILVER)}")
        silver = list(h.get("silver") or roles.SILVER[h["role"]])
        t.update(guessed=f"{t['role']} {'+'.join(t['silver'])}", role=h["role"], silver=silver, confidence="high",
                 decided_by="hint")
        t.pop("ask", None)
    return p


def table_plan(spark, catalog: str, schema: str, connection: str | None = None, database: str | None = None,
               source_schema: str | None = None, hints: dict | None = None, routes: dict | None = None,
               workspace=None, engine: str | None = None, count_rows: bool = True,
               busy_changes: int = 1000, busy_per_row: float = 2) -> dict:
    """Each table's role, silver and route, with its evidence: from the source through `connection` (database and
    source_schema name it there; MySQL: the database is the schema) when its engine has catalog queries (ENGINES;
    `engine` overrides the connection's type), else from bronze `catalog.schema`; then the hints."""
    fallback = ""
    if connection:
        engine = (engine or connection_engine(connection, workspace)).upper()
        if engine not in ENGINES:
            fallback, connection = f" (connection type {engine}: no catalog queries for it, bronze used)", None
    if connection:
        # MySQL: the database is the schema; elsewhere the source schema defaults to the bronze one's name
        source = source_schema or (database if engine == "MYSQL" and database else schema)
        s = connection_schema(spark, connection, database or source, source, engine)
        if not s["tables"]:
            raise ValueError(f"connection {connection} ({engine}): no tables in database {database or source}, "
                             f"schema {source} (check database and source_schema)")
    else:
        s = bronze_schema(spark, catalog, schema, workspace, count_rows)
    s["evidence"] += fallback
    inferred = {(t["name"], tuple(fk["columns"])) for t in s["tables"] for fk in t.get("fk_constraints") or []
                if fk.get("inferred")}
    p = roles.plan({"tables": s["tables"]}, 1, s["activity"], schema)
    for n, t in p["tables"].items():
        if n not in s["activity"]:
            t["why"] += "; no activity evidence (the source's counters show only its load)"
        elif not connection:
            t["why"] += "; activity counted after the snapshot (a lower bound)"
        fks = [f"{','.join(c)}" for (tn, c) in inferred if tn == n]
        if fks:
            t["why"] += "; foreign keys inferred from names: " + ", ".join(sorted(fks))
    apply_hints(p, hints)
    route = {**ROUTE, **(routes or {})}
    rows = {t["name"]: t.get("row_count") for t in s["tables"]}
    for n, t in p["tables"].items():
        t["route"] = route[t["role"]]
        a = s["activity"].get(n)
        if a and t["route"] != "pipeline" and t.get("decided_by") != "hint":
            # changes: updates and deletes, and inserts (the source's counters: only those beyond the rows there now,
            # the rest being its load; bronze: every insert after the snapshot)
            ins = max(0, a["inserts"] - (rows[n] or 0)) if connection else a["inserts"]
            changes = ins + a["updates"] + a["deletes"]
            if changes < busy_changes:
                continue
            if rows[n] is None:   # count_rows=False: the per-row rate can't be checked, so no override
                t["why"] += f"; {changes} changes recorded, rows not counted: busy not checked"
                continue
            per_row = changes / max(rows[n], 1)
            if per_row >= busy_per_row:
                t["route"] = "pipeline"
                t["why"] += f"; busy ({changes} changes recorded, {per_row:.1f} per row): pipeline"
    p["evidence"] = s["evidence"]
    return p
