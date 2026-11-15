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
current without putting each one in the pipeline's graph (ROUTE; `routes` overrides it per role).
"""
import json

from . import discover, roles
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


def pg_string(s: str) -> str:
    """A PostgreSQL string literal ('' is a quote), for the query remote_query runs on the source."""
    return "'" + str(s).replace("'", "''") + "'"


# --- 1. the source, through a Unity Catalog connection (PostgreSQL) ------------------------------------------------

def _remote(spark, connection, database, query):
    return _rows(spark, f"SELECT * FROM remote_query({spark_string(connection)}, database => {spark_string(database)}, "
                        f"query => {spark_string(query)})")


def connection_schema(spark, connection: str, database: str, schema: str) -> dict:
    """Columns, keys, foreign keys, row counts and activity of a PostgreSQL schema, read through the connection."""
    s = pg_string(schema)
    cols = _remote(spark, connection, database,
                   "SELECT table_name, column_name, data_type FROM information_schema.columns "
                   f"WHERE table_schema = {s} ORDER BY table_name, ordinal_position")
    # pg_catalog, not information_schema: table_constraints lists only tables the user owns or holds more than SELECT
    # on, and the connection's login usually has SELECT only
    keys = _remote(spark, connection, database,
                   "SELECT t.relname AS table_name, CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'FOREIGN KEY' END "
                   "AS constraint_type, c.conname AS constraint_name, a.attname AS column_name, "
                   "pt.relname AS parent_table FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
                   "JOIN pg_namespace n ON n.oid = t.relnamespace "
                   "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) "
                   "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum "
                   "LEFT JOIN pg_class pt ON pt.oid = c.confrelid "
                   f"WHERE n.nspname = {s} AND c.contype IN ('p', 'f') ORDER BY t.relname, c.conname, k.ord")
    stats = _remote(spark, connection, database, "SELECT relname, n_live_tup, n_tup_ins, n_tup_upd, n_tup_del "
                    f"FROM pg_stat_user_tables WHERE schemaname = {s}")
    tables = {}
    for r in cols:
        tables.setdefault(r.table_name, {"name": r.table_name, "columns": []})["columns"].append(
            {"name": r.column_name, "type": canonical_type(r.data_type)})
    fks = {}
    for r in keys:
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
    activity = {}
    for r in stats:
        if r.relname in tables:
            tables[r.relname]["row_count"] = int(r.n_live_tup or 0)
            activity[r.relname] = {"inserts": int(r.n_tup_ins or 0), "updates": int(r.n_tup_upd or 0),
                                   "deletes": int(r.n_tup_del or 0)}
    return {"tables": list(tables.values()), "activity": activity, "evidence": f"connection {connection}"}


# --- 2. bronze, as Lakeflow Connect wrote it ------------------------------------------------------------------------

KEYS_PROPERTY = "spark.internal.streaming_table.apply_changes.keys"


def history_activity(spark, table: str) -> dict:
    """Inserts, updates and deletes in the table's MERGEs after the first one that inserted rows (the snapshot)."""
    total, seen_snapshot = {"inserts": 0, "updates": 0, "deletes": 0}, False
    history = f"SELECT version, operation, operationMetrics FROM (DESCRIBE HISTORY {table}) ORDER BY version"
    for r in _rows(spark, history):
        if r.operation != "MERGE":
            continue
        m = r.operationMetrics or {}
        m = json.loads(m) if isinstance(m, str) else m
        ins = int(m.get("numTargetRowsInserted") or 0)
        if not seen_snapshot:
            seen_snapshot = ins > 0
            continue
        total["inserts"] += ins
        total["updates"] += int(m.get("numTargetRowsMatchedUpdated") or m.get("numTargetRowsUpdated") or 0)
        total["deletes"] += int(m.get("numTargetRowsMatchedDeleted") or m.get("numTargetRowsDeleted") or 0)
    return total


def infer_foreign_keys(tables: list[dict]) -> None:
    """Add a foreign key where a column's name, after its table's prefix, is another table's one-column key (TPC:
    l_suppkey -> s_suppkey), or is that key's own name (ORM: orders.customer_id -> customer.customer_id); and where a
    table has columns for every column of another's key of two or more (l_partkey, l_suppkey -> partsupp's key)."""
    def bare(col):
        return col.split("_", 1)[1] if "_" in col else col
    keys = {}
    for t in tables:
        pk = [c["name"] for c in t["columns"] if c.get("primary_key")]
        if len(pk) == 1:
            keys.setdefault(bare(pk[0]), []).append((t["name"], pk[0]))
            keys.setdefault(pk[0], []).append((t["name"], pk[0]))
    for t in tables:
        declared = {c for fk in t.get("fk_constraints") or [] for c in fk["columns"]}
        for c in t["columns"]:
            if c["name"] in declared:
                continue
            match = [(p, k) for p, k in keys.get(c["name"], []) + keys.get(bare(c["name"]), []) if p != t["name"]]
            if len({p for p, _ in match}) == 1:
                t.setdefault("fk_constraints", []).append({"columns": [c["name"]], "parent_table": match[0][0],
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


def bronze_schema(spark, catalog: str, schema: str, workspace=None) -> dict:
    """Columns, keys, row counts and activity of one bronze schema; foreign keys inferred from names."""
    tables, activity = [], {}
    for name in sorted(discover.tables(spark, catalog, "", [schema])[schema]):
        fq = f"`{catalog}`.`{schema}`.`{name}`"
        pk = recorded_keys(spark, fq, f"{catalog}.{schema}.{name}", workspace)
        cols = []
        for r in _rows(spark, f"DESCRIBE TABLE {fq}"):   # the columns, up to the first blank or "# ..." section
            if not r.col_name or r.col_name.startswith("#"):
                break
            col = {"name": r.col_name, "type": canonical_type(r.data_type)}
            cols.append({**col, "primary_key": True} if r.col_name in pk else col)
        n = _rows(spark, f"SELECT COUNT(*) AS n FROM {fq}")[0].n
        tables.append({"name": name, "columns": cols, "row_count": int(n)})
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
               workspace=None) -> dict:
    """Each table's role, silver and route, with its evidence: from the source through `connection` (database and
    source_schema name it there) when given, else from bronze `catalog.schema`; then the hints."""
    s = connection_schema(spark, connection, database, source_schema or schema) if connection else \
        bronze_schema(spark, catalog, schema, workspace)
    inferred = {(t["name"], tuple(fk["columns"])) for t in s["tables"] for fk in t.get("fk_constraints") or []
                if fk.get("inferred")}
    p = roles.plan({"tables": s["tables"]}, 1, s["activity"], schema)
    for n, t in p["tables"].items():
        fks = [f"{','.join(c)}" for (tn, c) in inferred if tn == n]
        if fks:
            t["why"] += "; foreign keys inferred from names: " + ", ".join(sorted(fks))
    apply_hints(p, hints)
    route = {**ROUTE, **(routes or {})}
    for t in p["tables"].values():
        t["route"] = route[t["role"]]
    p["evidence"] = s["evidence"]
    return p
