"""checksum: are the rows the same in the source, in bronze and in n:1? A row count and a digest per table per LOB,
computed where the rows are, so only one row per table crosses the network.

A row's digest is the first 8 hex digits of MD5 over its columns as canonical text, as a whole number (0 .. 2^32-1);
a table's checksum is COUNT(*) and the SUM of its rows' digests: the same in any order and on any engine that has MD5
(PostgreSQL md5, MySQL MD5, SQL Server HASHBYTES('MD5'), Oracle STANDARD_HASH, Databricks md5; an XOR aggregate isn't
on all of them). The sum stays inside 64 bits up to 2^31 rows. The canonical text of a column comes from its type in
the schema YAML (RENDER: one expression per type and engine), so every engine writes the same characters:

    integer types   the number                    decimal(p, s)   the number at scale s
    real types      the number at scale 6         char(n)         without trailing spaces
    date            days since 1970-01-01         timestamp       microseconds since 1970-01-01 (UTC)
    boolean         1 or 0                        NULL            chr(1) (the same character on both)
Columns are joined with | in the schema's order; columns a layer adds (the source column, Lakeflow Connect's own)
aren't in it. Nothing here names a model or a table.
"""
from __future__ import annotations


def _kind(c: dict) -> tuple[str, int]:
    t = str(c.get("type", "")).lower()
    if t in ("integer", "int", "bigint", "smallint", "tinyint"):
        return "int", 0
    if t in ("decimal", "numeric"):
        return "dec", int(c.get("scale") or 0)
    if t in ("double", "float", "real"):
        return "dec", 6
    if t in ("char", "nchar"):
        return "char", 0
    if t == "date":
        return "date", 0
    if t in ("timestamptz", "timestamp with time zone", "datetimeoffset"):
        return "tstz", 0   # an instant: its own epoch, not through the session's time zone
    if t in ("timestamp", "datetime", "timestamp without time zone"):
        return "ts", 0
    if t in ("boolean", "bool", "bit"):
        return "bool", 0
    return "text", 0


# per engine: one column's canonical text ({c}: the quoted column, {s}: a scale), the row's digest of a text ({t})
RENDER = {
    "postgres": {
        "int": "({c})::text", "dec": "CAST({c} AS numeric(38,{s}))::text", "char": "rtrim(({c})::text)",
        "date": "(({c}) - DATE '1970-01-01')::text",
        "ts": "(EXTRACT(EPOCH FROM ({c})::timestamp) * 1000000)::bigint::text",   # a wall time, taken as UTC
        "tstz": "(EXTRACT(EPOCH FROM ({c})::timestamptz) * 1000000)::bigint::text",   # an instant
        "bool": "CASE WHEN {c} THEN '1' ELSE '0' END", "text": "({c})::text",
        "null": "COALESCE({x}, chr(1))", "join": " || '|' || ",
        "digest": "('x' || substr(md5({t}), 1, 8))::bit(32)::bigint", "q": '"{n}"',
        "sum": "COALESCE(SUM({d}), 0)::bigint"},   # SUM of bigint is numeric in PostgreSQL
    "databricks": {
        "int": "CAST({c} AS STRING)", "dec": "CAST(CAST({c} AS DECIMAL(38,{s})) AS STRING)",
        "char": "rtrim(CAST({c} AS STRING))", "date": "CAST(unix_date(CAST({c} AS DATE)) AS STRING)",
        "ts": "CAST(unix_micros(CAST({c} AS TIMESTAMP)) AS STRING)",
        "tstz": "CAST(unix_micros(CAST({c} AS TIMESTAMP)) AS STRING)",
        "bool": "CASE WHEN {c} THEN '1' ELSE '0' END", "text": "CAST({c} AS STRING)",
        "null": "COALESCE({x}, chr(1))", "join": " || '|' || ",
        "digest": "CAST(conv(substr(md5({t}), 1, 8), 16, 10) AS BIGINT)", "q": "`{n}`",
        "sum": "COALESCE(SUM({d}), 0)"},
}


def row_text(columns: list[dict], engine: str) -> str:
    r = RENDER[engine]
    parts = []
    for c in columns:
        k, s = _kind(c)
        x = r[k].format(c=r["q"].format(n=c["name"]), s=s)
        parts.append(r["null"].format(x=x))
    return r["join"].join(parts)


def query(columns: list[dict], engine: str, relation: str, where: str = "", label: str | None = None,
          by: str | None = None) -> str:
    """SELECT rows, digest FROM relation [WHERE ...]: the table's count and digest sum on that engine. label: a
    constant first column `g` (one part of a UNION ALL over many tables); by: a column to group by, as `g` (every
    source of an n:1 table in one query)."""
    r = RENDER[engine]
    d = r["digest"].format(t=f"({row_text(columns, engine)})")
    g = f"'{label}' AS g, " if label is not None else f"{by} AS g, " if by else ""
    return (f"SELECT {g}COUNT(*) AS n_rows, {r['sum'].format(d=d)} AS digest FROM {relation}"
            + (f" WHERE {where}" if where else "") + (" GROUP BY 1" if by else ""))


def model_columns(model: str) -> dict[str, list[dict]]:
    """Each table's columns (name, type, scale) from the model's schema YAML, in its order."""
    import yaml
    from lfc_source_datagen.sources import model_schema
    return {t["name"]: [{"name": c["name"], "type": c.get("type"), "scale": c.get("scale")} for c in t["columns"]]
            for t in yaml.safe_load(model_schema(model, 1))["tables"]}


def _q(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run(spark, plan: dict, module: str, catalog: str, prefix: str, connection: str, database: str,
        lobs: list[str] | None = None) -> list[dict]:
    """Per table per LOB: rows and digest in the source (through the connection's remote_query, so the source
    computes them), in bronze, and in n:1; status `match` when all three agree, else which layers differ. The
    Spark session runs in UTC meanwhile: a wall time (TIMESTAMP_NTZ) cast to TIMESTAMP reads as UTC, as
    PostgreSQL's EXTRACT(EPOCH) takes a timestamp without time zone; the session's own zone is put back after."""
    key = "spark.sql.session.timeZone"
    was = spark.conf.get(key)
    spark.conf.set(key, "UTC")
    try:
        return _run(spark, plan, module, catalog, prefix, connection, database, lobs)
    finally:
        spark.conf.set(key, was)


def _run(spark, plan, module, catalog, prefix, connection, database, lobs, threads: int = 16) -> list[dict]:
    """The checksums in few round trips: per table one remote_query over every LOB (source) and one GROUP BY over the
    n:1 table, and each LOB's bronze table on a thread (16 at once): not three queries one after another per table
    and LOB, which grows with LOBs times tables."""
    m = next(x for x in plan["modules"] if x["module"] == module)
    cols = model_columns(m["model"])
    col, silver = m["source_column"], f"`{catalog}`.`{prefix}_{module}_silver`"
    route = {}   # each table's route, from the merge task's plan (lfc-medallion): merge or pipeline
    if spark.sql(f"SHOW TABLES IN {silver} LIKE '_lfc_medallion_plan'").collect():
        route = {r.table_name: r.route for r in spark.sql(
            f"SELECT DISTINCT table_name, route FROM {silver}._lfc_medallion_plan").collect()}
    xs = [x for x in m["lobs"] if not lobs or x["name"] in lobs]   # names: lower-case letters, digits and _
    got: dict = {}   # (lob, table) -> {layer: (rows, digest)}
    for t in m["tables"]:   # the source: every LOB's table in one remote_query (one round trip, PostgreSQL runs it)
        q = " UNION ALL ".join(query(cols[t], "postgres", f'"{x["source_schema"]}"."{t}"', label=x["name"]) for x in xs)
        for r in spark.sql("SELECT * FROM remote_query(:c, database => :d, query => :q)",
                           args={"c": connection, "d": database, "q": q}).collect():
            got.setdefault((r.g, t), {})["source"] = (int(r.n_rows), int(r.digest))
        # n:1: every LOB of the table in one query, grouped by the source column
        for r in spark.sql(query(cols[t], "databricks", f"{silver}.`all_{t}`", by=f"`{col}`")).collect():
            got.setdefault((r.g, t), {})["n1"] = (int(r.n_rows), int(r.digest))

    def bronze(xt):   # each LOB's bronze table is a table of its own: planned on its own thread
        x, t = xt
        r = spark.sql(query(cols[t], "databricks", f"`{catalog}`.`{prefix}_{module}_bronze_{x['name']}`.`{t}`")).collect()[0]
        return x["name"], t, (int(r.n_rows), int(r.digest))
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for name, t, v in ex.map(bronze, [(x, t) for x in xs for t in m["tables"]]):
            got.setdefault((name, t), {})["bronze"] = v
    out = []
    for x in xs:
        for t in m["tables"]:
            g = got.get((x["name"], t), {})
            g = {k: g.get(k, (0, 0)) for k in ("source", "bronze", "n1")}   # none: no rows in that layer
            off = [k for k in ("bronze", "n1") if g[k] != g["source"]]
            out.append({"module": module, "lob": x["name"], "table": t, "route": route.get(t, "pipeline"),
                        "source_rows": g["source"][0],
                        "bronze_rows": g["bronze"][0], "n1_rows": g["n1"][0], "source_digest": g["source"][1],
                        "bronze_digest": g["bronze"][1], "n1_digest": g["n1"][1],
                        "status": "match" if not off else "differs: " + ", ".join(
                            f"{k} {'rows' if g[k][0] != g['source'][0] else 'contents'}" for k in off)})
    return out


def save(spark, rows: list[dict], catalog: str, schema: str, table: str = "checksums") -> None:
    """Append the rows, with the time, to <catalog>.<schema>.<table> (the control schema's), for the dashboard."""
    if not rows:
        return
    df = spark.createDataFrame(rows).selectExpr("current_timestamp() AS checked_at", "*")
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"`{catalog}`.`{schema}`.`{table}`")

