"""history: introspection on demand, kept over time (docs/design-intelligent.md, "The steps", step 1). A person asks for
a run from the dashboard by adding a row to introspection_request; a job with a table update trigger on it runs
run_pending(), which introspects each source asked for and appends the run. Nothing is overwritten:

  introspection_request  a row a request: n, source (its connection), engine, database, src_schema, by, at, note
  introspection_runs     a row a run: run (the request's n), source, at, status, error, tables, changes (what differs
                         from that source's previous run), schema_json and stats_json (the documents classify reads)
  introspection_schema   a row a column a run: run, source, tbl, ordinal, col, type, native_type, not_null, pk
  introspection_stats    a row a table a run: run, source, tbl, row_count, data_size_bytes

    from lfc_introspection import history
    for q in history.ddl("`c`.`s`"): session.sql(q)
    history.run_pending(session, "`c`.`s`")            # what the job runs
    schema, stats = history.latest(session, "`c`.`s`", "lfcddemo-azurevm-pg")
"""
from __future__ import annotations

import json

REQUEST, RUNS, SCHEMA, STATS = "introspection_request", "introspection_runs", "introspection_schema", "introspection_stats"


def _lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def ddl(schema: str) -> list:
    """CREATE TABLE IF NOT EXISTS for the four tables in `schema` (a quoted catalog.schema)."""
    return [f"CREATE TABLE IF NOT EXISTS {schema}.{REQUEST} (n INT, source STRING, engine STRING, database STRING, "
            "src_schema STRING, `by` STRING, at TIMESTAMP, note STRING) TBLPROPERTIES (delta.enableChangeDataFeed = true) "
            "COMMENT 'lfc_introspection: add a row (n + 1, the source) to run an introspection'",
            f"CREATE TABLE IF NOT EXISTS {schema}.{RUNS} (run INT, source STRING, at TIMESTAMP, status STRING, "
            "error STRING, tables INT, changes STRING, schema_json STRING, stats_json STRING) "
            "COMMENT 'lfc_introspection: each run, its documents and what changed since the source''s previous run'",
            f"CREATE TABLE IF NOT EXISTS {schema}.{SCHEMA} (run INT, source STRING, tbl STRING, ordinal INT, col STRING, "
            "type STRING, native_type STRING, not_null BOOLEAN, pk BOOLEAN) COMMENT 'lfc_introspection: columns, by run'",
            f"CREATE TABLE IF NOT EXISTS {schema}.{STATS} (run INT, source STRING, tbl STRING, row_count BIGINT, "
            "data_size_bytes BIGINT) COMMENT 'lfc_introspection: rows and bytes, by run'"]


def rows(schema_doc: dict, stats_doc: dict) -> tuple:
    """(column rows, table rows) for introspection_schema and introspection_stats, without run and source."""
    cols = [{"tbl": t["name"], "ordinal": i + 1, "col": c["name"], "type": c.get("type"),
             "native_type": c.get("native_type"), "not_null": bool(c.get("not_null")), "pk": bool(c.get("primary_key"))}
            for t in schema_doc.get("tables") or [] for i, c in enumerate(t["columns"])]
    stats = [{"tbl": t["name"], "row_count": t.get("row_count"), "data_size_bytes": t.get("data_size_bytes")}
             for t in stats_doc.get("tables") or []]
    return cols, stats


def changes(old: tuple | None, new: tuple, grew: float = 0.5) -> list:
    """What differs between two runs' (schema, stats): tables and columns added or dropped, a column's type or key
    changed, a table's rows changed by more than `grew` (a fraction). [] when nothing did, or for a first run."""
    if old is None:
        return []
    out = []
    o = {t["name"]: t for t in old[0].get("tables") or []}
    n = {t["name"]: t for t in new[0].get("tables") or []}
    out += [f"table added: {t}" for t in sorted(set(n) - set(o))] + [f"table dropped: {t}" for t in sorted(set(o) - set(n))]
    for t in sorted(set(o) & set(n)):
        oc = {c["name"]: c for c in o[t]["columns"]}
        nc = {c["name"]: c for c in n[t]["columns"]}
        out += [f"{t}: column added: {c}" for c in nc if c not in oc] + [f"{t}: column dropped: {c}" for c in oc if c not in nc]
        for c in [c for c in nc if c in oc]:
            a, b = oc[c], nc[c]
            if (a.get("native_type"), a.get("type")) != (b.get("native_type"), b.get("type")):
                out.append(f"{t}.{c}: type {a.get('native_type') or a.get('type')} -> {b.get('native_type') or b.get('type')}")
            if bool(a.get("primary_key")) != bool(b.get("primary_key")):
                out.append(f"{t}.{c}: {'now' if b.get('primary_key') else 'no longer'} in the primary key")
    os_ = {t["name"]: t.get("row_count") or 0 for t in old[1].get("tables") or []}
    for t in new[1].get("tables") or []:
        a, b = os_.get(t["name"]), t.get("row_count") or 0
        if a is not None and max(a, b) and abs(b - a) / max(a, 1) > grew:
            out.append(f"{t['name']}: rows {a:,} -> {b:,}")
    return out


def pending(requests: list, runs: list) -> list:
    """The requests with no run yet (a run's number is its request's n), oldest first."""
    done = {(r["source"], int(r["run"])) for r in runs}
    return sorted((r for r in requests if (r["source"], int(r["n"])) not in done), key=lambda r: int(r["n"]))


def _dicts(rs) -> list:
    return [r.asDict() if hasattr(r, "asDict") else dict(r) for r in rs]


def latest(session, schema: str, source: str, run: int | None = None) -> tuple | None:
    """(schema, stats) of a source's latest successful run (or of `run`); None when it has none."""
    where = f"source = {_lit(source)} AND status = 'ok'" + (f" AND run = {int(run)}" if run is not None else "")
    got = _dicts(session.sql(f"SELECT schema_json, stats_json FROM {schema}.{RUNS} WHERE {where} "
                             "ORDER BY run DESC LIMIT 1").collect())
    return (json.loads(got[0]["schema_json"]), json.loads(got[0]["stats_json"])) if got else None


def record(session, schema: str, req: dict, docs: tuple | None, error: str | None = None) -> dict:
    """A run appended: its row in introspection_runs (with what changed since the source's previous run), its columns
    and its tables. Returns the run's row (without the documents)."""
    run, src = int(req["n"]), req["source"]
    prev = latest(session, schema, src)
    ch = changes(prev, docs) if docs else []
    row = {"run": run, "source": src, "status": "ok" if docs else "failed", "error": error,
           "tables": len(docs[0].get("tables") or []) if docs else None, "changes": ch}
    vals = [run, src, None, row["status"], error, row["tables"], json.dumps(ch),
            json.dumps(docs[0]) if docs else None, json.dumps(docs[1]) if docs else None]
    session.sql(f"INSERT INTO {schema}.{RUNS} VALUES ({', '.join('current_timestamp()' if i == 2 else _lit(v) for i, v in enumerate(vals))})").collect()
    if docs:
        cols, stats = rows(*docs)
        for table, rs, keys in ((SCHEMA, cols, ("tbl", "ordinal", "col", "type", "native_type", "not_null", "pk")),
                                (STATS, stats, ("tbl", "row_count", "data_size_bytes"))):
            for i in range(0, len(rs), 500):
                chunk = rs[i:i + 500]
                if chunk:
                    session.sql(f"INSERT INTO {schema}.{table} VALUES " + ", ".join(
                        "(" + ", ".join([str(run), _lit(src)] + [_lit(r[k]) for k in keys]) + ")" for r in chunk)).collect()
    return row


def run_pending(session, schema: str, introspect=None, via: str = "jdbc") -> list:
    """Every pending request run and recorded (a failure is a run too, with its error). introspect: the function
    that reads a source (lfc_introspection.introspect.introspect; another for a test). The runs' rows."""
    if introspect is None:
        from .introspect import introspect
    reqs = _dicts(session.sql(f"SELECT * FROM {schema}.{REQUEST}").collect())
    runs = _dicts(session.sql(f"SELECT run, source FROM {schema}.{RUNS}").collect())
    out = []
    for r in pending(reqs, runs):
        try:
            docs = introspect(session, r["source"], r["engine"], r.get("database"), r.get("src_schema") or "public",
                              via=via)
            out.append(record(session, schema, r, docs))
        except Exception as e:  # noqa: BLE001  the run is recorded as failed, with why
            out.append(record(session, schema, r, None, f"{type(e).__name__}: {str(e)[:500]}"))
    return out
