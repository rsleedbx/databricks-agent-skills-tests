"""silver: a typed silver table per source table from a change source (lfc-change-source: the shared change log a
gateway, Debezium or a changefeed lands into), SCD type 1.

A table's code path follows two things only, its silver method and where its snapshot is:

                     view (at read)   merge (each round)   auto_cdc (continuous)
  snapshot in log         1                  2                     3
  snapshot in place       4                  5                     6

Every method reads the table's current rows the same way, the source's current_sql (the snapshot wherever it is, then
each key's last change after it), so every cell gives the same table. routing.py's classes only pick a cell (and,
for merge, whether to skip a round with no change); no class name appears here.

  view      CREATE VIEW over the current rows: no work a round
  merge     build: CREATE TABLE from the current rows, kept at the run's position (in <schema>._log_silver);
            apply: one MERGE a table of each key's last change after its kept position; only_changed skips a table
            with no event past it (one grouped query for all)
  auto_cdc  auto_cdc_tables: the lists for pipelines/auto_cdc_from_log.py (a snapshot in place: a one-time flow)

    from lfc_log_silver import silver
    tables = {"orders": silver.Table(src, "merge"), "ledger": silver.Table(src2, "merge", only_changed=True,
              snapshot_sql=..., position=..., snapshot_files={"path": ..., "exprs": [...]})}
    silver.run(session, tables, "`main`.`silver`", to, first=True)    # views created, merged tables built
    silver.run(session, tables, "`main`.`silver`", to)                # a round
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

STATE = "_log_silver"
METHODS = ("view", "merge", "auto_cdc")
# routing.py's silver names onto the methods here: merge_changed is merge with only_changed
FROM_ROUTE = {"view": ("view", False), "merge_round": ("merge", False), "merge_changed": ("merge", True),
              "auto_cdc": ("auto_cdc", False)}


@dataclass
class Table:
    src: object                          # an lfc_change_source source: columns, keys, changes_sql, current_sql
    method: str = "merge"                # one of METHODS
    only_changed: bool = False           # merge: skip a round with no event past the kept position
    snapshot_sql: str | None = None      # its snapshot read in place, as SQL (None: the snapshot is in the log)
    position: object = None              # that snapshot's position (a Position; None: before every change)
    snapshot_files: dict | None = None   # auto_cdc: {"path", "exprs"} of that snapshot, for the pipeline's batch read

    def current_sql(self) -> str:
        return self.src.current_sql(self.snapshot_sql, self.position)


def from_route(src, route, snapshot_sql=None, position=None, snapshot_files=None) -> Table:
    """A Table from a routing.Route: its silver name as a method here."""
    method, only = FROM_ROUTE[route.silver]
    return Table(src, method, only, snapshot_sql, position, snapshot_files)


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _target(schema: str, name: str) -> str:
    return f"{schema}.{_q(name)}"


def ensure_state(session, schema: str) -> None:
    session.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{STATE} (target STRING, kind STRING, position STRING, "
                "rows BIGINT, seconds DOUBLE, updated_at TIMESTAMP) COMMENT 'lfc_log_silver: each silver table''s "
                "position in its change source'")


def positions(session, schema: str) -> dict:
    """{target: (kind, position)} kept for each silver table."""
    return {r[0]: (r[1], r[2]) for r in session.sql(f"SELECT target, kind, position FROM {schema}.{STATE}").collect()}


def _save(session, schema: str, rows: list) -> None:
    """[(target, kind, position, rows, seconds)] into the state, one MERGE for the run."""
    if not rows:
        return
    vals = ", ".join(f"({_lit(t)}, {_lit(k)}, {_lit(p)}, {int(n or 0)}, {s:.3f})" for t, k, p, n, s in rows)
    session.sql(f"MERGE INTO {schema}.{STATE} s USING (SELECT * FROM VALUES {vals} AS n(target, kind, position, rows, "
                "seconds)) n ON s.target = n.target WHEN MATCHED THEN UPDATE SET s.kind = n.kind, s.position = "
                "n.position, s.rows = n.rows, s.seconds = n.seconds, s.updated_at = current_timestamp() WHEN NOT "
                "MATCHED THEN INSERT (target, kind, position, rows, seconds, updated_at) VALUES (n.target, n.kind, "
                "n.position, n.rows, n.seconds, current_timestamp())")


def view_sql(t: Table, target: str) -> str:
    cols = ", ".join(_q(c) for c in t.src.columns)
    return f"CREATE OR REPLACE VIEW {target} AS SELECT {cols} FROM ({t.current_sql()})"


def build_sql(t: Table, target: str) -> str:
    cols = ", ".join(_q(c) for c in t.src.columns)
    return f"CREATE OR REPLACE TABLE {target} AS SELECT {cols} FROM ({t.current_sql()})"


def merge_sql(src, target: str, after, to) -> str:
    """Each key's last change after `after` up to `to` into the silver table: deletes, then updates and inserts."""
    from lfc_change_source.contract import DELETE, OP, last_change_sql
    cols = list(src.columns)
    on = " AND ".join(f"t.{_q(k)} <=> c.{_q(k)}" for k in src.keys)
    sets = ", ".join(f"t.{_q(c)} = c.{_q(c)}" for c in cols if c not in src.keys)
    names = ", ".join(_q(c) for c in cols)
    vals = ", ".join(f"c.{_q(c)}" for c in cols)
    return (f"MERGE INTO {target} t USING ({last_change_sql(src.changes_sql(after, to), src.keys)}) c ON {on} "
            f"WHEN MATCHED AND c.{OP} = '{DELETE}' THEN DELETE "
            + (f"WHEN MATCHED THEN UPDATE SET {sets} " if sets else "")
            + f"WHEN NOT MATCHED AND c.{OP} <> '{DELETE}' THEN INSERT ({names}) VALUES ({vals})")


def _each(session, statements: dict, threads: int) -> dict:
    """{name: statement} run `threads` at once: {name: (affected rows or None, seconds)}."""
    def one(item):
        name, sql = item
        t0 = time.time()
        r = session.sql(sql).collect()
        n = None
        try:
            n = int(r[0].asDict().get("num_affected_rows")) if r else 0
        except Exception:  # noqa: BLE001  a statement or session that returns no counts
            pass
        return name, (n, time.time() - t0)
    with ThreadPoolExecutor(threads) as pool:
        return dict(pool.map(one, statements.items()))


def views(session, tables: dict, schema: str, threads: int = 16) -> dict:
    """A view per table over its current rows. {name: (None, seconds)}."""
    return _each(session, {n: view_sql(t, _target(schema, n)) for n, t in tables.items()}, threads)


def build(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """Each table built from its current rows, kept at `to` (read before the builds: a change after it is applied
    again by the next apply, the same result for SCD type 1). {name: (None, seconds)}."""
    ensure_state(session, schema)
    done = _each(session, {n: build_sql(t, _target(schema, n)) for n, t in tables.items()}, threads)
    _save(session, schema, [(n, to.kind, to.value, 0, s) for n, (_, s) in done.items()])
    return done


def changed(session, tables: dict, schema: str, to) -> list:
    """The tables with a change event after their kept position, up to `to`: one grouped query over the log they share
    (each table's newest position against its kept one). A table never built counts as changed."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    if not tables:
        return []
    kept = positions(session, schema)
    one = next(iter(tables.values())).src
    seq = SEQUENCES[one.seq]
    lit = (lambda v: "'" + str(v).replace("'", "") + "'") if one.seq in TEXT else (lambda v: str(int(v)))
    by_dest = {t.src.destination: n for n, t in tables.items()}
    rows = session.sql(f"SELECT destination, max({seq}) FROM {one.fq} WHERE operation NOT IN ('read', 'resolved') "
                       f"AND destination IN ({', '.join(_lit(d) for d in by_dest)}) AND {seq} <= {lit(to.value)} "
                       "GROUP BY destination").collect()
    newest = {by_dest[r[0]]: r[1] for r in rows}
    order = (lambda v: str(v)) if one.seq in TEXT else (lambda v: int(v))
    return [n for n in tables if n not in kept or kept[n][1] is None
            or (n in newest and order(newest[n]) > order(kept[n][1]))]


def apply(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """Each table: one MERGE of its changes after its kept position up to `to`; a table with only_changed and no
    event past its position is skipped. {name: (rows, seconds)} for the tables merged."""
    from lfc_change_source.contract import Position
    skip_ok = {n: t for n, t in tables.items() if t.only_changed}
    todo = {n: t for n, t in tables.items() if not t.only_changed}
    todo.update({n: skip_ok[n] for n in changed(session, skip_ok, schema, to)})
    kept = positions(session, schema)

    def after(n):
        kind, pos = kept.get(n, (to.kind, None))
        return Position(kind, pos) if pos is not None else None
    done = _each(session, {n: merge_sql(t.src, _target(schema, n), after(n), to) for n, t in todo.items()}, threads)
    _save(session, schema, [(n, to.kind, to.value, r, s) for n, (r, s) in done.items()])
    return done


def auto_cdc_tables(tables: dict, per_pipeline: int = 999) -> list:
    """The table lists for this skill's pipelines/auto_cdc_from_log.py, `per_pipeline` tables each (a pipeline takes
    at most 1,000 tables; its setup grows with its flows, paid once in continuous mode): [[{destination, target, keys,
    exprs, seq, seq_type[, snapshot]}]]; a snapshot in place (snapshot_files) adds a flow reading it in the pipeline,
    sequenced by the same type (seq_type: the position's SQL type)."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    items = []
    for n, t in tables.items():
        item = {"destination": t.src.destination, "target": n, "keys": list(t.src.keys),
                "exprs": t.src.column_exprs(), "seq": SEQUENCES[t.src.seq],
                "seq_type": "STRING" if t.src.seq in TEXT else "BIGINT"}
        if t.snapshot_files:
            item["snapshot"] = {**t.snapshot_files, "position": "" if t.position is None else t.position.value}
        items.append(item)
    return [items[i:i + per_pipeline] for i in range(0, len(items), per_pipeline)]


def run(session, tables: dict, schema: str, to, threads: int = 16, first: bool = False) -> dict:
    """One round for every table by its method: views created on the first round only; merged tables built on the
    first round, merged after it (only_changed: only those with events past their position); auto_cdc tables left to
    their pipelines. {method: {"tables": n, "done": n, "seconds": s}}."""
    by = {m: {n: t for n, t in tables.items() if t.method == m} for m in METHODS}
    bad = [n for n, t in tables.items() if t.method not in METHODS]
    if bad:
        raise ValueError(f"tables with no method here: {bad[:5]} (one of {', '.join(METHODS)})")
    out = {}
    t0 = time.time()
    done = views(session, by["view"], schema, threads) if first and by["view"] else {}
    out["view"] = {"tables": len(by["view"]), "done": len(done), "seconds": round(time.time() - t0, 1)}
    t0 = time.time()
    done = (build if first else apply)(session, by["merge"], schema, to, threads) if by["merge"] else {}
    out["merge"] = {"tables": len(by["merge"]), "done": len(done), "seconds": round(time.time() - t0, 1)}
    out["auto_cdc"] = {"tables": len(by["auto_cdc"]), "done": 0, "seconds": 0.0}
    return out
