"""silver: a typed silver table per source table, kept current from a change source (lfc-change-source: the shared
change log a gateway, Debezium or a changefeed lands into) by one MERGE a table, SCD type 1.

  build  each silver table from its source's current rows (snapshot_sql: the snapshot and the changes so far), and
         the position it's current to, kept in <silver schema>._log_silver
  apply  each table: one MERGE of each key's last change after its kept position, up to one position read once for
         the run (a watermark every table shares), deletes included; then the new position

A table's MERGE reads only its destination's events (the log is clustered by destination). Tables run `threads` at
once; the warehouse's clusters set how many run without queueing.

    from lfc_log_silver import silver
    silver.build(session, sources, "`main`.`silver`", threads=16)    # sources: {name: an lfc_change_source source}
    silver.apply(session, sources, "`main`.`silver`", to=position)
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

STATE = "_log_silver"


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


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


def build_sql(src, target: str) -> str:
    """A silver table from the source's current rows, typed by its columns."""
    cols = ", ".join(_q(c) for c in src.columns)
    return f"CREATE OR REPLACE TABLE {target} AS SELECT {cols} FROM ({src.snapshot_sql()})"


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


def _target(schema: str, name: str) -> str:
    return f"{schema}.{_q(name)}"


def build(session, sources: dict, schema: str, to, threads: int = 16, at: dict | None = None) -> dict:
    """Every silver table from its source's current rows; each kept at position `to` (read before the builds: a change
    after it is applied again by the next apply, the same result for SCD type 1), or at its own position in `at`: a
    source whose snapshot is given (read in place, routing's direct path) is built from that snapshot alone and kept
    at the snapshot's position, so the next apply adds every change after it. {target: seconds}."""
    ensure_state(session, schema)

    def one(item):
        name, src = item
        t0 = time.time()
        session.sql(build_sql(src, _target(schema, name)))
        return name, time.time() - t0
    with ThreadPoolExecutor(threads) as pool:
        done = dict(pool.map(one, sources.items()))
    at = at or {}
    _save(session, schema, [(n, (at.get(n) or to).kind, (at.get(n) or to).value, 0, s) for n, s in done.items()])
    return done


def views(session, sources: dict, schema: str, threads: int = 16) -> dict:
    """Silver as a view per table over its current rows from the change log (snapshot_sql): no work a round, the read
    rebuilds the table (routing's small_slow: little data, few changes). {target: seconds to create}."""
    def one(item):
        name, src = item
        t0 = time.time()
        cols = ", ".join(_q(c) for c in src.columns)
        session.sql(f"CREATE OR REPLACE VIEW {_target(schema, name)} AS SELECT {cols} FROM ({src.snapshot_sql()})")
        return name, time.time() - t0
    with ThreadPoolExecutor(threads) as pool:
        return dict(pool.map(one, sources.items()))


def changed(session, sources: dict, schema: str, to) -> list:
    """The tables with a change event after their kept position, up to `to`: one grouped query over the log they share
    (each table's newest position against its kept one). A table never built counts as changed."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    if not sources:
        return []
    kept = positions(session, schema)
    one = next(iter(sources.values()))
    seq = SEQUENCES[one.seq]
    lit = (lambda v: "'" + str(v).replace("'", "") + "'") if one.seq in TEXT else (lambda v: str(int(v)))
    by_dest = {s.destination: n for n, s in sources.items()}
    rows = session.sql(f"SELECT destination, max({seq}) FROM {one.fq} WHERE operation NOT IN ('read', 'resolved') "
                       f"AND destination IN ({', '.join(_lit(d) for d in by_dest)}) AND {seq} <= {lit(to.value)} "
                       "GROUP BY destination").collect()
    newest = {by_dest[r[0]]: r[1] for r in rows}
    order = (lambda v: str(v)) if one.seq in TEXT else (lambda v: int(v))
    return [n for n in sources if n not in kept or kept[n][1] is None
            or (n in newest and order(newest[n]) > order(kept[n][1]))]


def auto_cdc_tables(sources: dict, per_pipeline: int = 999) -> list:
    """The table lists for this skill's pipelines/auto_cdc_from_log.py, `per_pipeline` tables each (a pipeline
    takes at most 1,000 tables; its setup grows with its flows, paid once in continuous mode): [[{destination, target,
    keys, exprs, seq}]]."""
    from lfc_change_source.cdc_log import SEQUENCES
    items = [{"destination": s.destination, "target": n, "keys": list(s.keys), "exprs": s.column_exprs(),
              "seq": SEQUENCES[s.seq]} for n, s in sources.items()]
    return [items[i:i + per_pipeline] for i in range(0, len(items), per_pipeline)]


def run(session, sources: dict, routes: dict, schema: str, to, threads: int = 16, first: bool = False,
        at: dict | None = None) -> dict:
    """One round of silver for every table by its route's method (lfc-change-source routing: view, merge_round,
    merge_changed, auto_cdc): views created on the first round only, merge_round tables merged, merge_changed tables
    merged only if changed, auto_cdc tables left to their pipelines (auto_cdc_tables). first: build the merged
    tables (and the views) instead of merging; at: {table: position} for tables built from a snapshot read in place
    (build). {method: {"tables": n, "merged": n, "seconds": s}}."""
    by = {}
    for n, s in sources.items():
        by.setdefault(routes[n].silver if n in routes else "merge_round", {})[n] = s
    out = {}
    t0 = time.time()
    if by.get("view") and first:
        views(session, by["view"], schema, threads)
    out["view"] = {"tables": len(by.get("view", {})), "merged": 0, "seconds": round(time.time() - t0, 1)}
    for method in ("merge_round", "merge_changed"):
        group = by.get(method, {})
        t0 = time.time()
        if first and group:
            build(session, group, schema, to, threads, at)
            todo = group
        else:
            todo = group if method == "merge_round" else {n: group[n] for n in changed(session, group, schema, to)}
            if todo:
                apply(session, todo, schema, to, threads)
        out[method] = {"tables": len(group), "merged": len(todo), "seconds": round(time.time() - t0, 1)}
    out["auto_cdc"] = {"tables": len(by.get("auto_cdc", {})), "merged": 0, "seconds": 0.0}
    return out


BATCH = "_log_silver_batch"


def stage_sql(sources: dict, schema: str, after, to) -> str:
    """One scan of the change log for the run: every source's change events after `after` (the oldest kept position)
    up to `to`, into a batch table clustered by destination, so each table's MERGE reads its own slice of a small
    table instead of the log's newest, unclustered files. The sources share one log (their fq) and one kind."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    one = next(iter(sources.values()))
    seq = SEQUENCES[one.seq]
    lit = (lambda v: "'" + str(v).replace("'", "") + "'") if one.seq in TEXT else (lambda v: str(int(v)))
    dests = ", ".join(_lit(s.destination) for s in sources.values())
    cut = ([f"{seq} > {lit(after.value)}"] if after is not None else []) + [f"{seq} <= {lit(to.value)}"]
    return (f"CREATE OR REPLACE TABLE {schema}.{BATCH} CLUSTER BY (destination) AS SELECT * FROM {one.fq} "
            f"WHERE operation NOT IN ('read', 'resolved') AND destination IN ({dests}) AND {' AND '.join(cut)}")


def apply_staged(session, sources: dict, schema: str, to, threads: int = 16) -> dict:
    """apply, with the run's changes read from the log once (stage_sql) and each table merged from that batch:
    {target: (rows, seconds)}, and "_stage" (None, the batch's seconds). A table built after the oldest kept position
    still starts at its own."""
    import dataclasses

    from lfc_change_source.contract import Position
    kept = positions(session, schema)
    olds = [kept[n][1] for n in sources if n in kept and kept[n][1] is not None]
    oldest = Position(to.kind, min(olds)) if olds and len(olds) == len(sources) else None
    t0 = time.time()
    session.sql(stage_sql(sources, schema, oldest, to))
    stage_s = time.time() - t0
    staged = {n: dataclasses.replace(s, fq=f"{schema}.{BATCH}") for n, s in sources.items()}
    done = apply(session, staged, schema, to, threads)
    done["_stage"] = (None, stage_s)
    return done


def apply(session, sources: dict, schema: str, to, threads: int = 16) -> dict:
    """Every silver table: one MERGE of its changes after its kept position up to `to`. {target: (rows, seconds)}."""
    from lfc_change_source.contract import Position
    kept = positions(session, schema)

    def one(item):
        name, src = item
        kind, pos = kept.get(name, (to.kind, None))
        after = Position(kind, pos) if pos is not None else None
        t0 = time.time()
        r = session.sql(merge_sql(src, _target(schema, name), after, to)).collect()
        n = None
        try:
            n = int(r[0].asDict().get("num_affected_rows")) if r else 0
        except Exception:  # noqa: BLE001  a session that doesn't return the counts
            pass
        return name, (n, time.time() - t0)
    with ThreadPoolExecutor(threads) as pool:
        done = dict(pool.map(one, sources.items()))
    _save(session, schema, [(n, to.kind, to.value, r, s) for n, (r, s) in done.items()])
    return done
