"""log_silver: a typed silver table per source table, kept current from a change source (lfc-change-source: the shared
change log a gateway, Debezium or a changefeed lands into) by one MERGE a table, SCD type 1.

  build  each silver table from its source's current rows (snapshot_sql: the snapshot and the changes so far), and
         the position it's current to, kept in <silver schema>._log_silver
  apply  each table: one MERGE of each key's last change after its kept position, up to one position read once for
         the run (a watermark every table shares), deletes included; then the new position

A table's MERGE reads only its destination's events (the log is clustered by destination). Tables run `threads` at
once; the warehouse's clusters set how many run without queueing.

    from lfc_medallion import log_silver
    log_silver.build(session, sources, "`main`.`silver`", threads=16)    # sources: {name: an lfc_change_source source}
    log_silver.apply(session, sources, "`main`.`silver`", to=position)
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
                "rows BIGINT, seconds DOUBLE, updated_at TIMESTAMP) COMMENT 'log_silver: each silver table''s "
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


def build(session, sources: dict, schema: str, to, threads: int = 16) -> dict:
    """Every silver table from its source's current rows; each kept at position `to` (read before the builds: a change
    after it is applied again by the next apply, the same result for SCD type 1). {target: seconds}."""
    ensure_state(session, schema)

    def one(item):
        name, src = item
        t0 = time.time()
        session.sql(build_sql(src, _target(schema, name)))
        return name, time.time() - t0
    with ThreadPoolExecutor(threads) as pool:
        done = dict(pool.map(one, sources.items()))
    _save(session, schema, [(n, to.kind, to.value, 0, s) for n, s in done.items()])
    return done


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
