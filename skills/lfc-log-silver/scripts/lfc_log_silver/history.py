"""history: a keyed table's every version (SCD type 2) in <table>_history: its columns, __START_AT (the version's
position) and __END_AT (its key's next point: the next version or a delete; at one position a delete first; NULL while
current), as AUTO CDC's SCD type 2 and zerobus-debezium-test's merge-silver.py keep them. Built from the snapshot's rows
(starting at the snapshot's position) and every change after; then each round one MERGE: the new versions inserted,
and the key's open version and each new one ended at the next point. Only open rows change, so history kept before a
checkpoint's trim stays. A change replayed at one position (same key, position and operation) counts once.

    from lfc_log_silver import history
    history.build_sql(table, "`<catalog>`.`silver`.`orders_history`", to)
    history.merge_sql(table, "`<catalog>`.`silver`.`orders_history`", after, to)
"""
from __future__ import annotations

SUFFIX = "_history"


def _q(n: str) -> str:
    return "`" + n.replace("`", "``") + "`"


def _points(t, events: str) -> str:
    """The key's points from events (columns, _op, _seq): p its position, ver 1 a version (one a key and position),
    0 a delete (one a key and position)."""
    from lfc_change_source.contract import DELETE, OP, SEQ
    keys = ", ".join(_q(k) for k in t.src.keys)
    cols = ", ".join(_q(c) for c in t.src.columns)
    return (f"SELECT {cols}, {SEQ}.v AS p, CASE WHEN {OP} = '{DELETE}' THEN 0 ELSE 1 END AS ver FROM ({events}) "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys}, {SEQ}.v, {OP} = '{DELETE}' ORDER BY {SEQ}.o DESC) = 1")


def _ended(t, points: str) -> str:
    """Each version with __END_AT: the next point of its key."""
    keys = ", ".join(_q(k) for k in t.src.keys)
    return (f"SELECT * FROM (SELECT *, LEAD(p) OVER (PARTITION BY {keys} ORDER BY p, ver) AS e FROM ({points})) "
            "WHERE ver = 1")


def build_sql(t, target: str, to) -> str:
    """The history from the snapshot's rows and every change after its position up to `to`."""
    from lfc_change_source.cdc_log import TEXT
    from lfc_change_source.contract import OP, SEQ, UPSERT
    cols = ", ".join(_q(c) for c in t.src.columns)
    changes = f"SELECT {cols}, {OP}, {SEQ} FROM ({t.src.changes_sql(t.position, to)})"
    events = changes
    if t.snapshot_table:
        v = (t.src.position_sql(t.position) if t.position is not None else ("''" if t.src.seq in TEXT else "-1"))
        typed = ", ".join(f"CAST({_q(c)} AS {ty}) AS {_q(c)}" for c, ty in t.src.columns.items())
        events = (f"SELECT {typed}, '{UPSERT}' AS {OP}, named_struct('v', {v}, 'o', 0) AS {SEQ} FROM "
                  f"{t.snapshot_table} UNION ALL {changes}")
    return (f"CREATE OR REPLACE TABLE {target} AS SELECT {cols}, p AS __START_AT, e AS __END_AT "
            f"FROM ({_ended(t, _points(t, events))})")


def merge_sql(t, target: str, after, to) -> str:
    """A round: the changes after `after` up to `to`, with each touched key's open version, into the history."""
    from lfc_change_source.contract import OP, SEQ, UPSERT
    keys = [_q(k) for k in t.src.keys]
    cols = ", ".join(_q(c) for c in t.src.columns)
    new = f"SELECT {cols}, {OP}, {SEQ} FROM ({t.src.changes_sql(after, to)})"
    touched = f"SELECT DISTINCT {', '.join(keys)} FROM ({new})"
    # each touched key's open version, as a version at its start (o 0: a new change at that position wins)
    opened = (f"SELECT {', '.join('h.' + _q(c) for c in t.src.columns)}, '{UPSERT}' AS {OP}, "
              f"named_struct('v', h.__START_AT, 'o', 0) AS {SEQ} FROM {target} h LEFT SEMI JOIN ({touched}) k ON "
              f"{' AND '.join(f'h.{k} <=> k.{k}' for k in keys)} WHERE h.__END_AT IS NULL")
    s = _ended(t, _points(t, f"{opened} UNION ALL {new}"))
    on = " AND ".join(f"h.{k} <=> s.{k}" for k in keys)
    return (f"MERGE INTO {target} h USING ({s}) s ON {on} AND h.__START_AT = s.p "
            "WHEN MATCHED AND NOT (h.__END_AT <=> s.e) THEN UPDATE SET h.__END_AT = s.e "
            f"WHEN NOT MATCHED THEN INSERT ({cols}, __START_AT, __END_AT) "
            f"VALUES ({', '.join('s.' + _q(c) for c in t.src.columns)}, s.p, s.e)")


def run(session, tables: dict, schema: str, to, threads: int = 16, first: bool = False, rebuild=()) -> dict:
    """Each table's history built (first, not built yet, or in `rebuild`: its columns changed) or merged from its kept
    position, kept at `to`. A new snapshot with the same columns merges: a rebuild would lose the versions before
    the snapshot once the log is trimmed. {name: (rows, seconds)}."""
    from lfc_change_source.contract import Position

    from . import silver
    kept = silver.positions(session, schema)
    stmts = {}
    for n, t in tables.items():
        target = silver._target(schema, n + SUFFIX)
        kind, pos = kept.get(n + SUFFIX, (to.kind, None))
        stmts[n + SUFFIX] = (build_sql(t, target, to) if first or pos is None or n in rebuild else
                             merge_sql(t, target, Position(kind, pos), to))
    done = silver._each(session, stmts, threads)
    silver._save(session, schema, [(n, to.kind, to.value, r, s) for n, (r, s) in done.items()])
    return done
