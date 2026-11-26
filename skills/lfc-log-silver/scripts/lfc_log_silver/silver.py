"""silver: a typed silver table per source table from a change source (lfc-change-source: the shared change log a
gateway, Debezium or a changefeed lands into), SCD type 1.

A table's code path follows two things only, its silver method and where its snapshot is:

                     shared (a group)   view (at read)   merge (each round)   auto_cdc (continuous)
  snapshot in log           0                 1                  2                     3
  snapshot in place         -                 4                  5                     6

Every method reads the table's current rows the same way, the source's current_sql (the snapshot wherever it is, then
each key's last change after it), so every cell gives the same table. routing.py's classes only pick a cell (and,
for merge, whether to skip a round with no change); no class name appears here.

  shared    a group of small tables in one table, <schema>._shared_<group> (destination, k: the key the landing
            filled in, data VARIANT: the event's row), and a typed view a table over it: one MERGE a round for the
            whole group, from one kept position (a statement has a fixed floor of seconds; a warehouse runs a few a
            second). The tables share one log and one transport, and need keys in the log
  view      CREATE VIEW over the current rows: no work a round
  merge     build: CREATE TABLE from the current rows, kept at the run's position (in <schema>._log_silver);
            apply: one MERGE a table of each key's last change after its kept position; only_changed skips a table
            with no event past it (one grouped query for all)
  auto_cdc  auto_cdc_tables: the lists for pipelines/auto_cdc_from_log.py (a snapshot in place: a one-time flow)

    from lfc_log_silver import silver
    tables = {"orders": silver.Table(src, "merge"), "ledger": silver.Table(src2, "merge", only_changed=True,
              snapshot_table="`main`.`snap`.`ledger`", position=...)}
    silver.run(session, tables, "`main`.`silver`", to, first=True)    # views created, merged tables built
    silver.run(session, tables, "`main`.`silver`", to)                # a round
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace

STATE = "_log_silver"
METHODS = ("shared", "view", "merge", "auto_cdc", "append")
# routing.py's silver names onto the methods here: merge_changed is merge with only_changed
FROM_ROUTE = {"shared": ("shared", False), "view": ("view", False), "merge_round": ("merge", False),
              "merge_changed": ("merge", True), "auto_cdc": ("auto_cdc", False), "append": ("append", False)}
SHARED = "_shared_{group}"   # a group's table in the silver schema


@dataclass
class Table:
    src: object                          # an lfc_change_source source: columns, keys, changes_sql, current_sql
    method: str = "merge"                # one of METHODS
    only_changed: bool = False           # merge: skip a round with no event past the kept position
    # its snapshot read in place: a Delta table (a streaming table too) with the source's column names and types,
    # whatever took it (routing's snapshot_by: qbc's table, files converted with CONVERT TO DELTA or landed). None:
    # the snapshot is in the log. Views and MERGE read it in batch, AUTO CDC as a stream
    snapshot_table: str | None = None
    position: object = None              # that snapshot's position (a Position; None: before every change)
    group: str = ""                      # shared: its group (routing.Route.group)
    history: bool = False                # every version kept too, in <name>_history (history.py)

    def current_sql(self) -> str:
        # silver's one rule: the snapshot table and the log's changes after it
        from .combine import current_sql
        return current_sql(self.src, self.snapshot_sql, self.position)

    @property
    def snapshot_sql(self) -> str | None:
        return f"SELECT * FROM {self.snapshot_table}" if self.snapshot_table else None


def from_route(src, route, snapshot_table=None, position=None) -> Table:
    """A Table from a routing.Route: its silver name as a method here (shared: in its route's group)."""
    method, only = FROM_ROUTE[route.silver] if src.keys else ("append", False)   # keyless: its rows, appended
    return Table(src, method, only, snapshot_table, position, getattr(route, "group", "") or "",
                 bool(getattr(route, "history", False)) and bool(src.keys))


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _target(schema: str, name: str) -> str:
    return f"{schema}.{_q(name)}"


def ensure_state(session, schema: str) -> None:
    session.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{STATE} (target STRING, kind STRING, position STRING, "
                "rows BIGINT, seconds DOUBLE, updated_at TIMESTAMP, snapshot_table STRING) COMMENT 'lfc_log_silver: "
                "each silver table''s position in its change log, and the snapshot table it was built from'")
    try:   # a state table from before the snapshot_table column
        cols = [r[0] for r in session.sql(f"DESCRIBE {schema}.{STATE}").collect()]
        if cols and "snapshot_table" not in cols:
            session.sql(f"ALTER TABLE {schema}.{STATE} ADD COLUMNS (snapshot_table STRING)").collect()
    except Exception:  # noqa: BLE001  a session that can't describe it: the CREATE above has the column
        pass



def positions(session, schema: str) -> dict:
    """{target: (kind, position)} kept for each silver table."""
    return {r[0]: (r[1], r[2]) for r in session.sql(f"SELECT target, kind, position FROM {schema}.{STATE}").collect()}


def snapshots(session, schema: str) -> dict:
    """{target: the snapshot table it was built from} (None: none, its snapshot was in the log)."""
    return {r[1]: r[0] for r in session.sql(f"SELECT snapshot_table, target FROM {schema}.{STATE}").collect()}




def _save(session, schema: str, rows: list) -> None:
    """[(target, kind, position, rows, seconds, snapshot table or None)] into the state, one MERGE for the run."""
    if not rows:
        return
    snap = lambda r: _lit(r[5]) if len(r) > 5 and r[5] else "CAST(NULL AS STRING)"
    vals = ", ".join(f"({_lit(r[0])}, {_lit(r[1])}, {_lit(r[2])}, {int(r[3] or 0)}, {r[4]:.3f}, {snap(r)})" for r in rows)
    session.sql(f"MERGE INTO {schema}.{STATE} s USING (SELECT * FROM VALUES {vals} AS n(target, kind, position, rows, "
                "seconds, snapshot_table)) n ON s.target = n.target WHEN MATCHED THEN UPDATE SET s.kind = n.kind, "
                "s.position = n.position, s.rows = n.rows, s.seconds = n.seconds, s.updated_at = current_timestamp(), "
                "s.snapshot_table = n.snapshot_table WHEN NOT MATCHED THEN INSERT (target, kind, position, rows, "
                "seconds, updated_at, snapshot_table) VALUES (n.target, n.kind, n.position, n.rows, n.seconds, "
                "current_timestamp(), n.snapshot_table)")



def view_sql(t: Table, target: str) -> str:
    cols = ", ".join(_q(c) for c in t.src.columns)
    return f"CREATE OR REPLACE VIEW {target} AS SELECT {cols} FROM ({t.current_sql()})"


def build_sql(t: Table, target: str) -> str:
    cols = ", ".join(_q(c) for c in t.src.columns)
    return f"CREATE OR REPLACE TABLE {target} AS SELECT {cols} FROM ({t.current_sql()})"


def _typed_snapshot(t: Table) -> str:
    return ", ".join(f"CAST({_q(c)} AS {ty}) AS {_q(c)}" for c, ty in t.src.columns.items())


def append_build_sql(t: Table, target: str, to) -> str:
    """A keyless table: its snapshot's rows and every insert after the snapshot's position up to `to` (an update or
    delete can't name a row). An insert replayed at the same position counts once."""
    cols = ", ".join(_q(c) for c in t.src.columns)
    ins = f"SELECT DISTINCT {cols}, _seq FROM ({t.src.inserts_sql(t.position, to)})"
    snap = f"SELECT {_typed_snapshot(t)} FROM {t.snapshot_table} UNION ALL " if t.snapshot_table else ""
    return f"CREATE OR REPLACE TABLE {target} AS {snap}SELECT {cols} FROM ({ins})"


def append_sql(t: Table, target: str, after, to) -> str:
    """A round of a keyless table: the inserts after its kept position up to `to`, appended."""
    cols = ", ".join(_q(c) for c in t.src.columns)
    return (f"INSERT INTO {target} ({cols}) SELECT {cols} FROM (SELECT DISTINCT {cols}, _seq FROM "
            f"({t.src.inserts_sql(after, to)}))")


def appended(session, tables: dict, schema: str, to, threads: int = 16, first: bool = False) -> dict:
    """Each keyless table built (first) or its new inserts appended from its kept position, kept at `to`."""
    from lfc_change_source.contract import Position
    ensure_state(session, schema)
    kept = positions(session, schema)

    def after(n):
        kind, pos = kept.get(n, (to.kind, None))
        return Position(kind, pos) if pos is not None else None
    stmts = {n: (append_build_sql(t, _target(schema, n), to) if first or n not in kept or
                 renewed({n: t}, snapshots(session, schema)) else append_sql(t, _target(schema, n), after(n), to))
             for n, t in tables.items()}
    done = _each(session, stmts, threads)
    _save(session, schema, [(n, to.kind, to.value, r, s, tables[n].snapshot_table) for n, (r, s) in done.items()])
    return done


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


def views(session, tables: dict, schema: str, threads: int = 16, to=None) -> dict:
    """A view per table over its current rows, its snapshot table recorded (a different one later: recreated).
    {name: (None, seconds)}."""
    done = _each(session, {n: view_sql(t, _target(schema, n)) for n, t in tables.items()}, threads)
    if to is not None and done:
        ensure_state(session, schema)
        _save(session, schema, [(n, to.kind, to.value, 0, sec, tables[n].snapshot_table) for n, (_, sec) in done.items()])
    return done



def build(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """Each table built from its current rows, kept at `to` (read before the builds: a change after it is applied
    again by the next apply, the same result for SCD type 1). {name: (None, seconds)}."""
    ensure_state(session, schema)
    done = _each(session, {n: build_sql(t, _target(schema, n)) for n, t in tables.items()}, threads)
    _save(session, schema, [(n, to.kind, to.value, 0, s, tables[n].snapshot_table) for n, (_, s) in done.items()])
    return done


def _norm_type(t: str) -> str:
    return "".join(str(t).lower().split()).replace("integer", "int")


def silver_columns(session, schema: str) -> dict:
    """{table: {column: type}} of every table and view in the silver schema (information_schema, one query)."""
    cat, sch = [x.strip("`").replace("``", "`") for x in schema.split("`.`")]
    rows = session.sql(f"SELECT table_name, column_name, full_data_type FROM {_q(cat)}.information_schema.columns "
                       f"WHERE table_schema = {_lit(sch)}").collect()
    out: dict = {}
    for r in rows:
        out.setdefault(r[0], {})[r[1].lower()] = _norm_type(r[2])
    return out


def schema_changed(session, tables: dict, schema: str) -> list:
    """The silver tables whose columns (names or types) differ from their source's now (a table not built yet left
    out): its view is recreated, its MERGE table rebuilt from the log."""
    have = silver_columns(session, schema)
    out = []
    for n, t in tables.items():
        cur = have.get(n.lower())
        want = {c.lower(): _norm_type(ty) for c, ty in t.src.columns.items()}
        if cur is not None and cur != want:
            out.append(n)
    return out


def renewed(tables: dict, kept_snap: dict) -> list:
    """The tables kept from a snapshot table other than the one they have now (a new snapshot is a new table; a
    table built before its snapshot was ready, from the log alone, counts too): rebuilt, or its view recreated."""
    return [n for n, t in tables.items() if n in kept_snap and kept_snap[n] != t.snapshot_table]



def changed(session, tables: dict, schema: str, to) -> list:
    """The tables with a change event after their kept position, up to `to`: one grouped query a log (each table's
    newest position against its kept one), so a table on a log of its own is seen too. A table never built counts as
    changed."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    if not tables:
        return []
    kept = positions(session, schema)
    by_log: dict = {}
    for n, t in tables.items():
        by_log.setdefault((t.src.fq, t.src.seq), {})[t.src.destination] = n
    newest = {}
    for (fq, kind), by_dest in by_log.items():
        lit = (lambda v: "'" + str(v).replace("'", "") + "'") if kind in TEXT else (lambda v: str(int(v)))
        rows = session.sql(f"SELECT destination, max({SEQUENCES[kind]}) FROM {fq} WHERE operation <> 'resolved' "
                           f"AND destination IN ({', '.join(_lit(d) for d in by_dest)}) AND {SEQUENCES[kind]} <= "
                           f"{lit(to.value)} GROUP BY destination").collect()
        newest.update({by_dest[r[0]]: r[1] for r in rows})

    def order(n, v):
        return str(v) if tables[n].src.seq in TEXT else int(v)
    return [n for n in tables if n not in kept or kept[n][1] is None
            or (n in newest and order(n, newest[n]) > order(n, kept[n][1]))]


def apply(session, tables: dict, schema: str, to, threads: int = 16, changed_schema: list | None = None) -> dict:
    """Each table: one MERGE of its changes after its kept position up to `to`; a table with only_changed and no
    event past its position is skipped. {name: (rows, seconds)} for the tables merged."""
    from lfc_change_source.contract import Position
    skip_ok = {n: t for n, t in tables.items() if t.only_changed}
    todo = {n: t for n, t in tables.items() if not t.only_changed}
    todo.update({n: skip_ok[n] for n in changed(session, skip_ok, schema, to)})
    kept = positions(session, schema)
    redo = set(renewed(tables, snapshots(session, schema)))          # a new snapshot table: rebuilt from it and the log
    redo |= set(changed_schema or ()) & set(tables)                    # a schema change: rebuilt from the log too

    def after(n):
        kind, pos = kept.get(n, (to.kind, None))
        return Position(kind, pos) if pos is not None else None
    stmts = {n: build_sql(tables[n], _target(schema, n)) for n in redo}
    stmts.update({n: merge_sql(t.src, _target(schema, n), after(n), to) for n, t in todo.items() if n not in redo})
    done = _each(session, stmts, threads)
    _save(session, schema, [(n, to.kind, to.value, r, s, tables[n].snapshot_table) for n, (r, s) in done.items()])
    return done


def shared_changes_sql(members: list, after, to) -> str:
    """Every member's change events after `after` up to `to` in one scan of the log they share: destination, k (the
    key the landing filled in, as text), data (the event's row, row_sql: the same expression for every table of a
    transport), _op, _seq. A member's snapshot is its snapshot table, read by its view. No expression a table, so a group's size isn't bound by its SQL."""
    from lfc_change_source.cdc_log import EVENT, SEQUENCES
    from lfc_change_source.contract import DELETE, OP, SEQ, UPSERT
    one = members[0]
    bad = [m.destination for m in members if m.fq != one.fq or m.seq != one.seq or m.row_sql() != one.row_sql()
           or not m.keys]
    if bad:
        raise ValueError(f"shared: every table in a group needs keys and the same log, position kind and transport: "
                         f"{bad[:5]}")
    seq = SEQUENCES[one.seq]
    lo, hi = one.position_sql(after), one.position_sql(to)
    where = [f"destination IN ({', '.join(_lit(m.destination) for m in members)})", EVENT] + \
        ([f"{seq} > {lo}"] if lo is not None else []) + ([f"{seq} <= {hi}"] if hi is not None else [])
    return (f"SELECT destination, key::STRING AS k, {one.row_sql()} AS data, "
            f"CASE operation WHEN 'delete' THEN '{DELETE}' ELSE '{UPSERT}' END AS {OP}, "
            f"named_struct('v', {seq}, 'o', CASE operation WHEN 'delete' THEN 0 ELSE 1 END) AS {SEQ} "
            f"FROM {one.fq} WHERE {' AND '.join(where)}")



def missing_keys(session, members: list) -> int:
    """Members' events with no key (landed before the landing filled keys in: land.backfill_keys fills them)."""
    one = members[0]
    from lfc_change_source.cdc_log import EVENT
    r = session.sql(f"SELECT count(*) FROM {one.fq} WHERE destination IN "
                    f"({', '.join(_lit(m.destination) for m in members)}) AND {EVENT} AND key IS NULL").collect()
    return int(r[0][0]) if r else 0


def shared_view_sql(t: Table, schema: str, target: str) -> str:
    """A member's typed view: its rows in the group's table (each key's last change, typed as its source types them:
    stored_values; a delete kept as a marker), and its snapshot table's rows for the keys the group hasn't changed or
    deleted (matched by the key typed from the group's stored key: stored_key_values). The view is a table's own SQL;
    the group's table and MERGE stay the same for every member."""
    g, d = _target(schema, SHARED.format(group=t.group or "0")), _lit(t.src.destination)
    cols = ", ".join(f"{e} AS {_q(c)}" for c, e in t.src.stored_values("data").items())
    live = f"SELECT {cols} FROM {g} WHERE destination = {d} AND NOT coalesce(deleted, false)"
    if not t.snapshot_table:
        return f"CREATE OR REPLACE VIEW {target} AS {live}"
    same = " AND ".join(f"{e} <=> s.{_q(c)}" for c, e in t.src.stored_key_values("x.k").items())
    snap = (f"SELECT {', '.join(f'CAST(s.{_q(c)} AS {ty}) AS {_q(c)}' for c, ty in t.src.columns.items())} "
            f"FROM {t.snapshot_table} s WHERE NOT EXISTS "
            f"(SELECT 1 FROM {g} x WHERE x.destination = {d} AND {same})")
    return f"CREATE OR REPLACE VIEW {target} AS {live} UNION ALL {snap}"



def _groups(tables: dict) -> dict:
    out: dict = {}
    for n, t in tables.items():
        if getattr(t.src, "edges", None):   # derived changes (fk) aren't in the raw log a group reads
            raise ValueError(f"{n}: a table with foreign key actions needs a table of its own (Policy.own_table)")
        out.setdefault(t.group or "0", {})[n] = t
    return out



def _drop_tables(session, schema: str, names: list) -> None:
    """Drop those of `names` that are tables, not views (a table moved into a group: its view takes the name)."""
    if not names:
        return
    cat, sch = [x.strip("`").replace("``", "`") for x in schema.split("`.`")]
    rows = session.sql(f"SELECT table_name FROM {_q(cat)}.information_schema.tables WHERE table_schema = {_lit(sch)} "
                       f"AND table_type <> 'VIEW' AND table_name IN ({', '.join(_lit(n.lower()) for n in names)})"
                       ).collect()
    for r in rows:
        session.sql(f"DROP TABLE {_target(schema, r[0])}").collect()


def build_shared(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """Each group's table from its members' current rows up to `to`, a view per member, the group kept at `to`.
    {group: (members, seconds)}."""
    ensure_state(session, schema)
    groups = _groups(tables)

    for g, members in groups.items():
        n = missing_keys(session, [m.src for m in members.values()])
        if n:
            raise ValueError(f"shared group {g}: {n:,} events with no key: land with keys (lfc_change_source "
                             "land.gateway_keys), or fill them in (land.backfill_keys)")

    with ThreadPoolExecutor(threads) as pool:
        done = dict(pool.map(lambda kv: (kv[0], _build_group(session, schema, kv[0], kv[1], to)), groups.items()))
    _drop_tables(session, schema, list(tables))
    _member_views(session, tables, schema, to, threads)
    _save(session, schema, [(SHARED.format(group=g), to.kind, to.value, n, s) for g, (n, s) in done.items()])
    return done


def _member_views(session, tables: dict, schema: str, to, threads: int) -> None:
    """Each member's view, and its snapshot table recorded (a different one later: the view recreated, no rebuild)."""
    done = _each(session, {n: shared_view_sql(t, schema, _target(schema, n)) for n, t in tables.items()}, threads)
    _save(session, schema, [(n, to.kind, to.value, 0, s, tables[n].snapshot_table) for n, (_, s) in done.items()])


def _ensure_deleted(session, target: str) -> None:
    try:
        cols = [r[0] for r in session.sql(f"DESCRIBE {target}").collect()]
        if cols and "deleted" not in cols:
            session.sql(f"ALTER TABLE {target} ADD COLUMNS (deleted BOOLEAN)").collect()
    except Exception:  # noqa: BLE001  not built yet: the build makes the column
        pass


def _build_group(session, schema: str, g: str, members: dict, to) -> tuple:
    """A group's table from its members' changes up to `to`: each key's last change, a delete kept as a marker (it
    hides the key's snapshot row). (members, seconds)."""
    from lfc_change_source.contract import DELETE, OP, last_change_sql
    t0 = time.time()
    cur = last_change_sql(shared_changes_sql([m.src for m in members.values()], None, to), ["destination", "k"])
    session.sql(f"CREATE OR REPLACE TABLE {_target(schema, SHARED.format(group=g))} CLUSTER BY (destination) AS "
                f"SELECT destination, k, data, {OP} = '{DELETE}' AS deleted FROM ({cur})").collect()
    return len(members), time.time() - t0




def apply_shared(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """One MERGE a group: each key's last change after the group's kept position up to `to` (a delete: its marker).
    A member whose snapshot table changed gets its view recreated. {group: (rows, s)}."""
    from lfc_change_source.contract import DELETE, OP, Position, last_change_sql
    groups = _groups(tables)
    kept = positions(session, schema)
    stmts = {}
    for g in groups:   # a group built before delete markers: the column added (NULL: not deleted)
        _ensure_deleted(session, _target(schema, SHARED.format(group=g)))
    for g, members in groups.items():   # events landed without a key would never match their row: refused
        n = missing_keys(session, [m.src for m in members.values()])
        if n:
            raise ValueError(f"shared group {g}: {n:,} events with no key: land with keys (lfc_change_source "
                             "land.gateway_keys), or fill them in (land.backfill_keys)")
    for g, members in groups.items():
        kind, pos = kept.get(SHARED.format(group=g), (to.kind, None))
        after = Position(kind, pos) if pos is not None else None
        c = last_change_sql(shared_changes_sql([m.src for m in members.values()], after, to), ["destination", "k"])
        stmts[g] = (f"MERGE INTO {_target(schema, SHARED.format(group=g))} t USING ({c}) c ON t.destination = "
                    f"c.destination AND t.k = c.k WHEN MATCHED THEN UPDATE SET t.data = c.data, t.deleted = "
                    f"c.{OP} = '{DELETE}' WHEN NOT MATCHED THEN INSERT (destination, k, data, deleted) VALUES "
                    f"(c.destination, c.k, c.data, c.{OP} = '{DELETE}')")
    done = _each(session, stmts, threads)
    _save(session, schema, [(SHARED.format(group=g), to.kind, to.value, r, s) for g, (r, s) in done.items()])
    again = renewed(tables, snapshots(session, schema))
    if again:
        _member_views(session, {n: tables[n] for n in again}, schema, to, threads)
    return done




def checkpoint(session, tables: dict, schema: str, to, into: str, threads: int = 16, stamp: str | None = None) -> dict:
    """An internal resnapshot: each table's current rows as of `to` into a new snapshot table in `into` (a quoted
    catalog.schema), from silver, never the source, so a rebuild or a view starts there instead of from the first
    snapshot and every change since (and the log up to `to` can be trimmed: lfc_change_source land.trim). A MERGE
    table's copy is the table itself (a copy, not a clone: VACUUM on silver can't break it), recorded so it isn't
    rebuilt; a view's or a shared member's is its rows, and its view is recreated over the new table; an AUTO CDC
    table's is its rows, for its pipeline's next start. {name: {"snapshot_table", "kind", "position"}}."""
    stamp = stamp or time.strftime("%Y%m%d%H%M%S", time.gmtime())
    merges = {n: t for n, t in tables.items() if t.method == "merge"}
    if merges:   # each MERGE table up to `to` first: its copy is then the table as of `to`, and the trim loses nothing
        apply(session, {n: replace(t, only_changed=False) for n, t in merges.items()}, schema, to, threads)
    new = {n: f"{into}.{_q(f'{n}__ck{stamp}')}" for n in tables}
    cols = lambda t: ", ".join(_q(c) for c in t.src.columns)
    stmts = {n: (f"CREATE TABLE {new[n]} AS SELECT {cols(t)} FROM {_target(schema, n)}" if t.method == "merge" else
                 f"CREATE TABLE {new[n]} AS SELECT {cols(t)} FROM ({t.current_sql()})") for n, t in tables.items()}
    _each(session, stmts, threads)
    moved = {n: replace(t, snapshot_table=new[n], position=to) for n, t in tables.items()}
    ensure_state(session, schema)
    _save(session, schema, [(n, to.kind, to.value, 0, 0.0, new[n]) for n, t in moved.items() if t.method == "merge"])
    views(session, {n: t for n, t in moved.items() if t.method == "view"}, schema, threads, to)
    shared = {n: t for n, t in moved.items() if t.method == "shared"}
    if shared:
        _member_views(session, shared, schema, to, threads)
    return {n: {"snapshot_table": new[n], "kind": to.kind, "position": to.value} for n in tables}

def auto_cdc_tables(tables: dict, per_pipeline: int = 999) -> list:
    """The table lists for this skill's pipelines/auto_cdc_from_log.py, `per_pipeline` tables each (a pipeline takes
    at most 1,000 tables; its setup grows with its flows, paid once in continuous mode): [[{destination, target, keys,
    exprs, seq, seq_type[, snapshot]}]]; a snapshot in place (snapshot_table) adds a flow streaming it in the pipeline,
    sequenced by the same type (seq_type: the position's SQL type)."""
    from lfc_change_source.cdc_log import SEQUENCES, TEXT
    items = []
    for n, t in tables.items():
        item = {"destination": t.src.destination, "target": n, "keys": list(t.src.keys),
                "exprs": t.src.column_exprs(), "seq": SEQUENCES[t.src.seq],
                "seq_type": "STRING" if t.src.seq in TEXT else "BIGINT"}
        if t.snapshot_table:
            item["snapshot"] = {"table": t.snapshot_table, "columns": list(t.src.columns),
                                "position": "" if t.position is None else t.position.value}
        items.append(item)
    return [items[i:i + per_pipeline] for i in range(0, len(items), per_pipeline)]


DRIFT = "schema_drift"


def record_drift(session, tables: dict, schema: str, to, threads: int = 16) -> dict:
    """Each table's drift (a change source with cast TRY_CAST: lfc_change_source CdcLog.drift_sql) after its kept
    position up to `to`, into <schema>.schema_drift: one row a table, source, kind and column, with the last position
    and a sample. What an evolution resolves (a table's new schema, then its rebuild). {name: (rows, seconds)}."""
    from lfc_change_source.contract import Position
    mine = {n: t for n, t in tables.items() if getattr(t.src, "cast", "CAST") == "TRY_CAST"}
    if not mine:
        return {}
    session.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{DRIFT} (tbl STRING, _source STRING, kind STRING, col STRING, "
                "_seq STRING, sample STRING) COMMENT 'lfc_log_silver: what the change log holds that a table''s "
                "columns don''t, one row a table, source, kind and column'").collect()
    kept = positions(session, schema)

    def stmt(n, t):
        kind, pos = kept.get(n, (to.kind, None))
        after = Position(kind, pos) if pos is not None else None
        found = (f"SELECT {_lit(n)} AS tbl, {_lit(t.src.name)} AS _source, kind, col, CAST(max(seq) AS STRING) AS _seq, "
                 f"max_by(sample, seq) AS sample FROM ({t.src.drift_sql(after, to)}) GROUP BY kind, col")
        return (f"MERGE INTO {schema}.{DRIFT} d USING ({found}) f ON d.tbl = f.tbl AND d._source = f._source AND "
                "d.kind = f.kind AND d.col = f.col WHEN MATCHED THEN UPDATE SET d._seq = f._seq, d.sample = f.sample "
                "WHEN NOT MATCHED THEN INSERT *")
    return _each(session, {n: stmt(n, t) for n, t in mine.items()}, threads)


def run(session, tables: dict, schema: str, to, threads: int = 16, first: bool = False) -> dict:
    """One round for every table by its method: shared groups built (with their views) on the first round, one MERGE
    a group after it; views created on the first round (and again after a schema change); merged tables built on the
    first round, merged after it (only_changed: only those with events past their position; rebuilt from the log
    after a schema change or a newer snapshot); auto_cdc tables left to their pipelines. {method: {"tables": n, "done": n, "seconds": s}} (shared: "done" counts groups)."""
    ensure_state(session, schema)
    record_drift(session, {n: t for n, t in tables.items() if t.method != "auto_cdc"}, schema, to, threads)   # kept positions
    by = {m: {n: t for n, t in tables.items() if t.method == m} for m in METHODS}
    moved = [] if first else schema_changed(session, {n: t for n, t in tables.items() if t.method != "auto_cdc"}, schema)
    bad = [n for n, t in tables.items() if t.method not in METHODS]
    if bad:
        raise ValueError(f"tables with no method here: {bad[:5]} (one of {', '.join(METHODS)})")
    out = {}
    t0 = time.time()
    done = (build_shared if first else apply_shared)(session, by["shared"], schema, to, threads) if by["shared"] else {}
    remade = [n for n in moved if n in by["shared"]]   # a member's new columns: its view over the group, again
    if remade:
        _each(session, {n: shared_view_sql(by["shared"][n], schema, _target(schema, n)) for n in remade}, threads)
    out["shared"] = {"tables": len(by["shared"]), "groups": len(_groups(by["shared"])) if by["shared"] else 0,
                     "done": len(done), "seconds": round(time.time() - t0, 1)}
    t0 = time.time()
    if by["view"] and not first:
        moved = list(dict.fromkeys(moved + renewed(by["view"], snapshots(session, schema))))
    done = views(session, by["view"] if first else {n: by["view"][n] for n in moved if n in by["view"]}, schema,
                 threads, to) if by["view"] else {}
    out["view"] = {"tables": len(by["view"]), "done": len(done), "seconds": round(time.time() - t0, 1)}
    t0 = time.time()
    done = (build(session, by["merge"], schema, to, threads) if first else
            apply(session, by["merge"], schema, to, threads, [n for n in moved if n in by["merge"]])) if by["merge"] else {}
    out["merge"] = {"tables": len(by["merge"]), "done": len(done), "seconds": round(time.time() - t0, 1)}
    t0 = time.time()
    done = appended(session, by["append"], schema, to, threads, first) if by["append"] else {}
    out["append"] = {"tables": len(by["append"]), "done": len(done), "seconds": round(time.time() - t0, 1)}
    out["auto_cdc"] = {"tables": len(by["auto_cdc"]), "done": 0, "seconds": 0.0}
    t0 = time.time()
    hist = {n: t for n, t in tables.items() if t.history and t.method != "auto_cdc"}   # AUTO CDC keeps its own
    from . import history
    done = history.run(session, hist, schema, to, threads, first) if hist else {}
    out["history"] = {"tables": len(hist), "done": len(done), "seconds": round(time.time() - t0, 1)}
    return out
