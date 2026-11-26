"""flowthrough: one FlowThrough round over a change log (docs/design.md): each table measured, routed, moved when its
route changed, and its silver kept, so the code follows the design's routing instead of a method chosen by hand.

  measure  each table's change rate from the log (its events since the last round over the time since), its size
           from the caller (the transport's adapter knows it: a gateway's snapshot bytes, a Volume's files); the first
           round has no rate yet, so every table starts slow and the next rounds move the busy ones
  plan     lfc-routing routing.plan on those numbers (classes, shared groups, the rules that take a small
           table out of its group), compared with the last round's plan kept in <schema>._flowthrough
  move     only the tables whose method or group changed: to a view (its table dropped), to a MERGE (built from the
           log, its view dropped), into a group (the group rebuilt), out of one (its rows deleted from the group's
           table), to AUTO CDC (its own table or view dropped, and its pipeline list marked to recreate)
  keep     lfc-log-silver silver.run for every table; the AUTO CDC lists returned for the caller's pipelines

Before a plan is pinned, observe() keeps every table a view while the snapshot is taken (CDC first, so its rates are
measured meanwhile), records the route the plan would give each (`proposed`, in <schema>._flowthrough, what the
dashboard shows), and recreates a table's view over its snapshot once that is ready; step() with the pinned policy
then moves each view to its route.

    from lfc_log_silver import flowthrough
    got = flowthrough.step(session, "`main`.`silver`", sources, to, policy, sizes={"orders": 5 << 40})   # a round

Every move rebuilds from the log; nothing reads the source again. A move's description is routing.transitions'.
"""
from __future__ import annotations

import json
import time
from dataclasses import replace

from . import silver

STATE = "_flowthrough"


def ensure_state(session, schema: str) -> None:
    session.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{STATE} (name STRING, route STRING, events BIGINT, at DOUBLE) "
                "COMMENT 'lfc_log_silver flowthrough: each table''s route and event count at the last round'")


def load(session, schema: str) -> dict:
    """{name: (route, events, at)} from the last round."""
    return {r[0]: (json.loads(r[1]), int(r[2] or 0), float(r[3] or 0)) for r in
            session.sql(f"SELECT name, route, events, at FROM {schema}.{STATE}").collect()}


def save(session, schema: str, routes: dict, counts: dict, at: float) -> None:
    if not routes:
        return
    vals = ", ".join(f"({silver._lit(n)}, {silver._lit(json.dumps(r))}, {int(counts.get(n, 0))}, {at:.3f})"
                     for n, r in routes.items())
    session.sql(f"MERGE INTO {schema}.{STATE} s USING (SELECT * FROM VALUES {vals} AS n(name, route, events, at)) n "
                "ON s.name = n.name WHEN MATCHED THEN UPDATE SET s.route = n.route, s.events = n.events, s.at = n.at "
                "WHEN NOT MATCHED THEN INSERT (name, route, events, at) VALUES (n.name, n.route, n.events, n.at)")


def counts(session, sources: dict) -> dict:
    """{name: its change events in its log} (resolved marks left out), one grouped query a log."""
    by_log: dict = {}
    for n, s in sources.items():
        by_log.setdefault(s.fq, {})[s.destination] = n
    out = {}
    for fq, dests in by_log.items():
        rows = session.sql(f"SELECT destination, count(*) FROM {fq} WHERE operation <> 'resolved' AND "
                           f"destination IN ({', '.join(silver._lit(d) for d in dests)}) GROUP BY destination").collect()
        out.update({dests[r[0]]: int(r[1]) for r in rows})
    return out


def route_dict(r) -> dict:
    return {"class": r.cls, "log": r.log, "shards": r.shards, "silver": r.silver,
            "group": r.group, "why": r.why,
            "snapshot_by": r.snapshot_by}


def _drop(session, schema: str, names: list, kind: str) -> None:
    """The VIEWs (kind) or TABLEs of `names` dropped; an object of the other kind stays."""
    silver._drop_kind(session, schema, names, view=kind == "VIEW")


def measure(session, schema: str, sources: dict, now: float | None = None) -> tuple:
    """(now, the last round's state, each table's events in its log, each table's events a second since the last
    round): the first round has no rate yet (0)."""
    ensure_state(session, schema)
    now = now or time.time()
    prev = load(session, schema)
    got = counts(session, sources)
    rates = {n: max(0, got.get(n, 0) - prev[n][1]) / max(now - prev[n][2], 1e-9) if n in prev else 0.0
             for n in sources}
    return now, prev, got, rates


def observe(session, schema: str, sources: dict, policy, ready: dict | None = None, sizes: dict | None = None,
            reads_per_day: dict | None = None, threads: int = 16, now: float | None = None) -> dict:
    """While the snapshot is taken and the plan isn't pinned: every table a view (no stored table, no MERGE), its rate
    measured, and the route the plan would give it recorded as `proposed` for the customer to check. ready: {name: its
    snapshot table} for each snapshot that is complete; a table's view is created on the first round over the log alone
    (the rows changed so far), and recreated once over its snapshot when that becomes ready (a metadata change). Then
    step() with the pinned policy moves each view to its route, from its snapshot and the log. Returns the proposal
    by method, the rates, and the views made this round."""
    from lfc_routing import routing
    ready = ready or {}
    silver.ensure_state(session, schema)   # empty until step() stores a table: the dashboard reads it from the start
    now, prev, got, rates = measure(session, schema, sources, now)
    plan = routing.plan({n: ((sizes or {}).get(n, 0), rates[n]) for n in sources}, policy, reads_per_day)
    # a table already pinned (step() gave it its route) is left as it is: its objects and its saved route
    pinned = {n for n in sources if n in prev and prev[n][0].get("why") != "observing"}
    # a keyless table has no route to decide (always appended, and a view can't pick a key's last change): appended
    # from the start, once its snapshot is ready, and pinned as append
    keyless = {n: silver.Table(s, "append", snapshot_table=ready[n]) for n, s in sources.items()
               if not s.keys and n not in pinned and n in ready}
    for n, t in list(keyless.items()):   # up to its own latest event; none yet: left for a later round
        w = t.src.watermark(session)
        if w.value in (None, "None"):
            del keyless[n]
            continue
        silver.appended(session, {n: t}, schema, w, threads, first=True)
    made = {n: silver.Table(s, "view", snapshot_table=ready.get(n)) for n, s in sources.items()
            if s.keys and n not in pinned and (n not in prev or prev[n][0].get("snapshot_table") != ready.get(n))}
    if made:   # recorded with their snapshot table, so a later one recreates them (silver.renewed)
        done = silver.views(session, made, schema, threads)
        silver._save(session, schema, [(n, "observe", "", 0, s, made[n].snapshot_table) for n, (_, s) in done.items()])
    routes = {n: {"class": plan[n].cls, "log": plan[n].log, "shards": plan[n].shards,
                  "silver": "view", "group": "", "why": "observing", "snapshot_table": ready.get(n),
                  "destination": sources[n].destination,
                  "proposed": route_dict(plan[n])} for n in sources if n not in pinned and sources[n].keys}
    routes.update({n: {**route_dict(plan[n]), "silver": "append", "method": "append", "why": "keyless: appended",
                       "snapshot_table": ready[n], "destination": sources[n].destination} for n in keyless})
    save(session, schema, routes, got, now)
    by_method: dict = {}
    for r in plan.values():
        by_method[r.silver] = by_method.get(r.silver, 0) + 1
    return {"proposed": by_method, "rates": {n: float(f"{r:.4g}") for n, r in rates.items() if r},
            "views": sorted(made), "snapshots_ready": len(ready), "tables": len(sources), "pinned": len(pinned)}


def step(session, schema: str, sources: dict, to, policy, sizes: dict | None = None, reads_per_day: dict | None = None,
          extra: dict | None = None, threads: int = 16, now: float | None = None) -> dict:
    """One round (module doc). sources: {name: an lfc_change_source source}; sizes: {name: snapshot bytes};
    extra: {name: {snapshot_table, position}} for a snapshot read in place (a Delta table). Returns the routes by
    method, the moves with what each took, silver.run's result and the AUTO CDC lists (changed: recreate them)."""
    from lfc_routing import routing
    now, prev, got, rates = measure(session, schema, sources, now)
    routes = routing.plan({n: ((sizes or {}).get(n, 0), rates[n]) for n in sources}, policy, reads_per_day)
    tables = {n: silver.from_route(s, routes[n], **snapshot_of(prev, n, extra)) for n, s in sources.items()}
    first = not prev
    moves, held, fresh = [], [], []
    if not first:
        for n in tables:
            old = prev.get(n, (None,))[0]
            new = route_dict(routes[n])
            if tables[n].method == "append":   # keyless: appended whatever its route says, never moved after
                if (old or {}).get("method") != "append":   # its first round as append: what was there goes
                    fresh.append(n)
                continue
            if old is None or (old.get("silver"), old.get("group")) != (new["silver"], new["group"]):
                if (old or {}).get("silver") == "auto_cdc":   # its streaming table belongs to its pipeline: held
                    held.append({"table": n, "to": new["silver"], "why": "leaving AUTO CDC: remove it from its "
                                 "pipeline's list (auto_cdc lists) and let the pipeline drop it first"})
                    routes[n] = replace(routes[n], silver="auto_cdc", group="")
                    tables[n] = silver.from_route(sources[n], routes[n], **snapshot_of(prev, n, extra))
                    continue
                moves.append((n, old, new))
        _move(session, schema, tables, moves, prev, to, threads)
        _to_append(session, schema, {n: tables[n] for n in fresh}, prev, to, threads)
    ran = silver.run(session, tables, schema, to, threads, first=first)
    save(session, schema, {n: {**route_dict(r), "method": tables[n].method, "snapshot_table": tables[n].snapshot_table,
                               "destination": sources[n].destination,
                               **({"checkpoint": prev[n][0]["checkpoint"]} if prev.get(n, ({},))[0].get("checkpoint") else {})}
                           for n, r in routes.items()},
         got, now)   # snapshot_table, destination: where the dashboard's links go; a checkpoint stays
    by_method: dict = {}
    for r in routes.values():
        by_method[r.silver] = by_method.get(r.silver, 0) + 1
    auto = {n: t for n, t in tables.items() if t.method == "auto_cdc"}
    return {"routes": by_method, "rates": {n: float(f"{r:.4g}") for n, r in rates.items() if r},
            "moves": [{"table": n, "from": (o or {}).get("silver"), "to": w["silver"], "why": w.get("why") or "",
                       "takes": [x for _, x in routing.transitions(o or {}, w)]} for n, o, w in moves],
            "held": held, "silver": ran, "auto_cdc": silver.auto_cdc_tables(auto) if auto else [],
            "auto_cdc_changed": any(w["silver"] == "auto_cdc" or (o or {}).get("silver") == "auto_cdc" for _, o, w in moves)}



def snapshot_of(prev: dict, name: str, extra: dict | None = None) -> dict:
    """A table's snapshot table and position: its checkpoint, else the caller's (extra), else the one recorded last
    round (observe's, or the last step's)."""
    rec = prev.get(name, ({},))[0].get("snapshot_table")
    return {**({"snapshot_table": rec} if rec else {}), **(extra or {}).get(name, {}), **checkpointed(prev, name)}


def checkpointed(prev: dict, name: str) -> dict:
    """A table's checkpoint from the last round's state, as from_route's snapshot_table and position ({}: none)."""
    from lfc_change_source.contract import Position
    ck = prev.get(name, ({},))[0].get("checkpoint")
    return {"snapshot_table": ck["snapshot_table"], "position": Position(ck["kind"], ck["position"])} if ck else {}


def checkpoint(session, schema: str, sources: dict, to, into: str, names: list | None = None, trim: bool = True,
               threads: int = 16) -> dict:
    """An internal resnapshot of `names` (default: every table) at `to` (silver.checkpoint), recorded in
    <schema>._flowthrough so every later round starts from it, then (trim) the log's changes up to `to` for those
    tables deleted (lfc_change_source land.trim; their silver keeps a position at or after `to`). No source read.
    {"tables": n, "seconds": s, "trimmed": rows}."""
    from lfc_change_source import land
    t0 = time.time()
    prev = load(session, schema)
    # a keyless (append) table isn't checkpointed: its rows are its log's inserts, which the trim would delete
    names = [n for n in (names or sources) if n in prev and sources[n].keys]
    tables = {}
    for n in names:
        r = prev[n][0]
        method, only = silver.method_of(sources[n], r["silver"])
        tables[n] = silver.Table(sources[n], method, only, group=r.get("group") or "", **snapshot_of(prev, n))
    got = silver.checkpoint(session, tables, schema, to, into, threads)
    save(session, schema, {n: {**prev[n][0], "snapshot_table": got[n]["snapshot_table"], "checkpoint": got[n]}
                           for n in got}, {n: prev[n][1] for n in got}, prev[names[0]][2] if names else time.time())
    trimmed = 0
    if trim:
        by_log: dict = {}
        for n in got:
            by_log.setdefault(sources[n].fq, []).append(sources[n].destination)
        for fq, dests in by_log.items():
            trimmed += land.trim(session, fq, dests, to.kind, to.value) or 0
    if trim:   # the counts after the trim, so the next round's rate isn't read as zero
        after = counts(session, {n: sources[n] for n in got})
        save(session, schema, {n: {**prev[n][0], "snapshot_table": got[n]["snapshot_table"], "checkpoint": got[n]}
                               for n in got}, after, prev[names[0]][2] if names else time.time())
    return {"tables": len(got), "seconds": round(time.time() - t0, 1), "trimmed": trimmed}

def _to_append(session, schema: str, tables: dict, prev: dict, to, threads: int) -> None:
    """Keyless tables on their first round as append (after observe's view, or a route's object from before): the
    view or table of the name dropped, its rows out of a group it was in, then built from its snapshot and the log."""
    if not tables:
        return
    for n, t in tables.items():
        old = prev.get(n, ({},))[0]
        if old.get("silver") == "shared":
            g = silver._target(schema, silver.SHARED.format(group=old.get("group") or "0"))
            session.sql(f"DELETE FROM {g} WHERE destination = {silver._lit(t.src.destination)}").collect()
    _drop(session, schema, list(tables), "VIEW")
    silver._drop_tables(session, schema, list(tables))
    silver.appended(session, tables, schema, to, threads, first=True)


def _move(session, schema: str, tables: dict, moves: list, prev: dict, to, threads: int) -> None:
    """Each moved table into its new method, from the log: the other methods' objects of that name dropped, rows
    leaving a group deleted from it, a group a table joins rebuilt."""
    if not moves:
        return
    by_new: dict = {}
    for n, old, new in moves:
        by_new.setdefault(new["silver"], []).append(n)
        if old and old.get("silver") == "shared" and new["silver"] != "shared":   # out of its group
            g = silver._target(schema, silver.SHARED.format(group=old.get("group") or "0"))
            session.sql(f"DELETE FROM {g} WHERE destination = {silver._lit(tables[n].src.destination)}").collect()
    views = by_new.get("view", [])
    merges = by_new.get("merge_round", []) + by_new.get("merge_changed", [])
    autos = by_new.get("auto_cdc", [])
    joined = {tables[n].group or "0" for n in by_new.get("shared", [])}
    silver._drop_tables(session, schema, views)   # a view takes the name: a table of it goes
    _drop(session, schema, merges, "VIEW")         # a table takes the name: a view of it goes
    for n in autos:   # the pipeline makes its own streaming table: whatever had the name goes
        _drop(session, schema, [n], "VIEW")
    silver._drop_tables(session, schema, autos)
    if views:
        silver.views(session, {n: tables[n] for n in views}, schema, threads, to)
    if merges:
        silver.build(session, {n: tables[n] for n in merges}, schema, to, threads)
    for g in sorted(joined):   # a group a table joined: rebuilt with all its members
        members = {n: t for n, t in tables.items() if t.method == "shared" and (t.group or "0") == g}
        silver.build_shared(session, members, schema, to, threads)
