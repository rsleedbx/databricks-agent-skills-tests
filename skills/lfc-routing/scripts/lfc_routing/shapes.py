"""shapes: tables that repeat across a source's databases or schemas (per customer, per shard), found by their shape,
so classify decides 10 shapes instead of 10,000 tables (docs/design-intelligent.md, "Classify finds the n:1 on its
own"). A shape is a table's name and its columns (name, canonical type, in the primary key or not); the same name
with other columns is a variant, its own group, so the dashboard shows where a member deviates. A decision on a group
applies to every member (a mapping override too; revisit after use).

    groups = shapes.group({"tenant1": schema1, "tenant2": schema2, ...})     # {group: {name, shape, members, variant}}
    plan = shapes.classify_many({"tenant1": (schema1, stats1), ...})        # classify once a group
"""
from __future__ import annotations

import hashlib
import json


def shape(table: dict) -> tuple:
    """A table's shape: its columns in order (lower-case name, canonical type, in the primary key)."""
    return tuple((c["name"].lower(), c.get("type"), bool(c.get("primary_key"))) for c in table.get("columns") or [])


def group(docs: dict) -> dict:
    """{group: {"name", "shape" (a short hash), "members": [(label, table)], "variant": bool}} for {label: schema}.
    A name with one shape is its group's name; with several, the most common shape keeps the name and the others are
    <name>~<hash>, variants."""
    by: dict = {}
    for label in sorted(docs):
        for t in docs[label].get("tables") or []:
            by.setdefault(t["name"].lower(), {}).setdefault(shape(t), []).append((label, t["name"]))
    out = {}
    for name, shapes_ in sorted(by.items()):
        ranked = sorted(shapes_.items(), key=lambda kv: (-len(kv[1]), json.dumps(kv[0])))
        from collections import Counter   # named as most members spell it, ties by the spelling: stable across rounds
        spell = Counter(t for _, ms in shapes_.items() for _, t in ms)
        own = sorted(spell, key=lambda n: (-spell[n], n))[0]
        for i, (sh, members) in enumerate(ranked):
            h = hashlib.sha1(json.dumps(sh).encode()).hexdigest()[:8]
            out[own if i == 0 else f"{own}~{h}"] = {"name": own, "shape": h, "members": members, "variant": i > 0}
    return out


def representative(docs: dict, stats: dict, groups: dict) -> tuple:
    """(schema, stats) with one table a group, named by the group: the first member's columns and foreign keys
    (parents renamed to their groups), the members' rows and bytes summed."""
    where = {(label, t): g for g, d in groups.items() for label, t in d["members"]}
    tables = {(label, t["name"]): t for label, d in docs.items() for t in d.get("tables") or []}
    st = {(label, t["name"]): t for label, d in stats.items() for t in (d or {}).get("tables") or []}
    out_s, out_t = [], []
    for g, d in groups.items():
        label, name = d["members"][0]
        t = dict(tables[(label, name)], name=g)
        if t.get("fk_constraints"):
            t["fk_constraints"] = [dict(f, parent_table=where.get((label, next((n for (lb, n) in tables if lb == label and
                                   n.lower() == str(f["parent_table"]).lower()), f["parent_table"])), f["parent_table"]))
                                   for f in t["fk_constraints"]]
        out_s.append(t)
        rows = [st.get(m, {}) for m in d["members"]]
        agg = {"name": g, "row_count": sum(int(r.get("row_count") or 0) for r in rows)}
        b = [r.get("data_size_bytes") for r in rows if r.get("data_size_bytes") is not None]
        if b:
            agg["data_size_bytes"] = sum(b)
        out_t.append(agg)
    return {"version": "1.0", "tables": out_s}, {"version": "1.0", "tables": out_t}


def classify_many(sources: dict, observed: dict | None = None, decisions: dict | None = None, **kw) -> dict:
    """classify once a group: sources {label: (schema, stats)}; observed and decisions by group name (a member's
    observed rate adds to its group's). The plan has "groups" (members, variant) beside "tables" (by group)."""
    from .classify import classify
    docs = {k: v[0] for k, v in sources.items()}
    groups = group(docs)
    schema, stats = representative(docs, {k: v[1] for k, v in sources.items()}, groups)
    obs = None
    if observed:
        where = {f"{label}.{t}": g for g, d in groups.items() for label, t in d["members"]}
        rates: dict = {}
        for n, o in (observed.get("tables") or {}).items():
            g = where.get(n, n)
            rates[g] = rates.get(g, 0.0) + float(o.get("events_per_s") or 0)
        obs = {**observed, "tables": {g: {"events_per_s": r} for g, r in rates.items()}}
    plan = classify(schema, stats, observed=obs, decisions=decisions, **kw)
    plan["groups"] = {g: {"members": [f"{label}.{t}" for label, t in d["members"]], "shape": d["shape"],
                          "variant": d["variant"]} for g, d in groups.items()}
    return plan
