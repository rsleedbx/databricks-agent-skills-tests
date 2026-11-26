"""classify: routing's second box (docs/design-intelligent.md, "Two boxes"). Plain data in, a plan out, no database
and no workspace, so a round is replayed and tested from its files:

  schema     the canonical schema (lfc-source-statschema): tables, columns, keys, foreign keys, row counts
  stats      statschema's stats.yaml: each table's data_size_bytes, its columns' avg_width_bytes (optional)
  activity   each table's inserts, updates, deletes since the statistics were reset, over activity_seconds (optional)
  observed   what the change log showed (flowthrough.measure): {"round": n, "tables": {table: {"events_per_s": r}}};
             a table's observed rate replaces the activity's (optional)
  decisions  what a person chose: {"tables": {table: {role, silver, class, method, snapshot_by, own_table, history}},
             "policy": {Policy field: value}} (optional)

The plan: {version, model, round, tables: {table: {role, silver, confidence, decided_by, why, keys, key, route}}}.
`keys` are the columns a change names its row by: the primary key, else every column (`key: all_columns`), as a
query-based connector and a gateway do for a table with no key. `route` is lfc_routing.routing's suggestion (class,
snapshot_by, log, shards, silver method, group, why).

    from lfc_routing.classify import classify
    plan = classify(schema, stats=stats, observed={"round": 3, "tables": {"orders": {"events_per_s": 900}}})
"""
from __future__ import annotations

from lfc_medallion.roles import SILVER
from lfc_medallion.roles import plan as roles_plan

from . import routing

# a decision's field -> the Policy dict it goes into (by table)
CHOICES = {"class": "chosen", "method": "silver_chosen", "snapshot_by": "snapshot_chosen", "own_table": "own_table"}


def table_bytes(stats: dict | None, schema: dict, sf: float = 1) -> dict:
    """{table: bytes or None}: stats.yaml's data_size_bytes, else row_count x the columns' avg_width_bytes."""
    by = {t["name"]: t for t in (stats or {}).get("tables") or []}
    out = {}
    for t in schema.get("tables") or []:
        s = by.get(t["name"]) or {}
        rows = t.get("row_count") or (t.get("row_count_per_sf") or 0) * sf or s.get("row_count")
        width = sum(c.get("avg_width_bytes") or 0 for c in s.get("columns") or [])
        out[t["name"]] = s.get("data_size_bytes") or (int(rows * width) if rows and width else None)
    return out


def rates(activity: dict | None, seconds: float | None, observed: dict | None) -> dict:
    """{table: change events a second}: observed in the log if it was, else the activity's over its seconds."""
    out = {}
    for n, a in (activity or {}).items():
        if isinstance(a, dict) and seconds:
            out[n] = sum(a.get(k) or 0 for k in ("inserts", "updates", "deletes")) / seconds
    for n, o in ((observed or {}).get("tables") or {}).items():
        if o.get("events_per_s") is not None:
            out[n] = float(o["events_per_s"])
    return out


def keys(table: dict) -> tuple:
    """(columns, "primary_key" or "all_columns"): what a change names its row by."""
    pk = [c["name"] for c in table["columns"] if c.get("primary_key")]
    return (pk, "primary_key") if pk else ([c["name"] for c in table["columns"]], "all_columns")


def policy(decisions: dict | None, shared_log: str = "shared") -> routing.Policy:
    """A routing.Policy with the decisions' per-table choices and policy fields in."""
    d = decisions or {}
    p = routing.Policy(shared_log=shared_log, **(d.get("policy") or {}))
    hist = set(p.history) if p.history != "all" else None
    for n, c in (d.get("tables") or {}).items():
        for field_, attr in CHOICES.items():
            if c.get(field_) is not None:
                getattr(p, attr)[n] = c[field_]
        if c.get("history") and hist is not None:
            hist.add(n)
    if hist is not None:
        p.history = tuple(sorted(hist))
    return p


def review(plan: dict, roles: dict, cite: str) -> list:
    """Roles a person (or the docs) gave, applied to the plan: {table: role or {role, silver, cite}}. The problems."""
    bad = []
    for n, r in (roles or {}).items():
        r = r if isinstance(r, dict) else {"role": r}
        t = plan["tables"].get(n)
        if t is None:
            bad.append(f"review: no table {n} in the schema")
            continue
        if r.get("role") not in SILVER:
            bad.append(f"review: {n}: role {r.get('role')!r} isn't one of {', '.join(SILVER)}; skipped")
            continue
        silver = list(r.get("silver") or SILVER[r["role"]])
        if (t["role"], t["silver"]) != (r["role"], silver):
            t.update(guessed=f"{t['role']} {'+'.join(t['silver'])}", role=r["role"], silver=silver, confidence="high",
                     decided_by="docs", cite=r.get("cite") or cite)
            t.pop("ask", None)
    return bad


def classify(schema: dict, stats: dict | None = None, activity: dict | None = None, observed: dict | None = None,
             decisions: dict | None = None, activity_seconds: float | None = None, sf: float = 1, model: str = "",
             shared_log: str = "shared", with_keys: bool = True) -> dict:
    """The plan (module doc) from the box's inputs. The schema's own role: lines are never read (the guess is scored
    against them); a role to apply goes in decisions["tables"][t]["role"]."""
    blind = {**schema, "tables": [{k: v for k, v in t.items() if k != "role"} for t in schema.get("tables") or []]}
    p = roles_plan(blind, sf, activity, model)
    size, rate = table_bytes(stats, blind, sf), rates(activity, activity_seconds, observed)
    pol = policy(decisions, shared_log)
    for n, r in routing.suggest({n: {"bytes": size.get(n), "events_per_s": rate.get(n)} for n in size}, pol).items():
        if n in p["tables"]:
            p["tables"][n]["route"] = r
    if with_keys:
        for t in blind["tables"]:
            if t["name"] in p["tables"]:
                cols, kind = keys(t)
                p["tables"][t["name"]].update(keys=cols, key=kind)
    picked = {n: {k: c[k] for k in ("role", "silver", "cite") if k in c}
              for n, c in ((decisions or {}).get("tables") or {}).items() if c.get("role")}
    p["problems"] = review(p, picked, "decisions")
    if observed and observed.get("round") is not None:
        p["round"] = observed["round"]
    return p
