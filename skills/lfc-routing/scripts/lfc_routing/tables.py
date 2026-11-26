"""tables: a person in routing's loop through three tables (docs/design-intelligent.md, "A person in the loop"):

  routing_plan       classify's plan, a row a table a round: the heuristic role, class, cell, snapshot method, log
  routing_decisions  a row a table: override_* columns, NULL meaning "take the heuristic", set by a person in the
                     dashboard's editable table (an app later)
  routing_iteration  a row a round a person asks for (n, by, at, note): a job's table update trigger on it runs a round

Plain conversions between those rows and classify's documents, and the tables' DDL; running the SQL is the caller's.

    decisions = tables.decisions(rows)          # routing_decisions rows -> classify's decisions
    rows = tables.plan_rows(plan, round_=3)     # classify's plan -> routing_plan rows
"""
from __future__ import annotations

# override column -> classify's decisions field: role (the role, its silver from it), class (small_slow ...), cell
# (the silver cell: view, shared, merge_round, merge_changed, auto_cdc), snapshot_by, log (a log of its own: a split)
OVERRIDES = {"override_role": "role", "override_class": "class", "override_cell": "method",
             "override_snapshot_by": "snapshot_by", "override_log": "log"}
PLAN_COLUMNS = ("round", "tbl", "role", "class", "cell", "snapshot_by", "log", "keys", "key_kind", "why")


def ddl(schema: str) -> list:
    """CREATE TABLE IF NOT EXISTS for the three tables in `schema` (a quoted catalog.schema); the iteration table keeps
    its change data feed, for the history of who asked for which round."""
    return [f"CREATE TABLE IF NOT EXISTS {schema}.routing_plan (round INT, tbl STRING, role STRING, class STRING, "
            "cell STRING, snapshot_by STRING, log STRING, `keys` ARRAY<STRING>, key_kind STRING, why STRING) "
            "COMMENT 'lfc_routing: classify''s plan, a row a table a round'",
            f"CREATE TABLE IF NOT EXISTS {schema}.routing_decisions (tbl STRING, "
            + ", ".join(f"{c} STRING" for c in OVERRIDES)
            + ", note STRING, `by` STRING, at TIMESTAMP) COMMENT 'lfc_routing: a person''s overrides, NULL: the heuristic'",
            f"CREATE TABLE IF NOT EXISTS {schema}.routing_iteration (n INT, `by` STRING, at TIMESTAMP, note STRING) "
            "TBLPROPERTIES (delta.enableChangeDataFeed = true) "
            "COMMENT 'lfc_routing: add a row (n + 1) when the decisions are done: the next round runs'"]


def decisions(rows: list) -> dict:
    """classify's decisions from routing_decisions rows (dicts or Rows): each non-null override; the last row of a
    table wins, a column at a time."""
    out: dict = {}
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        got = {f: d[c] for c, f in OVERRIDES.items() if d.get(c) not in (None, "")}
        if got:
            out.setdefault(d["tbl"], {}).update(got)
    return {"tables": out}


def plan_rows(plan: dict, round_: int | None = None) -> list:
    """routing_plan rows (dicts, PLAN_COLUMNS) from classify's plan."""
    n = round_ if round_ is not None else plan.get("round")
    rows = []
    for t, p in sorted(plan["tables"].items()):
        r = p.get("route") or {}
        rows.append({"round": n, "tbl": t, "role": p.get("role"), "class": r.get("class"), "cell": r.get("silver"),
                     "snapshot_by": r.get("snapshot_by"), "log": r.get("log"), "keys": p.get("keys"),
                     "key_kind": p.get("key"), "why": "; ".join(x for x in (p.get("why"), r.get("why")) if x)})
    return rows


def latest_round(rows: list) -> int:
    """The round a person asked for last (routing_iteration's largest n; 0 when it has none)."""
    ns = [int((r.asDict() if hasattr(r, "asDict") else dict(r))["n"]) for r in rows]
    return max(ns) if ns else 0
