"""tables: a person in routing's loop through three tables (docs/design-intelligent.md, "A person in the loop"):

  routing_plan       classify's plan, a row a table a round: the heuristic role, class, cell, snapshot method, log
  routing_decisions  a row a table: override_* columns, NULL meaning "take the heuristic", set by a person (Genie or
                     the assistant writes the UPDATE from words; each column's comment lists its values)
  routing_iteration  a row a round a person asks for (n, by, at, note): a job's table update trigger on it runs a round

Plain conversions between those rows and classify's documents, and the tables' DDL; running the SQL is the caller's.

    decisions = tables.decisions(rows)          # routing_decisions rows -> classify's decisions
    rows = tables.plan_rows(plan, round_=3)     # classify's plan -> routing_plan rows
"""
from __future__ import annotations

import json

# override column -> classify's decisions field: role (the role, its silver from it), class (small_slow ...), cell
# (the silver cell: view, shared, merge_round, merge_changed, auto_cdc), snapshot_by, log (a log of its own: a split)
OVERRIDES = {"override_role": "role", "override_class": "class", "override_cell": "method",
             "override_snapshot_by": "snapshot_by", "override_log": "log"}
# what each override takes, and what each value does: the comment on its column, so a person or an assistant (Genie)
# can suggest a value as well as write it. NULL in every column: take the plan's (routing_plan) choice
COMMENTS = {
    "override_role": "The table's role, which picks how silver keeps its rows. NULL: the plan's. "
                     "dimension: a described thing others point at (customer); current rows, and its history if asked. "
                     "lookup: small reference data (country codes); current rows. "
                     "bridge: links two tables (a key of two foreign keys); current rows. "
                     "fact: business events with several parents (orders); current rows. "
                     "log: rows only added, never changed (audit, clicks); appended. "
                     "queue: rows added and removed, rarely changed (jobs to do); current rows.",
    "override_class": "Its size and change rate, which pick its log and silver cell. NULL: the plan's (measured). "
                      "small_slow: small, few changes (most tables). small_fast: small, many changes. "
                      "big_slow: big (over 1 GB), few changes. big_fast: big and busy; gets a log of its own, sharded.",
    "override_cell": "How silver keeps it each round (cost and freshness). NULL: the plan's. "
                     "view: a view over its snapshot and log; nothing stored, every read pays the work (rarely read tables). "
                     "shared: its changes in a group table with other small tables, one MERGE a round for the group "
                     "(cheapest for many small tables; reads cast from VARIANT). "
                     "merge_round: a table of its own, MERGEd every round (read often, or streaming reads, CDF, time travel). "
                     "merge_changed: a table of its own, MERGEd only in rounds that changed it (big, quiet tables). "
                     "auto_cdc: a continuous pipeline keeps it (big and busy; lowest latency, a pipeline to pay for).",
    "override_snapshot_by": "How its one snapshot is taken. NULL: the plan's (by size). "
                            "qbc: a query-based read through its connection (default). "
                            "transport: the change capture's own (a gateway snapshots each new table; no second read). "
                            "files: files the customer exports (past 10 TB), made a Delta table in place.",
    "override_log": "The change log it goes to. NULL: the shared log. A catalog.schema.table name: a log of its own "
                    "(a split): its changes land there from now on, the shared log stops taking them, its snapshot is "
                    "kept (make before break, no gap). For a table whose changes slow the others' reads.",
}
PLAN_COLUMNS = ("round", "tbl", "role", "class", "cell", "snapshot_by", "log", "keys", "key_kind", "members", "variant",
                "introspection_runs", "why")


def decisions_comment(schema: str, warehouse: str | None = None) -> str:
    """routing_decisions' table comment: what it is, and how an assistant (Genie) carries out a person's change by
    itself, with no further prompt: the UPDATE, then the next round, each through the SQL Statement Execution API."""
    on = f" on warehouse {warehouse}" if warehouse else ""
    return ("lfc_routing: a person's overrides of how FlowThrough ingests each source table, a row a table (tbl, as in "
            "routing_plan.tbl; a group of repeating tables is one row and applies to every member). Each override_* "
            "column's comment says what each value does; NULL keeps the plan's choice. A change takes effect in the next "
            "round (routing_iteration) and shows in routing_plan with 'chosen' in its why. To help: explain the "
            "plan's choice (routing_plan.why) and what a value would change before setting it. To carry out a "
            "change a person asks for, without asking again: (1) POST /api/2.0/sql/statements" + on + " with the "
            f"statement UPDATE {schema}.routing_decisions SET override_<x> = '<value>', note = '<why>', `by` = "
            "current_user(), at = current_timestamp() WHERE tbl = '<table>' (each column's comment lists its values; "
            "update the existing row, never add one); (2) the same way, INSERT INTO "
            f"{schema}.routing_iteration SELECT coalesce(max(n), 0) + 1, current_user(), current_timestamp(), '<why>' "
            f"FROM {schema}.routing_iteration, which runs the next round; (3) say the round number and that the "
            "dashboard shows it after a refresh.")


def ddl(schema: str, warehouse: str | None = None) -> list:
    """CREATE TABLE IF NOT EXISTS for the three tables in `schema` (a quoted catalog.schema); the iteration table keeps
    its change data feed, for the history of who asked for which round."""
    return [f"CREATE TABLE IF NOT EXISTS {schema}.routing_plan (round INT, tbl STRING, role STRING, class STRING, "
            "cell STRING, snapshot_by STRING, log STRING, `keys` ARRAY<STRING>, key_kind STRING, members INT, variant BOOLEAN, "
            "introspection_runs STRING, why STRING) "
            "COMMENT 'lfc_routing: classify''s plan, a row a table a round'",
            f"CREATE TABLE IF NOT EXISTS {schema}.routing_decisions (tbl STRING COMMENT 'the table (or group of "
            "repeating tables) in routing_plan.tbl', "
            + ", ".join(f"{c} STRING COMMENT '{COMMENTS[c]}'" for c in OVERRIDES)
            + ", note STRING COMMENT 'why', `by` STRING COMMENT 'who', at TIMESTAMP COMMENT 'when') "
            "COMMENT '" + decisions_comment(schema, warehouse).replace("'", "''") + "'",
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
    groups = plan.get("groups") or {}   # shapes.classify_many: a row a group of repeating tables
    rows = []
    for t, p in sorted(plan["tables"].items()):
        r = p.get("route") or {}
        g = groups.get(t) or {}
        rows.append({"round": n, "tbl": t, "role": p.get("role"), "class": r.get("class"), "cell": r.get("silver"),
                     "snapshot_by": r.get("snapshot_by"), "log": r.get("log"), "keys": p.get("keys"),
                     "key_kind": p.get("key"), "members": len(g.get("members") or [None]),
                     "variant": bool(g.get("variant")),
                     "introspection_runs": json.dumps(plan["introspection_runs"]) if plan.get("introspection_runs") else None,
                     "why": "; ".join(x for x in (p.get("why"), r.get("why")) if x)})
    return rows


def latest_round(rows: list) -> int:
    """The round a person asked for last (routing_iteration's largest n; 0 when it has none)."""
    ns = [int((r.asDict() if hasattr(r, "asDict") else dict(r))["n"]) for r in rows]
    return max(ns) if ns else 0
