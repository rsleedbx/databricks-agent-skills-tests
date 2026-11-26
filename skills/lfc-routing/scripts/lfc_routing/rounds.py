"""rounds: routing's step 4, a classify round from the tables (docs/design-intelligent.md, "The steps"). A person adds a
row to routing_iteration (n + 1) when the decisions are done; a job with a table update trigger on it runs run_round():
each source's latest introspection (lfc_introspection.history), what its change log showed since the last round
(counted by destination, kept in routing_observed), the overrides in routing_decisions, then classify with the
repeating tables grouped (shapes.classify_many), appended to routing_plan as round n with the introspection runs it
came from. Nothing is overwritten.

    from lfc_routing import rounds
    rounds.run_round(session, "`c`.`s`", {"pg1": {"log": "`c`.`s`.cdc_log", "prefix": "pg1.tpch1."}})
"""
from __future__ import annotations

import json
import time

from . import shapes, tables

OBSERVED = "routing_observed"


def observed(prev: dict, got: dict, now: float, logs: dict | None = None, round_: int | None = None) -> dict:
    """What the change logs showed this round, as routing's classify reads it (lfc_routing.classify, `observed`):
    {round, at, seconds, tables: {name: {log, events, events_per_s}}}. prev: load()'s last round, got: counts() now;
    a table first seen has no rate yet (0). Plain data: written as a file, a round replays from it."""
    seconds = [now - prev[n][2] for n in got if n in prev]
    tables = {}
    for n in sorted(set(got) | set(logs or {})):
        e = got.get(n, 0)
        rate = max(0, e - prev[n][1]) / max(now - prev[n][2], 1e-9) if n in prev else 0.0
        tables[n] = {"log": (logs or {}).get(n), "events": e, "events_per_s": rate}
    return {"round": round_, "at": round(now, 3), "seconds": round(max(seconds), 3) if seconds else None,
            "tables": tables}
def ddl(schema: str) -> list:
    return [f"CREATE TABLE IF NOT EXISTS {schema}.{OBSERVED} (round INT, source STRING, tbl STRING, log STRING, "
            "events BIGINT, at DOUBLE) COMMENT 'lfc_routing: each round''s event count a table in its log'"]


def _lit(v) -> str:
    return "NULL" if v is None else "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _dicts(rs) -> list:
    return [r.asDict() if hasattr(r, "asDict") else dict(r) for r in rs]


def run_round(session, schema: str, sources: dict, n: int | None = None, now: float | None = None) -> dict:
    """Round n (routing_iteration's latest when None) for {source: {"log": quoted log, "prefix": its destinations'
    prefix (pg1.tpch1.)}}: returns the plan (with "groups" and "introspection_runs")."""
    from lfc_introspection import history
    now = now or time.time()
    if n is None:
        n = tables.latest_round(_dicts(session.sql(f"SELECT n FROM {schema}.routing_iteration").collect()))
    docs, runs = {}, {}
    for src in sources:
        got = _dicts(session.sql(f"SELECT run FROM {schema}.{history.RUNS} WHERE source = {_lit(src)} AND status = 'ok' "
                                 "ORDER BY run DESC LIMIT 1").collect())
        if not got:
            raise ValueError(f"{src}: no introspection run yet")
        runs[src] = int(got[0]["run"])
        docs[src] = history.latest(session, schema, src, runs[src])
    # what each log showed: events a table now, against the last round's counts
    prev_rows = _dicts(session.sql(f"SELECT o.source, o.tbl, o.events, o.at FROM {schema}.{OBSERVED} o WHERE o.round = "
                                   f"(SELECT max(round) FROM {schema}.{OBSERVED} WHERE round < {int(n)})").collect())
    prev = {f"{r['source']}.{r['tbl']}": ({}, int(r["events"]), float(r["at"])) for r in prev_rows}
    got, logs = {}, {}
    for src, s in sources.items():
        rows = _dicts(session.sql(f"SELECT destination, count(*) AS n FROM {s['log']} WHERE destination LIKE "
                                  f"{_lit(s['prefix'] + '%')} GROUP BY destination").collect())
        for t in docs[src][0].get("tables") or []:
            got[f"{src}.{t['name']}"], logs[f"{src}.{t['name']}"] = 0, s["log"]
        for r in rows:
            got[f"{src}.{r['destination'][len(s['prefix']):]}"] = int(r["n"])
    obs = observed(prev, got, now, logs, round_=n)
    decisions = tables.decisions(_dicts(session.sql(f"SELECT * FROM {schema}.routing_decisions").collect()))
    plan = shapes.classify_many(docs, observed=obs, decisions=decisions)
    plan["introspection_runs"] = runs
    session.sql(f"DELETE FROM {schema}.{OBSERVED} WHERE round = {int(n)}").collect()   # a round run again counts again
    vals = [f"({int(n)}, {_lit(k.split('.', 1)[0])}, {_lit(k.split('.', 1)[1])}, {_lit(logs.get(k))}, {e}, {now:.3f})"
            for k, e in got.items()]
    for i in range(0, len(vals), 500):
        session.sql(f"INSERT INTO {schema}.{OBSERVED} VALUES " + ", ".join(vals[i:i + 500])).collect()
    rows = tables.plan_rows(plan, round_=n)
    session.sql(f"DELETE FROM {schema}.routing_plan WHERE round = {int(n)}").collect()
    cols = tables.PLAN_COLUMNS
    for i in range(0, len(rows), 200):
        session.sql(f"INSERT INTO {schema}.routing_plan ({', '.join('`' + c + '`' for c in cols)}) VALUES " + ", ".join(
            "(" + ", ".join(_sql(r[c]) for c in cols) + ")" for r in rows[i:i + 200])).collect()
    return plan


def _sql(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, tuple)):
        return "array(" + ", ".join(_lit(x) for x in v) + ")"
    if isinstance(v, dict):
        return _lit(json.dumps(v))
    return _lit(v)
