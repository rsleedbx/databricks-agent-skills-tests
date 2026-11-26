"""snapshots: routing's provisional snapshot (step 3) and whether step 7 keeps it (docs/design-intelligent.md, "The
provisional snapshot"). Plain decisions; the read is lfc_change_source.intake.jdbc, run by the generic notebook
(bundle/notebooks/provisional_snapshot.py), which records each in snapshot_runs.

    picked = snapshots.pick(stats_doc)                 # [(table, why)] small tables with statistics; the rest: why not
    verdict = snapshots.keep_or_take(run, logs, changed=set())   # step 7: keep it, or take a new one, with why
"""
from __future__ import annotations

from .routing import SMALL_MAX_BYTES

RUNS = "snapshot_runs"


def ddl(schema: str) -> list:
    return [f"CREATE TABLE IF NOT EXISTS {schema}.{RUNS} (source STRING, tbl STRING, at TIMESTAMP, method STRING, "
            "status STRING, error STRING, snapshot_table STRING, position_kind STRING, position STRING, `rows` BIGINT, "
            "seconds DOUBLE, introspection_run INT) "
            "COMMENT 'lfc_routing: each snapshot taken (provisional or real), with the position the log applies after'"]


def pick(stats_doc: dict, max_bytes: int = SMALL_MAX_BYTES) -> tuple:
    """(picked, skipped): [(table, why)] each. A table gets a provisional snapshot only when its statistics say it is
    small: bytes known and at most max_bytes (a table with no statistics could take long, so none)."""
    picked, skipped = [], []
    for t in stats_doc.get("tables") or []:
        b = t.get("data_size_bytes")
        if b is None:
            skipped.append((t["name"], "no statistics of its size: no provisional snapshot"))
        elif b > max_bytes:
            skipped.append((t["name"], f"{b:,} bytes, over {max_bytes:,}: its snapshot waits for step 7"))
        else:
            picked.append((t["name"], f"{b:,} bytes, {t.get('row_count') or 0:,} rows: small, with statistics"))
    return picked, skipped


def keep_or_take(run: dict | None, logs: dict, changed: set | None = None) -> dict:
    """Step 7 for one table: {"keep": bool, "why"}. run: its snapshot_runs row (None: it has none); logs: {log: the
    first position it holds for the table} of the logs the table reads in the plan; changed: what the decisions
    changed for it since the snapshot ("schema", "method"). A snapshot is kept while some log the table reads still
    holds its position (a split overlaps, so it does)."""
    if not run or run.get("status") != "ok":
        return {"keep": False, "why": "no snapshot yet: take one"}
    for c in sorted(changed or ()):
        return {"keep": False, "why": f"its {c} changed since the snapshot: take a new one"}
    p = int(run["position"])
    held = [log for log, first in logs.items() if first is not None and int(first) <= p]
    if held:
        return {"keep": True, "why": f"its position {p} is in {held[0]}: the log applies after it"}
    return {"keep": False, "why": f"no log the table reads holds its position {p} (trimmed or started later): take a new one"}
