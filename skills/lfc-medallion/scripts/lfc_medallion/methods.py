"""methods: which refresh method each materialized view took, and what its time followed, from a pipeline event log.

A materialized view's refresh picks a technique per update (its `planning_information` event): skip (NO_OP), append
only, incremental (ROW_BASED, GROUP_AGGREGATE, WINDOW_FUNCTION, ...), or a complete recompute, with the cost model's
reason (`cost_model_decision_path`, such as a small-changes threshold or the refresh history) and, per source table,
its full size, change size, rows and changed rows, files and changed files. Each technique processes different bytes:
a recompute all of the sources, an incremental refresh the changes, append only the new rows, a skip none. So time is
predicted per technique from the input it follows (references/performance-reasoning.md).

    from lfc_medallion import methods
    rows = methods.refreshes(spark, "main.my_control.events_my_pipeline")      # or system.lakeflow_pipeline_events_preview.pipeline_events
    print(methods.summarize(rows))

The events table may be a pipeline's event log (details as JSON text) or the account's pipeline events system table
(details as VARIANT): the query reads `details:planning_information` either way. Seconds are a flow's first
flow_progress event to its COMPLETED one in the update.
"""
from __future__ import annotations

import math
from collections import Counter

PLAN = ("struct<technique_information: array<struct<maintenance_type: string, is_chosen: boolean, "
        "is_applicable: boolean>>, source_table_information: array<struct<full_size: double, change_size: double, "
        "change_file_read_size: double, num_rows: bigint, num_changed_rows: bigint, num_files: bigint, "
        "num_changed_files: bigint, is_deletion_vector_enabled: boolean, is_row_id_enabled: boolean>>, "
        "planning_wall_time_ms: bigint, cost_model_decision_path: string, incrementalization_status: string>")
SUMS = {"full_bytes": "full_size", "change_bytes": "change_size", "change_read_bytes": "change_file_read_size",
        "rows_": "num_rows", "changed_rows": "num_changed_rows", "files": "num_files", "changed_files": "num_changed_files"}
DRIVERS = {"source bytes": ("full_bytes",), "change bytes": ("change_bytes",),
           "changed rows and files": ("changed_rows", "changed_files"), "planning time": ("planning_ms",)}


def refreshes_sql(events: str, since: str | None = None) -> str:
    """One row per materialized-view refresh in `events`: the technique chosen, why, the sources' sizes, the seconds."""
    when = f" AND timestamp >= '{since}'" if since else ""
    sums = ", ".join(f"aggregate(p.source_table_information, 0D, (a, s) -> a + coalesce(CAST(s.{c} AS DOUBLE), 0D)) "
                     f"AS {k}" for k, c in SUMS.items())
    return f"""
WITH plan AS (
  SELECT origin.update_id AS u, origin.flow_name AS f, from_json(details:planning_information::string, '{PLAN}') AS p
  FROM {events} WHERE event_type = 'planning_information'{when}),
progress AS (
  SELECT origin.update_id AS u, origin.flow_name AS f, min(timestamp) AS t0,
         max(CASE WHEN details:flow_progress.status::string = 'COMPLETED' THEN timestamp END) AS t1
  FROM {events} WHERE event_type = 'flow_progress'{when} GROUP BY 1, 2)
SELECT replace(filter(p.technique_information, x -> x.is_chosen)[0].maintenance_type, 'MAINTENANCE_TYPE_', '')
         AS technique,
       p.cost_model_decision_path AS decision, size(p.source_table_information) AS sources, {sums},
       p.planning_wall_time_ms AS planning_ms,
       CAST(unix_millis(CAST(progress.t1 AS TIMESTAMP)) - unix_millis(CAST(progress.t0 AS TIMESTAMP)) AS DOUBLE) / 1000
         AS seconds
FROM plan JOIN progress USING (u, f)
WHERE progress.t1 IS NOT NULL"""


def refreshes(session, events: str, since: str | None = None) -> list[dict]:
    """The refreshes as dicts, on any session with sql().collect() (Spark, or engine.Warehouse)."""
    return [r.asDict() for r in session.sql(refreshes_sql(events, since)).collect()]


def _median(xs: list[float]) -> float | None:
    xs = sorted(x for x in xs if x is not None)
    return xs[len(xs) // 2] if xs else None


def summarize(rows: list[dict], min_rows: int = 30) -> list[dict]:
    """Per technique: refreshes, median seconds and planning, and the R² of log seconds on each driver (log10 of
    1 + the value), so what a technique's time follows is read off its row."""
    from .paths import fit_r2
    out = []
    for tech, n in Counter(r.get("technique") or "NONE" for r in rows).most_common():
        mine = [r for r in rows if (r.get("technique") or "NONE") == tech and (r.get("seconds") or 0) > 0]
        row = {"technique": tech, "refreshes": n, "median_s": _median([float(r["seconds"]) for r in mine]),
               "median_planning_s": _median([(r.get("planning_ms") or 0) / 1e3 for r in mine])}
        if len(mine) >= min_rows:
            ys = [math.log10(float(r["seconds"])) for r in mine]
            for name, cols in DRIVERS.items():
                _, r2 = fit_r2([tuple(math.log10(1 + float(r.get(c) or 0)) for c in cols) for r in mine], ys)
                row[f"r2 {name}"] = None if r2 is None else round(r2, 2)
        out.append(row)
    return out


def main(argv=None) -> None:
    import argparse
    import json
    ap = argparse.ArgumentParser(description="Refresh methods and what their time follows, from an event log")
    ap.add_argument("--events", required=True, help="an event log table, or the pipeline events system table")
    ap.add_argument("--warehouse", required=True, help="a SQL warehouse id")
    ap.add_argument("--since", default=None, help="a date or timestamp: only events from then")
    a = ap.parse_args(argv)
    from .engine import Warehouse
    print(json.dumps(summarize(refreshes(Warehouse(a.warehouse), a.events, a.since)), indent=1, default=str))


if __name__ == "__main__":
    main()
