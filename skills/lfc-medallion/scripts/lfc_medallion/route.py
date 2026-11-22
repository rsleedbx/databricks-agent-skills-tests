"""route: the merge route as one job task, and the pipeline's side of it.

merge_route() is what runs before the pipeline each refresh: it plans every source's bronze (table_plan, with hints
where the data can't tell), agrees one route per table (`merge` only when every source's plan says so), applies the
routes a table keeps or an operator's tag asks for (routes_to_apply, with last run's routes so an interrupted switch
finishes), hands tables back to the pipeline (release), merges the rest one table at a time (merge_all, taking over a
view the pipeline left), recording the plan in <target>._lfc_medallion_plan before and after.
merged_tables() is what the pipeline reads, in its source file, to leave those tables out (stack_all(skip=...)).

A job runs it with the package's entry point (python_wheel_task, package lfc-medallion, entry point
lfc-medallion-merge) or a spark_python_task of two lines; arguments: --catalog --prefix --target [--source-column]
[--hints '{"nation": "lookup"}']. Every source <prefix>* takes part: a subset would make merge_all drop the others'
rows.
"""
import argparse
import json

from . import discover, evidence, merge
from .stack import spark_string

PLAN = "_lfc_medallion_plan"


def _has(spark, catalog: str, target: str, table: str) -> bool:
    """SHOW TABLES ... LIKE, which a pipeline's source file may run (spark.catalog.tableExists is not allowed there)."""
    return bool(spark.sql(f"SHOW TABLES IN `{catalog}`.`{target}` LIKE {spark_string(table)}").collect())


def merged_tables(spark, catalog: str, target: str) -> list[str]:
    """The tables on the `merge` route in the last plan merge_route wrote; none before it first ran."""
    if not _has(spark, catalog, target, PLAN):
        return []
    return [r.table_name for r in spark.sql(f"SELECT DISTINCT table_name FROM `{catalog}`.`{target}`.{PLAN} "
                                            "WHERE route = 'merge'").collect()]


def write_plan(spark, catalog: str, target: str, rows: list, apply: dict, proposed: dict) -> None:
    """<target>._lfc_medallion_plan: each source's plan of each table, the route applied, and any proposal."""
    spark.createDataFrame([(*r, apply[r[1]], json.dumps(proposed[r[1]]) if r[1] in proposed else "") for r in rows],
                          "source string, table_name string, role string, planned string, decided_by string, "
                          "why string, route string, proposed string") \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{target}`.{PLAN}")


class _Workspace:
    """One databricks.sdk WorkspaceClient for the whole run, made on first use (the lookups that need it are few)."""

    def __init__(self):
        self._w = None

    def __getattr__(self, attr):
        if self._w is None:
            from databricks.sdk import WorkspaceClient
            self._w = WorkspaceClient()
        return getattr(self._w, attr)


def _kinds(spark, catalog: str, target: str) -> dict:
    return {r.table_name: r.table_type for r in spark.sql(
        f"SELECT table_name, table_type FROM `{catalog}`.information_schema.tables "
        f"WHERE table_schema = {spark_string(target)}").collect()}


def merge_route(spark, catalog: str, prefix: str, target: str, source_column: str = "_source", hints=None,
                name: str = "all_{table}", workspace=None, log: str | None = None, replan: bool = False) -> dict:
    """Plan, release, merge, then record, for every source <prefix><source> into <target>. Returns what it did.
    A table bound for `merge` with no primary key in some source (or whose key can't be read) stays on the pipeline;
    one merged before keeps its table and route, with no merge this run. A merge that fails keeps a table merged
    before (retried next run) and sends a first take-over back to the pipeline (dropping only the table this run
    created); all are noted in `proposed`. The plan is written before any release or merge, with first take-overs
    still on the pipeline (a table isn't skipped before it arrives), and again at the end, from what was done.
    log: the merge log's table (merge_all), e.g. a control schema's; each run's rows carry one run id.
    The last plan is kept for the sources it has (a table's route changes only by a tag, routes_to_apply): only new
    sources are planned, in parallel; replan=True plans every source again (its evidence: keys, rows, history)."""
    import uuid
    run_id = str(uuid.uuid4())
    workspace = workspace or _Workspace()
    sources = discover.sources(spark, catalog, prefix)
    held = discover.tables(spark, catalog, prefix, sources)
    rows, planned = [], {}
    kept: dict[str, list] = {}   # the last plan's rows, per source
    if not replan and _has(spark, catalog, target, PLAN):
        for r in spark.sql(f"SELECT source, table_name, role, planned, decided_by, why "
                           f"FROM `{catalog}`.`{target}`.{PLAN}").collect():
            kept.setdefault(r.source, []).append((r.source, r.table_name, r.role, r.planned, r.decided_by, r.why))
    new = [x for x in sources if x not in kept or {t for _, t, *_ in kept[x]} != set(held[x])]

    def plan_one(source):
        return [(source, table, t["role"], t["route"], t["decided_by"], t["why"]) for table, t in
                evidence.table_plan(spark, catalog, prefix + source, hints=hints, workspace=workspace)["tables"].items()]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(new)))) as ex:
        fresh = dict(zip(new, ex.map(plan_one, new)))
    for source in sources:
        for row in fresh.get(source) or kept.get(source, []):
            table, route = row[1], row[3]
            planned[table] = "merge" if route == "merge" and planned.get(table, "merge") == "merge" else "pipeline"
            rows.append(row)
    previous = {r.table_name: r.route for r in spark.sql(
        f"SELECT DISTINCT table_name, route FROM `{catalog}`.`{target}`.{PLAN}").collect()} \
        if _has(spark, catalog, target, PLAN) else {}
    apply, proposed = merge.routes_to_apply(spark, catalog, target, planned, name=name, previous=previous)
    # merged before: merge_all's state has the table (a merge finished), not just a plan that said so
    established = {t for t in planned if name.format(table=t) in merge.merged_targets(spark, catalog, target)}
    skip = set()
    for table in [t for t, r in apply.items() if r == "merge"]:   # merge_all needs every source's key
        keyless = []
        for s in [x for x in sources if table in held[x]]:
            try:
                if not evidence.recorded_keys(spark, f"`{catalog}`.`{prefix}{s}`.`{table}`",
                                              f"{catalog}.{prefix}{s}.{table}", workspace):
                    keyless.append(s)
            except Exception as e:  # noqa: BLE001  a key that can't be read counts as missing, this run only
                keyless.append(f"{s} ({str(e).splitlines()[0][:80]})")
        if keyless and table in established:   # keep the table and route; no merge this run
            skip.add(table)
            proposed[table] = {**proposed.get(table, {}), "refused": f"no primary key in {', '.join(keyless)}",
                               "kept": "merge, retried next run"}
        elif keyless:
            apply[table] = "pipeline"
            proposed[table] = {**proposed.get(table, {}), "refused": f"no primary key in {', '.join(keyless)}"}
    # first: first take-overs stay `pipeline` until their merge succeeds, so the pipeline doesn't skip a table before
    # it arrives; switches back to the pipeline are recorded, so a run that dies after release finishes them
    first = {t for t, r in apply.items() if r == "merge" and t not in established}
    write_plan(spark, catalog, target, rows, {t: "pipeline" if t in first else r for t, r in apply.items()}, proposed)
    released = merge.release(spark, catalog, target, sorted(t for t, r in apply.items() if r == "pipeline"), name=name)
    before = _kinds(spark, catalog, target)
    done = {}
    for table in sorted(t for t, r in apply.items() if r == "merge" and t not in skip):
        try:
            report = merge.merge_all(spark, catalog, prefix, target, [table], source_column=source_column, name=name,
                                     workspace=workspace, take_over=True, run_id=run_id, log=log)
            done.update({t: {s: x["done"] for s, x in v.items()} for t, v in report.items()})
        except Exception as e:  # noqa: BLE001  one table's failure doesn't stop the others
            error = str(e).splitlines()[0][:200]
            if table in established:   # merged before: keep the table and its route, retry next run
                proposed[table] = {**proposed.get(table, {}), "failed": error, "kept": "merge, retried next run"}
                continue
            apply[table] = "pipeline"   # a first take-over that failed: back to the pipeline
            out = name.format(table=table)
            # drop only a table this run created: not a table before it, and a plain managed table now (never a
            # pipeline's view or streaming table, never one that was there already)
            if before.get(out) not in ("MANAGED", "EXTERNAL") and _kinds(spark, catalog, target).get(out) == "MANAGED":
                spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{target}`.`{out}`")
                released.append(out)
            released += merge.release(spark, catalog, target, [table], name=name)   # its state, if any was saved
            proposed[table] = {**proposed.get(table, {}), "failed": error}
    write_plan(spark, catalog, target, rows, apply, proposed)   # again: what was done
    return {"run_id": run_id, "planned_sources": sorted(new), "merge": sorted(t for t, r in apply.items() if r == "merge"),
            "pipeline": sorted(t for t, r in apply.items() if r == "pipeline"), "released": sorted(set(released)),
            "proposed": proposed, "done": done}


def main(argv=None) -> None:
    """The job task: python_wheel_task (entry point lfc-medallion-merge) or spark_python_task."""
    ap = argparse.ArgumentParser(prog="lfc-medallion-merge")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--prefix", required=True, help="the sources' schemas: <prefix><source>")
    ap.add_argument("--target", required=True, help="the schema of the n:1 tables (the pipeline's)")
    ap.add_argument("--source-column", default="_source")
    ap.add_argument("--hints", default="", help='JSON: {"table": "role"}')
    ap.add_argument("--log", default="", help="the merge log's table, catalog.schema.table (default in --target)")
    ap.add_argument("--replan", action="store_true", help="plan every source again, not only new ones")
    a = ap.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(json.dumps(merge_route(spark, a.catalog, a.prefix, a.target, a.source_column,
                                 json.loads(a.hints) if a.hints else None, log=a.log or None,
                                 replan=a.replan)))
