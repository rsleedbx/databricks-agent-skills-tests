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


def merge_route(spark, catalog: str, prefix: str, target: str, source_column: str = "_source", hints=None,
                name: str = "all_{table}", workspace=None) -> dict:
    """Plan, release, merge, then record, for every source <prefix><source> into <target>. Returns what it did.
    A table bound for `merge` with no primary key in some source stays on the pipeline (merge_all needs the key). A
    merge that fails keeps a table merged before (retried next run) and sends a first take-over back to the pipeline
    (its partial table released); all are noted in `proposed`.
    The plan is written before any release or merge (so a run that dies mid-switch has `previous` to finish it) and
    again at the end, from what was done (so the pipeline never skips a table that didn't arrive)."""
    sources = discover.sources(spark, catalog, prefix)
    held = discover.tables(spark, catalog, prefix, sources)
    rows, planned = [], {}
    for source in sources:
        for table, t in evidence.table_plan(spark, catalog, prefix + source, hints=hints,
                                            workspace=workspace)["tables"].items():
            planned[table] = "merge" if t["route"] == "merge" and planned.get(table, "merge") == "merge" else "pipeline"
            rows.append((source, table, t["role"], t["route"], t["decided_by"], t["why"]))
    previous = {r.table_name: r.route for r in spark.sql(
        f"SELECT DISTINCT table_name, route FROM `{catalog}`.`{target}`.{PLAN}").collect()} \
        if _has(spark, catalog, target, PLAN) else {}
    apply, proposed = merge.routes_to_apply(spark, catalog, target, planned, name=name, previous=previous)
    for table in [t for t, r in apply.items() if r == "merge"]:   # merge_all needs every source's key
        keyless = [s for s in sources if table in held[s] and not evidence.recorded_keys(
            spark, f"`{catalog}`.`{prefix}{s}`.`{table}`", f"{catalog}.{prefix}{s}.{table}", workspace)]
        if keyless:
            apply[table] = "pipeline"
            proposed[table] = {**proposed.get(table, {}), "refused": f"no primary key in {', '.join(keyless)}"}
    # first, the routes being applied: a run that dies mid-switch leaves previous= to finish it next time
    write_plan(spark, catalog, target, rows, apply, proposed)
    released = merge.release(spark, catalog, target, sorted(t for t, r in apply.items() if r == "pipeline"), name=name)
    done = {}
    for table in sorted(t for t, r in apply.items() if r == "merge"):
        try:
            report = merge.merge_all(spark, catalog, prefix, target, [table], source_column=source_column, name=name,
                                     workspace=workspace, take_over=True)
            done.update({t: {s: x["done"] for s, x in v.items()} for t, v in report.items()})
        except Exception as e:  # noqa: BLE001  one table's failure doesn't stop the others
            error = str(e).splitlines()[0][:200]
            if previous.get(table) == "merge":   # merged before: keep the table and its route, retry next run
                proposed[table] = {**proposed.get(table, {}), "failed": error, "kept": "merge, retried next run"}
                continue
            apply[table] = "pipeline"   # a first take-over that failed: back to the pipeline
            # force: a partial table, created before merge_all saved any state, goes too
            released += merge.release(spark, catalog, target, [table], name=name, force=True)
            proposed[table] = {**proposed.get(table, {}), "failed": error}
    write_plan(spark, catalog, target, rows, apply, proposed)   # again: what was done
    return {"merge": sorted(t for t, r in apply.items() if r == "merge"),
            "pipeline": sorted(t for t, r in apply.items() if r == "pipeline"), "released": released,
            "proposed": proposed, "done": done}


def main(argv=None) -> None:
    """The job task: python_wheel_task (entry point lfc-medallion-merge) or spark_python_task."""
    ap = argparse.ArgumentParser(prog="lfc-medallion-merge")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--prefix", required=True, help="the sources' schemas: <prefix><source>")
    ap.add_argument("--target", required=True, help="the schema of the n:1 tables (the pipeline's)")
    ap.add_argument("--source-column", default="_source")
    ap.add_argument("--hints", default="", help='JSON: {"table": "role"}')
    a = ap.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(json.dumps(merge_route(spark, a.catalog, a.prefix, a.target, a.source_column,
                                 json.loads(a.hints) if a.hints else None)))
