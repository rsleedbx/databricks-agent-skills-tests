"""route: the merge route as one job task, and the pipeline's side of it.

merge_route() is what runs before the pipeline each refresh: it plans every source's bronze (table_plan, with hints
where the data can't tell), agrees one route per table (`merge` only when every source's plan says so), applies the
routes a table keeps or an operator's tag asks for (routes_to_apply, with last run's routes so an interrupted switch
finishes), records the plan in <target>._lfc_medallion_plan, hands tables back to the pipeline (release) and merges
the rest (merge_all, taking over a view the pipeline left). merged_tables() is what the pipeline reads, in its source
file, to leave those tables out (stack_all(skip=...)).

A job runs it with the package's entry point (python_wheel_task, package lfc-medallion, entry point
lfc-medallion-merge) or a spark_python_task of two lines; arguments: --catalog --prefix --target [--source-column]
[--hints '{"nation": "lookup"}'] [--only a,b].
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


def merge_route(spark, catalog: str, prefix: str, target: str, source_column: str = "_source", hints=None,
                only=None, name: str = "all_{table}") -> dict:
    """Plan, record, release and merge, for every source <prefix><source> into <target>. Returns what it did."""
    rows, planned = [], {}
    for source in discover.sources(spark, catalog, prefix, only):
        for table, t in evidence.table_plan(spark, catalog, prefix + source, hints=hints)["tables"].items():
            planned[table] = "merge" if t["route"] == "merge" and planned.get(table, "merge") == "merge" else "pipeline"
            rows.append((source, table, t["role"], t["route"], t["decided_by"], t["why"]))
    previous = {r.table_name: r.route for r in spark.sql(
        f"SELECT DISTINCT table_name, route FROM `{catalog}`.`{target}`.{PLAN}").collect()} \
        if _has(spark, catalog, target, PLAN) else {}
    apply, proposed = merge.routes_to_apply(spark, catalog, target, planned, name=name, previous=previous)
    spark.createDataFrame([(*r, apply[r[1]], json.dumps(proposed[r[1]]) if r[1] in proposed else "") for r in rows],
                          "source string, table_name string, role string, planned string, decided_by string, "
                          "why string, route string, proposed string") \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{catalog}`.`{target}`.{PLAN}")
    quiet = sorted(t for t, r in apply.items() if r == "merge")
    released = merge.release(spark, catalog, target, sorted(t for t, r in apply.items() if r == "pipeline"), name=name)
    report = merge.merge_all(spark, catalog, prefix, target, quiet, source_column=source_column, only=only, name=name,
                             take_over=True)
    return {"merge": quiet, "pipeline": sorted(t for t, r in apply.items() if r == "pipeline"), "released": released,
            "proposed": proposed, "done": {t: {s: x["done"] for s, x in v.items()} for t, v in report.items()}}


def main(argv=None) -> None:
    """The job task: python_wheel_task (entry point lfc-medallion-merge) or spark_python_task."""
    ap = argparse.ArgumentParser(prog="lfc-medallion-merge")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--prefix", required=True, help="the sources' schemas: <prefix><source>")
    ap.add_argument("--target", required=True, help="the schema of the n:1 tables (the pipeline's)")
    ap.add_argument("--source-column", default="_source")
    ap.add_argument("--hints", default="", help='JSON: {"table": "role"}')
    ap.add_argument("--only", default="", help="sources, comma-separated (default: every <prefix>* schema)")
    a = ap.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(json.dumps(merge_route(spark, a.catalog, a.prefix, a.target, a.source_column,
                                 json.loads(a.hints) if a.hints else None, a.only or None)))
