"""Offline checks of lfc_medallion's route: merge_route's orchestration with table_plan, routes_to_apply, release and
merge_all stubbed (one route per table across sources, last run's routes passed on, the plan written, what is released
and merged), merged_tables before and after a plan exists, and the entry point's arguments.

    python3 -m unittest skills/lfc-medallion/tests/test_route.py
"""
import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import merged_tables, route

R = types.SimpleNamespace


class Spark:
    """The plan table: present or not, its routes; createDataFrame and the write are recorded."""

    def __init__(self, plan=None):
        self.plan = plan   # {table: route} or None (no plan table yet)
        self.written = None
        self.writes = []   # every plan written, in order

    def sql(self, q):
        if q.startswith("SHOW TABLES"):
            rows = [R(tableName=route.PLAN)] if self.plan is not None else []
        elif "WHERE route = 'merge'" in q:
            rows = [R(table_name=t) for t, r in (self.plan or {}).items() if r == "merge"]
        elif q.startswith("SELECT DISTINCT table_name, route"):
            rows = [R(table_name=t, route=r) for t, r in (self.plan or {}).items()]
        else:
            raise AssertionError(q)
        return R(collect=lambda: rows)

    def createDataFrame(self, rows, schema):
        spark = self

        class Writer:
            def __getattr__(self, _):
                return lambda *a, **k: self

            def saveAsTable(self, name):
                spark.written = (name, rows, schema)
                spark.writes.append({r[1]: r[6] for r in rows})
        return R(write=Writer())


PLANS = {   # what table_plan says per source
    "east": {"lines": "pipeline", "nation": "merge", "part": "merge"},
    "west": {"lines": "pipeline", "nation": "merge", "part": "pipeline"},   # part is busy in west
}


def table_plan(spark, catalog, schema, hints=None, workspace=None):
    source = schema.rsplit("_", 1)[1]
    return {"tables": {t: {"route": r, "role": "fact" if r == "pipeline" else "dimension", "decided_by": "relations",
                           "why": f"{t} in {source}"} for t, r in PLANS[source].items()}}


class MergeRoute(unittest.TestCase):
    def run_route(self, spark, apply=None, keys=lambda table, source: True, fail=(), established=(), **kw):
        calls = {"merged": [], "released": []}

        def routes_to_apply(spark, catalog, target, planned, name, previous):
            calls["planned"], calls["previous"] = planned, previous
            return (apply or planned), {}

        def release(spark, catalog, target, tables, name, force=False):
            calls["released"] += tables
            calls.setdefault("forced", []).extend(tables if force else [])
            return [f"all_{t}" for t in tables if t in ("nation", *fail)]

        def merge_all(spark, catalog, prefix, target, tables, **k):
            calls["merged"] += tables
            calls["merge_kw"] = k
            if tables[0] in fail:
                raise RuntimeError("MERGE failed\nmore")
            return {f"all_{t}": {"east": {"done": "unchanged"}} for t in tables}

        def recorded_keys(spark, fq, full_name, workspace):
            table, source = full_name.rsplit(".", 1)[1], full_name.split(".")[1].rsplit("_", 1)[1]
            return ["k"] if keys(table, source) else []
        with mock.patch.object(route.discover, "sources", lambda *a: ["east", "west"]), \
                mock.patch.object(route.discover, "tables", lambda spark, c, p, names: {
                    s: set(PLANS[s]) for s in names}), \
                mock.patch.object(route.evidence, "recorded_keys", recorded_keys), \
                mock.patch.object(route.merge, "merged_targets", lambda *a: {f"all_{t}" for t in established}), \
                mock.patch.object(route.evidence, "table_plan", table_plan), \
                mock.patch.object(route.merge, "routes_to_apply", routes_to_apply), \
                mock.patch.object(route.merge, "release", release), \
                mock.patch.object(route.merge, "merge_all", merge_all):
            out = route.merge_route(spark, "main", "shop_bronze_", "shop_silver", source_column="division", **kw)
        return out, calls

    def test_merge_only_when_every_source_agrees(self):
        out, calls = self.run_route(Spark())
        self.assertEqual(calls["planned"], {"lines": "pipeline", "nation": "merge", "part": "pipeline"})
        self.assertEqual(calls["previous"], {})   # no plan table yet
        self.assertEqual(calls["merged"], ["nation"])   # one merge_all per table
        self.assertEqual(calls["released"], ["lines", "part"])
        self.assertTrue(calls["merge_kw"]["take_over"])
        self.assertEqual(out["merge"], ["nation"])

    def test_last_runs_routes_passed_on(self):
        _, calls = self.run_route(Spark(plan={"lines": "pipeline", "nation": "pipeline"}))
        self.assertEqual(calls["previous"], {"lines": "pipeline", "nation": "pipeline"})

    def test_plan_written_with_the_applied_route(self):
        spark = Spark()
        self.run_route(spark, apply={"lines": "pipeline", "nation": "pipeline", "part": "merge"})
        name, rows, schema = spark.written
        self.assertEqual(name, "`main`.`shop_silver`._lfc_medallion_plan")
        self.assertIn("source string", schema)
        self.assertIn(("east", "nation", "dimension", "merge", "relations", "nation in east", "pipeline", ""), rows)
        self.assertEqual(len(rows), 6)   # 3 tables x 2 sources


class Safety(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_keyless_table_stays_on_the_pipeline(self):   # tagged to merge, but west records no key for nation
        spark = Spark()
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "pipeline"},
                                    keys=lambda table, source: not (table == "nation" and source == "west"))
        self.assertEqual(calls["merged"], [])
        self.assertIn("nation", calls["released"])
        self.assertEqual(out["proposed"]["nation"]["refused"], "no primary key in west")
        self.assertTrue(all(r[6] == "pipeline" for r in spark.written[1] if r[1] == "nation"))

    def test_failed_merge_goes_back_and_the_plan_says_so(self):
        spark = Spark()
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"},
                                    fail=("part",))
        self.assertEqual(out["merge"], ["nation"])
        self.assertIn("all_part", out["released"])
        self.assertEqual(calls["forced"], ["part"])   # a partial table, not in the state yet, goes too
        self.assertEqual(out["proposed"]["part"]["failed"], "MERGE failed")
        routes = {r[1]: r[6] for r in spark.written[1]}
        self.assertEqual(routes, {"lines": "pipeline", "nation": "merge", "part": "pipeline"})


class Retry(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_failure_on_a_table_merged_before_keeps_it(self):   # a transient error isn't a reason to switch back
        spark = Spark(plan={"lines": "pipeline", "nation": "merge", "part": "merge"})
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"},
                                    fail=("part",), established=("nation", "part"))
        self.assertEqual(out["merge"], ["nation", "part"])
        self.assertEqual(calls["forced"], [])   # nothing force-released
        self.assertEqual(out["proposed"]["part"]["kept"], "merge, retried next run")
        self.assertEqual({r[1]: r[6] for r in spark.written[1]}["part"], "merge")


class Established(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_plan_saying_merge_isnt_proof(self):   # a first take-over whose run died: the plan says merge, no state
        spark = Spark(plan={"lines": "pipeline", "nation": "merge", "part": "merge"})
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"},
                                    fail=("part",), established=("nation",))
        self.assertEqual(calls["forced"], ["part"])   # treated as a first take-over: back to the pipeline
        self.assertNotIn("part", out["merge"])

    def test_keyless_established_table_kept(self):   # a key missing for a while doesn't hand back a merged table
        spark = Spark()
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "pipeline"},
                                    keys=lambda table, source: table != "nation", established=("nation",))
        self.assertEqual(out["merge"], ["nation"])
        self.assertNotIn("nation", calls["merged"])   # no merge this run
        self.assertNotIn("nation", calls["released"])
        self.assertEqual(out["proposed"]["nation"]["kept"], "merge, retried next run")


class PlanWrites(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_written_before_and_after(self):   # before: the routes being applied; after: what was done
        spark = Spark()
        self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"}, fail=("part",))
        self.assertEqual(spark.writes, [{"lines": "pipeline", "nation": "merge", "part": "merge"},
                                        {"lines": "pipeline", "nation": "merge", "part": "pipeline"}])


class Pipeline(unittest.TestCase):
    def test_merged_tables(self):
        self.assertEqual(merged_tables(Spark(), "main", "shop_silver"), [])   # before the first plan
        self.assertEqual(merged_tables(Spark(plan={"nation": "merge", "lines": "pipeline"}), "main", "shop_silver"),
                         ["nation"])


class EntryPoint(unittest.TestCase):
    def test_arguments(self):
        seen = {}
        fake = types.ModuleType("pyspark.sql")
        fake.SparkSession = R(builder=R(getOrCreate=lambda: "spark"))
        with mock.patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": fake}), \
                mock.patch.object(route, "merge_route", lambda *a: seen.setdefault("args", a) and {}):
            route.main(["--catalog", "main", "--prefix", "shop_bronze_", "--target", "shop_silver",
                        "--source-column", "division", "--hints", '{"nation": "lookup"}'])
        self.assertEqual(seen["args"], ("spark", "main", "shop_bronze_", "shop_silver", "division",
                                        {"nation": "lookup"}))

    def test_no_subset_of_sources(self):   # a subset would make merge_all drop the other sources' rows
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            route.main(["--catalog", "c", "--prefix", "p_", "--target", "t", "--only", "east"])


if __name__ == "__main__":
    unittest.main()
