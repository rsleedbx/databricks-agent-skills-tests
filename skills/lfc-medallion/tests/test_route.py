"""Offline checks of lfc_medallion's route: merge_route's orchestration with table_plan, routes_to_apply, release and
merge_all stubbed (one route per table across sources, last run's routes passed on, the plan written, what is released
and merged), merged_tables before and after a plan exists, and the entry point's arguments.

    python3 -m unittest skills/lfc-medallion/tests/test_route.py
"""
import os
import re
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import merged_tables, route

R = types.SimpleNamespace


class Spark:
    """The plan table: present or not, its routes; createDataFrame and the write are recorded."""

    def __init__(self, plan=None, kinds=None, plan_rows=None):
        self.plan = plan   # {table: route} or None (no plan table yet)
        self.plan_rows = plan_rows or []   # the last plan's rows (source, table, role, planned, decided_by, why)
        self.kinds = dict(kinds or {})   # the target's objects: {name: table_type}; a merge may add to it
        self.dropped = []
        self.written = None
        self.writes = []   # every plan written, in order

    def sql(self, q):
        if q.startswith("SHOW TABLES"):
            rows = [R(tableName=route.PLAN)] if self.plan is not None else []
        elif "WHERE route = 'merge'" in q:
            rows = [R(table_name=t) for t, r in (self.plan or {}).items() if r == "merge"]
        elif q.startswith("SELECT source, table_name, role, planned"):
            rows = [R(source=a, table_name=b, role=c, planned=d, decided_by=e, why=f) for a, b, c, d, e, f in
                    self.plan_rows]
        elif q.startswith("SELECT DISTINCT table_name, route"):
            rows = [R(table_name=t, route=r) for t, r in (self.plan or {}).items()]
        elif "information_schema.tables" in q:
            rows = [R(table_name=n, table_type=k) for n, k in self.kinds.items()]
        elif q.startswith("DROP TABLE IF EXISTS"):
            self.dropped.append(q.rsplit("`.`", 1)[1].rstrip("`"))
            rows = []
        elif q.startswith("CREATE OR REPLACE TABLE") and route.PLAN in q:   # write_plan, as SQL: its rows recorded
            name = q.split(" AS ", 1)[0].split()[-1]
            body = q.split(" AS ", 1)[1]
            got = [tuple(v for v, _ in re.findall(r"'((?:[^'\\]|\\.)*)' AS (\w+)", part))
                   for part in body.split(" UNION ALL ")] if "WHERE false" not in body else []
            self.written = (name, got, None)
            self.writes.append({r[1]: r[6] for r in got})
            rows = []
        elif "_lfc_medallion_merge_paths" in q or q.startswith("SELECT engine, k, sources"):   # the paths log
            rows = []
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
    def run_route(self, spark, apply=None, keys=lambda table, source: True, fail=(), established=(), creates=(),
                  **kw):
        calls = {"merged": [], "released": []}

        def routes_to_apply(spark, catalog, target, planned, name, previous):
            calls["planned"], calls["previous"] = planned, previous
            return (apply or planned), {}

        def release(spark, catalog, target, tables, name):
            calls["released"] += tables
            return [f"all_{t}" for t in tables if t in ("nation", *fail)]

        def merge_all(spark, catalog, prefix, target, tables, **k):
            calls["merged"] += tables
            calls["merge_kw"] = k
            if tables[0] in creates:   # merge_all created its target before failing
                spark.kinds[f"all_{tables[0]}"] = "MANAGED"
            if tables[0] in fail:
                raise RuntimeError("MERGE failed\nmore")
            return {f"all_{t}": {"east": {"done": "unchanged"}} for t in tables}

        def recorded_keys(spark, fq, full_name, workspace):
            table, source = full_name.rsplit(".", 1)[1], full_name.split(".")[1].rsplit("_", 1)[1]
            k = keys(table, source)
            if isinstance(k, Exception):
                raise k
            return ["k"] if k else []
        ctx = {"names": ["east", "west"], "held": {x: set(PLANS[x]) for x in ("east", "west")}, "cols": None,
               "state": {}, "kinds": dict(spark.kinds)}
        with mock.patch.object(route.merge, "load_context", lambda *a, **k: ctx), \
                mock.patch.object(route.discover, "sources", lambda *a: ["east", "west"]), \
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

    def test_the_last_plan_is_kept_for_its_sources(self):   # only a new source (or one with new tables) is planned
        kept = [("east", t, "dimension", r, "relations", "") for t, r in PLANS["east"].items()]
        planned = []
        with mock.patch.object(route.evidence, "table_plan", lambda spark, c, schema, **k: planned.append(schema) or
                               table_plan(spark, c, schema, **k)):
            out, calls = self.run_route(Spark(plan={"nation": "merge"}, plan_rows=kept))
        self.assertEqual(out["planned_sources"], ["west"])
        self.assertEqual(calls["planned"], {"lines": "pipeline", "nation": "merge", "part": "pipeline"})
        out, _ = self.run_route(Spark(plan={"nation": "merge"}, plan_rows=kept), replan=True)
        self.assertEqual(out["planned_sources"], ["east", "west"])

    def test_a_kept_plan_made_under_other_hints_is_planned_again(self):   # a role changed in the schema YAML
        kept = [("east", t, "dimension", r, "relations", "") for t, r in PLANS["east"].items()]
        out, _ = self.run_route(Spark(plan={"nation": "merge"}, plan_rows=kept), hints={"nation": "fact"})
        self.assertEqual(out["planned_sources"], ["east", "west"])
        hinted = [("east", t, "fact" if t == "nation" else "dimension", r, "hint" if t == "nation" else "relations",
                   "") for t, r in PLANS["east"].items()]
        out, _ = self.run_route(Spark(plan={"nation": "merge"}, plan_rows=hinted), hints={"nation": "fact"})
        self.assertEqual(out["planned_sources"], ["west"])   # the same hints: kept
        out, _ = self.run_route(Spark(plan={"nation": "merge"}, plan_rows=hinted))
        self.assertEqual(out["planned_sources"], ["east", "west"])   # the hint dropped

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
        self.assertIsNone(schema)   # written as SQL (CREATE OR REPLACE TABLE ... AS), so either engine runs it
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
        self.assertIn("part", calls["released"])
        self.assertEqual(out["proposed"]["part"]["failed"], "MERGE failed")
        routes = {r[1]: r[6] for r in spark.written[1]}
        self.assertEqual(routes, {"lines": "pipeline", "nation": "merge", "part": "pipeline"})


class Retry(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_failure_on_a_table_merged_before_keeps_it(self):   # a transient error isn't a reason to switch back
        spark = Spark(plan={"lines": "pipeline", "nation": "merge", "part": "merge"})
        out, _ = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"},
                                    fail=("part",), established=("nation", "part"))
        self.assertEqual(out["merge"], ["nation", "part"])
        self.assertEqual(spark.dropped, [])   # nothing dropped
        self.assertEqual(out["proposed"]["part"]["kept"], "merge, retried next run")
        self.assertEqual({r[1]: r[6] for r in spark.written[1]}["part"], "merge")


class Established(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_plan_saying_merge_isnt_proof(self):   # a first take-over whose run died: the plan says merge, no state
        spark = Spark(plan={"lines": "pipeline", "nation": "merge", "part": "merge"})
        out, calls = self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "merge"},
                                    fail=("part",), established=("nation",))
        self.assertIn("part", calls["released"])   # treated as a first take-over: back to the pipeline
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
        # nation and part are first take-overs: pipeline until their merge succeeds
        self.assertEqual(spark.writes, [{"lines": "pipeline", "nation": "pipeline", "part": "pipeline"},
                                        {"lines": "pipeline", "nation": "merge", "part": "pipeline"}])

    def test_established_tables_are_merge_in_the_first_write(self):
        spark = Spark()
        self.run_route(spark, apply={"lines": "pipeline", "nation": "merge", "part": "pipeline"},
                       established=("nation",))
        self.assertEqual(spark.writes[0]["nation"], "merge")


class Drops(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_failed_take_over_drops_the_table_it_created(self):
        spark = Spark(kinds={"all_part": "MATERIALIZED_VIEW"})   # the pipeline's view, taken over, then the copy fails
        self.run_route(spark, apply={"lines": "pipeline", "nation": "pipeline", "part": "merge"}, fail=("part",),
                       creates=("part",))
        self.assertEqual(spark.dropped, ["all_part"])

    def test_never_a_table_that_was_there(self):   # a table under the name before this run: not this run's to drop
        spark = Spark(kinds={"all_part": "MANAGED"})
        self.run_route(spark, apply={"lines": "pipeline", "nation": "pipeline", "part": "merge"}, fail=("part",))
        self.assertEqual(spark.dropped, [])

    def test_never_a_streaming_table(self):
        spark = Spark(kinds={"all_part": "STREAMING_TABLE"})
        self.run_route(spark, apply={"lines": "pipeline", "nation": "pipeline", "part": "merge"}, fail=("part",))
        self.assertEqual(spark.dropped, [])


class KeyLookups(unittest.TestCase):
    run_route = MergeRoute.run_route

    def test_failed_lookup_counts_as_keyless(self):
        out, calls = self.run_route(Spark(), apply={"lines": "pipeline", "nation": "merge", "part": "pipeline"},
                                    keys=lambda t, s: RuntimeError("UC API timeout") if t == "nation" else True)
        self.assertNotIn("nation", calls["merged"])
        self.assertIn("UC API timeout", out["proposed"]["nation"]["refused"])


class Pipeline(unittest.TestCase):
    def test_merged_tables(self):
        self.assertEqual(merged_tables(Spark(), "main", "shop_silver"), [])   # before the first plan
        self.assertEqual(merged_tables(Spark(plan={"nation": "merge", "lines": "pipeline"}), "main", "shop_silver"),
                         ["nation"])


class ChangedSince(unittest.TestCase):
    def test_flows_with_rows_after_the_state_and_sources_never_merged(self):
        import datetime as dt
        t0 = dt.datetime(2026, 11, 22, 3, 0, tzinfo=dt.timezone.utc)
        ctx = {"held": {"east": {"nation", "part"}, "west": {"nation"}}, "state": {
            ("all_nation", "east"): (5, "i", ["k"], t0), ("all_nation", "west"): (7, "i", ["k"], t0),
            ("all_part", "east"): (3, "i", ["k"], t0)}}
        seen = []

        class S:
            def sql(self, q):
                seen.append(q)
                rows = [R(flow="main_shop_bronze_east_nation_cdc_flow", at=t0 + dt.timedelta(minutes=5)),
                        R(flow="main_shop_bronze_west_nation_cdc_flow", at=t0 - dt.timedelta(minutes=30)),
                        R(flow="main_shop_bronze_east_part_snapshot_flow", at=t0 + dt.timedelta(minutes=1))]
                return R(collect=lambda: rows)
        got = route.changed_since(S(), "main", "shop_bronze_", ctx, ["main.ctl.events_ingest_1"])
        self.assertEqual(set(got), {("nation", "east"), ("part", "east")})   # west's change is older than its state
        self.assertEqual(len(seen), 1)   # one query over the logs
        self.assertIn("endswith(f, '_snapshot_flow') AND status = 'COMPLETED'", seen[0])   # a refresh of no rows
        ctx["state"].pop(("all_nation", "west"))
        self.assertIn(("nation", "west"), route.changed_since(S(), "main", "shop_bronze_", ctx, ["e"]))   # never merged


class EntryPoint(unittest.TestCase):
    def test_arguments(self):
        seen = {}
        fake = types.ModuleType("pyspark.sql")
        fake.SparkSession = R(builder=R(getOrCreate=lambda: "spark"))
        with mock.patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": fake}), \
                mock.patch.object(route, "merge_route", lambda *a, **k: seen.update(args=a, kw=k) or {}):
            route.main(["--catalog", "main", "--prefix", "shop_bronze_", "--target", "shop_silver",
                        "--source-column", "division", "--hints", '{"nation": "lookup"}',
                        "--log", "main.shop_control.merge_log"])
        self.assertEqual(seen["args"], ("spark", "main", "shop_bronze_", "shop_silver", "division",
                                        {"nation": "lookup"}))
        self.assertEqual(seen["kw"], {"log": "main.shop_control.merge_log", "replan": False, "events": None,
                                      "history_target": None, "warehouse": None, "path": "auto"})

    def test_no_subset_of_sources(self):   # a subset would make merge_all drop the other sources' rows
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            route.main(["--catalog", "c", "--prefix", "p_", "--target", "t", "--only", "east"])




class WorkspaceOnce(unittest.TestCase):
    def test_one_client_for_many_threads(self):
        import threading
        made = []

        class W:
            def __init__(self):
                made.append(1)
                import time
                time.sleep(0.01)
                self.tables = "tables"
        fake = types.ModuleType("databricks.sdk")
        fake.WorkspaceClient = W
        ws = route._Workspace()
        with mock.patch.dict(sys.modules, {"databricks": types.ModuleType("databricks"), "databricks.sdk": fake}):
            ts = [threading.Thread(target=lambda: ws.tables) for _ in range(8)]
            [t.start() for t in ts]
            [t.join() for t in ts]
        self.assertEqual(len(made), 1)


HISTORY_CTX = {"names": ["east", "west"], "held": {"east": {"lines", "nation"}, "west": {"lines", "nation"}},
               "cols": {"east": {"lines": [("id", "int"), ("__START_AT", "struct<...>")], "nation": [("id", "int")]},
                       "west": {"lines": [("id", "int")], "nation": [("id", "int")]}}}


class History(unittest.TestCase):
    """history_target: the SCD type 2 tables left on the pipeline are kept by merge_all in their own schema."""

    def test_scd2_tables_from_the_catalog_columns(self):   # one source with __START_AT is enough
        self.assertEqual(route.scd2_tables(None, "main", "shop_bronze_", HISTORY_CTX, ["lines", "nation"]), ["lines"])

    def test_scd2_tables_read_when_the_catalog_has_none(self):
        ctx = {**HISTORY_CTX, "cols": None}
        with mock.patch.object(route.merge, "columns", side_effect=lambda s, fq: [("id", "int")] +
                               ([("__START_AT", "x")] if "lines" in fq else [])) as cols:
            self.assertEqual(route.scd2_tables(None, "main", "shop_bronze_", ctx, ["lines", "nation"]), ["lines"])
        self.assertEqual(cols.call_count, 2)   # one DESCRIBE per table, its first source

    def test_keep_history_merges_only_scd2_tables_into_the_history_schema(self):
        calls = []

        def fake_merge_all(spark, catalog, prefix, target, tables, **kw):
            calls.append((target, tables, kw.get("take_over", False)))
            return {f"all_{tables[0]}": {"east": {"done": "merged"}}}
        with mock.patch.object(route.merge, "load_context", return_value=HISTORY_CTX), \
                mock.patch.object(route.merge, "merge_all", side_effect=fake_merge_all):
            got = route.keep_history(None, "main", "shop_bronze_", "shop_history", HISTORY_CTX, ["lines", "nation"],
                                     "division", "all_{table}", None, "run", None, None, 4)
        self.assertEqual(calls, [("shop_history", ["lines"], False)])   # never takes over a pipeline's view
        self.assertEqual(got, {"all_lines": {"east": "merged"}})

    def test_one_failing_table_is_reported_the_others_go_on(self):
        ctx = {**HISTORY_CTX, "cols": {s: {"lines": [("__START_AT", "x")], "nation": [("__START_AT", "x")]}
                                    for s in ("east", "west")}}

        def fake_merge_all(spark, catalog, prefix, target, tables, **kw):
            if tables == ["lines"]:
                raise RuntimeError("boom\nmore")
            return {"all_nation": {"east": {"done": "copied"}}}
        with mock.patch.object(route.merge, "load_context", return_value=ctx), \
                mock.patch.object(route.merge, "merge_all", side_effect=fake_merge_all):
            got = route.keep_history(None, "main", "shop_bronze_", "shop_history", ctx, ["lines", "nation"],
                                     "division", "all_{table}", None, "run", None, None, 4)
        self.assertEqual(got, {"all_lines": {"error": "boom"}, "all_nation": {"east": "copied"}})

    def test_entry_point_passes_history_target(self):
        with mock.patch.object(route, "merge_route", return_value={}) as mr, \
                mock.patch.dict(sys.modules, {"pyspark.sql": types.SimpleNamespace(SparkSession=mock.MagicMock())}):
            route.main(["--catalog", "main", "--prefix", "p_", "--target", "t", "--history-target", "h"])
        self.assertEqual(mr.call_args.kwargs["history_target"], "h")


if __name__ == "__main__":
    unittest.main()
