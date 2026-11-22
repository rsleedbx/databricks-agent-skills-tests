"""Offline checks of lfc_medallion's merge_all with a fake spark that records each statement and answers the catalog,
history and state queries for two sources: the first read copies, a later one MERGEs the change feed since the
version read, an unchanged source is left alone, a full refresh copies again, a new bronze column is added, an SCD
type 2 key gets __START_AT, a table with no key is refused, and stack_all leaves the merged tables out.

    python3 -m unittest skills/lfc-medallion/tests/test_merge.py
"""
import json
import os
import re
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import evidence, merge_all, stack_all
from lfc_medallion.merge import STATE, merge_sql

R = types.SimpleNamespace
COLS = [("n_nationkey", "int"), ("n_name", "string")]


class Spark:
    """Sources east and west under prefix shop_bronze_, each with `nation`; target schema shop_silver."""

    def __init__(self, versions=None, state=None, history=None, cols=None, keys=("n_nationkey",), targets=(),
                 ids=None, tables=None, gaps=None, views=()):
        self.versions = versions or {"east": 5, "west": 7}
        # state: (target, source) -> version, or (version, table id); ids: source -> its table's id now
        self.ids = ids or {}
        self.state = {k: v if isinstance(v, tuple) else (v, self.ids.get(k[1], f"id-{k[1]}"))
                      for k, v in (state or {}).items()}
        self.tables = tables or {}   # source -> its tables (default: nation)
        self.gaps = gaps or {}   # source -> versions log retention dropped
        self.views = set(views)   # targets that are still a pipeline's materialized views
        self.history = history or {}   # source -> [(version, operation, params, metrics)]
        self.cols = cols or {"east": COLS, "west": COLS}
        self.keys = list(keys)
        self.targets = set(targets)
        self.target_cols = {"all_nation": [("division", "string")] + COLS}
        self.done = []   # the statements that change something
        self.seen = ()   # the sources information_schema.columns returns (none: SHOW TABLES for every source)
        self.lag = set()   # (source, table) information_schema doesn't show yet

    def sql(self, q):
        rows = []
        quoted = re.findall(r"`([^`]+)`", q)
        if q.startswith("SHOW SCHEMAS"):
            rows = [R(databaseName=f"shop_bronze_{s}") for s in self.versions]
        elif q.startswith("SHOW TABLES IN") and "shop_bronze_" in q:
            source = quoted[1][len("shop_bronze_"):]
            rows = [R(tableName=t) for t in self.tables.get(source, ["nation"])]
        elif "information_schema.columns" in q:   # catalog_columns: only the sources it can see
            rows = [R(table_schema=f"shop_bronze_{src}", table_name=t, column_name=c, full_data_type=ty)
                    for src in self.seen for t in self.tables.get(src, ["nation"]) if (src, t) not in self.lag
                    for c, ty in self.cols[src]]
        elif "information_schema.tables" in q:   # the target schema's tables and their kinds
            rows = [R(table_name=t, table_type="MANAGED") for t in self.targets] + \
                   [R(table_name=v, table_type="MATERIALIZED_VIEW") for v in self.views]
        elif q.startswith("SHOW TABLES IN") and "LIKE" in q:   # release(): is there a state table
            rows = [R(tableName=STATE)] if self.state else []
        elif q.startswith("SELECT DISTINCT target_table"):
            rows = [R(target_table=t) for t, _ in self.state]
        elif q.startswith("SHOW TABLES IN"):
            rows = [R(tableName=t) for t in self.targets]
        elif q.startswith("SHOW TBLPROPERTIES"):
            rows = [R(key=evidence.KEYS_PROPERTY, value=json.dumps(self.keys))]
        elif q.startswith("DESCRIBE DETAIL"):
            source = quoted[1][len("shop_bronze_"):]
            rows = [R(id=self.ids.get(source, f"id-{source}"))]
        elif q.startswith("DESCRIBE TABLE") and STATE in q:
            names = ("target_table", "source", "version", "table_id", "updated_at", "key_columns")
            rows = [R(col_name=c, data_type="") for c in names]
        elif q.startswith("DESCRIBE TABLE"):
            schema, table = quoted[1], quoted[2]
            source = schema[len("shop_bronze_"):]
            cols = self.target_cols.get(table, []) if schema == "shop_silver" else self.cols[source]
            rows = [R(col_name=c, data_type=t) for c, t in cols] + [R(col_name="", data_type="")]
        elif q.startswith("SELECT MAX(version)"):
            rows = [R(v=self.versions[quoted[1][len("shop_bronze_"):]])]
        elif q.startswith("SELECT version, operation, operationParameters"):
            since = int(q.rsplit("version > ", 1)[1])
            source = quoted[1][len("shop_bronze_"):]
            given = {h[0]: h for h in self.history.get(source, [])}   # other versions: no data touched
            history = [given.get(v, (v, "OPTIMIZE", {}, {})) for v in range(1, self.versions[source] + 1)
                       if v not in self.gaps.get(source, ())]
            rows = [R(version=h[0], operation=h[1], operationParameters=h[2],
                      operationMetrics=h[3] if len(h) > 3 else {}) for h in history if h[0] > since]
        elif q.startswith("SELECT target_table, source, version"):
            rows = [R(target_table=t, source=s, version=v, table_id=i) for (t, s), (v, i) in self.state.items()]
        elif q.startswith("CREATE TABLE IF NOT EXISTS"):
            pass
        else:
            self.done.append(q)
            if q.startswith("CREATE TABLE"):
                self.targets.add(quoted[2])
            altered = re.match(r"ALTER TABLE `[^`]+`\.`[^`]+`\.`([^`]+)` ALTER COLUMN `([^`]+)` TYPE (.+)$", q)
            if altered:   # as the table would: the next DESCRIBE shows the new type
                t, c, ty = altered.groups()
                self.target_cols[t] = [(n, ty.lower() if n == c else x) for n, x in self.target_cols[t]]
            if q.startswith(f"MERGE INTO `main`.`shop_silver`.`{STATE}`"):
                for t, s, v, i in re.findall(r"\('([^']+)', '([^']+)', (\d+), '([^']+)', (?:'[^']*'|NULL)\)", q):
                    self.state[(t, s)] = (int(v), i)
        return R(collect=lambda: rows)

    def merges(self):
        return [q for q in self.done if q.startswith("MERGE INTO `main`.`shop_silver`.`all_nation`")]


def run(spark, **kw):
    return merge_all(spark, "main", "shop_bronze_", "shop_silver", ["nation"], source_column="division", **kw)


class FirstAndLater(unittest.TestCase):
    def test_first_read_copies_every_source(self):
        spark = Spark()
        report = run(spark)
        self.assertEqual(report, {"all_nation": {"east": {"done": "copied", "version": 5},
                                                 "west": {"done": "copied", "version": 7}}})
        created = next(q for q in spark.done if q.startswith("CREATE TABLE"))   # after the one catalog query
        self.assertTrue(created.startswith("CREATE TABLE `main`.`shop_silver`.`all_nation` (`division` STRING, "
                                           "`n_nationkey` int, `n_name` string) TBLPROPERTIES"))
        self.assertIn("'delta.enableChangeDataFeed' = 'true'", created)
        m = spark.merges()
        self.assertEqual(len(m), 2)
        self.assertIn("FROM `main`.`shop_bronze_east`.`nation` VERSION AS OF 5", m[0])   # the version saved
        self.assertNotIn("table_changes", m[0])
        versions = {k: v for k, (v, _) in spark.state.items()}
        self.assertEqual(versions, {("all_nation", "east"): 5, ("all_nation", "west"): 7})

    def test_later_read_merges_the_change_feed(self):
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})]},
                      targets={"all_nation"})
        report = run(spark)
        self.assertEqual(report["all_nation"]["east"], {"done": "merged", "version": 9})
        self.assertEqual(report["all_nation"]["west"], {"done": "unchanged", "version": 7})
        (m,) = spark.merges()
        self.assertIn("table_changes('main.shop_bronze_east.nation', 6, 9)", m)
        self.assertIn("WHERE _change_type != 'update_preimage'", m)
        self.assertIn("ON t.`division` = c.`division` AND t.`n_nationkey` = c.`n_nationkey`", m)
        self.assertIn("WHEN MATCHED AND c.`__lfc_change` = 'delete' THEN DELETE", m)
        self.assertIn("WHEN MATCHED THEN UPDATE SET t.`n_name` = c.`n_name`", m)
        self.assertNotIn("CREATE TABLE `main`", " ".join(spark.done))   # the target exists
        self.assertEqual(spark.state[("all_nation", "east")][0], 9)

    def test_changed_sources_share_one_merge_and_one_state_write(self):   # a few statements per table, not per source
        spark = Spark(versions={"east": 9, "west": 8}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})],
                               "west": [(8, "MERGE", {}, {"numTargetRowsInserted": "2"})]},
                      targets={"all_nation"})
        report = run(spark)
        self.assertEqual({s: r["done"] for s, r in report["all_nation"].items()}, {"east": "merged", "west": "merged"})
        (m,) = spark.merges()
        self.assertIn("table_changes('main.shop_bronze_east.nation', 6, 9)", m)
        self.assertIn(" UNION ALL ", m)
        self.assertIn("table_changes('main.shop_bronze_west.nation', 8, 8)", m)
        saves = [q for q in spark.done if q.startswith(f"MERGE INTO `main`.`shop_silver`.`{STATE}`")]
        self.assertEqual(len(saves), 1)
        self.assertEqual({k: v for k, (v, _) in spark.state.items()}, {("all_nation", "east"): 9, ("all_nation", "west"): 8})

    def test_only_the_changed_sources_are_read(self):   # merge_route found west quiet in the event logs
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})]},
                      targets={"all_nation"})
        report = run(spark, changed={("nation", "east")})
        self.assertEqual(report["all_nation"]["west"], {"done": "unchanged", "version": 7})
        self.assertEqual(report["all_nation"]["east"]["done"], "merged")
        self.assertFalse([q for q in spark.done if "shop_bronze_west" in q])   # no read of west at all

    def test_new_versions_without_rows_written_are_unchanged(self):   # an ingestion update touches every table
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(6, "MERGE", {}, {"numTargetRowsInserted": "0"}),
                                        (7, "DELETE", {}, {"numDeletedRows": "0"}),
                                        (8, "MERGE", {}, {"numOutputRows": "0"}), (9, "OPTIMIZE", {}, {})]},
                      targets={"all_nation"})
        report = run(spark)
        self.assertEqual(report["all_nation"]["east"], {"done": "unchanged", "version": 9})
        self.assertEqual(spark.merges(), [])
        self.assertEqual(spark.state[("all_nation", "east")][0], 9)   # read up to 9: not looked at again

    def test_unknown_operation_or_missing_metrics_reads_the_feed(self):
        for h in ([(8, "SOMETHING NEW", {}, {})], [(8, "MERGE", {}, {})]):
            spark = Spark(versions={"east": 9, "west": 7},
                          state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, history={"east": h},
                          targets={"all_nation"})
            self.assertEqual(run(spark)["all_nation"]["east"]["done"], "merged", h)

    def test_restore_copies_again(self):
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "RESTORE", {}, {})]}, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")

    def test_unreadable_change_feed_copies(self):
        class Vacuumed(Spark):
            def sql(self, q):
                if "table_changes(" in q:
                    raise RuntimeError("[DELTA_CHANGE_DATA_FILE_NOT_FOUND] the change data was vacuumed\nmore")
                return super().sql(q)
        spark = Vacuumed(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                         history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})]},
                         targets={"all_nation"})
        done = run(spark)["all_nation"]["east"]["done"]
        self.assertTrue(done.startswith("copied (change feed unreadable: [DELTA_CHANGE_DATA_FILE_NOT_FOUND]"), done)
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND t.`division` = 'east' THEN DELETE", spark.merges()[-1])
        self.assertEqual(spark.state[("all_nation", "east")][0], 9)

    def test_removed_source_goes(self):   # north was read before; its schema is gone now
        spark = Spark(versions={"east": 5, "west": 7}, targets={"all_nation"},
                      state={("all_nation", "east"): 5, ("all_nation", "west"): 7, ("all_nation", "north"): 3})
        report = run(spark)
        self.assertEqual(report["all_nation"]["north"], {"done": "removed"})
        self.assertIn("DELETE FROM `main`.`shop_silver`.`all_nation` WHERE `division` IN ('north')", spark.done)
        self.assertTrue(any(q.startswith(f"DELETE FROM `main`.`shop_silver`.`{STATE}`") and "('north')" in q
                            for q in spark.done))

    def test_full_refresh_copies_again(self):
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "CREATE OR REPLACE TABLE", {})]}, targets={"all_nation"})
        report = run(spark)
        self.assertEqual(report["all_nation"]["east"]["done"], "copied")
        (m,) = spark.merges()
        self.assertNotIn("table_changes", m)
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND t.`division` = 'east' THEN DELETE", m)   # one atomic MERGE
        self.assertFalse(any(q.startswith("DELETE FROM `main`.`shop_silver`.`all_nation`") for q in spark.done))

    def test_overwrite_counts_as_a_refresh(self):
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(6, "WRITE", {"mode": "Overwrite"})]}, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")


class Recreated(unittest.TestCase):
    def test_new_table_id_copies(self):   # dropped and recreated: a new id, the version even back at 0
        state = {("all_nation", "east"): (5, "old"), ("all_nation", "west"): 7}
        spark = Spark(versions={"east": 2, "west": 7}, state=state, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")
        self.assertEqual(spark.state[("all_nation", "east")], (2, "id-east"))

    def test_lower_version_without_an_id_copies(self):   # a state from before ids were kept
        state = {("all_nation", "east"): (5, None), ("all_nation", "west"): 7}
        spark = Spark(versions={"east": 2, "west": 7}, state=state, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")

    def test_not_matched_by_source_changes_count(self):
        h = [(8, "MERGE", {}, {"numTargetRowsInserted": "0", "numTargetRowsNotMatchedBySourceDeleted": "3",
                               "numTargetRowsCopied": "40"})]
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": h}, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "merged")

    def test_copied_rows_alone_arent_a_change(self):
        h = [(8, "MERGE", {}, {"numTargetRowsInserted": "0", "numTargetRowsCopied": "40"})]
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": h}, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "unchanged")


class Boundaries(unittest.TestCase):
    def later(self, **kw):
        return Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                     targets={"all_nation"}, **kw)

    def test_history_gap_copies(self):   # log retention dropped versions 6 and 7
        spark = self.later(gaps={"east": (6, 7)})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")

    def test_replace_by_other_names(self):
        for op, params in (("REPLACE TABLE AS SELECT", {}), ("CREATE TABLE AS SELECT", {}),
                           ("STREAMING UPDATE", {"outputMode": "Complete"})):
            spark = self.later(history={"east": [(8, op, params, {"numOutputRows": "3"})]})
            self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied", op)

    def test_table_id_alone(self):   # recreated, its history already past (9) or at (5) the version saved
        for now in (9, 5):
            state = {("all_nation", "east"): (5, "old"), ("all_nation", "west"): 7}
            spark = Spark(versions={"east": now, "west": 7}, state=state, targets={"all_nation"})
            self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied", now)
            (m,) = [q for q in spark.merges() if "shop_bronze_east" in q]
            self.assertNotIn("table_changes", m)
            self.assertIn("WHEN NOT MATCHED BY SOURCE", m)
            self.assertEqual(spark.state[("all_nation", "east")], (now, "id-east"))

    def test_target_dropped_by_hand_copies_every_source(self):
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7})   # state, but no target
        report = run(spark)
        self.assertEqual({s: x["done"] for s, x in report["all_nation"].items()}, {"east": "copied", "west": "copied"})


class Switch(unittest.TestCase):
    """Moving a table between the pipeline and this route, both ways."""

    def test_pipeline_view_is_refused_without_take_over(self):
        spark = Spark(views={"all_nation"})
        with self.assertRaisesRegex(ValueError, "is a view .* take_over=True"):
            run(spark)
        self.assertFalse(any(q.startswith("DROP") for q in spark.done))

    def test_take_over_drops_the_view_and_copies(self):
        spark = Spark(views={"all_nation"}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7})
        report = run(spark, take_over=True)
        self.assertIn("DROP MATERIALIZED VIEW `main`.`shop_silver`.`all_nation`", spark.done)
        self.assertTrue(any(q.startswith("CREATE TABLE `main`.`shop_silver`.`all_nation`") for q in spark.done))
        self.assertEqual({d: x["done"] for d, x in report["all_nation"].items()}, {"east": "copied", "west": "copied"})

    def test_release_hands_tables_back(self):
        from lfc_medallion import release
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation", "all_x"})
        self.assertEqual(release(spark, "main", "shop_silver", ["nation", "x"]), ["all_nation"])   # only what it wrote
        self.assertIn("DROP TABLE IF EXISTS `main`.`shop_silver`.`all_nation`", spark.done)
        self.assertNotIn("all_x", " ".join(spark.done))
        self.assertTrue(any(q.startswith(f"DELETE FROM `main`.`shop_silver`.`{STATE}`") for q in spark.done))

    def test_merged_targets(self):   # what merge_route reads to tell a table merged before from a first take-over
        from lfc_medallion.merge import merged_targets
        self.assertEqual(merged_targets(Spark(), "main", "shop_silver"), set())   # no state table
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7, ("all_part", "east"): 2})
        queries = []
        sql = spark.sql
        spark.sql = lambda q: (queries.append(q), sql(q))[1]
        self.assertEqual(merged_targets(spark, "main", "shop_silver"), {"all_nation", "all_part"})
        self.assertTrue(any(q.startswith("SHOW TABLES IN") and STATE in q for q in queries))

    def test_release_without_state(self):
        from lfc_medallion import release
        self.assertEqual(release(Spark(), "main", "shop_silver", ["nation"]), [])


class OnRequest(unittest.TestCase):
    """routes_to_apply: a table switches only when an operator tags it; otherwise the plan only proposes."""

    class Catalog:
        def __init__(self, kinds, tags):
            self.kinds, self.tags = kinds, tags

        def sql(self, q):
            if "information_schema.table_tags" in q:
                rows = [R(table_name=t, tag_value=v) for t, v in self.tags.items()]
            else:
                rows = [R(table_name=t, table_type=k) for t, k in self.kinds.items()]
            return R(collect=lambda: rows)

    def test_tag_wins_untagged_stay_new_follow_the_plan(self):
        from lfc_medallion import routes_to_apply
        spark = self.Catalog({"all_nation": "MANAGED", "all_region": "MANAGED", "all_lines": "MATERIALIZED_VIEW"},
                             {"all_nation": "Pipeline"})
        planned = {"nation": "merge", "region": "pipeline", "lines": "merge", "part": "merge"}
        apply, proposed = routes_to_apply(spark, "main", "shop_silver", planned)
        self.assertEqual(apply, {"nation": "pipeline", "region": "merge", "lines": "pipeline", "part": "merge"})
        self.assertEqual(proposed, {"region": {"current": "merge", "planned": "pipeline"},
                                    "lines": {"current": "pipeline", "planned": "merge"}})

    def test_missing_object_keeps_the_previous_route(self):   # the run failed between release and the new view
        from lfc_medallion import routes_to_apply
        spark = self.Catalog({}, {})
        apply, proposed = routes_to_apply(spark, "main", "s", {"nation": "merge", "part": "merge"},
                                          previous={"nation": "pipeline"})
        self.assertEqual(apply, {"nation": "pipeline", "part": "merge"})
        self.assertEqual(proposed, {"nation": {"current": "pipeline", "planned": "merge"}})

    def test_unknown_tag_value_is_ignored(self):
        from lfc_medallion import routes_to_apply
        spark = self.Catalog({"all_nation": "MANAGED"}, {"all_nation": "maybe"})
        self.assertEqual(routes_to_apply(spark, "main", "s", {"nation": "pipeline"})[0], {"nation": "merge"})


class Removed(unittest.TestCase):
    def deletes(self, spark):
        return [q for q in spark.done if q.startswith("DELETE FROM `main`.`shop_silver`.`all_nation`")]

    def test_left_out_by_only(self):
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"})
        report = run(spark, only="east")
        self.assertEqual(report["all_nation"]["west"], {"done": "removed"})
        want = "DELETE FROM `main`.`shop_silver`.`all_nation` WHERE `division` IN ('west')"
        self.assertEqual(self.deletes(spark), [want])

    def test_schema_still_there_table_gone(self):
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"},
                      tables={"west": ["orders"]})
        report = run(spark)
        self.assertEqual(report["all_nation"]["west"], {"done": "removed"})
        self.assertNotIn("east", " ".join(self.deletes(spark)))

    def test_a_source_the_catalog_query_misses_is_not_gone(self):   # information_schema lags a new schema
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"})
        spark.seen = ("east",)
        report = run(spark)
        self.assertEqual(self.deletes(spark), [])
        self.assertEqual(report["all_nation"]["west"], {"done": "unchanged", "version": 7})

    def test_a_table_the_catalog_query_lags_is_not_gone(self):   # its source is listed, the table not yet
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"},
                      tables={"west": ["nation", "orders"]})
        spark.seen, spark.lag = ("east", "west"), {("west", "nation")}
        run(spark)
        self.assertEqual(self.deletes(spark), [])

    def test_a_failed_check_deletes_nothing(self):   # SHOW TABLES failing for another reason than a dropped schema
        class Denied(Spark):
            def sql(self, q):
                if q.startswith("SHOW TABLES IN `main`.`shop_bronze_west`"):
                    raise RuntimeError("PERMISSION_DENIED")
                return super().sql(q)
        spark = Denied(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"},
                       tables={"west": ["nation"]})
        spark.seen, spark.lag = ("east", "west"), {("west", "nation")}
        with self.assertRaisesRegex(RuntimeError, "PERMISSION_DENIED"):
            run(spark)
        self.assertEqual(self.deletes(spark), [])

    def test_nothing_deleted_without_a_target(self):   # no source has the table, and its target was dropped by hand
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      tables={"east": ["orders"], "west": ["orders"]})
        run(spark)
        self.assertEqual(self.deletes(spark), [])


class PartWay(unittest.TestCase):
    def test_a_failed_copy_keeps_the_state_of_what_finished(self):   # the next run doesn't copy east again
        class Fails(Spark):
            def sql(self, q):
                if q.startswith("MERGE INTO `main`.`shop_silver`.`all_nation`") and "'west'" in q:
                    raise RuntimeError("boom")
                return super().sql(q)
        spark = Fails()
        with self.assertRaisesRegex(RuntimeError, "boom"):
            run(spark)
        self.assertIn(("all_nation", "east"), spark.state)
        self.assertNotIn(("all_nation", "west"), spark.state)


class Shapes(unittest.TestCase):
    def test_new_bronze_column_is_added(self):
        cols = {"east": COLS + [("n_comment", "string")], "west": COLS}
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      cols=cols, history={"east": [(8, "MERGE", {}, {"numTargetRowsInserted": "1"})]},
                      targets={"all_nation"})
        run(spark)
        self.assertIn("ALTER TABLE `main`.`shop_silver`.`all_nation` ADD COLUMNS (`n_comment` string)", spark.done)

    def widened(self, target, source):
        """The statements _ensure_target runs for a target column of type `target` and a source's `source`."""
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      cols={s: [("n_nationkey", "int"), ("n_name", source)] for s in ("east", "west")},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsInserted": "1"})]}, targets={"all_nation"})
        spark.target_cols["all_nation"] = [("division", "string"), ("n_nationkey", "int"), ("n_name", target)]
        run(spark)
        return [q for q in spark.done if q.startswith("ALTER TABLE")]

    def test_wider_source_widens_the_target_in_place(self):
        tgt = "ALTER TABLE `main`.`shop_silver`.`all_nation`"
        self.assertEqual(self.widened("int", "bigint"), [f"{tgt} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
                                                         f"{tgt} ALTER COLUMN `n_name` TYPE BIGINT"])
        self.assertEqual(self.widened("smallint", "int")[-1], f"{tgt} ALTER COLUMN `n_name` TYPE INT")
        self.assertEqual(self.widened("decimal(10,2)", "decimal(12,4)")[-1], f"{tgt} ALTER COLUMN `n_name` TYPE DECIMAL(12,4)")
        self.assertEqual(self.widened("decimal(10,2)", "decimal(12,1)")[-1], f"{tgt} ALTER COLUMN `n_name` TYPE DECIMAL(13,2)")  # 11 integer digits, scale 2

    def test_a_wider_type_in_another_source_widens_the_target(self):   # same names, another type
        spark = Spark(versions={"east": 9, "west": 9}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      cols={"east": [("n_nationkey", "int"), ("n_name", "int")],
                            "west": [("n_nationkey", "int"), ("n_name", "bigint")]},
                      history={s: [(8, "MERGE", {}, {"numTargetRowsInserted": "1"})] for s in ("east", "west")},
                      targets={"all_nation"})
        spark.target_cols["all_nation"] = [("division", "string"), ("n_nationkey", "int"), ("n_name", "int")]
        run(spark)
        self.assertIn("ALTER TABLE `main`.`shop_silver`.`all_nation` ALTER COLUMN `n_name` TYPE BIGINT", spark.done)

    def test_date_into_timestamp_ntz_turns_its_feature_on(self):
        tgt = "ALTER TABLE `main`.`shop_silver`.`all_nation`"
        props = "('delta.enableTypeWidening' = 'true', 'delta.feature.timestampNtz' = 'supported')"
        self.assertEqual(self.widened("date", "timestamp_ntz"),
                         [f"{tgt} SET TBLPROPERTIES {props}", f"{tgt} ALTER COLUMN `n_name` TYPE TIMESTAMP_NTZ"])

    def test_narrower_or_same_source_changes_nothing(self):
        self.assertEqual(self.widened("bigint", "int"), [])
        self.assertEqual(self.widened("decimal(12,4)", "decimal(10,2)"), [])
        self.assertEqual(self.widened("string", "varchar(20)"), [])

    def test_widening_delta_cant_do_in_place_is_an_error(self):
        for target, source in (("int", "string"), ("date", "int"), ("timestamp", "timestamp_ntz")):
            with self.assertRaisesRegex(ValueError, "all_nation.*`n_name`.*isn't a change Delta makes in place"):
                self.widened(target, source)

    def test_scd2_key_gets_start_at(self):
        cols = {s: COLS + [("__START_AT", "timestamp"), ("__END_AT", "timestamp")] for s in ("east", "west")}
        spark = Spark(cols=cols)
        run(spark)
        self.assertIn("AND t.`__START_AT` = c.`__START_AT`", spark.merges()[0])

    def test_no_key_is_refused_unless_given(self):
        with self.assertRaisesRegex(ValueError, "no primary key recorded"):
            run(Spark(keys=()))
        spark = Spark(keys=())
        run(spark, keys={"nation": ["n_nationkey"]})
        self.assertIn("ON t.`division` = c.`division` AND t.`n_nationkey` = c.`n_nationkey`", spark.merges()[0])

    def test_key_only_table_has_no_update_clause(self):
        sql = merge_sql("`c`.`s`.`t`", "`c`.`b`.`t`", "east", "division", ["k"], ["k"], 1, 2)
        self.assertNotIn("UPDATE SET", sql)
        self.assertIn("INSERT (`division`, `k`) VALUES (c.`division`, c.`k`)", sql)

    def test_replacement_in_one_version_keeps_the_insert(self):
        sql = merge_sql("`c`.`s`.`t`", "`c`.`b`.`t`", "east", "division", ["k"], ["k", "v"], 1, 2)
        self.assertIn("ORDER BY _commit_version DESC, CASE _change_type WHEN 'delete' THEN 1 ELSE 0 END", sql)


class StackSkips(unittest.TestCase):
    def test_merged_tables_left_out_of_the_pipeline(self):
        class Dp:
            def __init__(self):
                self.names = []

            def materialized_view(self, name, comment):
                self.names.append(name)
                return lambda fn: fn

        class Bronze:
            def sql(self, q):
                if q.startswith("SHOW SCHEMAS"):
                    rows = [R(databaseName="shop_bronze_east")]
                else:
                    rows = [R(tableName=t) for t in ("lines", "orders", "nation")]
                return R(collect=lambda: rows)
        dp = Dp()
        defined = stack_all(dp, Bronze(), "main", "shop_bronze_", skip=["nation"])
        self.assertEqual(sorted(defined), ["all_lines", "all_orders"])
        self.assertEqual(sorted(dp.names), ["all_lines", "all_orders"])


if __name__ == "__main__":
    unittest.main()
