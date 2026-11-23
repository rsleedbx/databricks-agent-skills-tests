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
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-change-source", "scripts"))
from lfc_medallion import evidence, merge, merge_all, stack_all
from lfc_medallion import merge as merge_mod
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
        self.target_version, self.target_changes, self.reads = 3, {}, []   # the target's head, its feed's rows

    def sql(self, q):
        rows = []
        quoted = re.findall(r"`([^`]+)`", q)
        if q.startswith("SHOW SCHEMAS"):
            rows = [R(databaseName=f"shop_bronze_{s}") for s in self.versions]
        elif q.startswith("SHOW TABLES IN") and "shop_bronze_" in q:
            source = quoted[1][len("shop_bronze_"):]
            rows = [R(tableName=t) for t in self.tables.get(source, ["nation"])]
        elif q.startswith("SELECT version FROM (DESCRIBE HISTORY `main`.`shop_silver`."):   # a target's head
            rows = [R(version=self.target_version)]
        elif "FROM table_changes('main.shop_silver." in q:   # the target's feed after its MERGE
            self.reads.append(q)
            rows = [R(s=s, i=i, u=u, d=d) for s, (i, u, d) in self.target_changes.items()]
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
            names = ("target_table", "source", "version", "table_id", "updated_at", "key_columns", "snapshots")
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
            if q.startswith("MERGE INTO `main`.`shop_silver`.`all_"):
                self.target_version += 1
            if q.startswith(f"MERGE INTO `main`.`shop_silver`.`{STATE}`"):
                for t, s, v, i in re.findall(r"\('([^']+)', '([^']+)', (\d+), '([^']+)', (?:'[^']*'|NULL), (?:'[^']*'|NULL)\)", q):
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

    def test_counts_come_from_the_targets_feed_not_a_second_read_of_the_sources(self):
        spark = Spark(versions={"east": 9, "west": 8}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})],
                               "west": [(8, "MERGE", {}, {"numTargetRowsInserted": "2"})]},
                      targets={"all_nation"})
        spark.target_changes = {"east": (0, 1, 0), "west": (2, 0, 0)}
        logged = []
        with mock.patch.object(merge_mod, "_log_rows", lambda spark, fq, rows: logged.extend(rows)):
            run(spark)
        self.assertEqual(spark.reads, ["SELECT `division` AS s, SUM(CASE WHEN _change_type = 'insert' THEN 1 ELSE 0 END)"
                                       " AS i, SUM(CASE WHEN _change_type = 'update_postimage' THEN 1 ELSE 0 END) AS u, "
                                       "SUM(CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END) AS d FROM "
                                       "table_changes('main.shop_silver.all_nation', 4, 4) GROUP BY 1"])   # its version
        sources_read = [q for q in spark.done if "table_changes('main.shop_bronze_" in q]
        self.assertEqual(len(sources_read), 1)   # each source's feed only inside the one MERGE
        self.assertEqual({r[2]: r[6:9] for r in logged}, {"east": (0, 1, 0), "west": (2, 0, 0)})

    def test_a_forced_split_runs_k_merges_and_is_recorded(self):   # the optimizer's k: here 2 MERGEs at once
        from lfc_medallion import paths
        spark = Spark(versions={"east": 9, "west": 8}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})],
                               "west": [(8, "MERGE", {}, {"numTargetRowsInserted": "2"})]},
                      targets={"all_nation"})
        planner = paths.Planner({"spark": spark}, forced="spark:2", rows={("nation", "east"): 1, ("nation", "west"): 2})
        report = run(spark, planner=planner)
        self.assertEqual({s: r["done"] for s, r in report["all_nation"].items()}, {"east": "merged", "west": "merged"})
        merges = spark.merges()
        self.assertEqual(len(merges), 2)
        self.assertTrue(all("UNION ALL" not in m for m in merges))   # one source each
        (rec,) = planner.records
        self.assertEqual((rec["engine"], rec["k"], rec["sources"], rec["change_rows"], rec["forced"]),
                         ("spark", 2, 2, 3, True))

    def test_a_large_share_of_changes_is_copied_not_merged(self):   # the planner's recompute, by source
        from lfc_medallion import paths
        spark = Spark(versions={"east": 9, "west": 8}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})],
                               "west": [(8, "MERGE", {}, {"numTargetRowsInserted": "2"})]},
                      targets={"all_nation"})
        real = spark.sql
        counts = [("east", 60000), ("west", 9)]   # each source's rows in the target
        spark.sql = lambda q: R(collect=lambda: counts) if " GROUP BY 1" in q and q.startswith(
            "SELECT `division`, COUNT(*) FROM `main`.`shop_silver`.`all_nation`") else real(q)
        planner = paths.Planner({"spark": spark}, rows={("nation", "east"): 50000, ("nation", "west"): 2})
        report = run(spark, planner=planner)
        self.assertEqual(report["all_nation"]["east"]["done"], "copied (THRESHOLD_LARGE_CHANGES_RECOMPUTE)")
        self.assertEqual(report["all_nation"]["west"]["done"], "merged")
        feeds = [m for m in spark.merges() if "table_changes(" in m]
        self.assertEqual(len(feeds), 1)   # one MERGE of the change feeds, west's only: east was copied whole
        self.assertNotIn("shop_bronze_east", feeds[0])
        (rec,) = planner.records
        self.assertEqual(rec["sources"], 1)
        self.assertRegex(rec["fingerprint"], r"^[0-9a-f]{12}$")   # the table's key and columns

    def test_a_merge_that_commits_nothing_counts_nothing(self):   # no new version: not the commit before it
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(8, "MERGE", {}, {"numTargetRowsMatchedUpdated": "1"})]}, targets={"all_nation"})
        real = spark.sql
        spark.sql = lambda q: R(collect=lambda: []) if q.startswith("MERGE INTO `main`.`shop_silver`.`all_") else real(q)
        logged = []
        with mock.patch.object(merge_mod, "_log_rows", lambda spark, fq, rows: logged.extend(rows)):
            run(spark)
        self.assertEqual(spark.reads, [])
        self.assertEqual([r[6:9] for r in logged], [(0, 0, 0)])

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

    def test_keep_gone_keeps_a_gone_sources_rows(self):   # a history schema keeps what it has
        spark = Spark(state={("all_nation", "east"): 5, ("all_nation", "west"): 7}, targets={"all_nation"})
        report = run(spark, only="east", keep_gone=True)
        self.assertEqual(self.deletes(spark), [])
        self.assertNotIn("west", report["all_nation"])

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
        self.assertIn("ORDER BY _seq DESC", sql)   # the version, then the insert after the delete: the insert wins
        self.assertIn("named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END)", sql)


def _h(version, operation, ts="2026-11-22T17:00:00.000Z"):
    """A DESCRIBE HISTORY row for refresh_plan / generations."""
    return R(version=version, operation=operation, operationParameters={}, timestamp=ts)


# the history of a probe run with two full refreshes (lfc-downstream-history sqlserver-scd2-refresh-probe.sh REFRESHES=2,
# 2026-11-22): load at 2, the gap to 8, a refresh at 9 reloaded by 12, a change at 13, a second refresh at 15 reloaded at 18
TWO_REFRESHES = [_h(0, "CREATE TABLE", "T0"), _h(1, "DLT SETUP"), _h(2, "MERGE"), _h(3, "MERGE"), _h(4, "OPTIMIZE"),
                 _h(5, "MERGE"), _h(6, "OPTIMIZE"), _h(7, "MERGE"), _h(8, "OPTIMIZE"), _h(9, "DLT REFRESH", "T9"),
                 _h(10, "SET TBLPROPERTIES"), _h(11, "DLT SETUP"), _h(12, "MERGE"), _h(13, "MERGE"), _h(14, "OPTIMIZE"),
                 _h(15, "DLT REFRESH", "T15"), _h(16, "SET TBLPROPERTIES"), _h(17, "DLT SETUP"), _h(18, "MERGE")]


class Scd2AcrossRefresh(unittest.TestCase):
    """An SCD type 2 source across full refreshes: what is read, and that nothing the target has is deleted."""

    def test_dlt_refresh_is_a_reset(self):
        self.assertTrue(evidence.is_reset("DLT REFRESH", {}))

    def test_generations_are_the_first_version_and_each_refresh(self):
        self.assertEqual(merge.generations(TWO_REFRESHES), [(0, "T0"), (9, "T9"), (15, "T15")])
        gens = merge.generations(TWO_REFRESHES)
        self.assertEqual(merge.generation_at(gens, 8), (0, "T0"))     # the first load's snapshot rows
        self.assertEqual(merge.generation_at(gens, 12), (9, "T9"))    # the first reload's
        self.assertEqual(merge.generation_at(gens, 18), (15, "T15"))

    def test_plan_reads_up_to_the_version_before_each_refresh(self):
        plan = merge.refresh_plan(TWO_REFRESHES, last=2, now=18)
        # the change feed to 8 (reads after the refresh), the version before the second refresh, then now
        self.assertEqual(plan, {"waiting": False, "steps": [("changes", 2, 8), ("copy", 14), ("copy", 18)]})

    def test_plan_with_one_refresh(self):
        self.assertEqual(merge.refresh_plan(TWO_REFRESHES[:13], last=2, now=12)["steps"], [("changes", 2, 8), ("copy", 12)])

    def test_no_changes_step_when_read_up_to_the_refresh(self):
        self.assertEqual(merge.refresh_plan(TWO_REFRESHES[:13], last=8, now=12)["steps"], [("copy", 12)])

    def test_no_plan_without_a_refresh(self):
        self.assertIsNone(merge.refresh_plan(TWO_REFRESHES, last=9, now=14))

    def test_waiting_while_the_reload_has_no_write(self):
        self.assertTrue(merge.refresh_plan(TWO_REFRESHES[:11], last=2, now=10, now_s=1795366800.0 + 60)["waiting"])

    def test_snapshot_start_filled_cdc_start_kept(self):
        sql = merge.start_sql("cdc", "T9")
        self.assertIn("__cdc_timestamp_value IS NULL THEN named_struct('__cdc_internal_value', "
                      "coalesce(`__START_AT`.__cdc_internal_value, ''), '__cdc_timestamp_value', TIMESTAMP 'T9') ELSE `__START_AT`", sql)
        self.assertEqual(merge.start_sql("timestamp", "T9"), "coalesce(`__START_AT`, TIMESTAMP 'T9') AS `__START_AT`")
        self.assertEqual(merge.start_sql(None, "T9"), "`__START_AT`")   # another shape: as it is

    def test_kind(self):
        # Lakeflow Connect's (SQL Server CDC, 2026-11-22): the time is a timestamp
        self.assertEqual(merge._kind("struct<__cdc_internal_value:string,__cdc_timestamp_value:timestamp>"), "cdc")
        self.assertEqual(merge._kind("STRUCT<__cdc_internal_value: STRING, __cdc_timestamp_value: TIMESTAMP>"), "cdc")
        self.assertEqual(merge._kind("struct<__cdc_internal_value:string,__cdc_timestamp_value:string>"), "cdc_string")
        self.assertIsNone(merge._kind("struct<__cdc_internal_value:string,__cdc_timestamp_value:timestamp,x:int>"))
        self.assertEqual(merge._kind("timestamp"), "timestamp")
        self.assertIsNone(merge._kind("bigint"))

    def test_copy_closes_open_versions_and_deletes_none(self):
        end = merge.close_sql("cdc", "T9", "lfc_medallion: closed by full refresh at version 9")
        sql = merge.merge_into("`c`.`s`.`t`", "SELECT 1", "division", ["k", "__START_AT"], ["k", "v", "__START_AT",
                               "__END_AT"], whole_source="east", close=end)
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND t.`division` = 'east' AND t.`__END_AT` IS NULL THEN UPDATE SET "
                      "t.`__END_AT` = named_struct('__cdc_internal_value', 'lfc_medallion: closed by full refresh at "
                      "version 9', '__cdc_timestamp_value', TIMESTAMP 'T9')", sql)
        self.assertNotIn("NOT MATCHED BY SOURCE AND t.`division` = 'east' THEN DELETE", sql)

    def test_copy_without_close_deletes_as_before(self):
        sql = merge.merge_into("`c`.`s`.`t`", "SELECT 1", "division", ["k"], ["k", "v"], whole_source="east")
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND t.`division` = 'east' THEN DELETE", sql)

    def test_changes_take_the_filled_start(self):
        sql = merge.changes_sql("`c`.`b`.`t`", "east", "division", ["k", "__START_AT"], ["k", "v", "__START_AT"], 2, 8,
                                merge.start_sql("cdc", "T0"))
        # the filled start in the outer SELECT; the inner dedup partitions by the raw start (a range never crosses a
        # refresh, so its snapshot rows share one snapshot)
        self.assertIn("TIMESTAMP 'T0') ELSE `__START_AT` END AS `__START_AT`, CASE _change_type", sql)   # filled
        self.assertIn("_change_type AS `__lfc_change`", sql)

    def test_a_column_added_since_is_null_in_an_older_copy(self):
        sql = merge.changes_sql("`c`.`b`.`t`", "east", "division", ["k"], ["k", "v", "c"], None, 8, absent={"c": "int"})
        self.assertIn("`k`, `v`, CAST(NULL AS int) AS `c`, 'insert' AS `__lfc_change` FROM (SELECT `k`, `v`, "
                      "CAST(NULL AS int) AS `c`, 'upsert' AS _op FROM `c`.`b`.`t` VERSION AS OF 8)", sql)

    def test_close_time_is_the_refresh_after_the_last_read(self):
        i = {"last": 2, "gens": merge.generations(TWO_REFRESHES), "end_kind": "cdc",
             "times": {h.version: h.timestamp for h in TWO_REFRESHES}}
        self.assertIn("closed by full refresh at version 15', '__cdc_timestamp_value', TIMESTAMP 'T15'", merge._close(i, 18))
        self.assertIn("closed by full refresh at version 9', '__cdc_timestamp_value', TIMESTAMP 'T9'", merge._close(i, 14))
        first = {**i, "last": None}   # a first read: not a refresh
        self.assertIn("not in a whole copy at version 8", merge._close(first, 8))


class Scd2Review(unittest.TestCase):
    """The review's cases (2026-11-22): snapshot times kept, no endless wait, a catch-up that fails, old targets."""

    def test_stored_generations_win_over_a_trimmed_history(self):
        stored = [[0, "T0"], [9, "T9"]]
        trimmed = [(5, "T5"), (9, "T9"), (15, "T15")]   # retention dropped 0-4: version 5 looks like the first
        self.assertEqual(merge.keep_generations(stored, trimmed), [(0, "T0"), (9, "T9"), (15, "T15")])
        self.assertEqual(merge.keep_generations(None, trimmed), trimmed)   # nothing stored: the history's
        # trimmed so that a refresh after the stored ones is the oldest version left: kept
        self.assertEqual(merge.keep_generations([[0, "T0"]], [(9, "T9"), (15, "T15")], {9, 15}),
                         [(0, "T0"), (9, "T9"), (15, "T15")])
        self.assertEqual(merge.keep_generations([[0, "T0"]], [(5, "T5"), (15, "T15")], {15}),   # 5: just the oldest
                         [(0, "T0"), (15, "T15")])

    def test_no_wait_after_a_reset_that_writes_its_rows(self):
        rows = [_h(0, "CREATE TABLE"), _h(2, "MERGE"), _h(5, "CREATE OR REPLACE TABLE AS SELECT")]
        self.assertFalse(merge.refresh_plan(rows, last=2, now=5)["waiting"])

    def test_an_old_refresh_with_no_write_is_an_empty_reload(self):
        rows = [_h(0, "CREATE TABLE"), _h(2, "MERGE"), _h(5, "DLT REFRESH", "2026-11-22T17:00:00.000Z"),
                _h(6, "DLT SETUP", "2026-11-22T17:00:05.000Z")]
        t = 1795366800.0   # 2026-11-22T17:00:00Z
        self.assertTrue(merge.refresh_plan(rows, 2, 6, now_s=t + 60)["waiting"])
        self.assertFalse(merge.refresh_plan(rows, 2, 6, now_s=t + merge.WAIT_MAX_S + 60)["waiting"])

    def test_a_reload_after_now_doesnt_end_the_wait(self):   # read after `now` was fixed: not counted
        rows = TWO_REFRESHES[:11] + [_h(11, "DLT SETUP"), _h(12, "MERGE")]
        self.assertTrue(merge.refresh_plan(rows, last=2, now=10, now_s=1795366800.0 + 60)["waiting"])
        self.assertFalse(merge.refresh_plan(rows, last=2, now=12, now_s=1795366800.0 + 60)["waiting"])

    def test_only_data_gone_falls_back(self):
        self.assertTrue(merge.data_gone(RuntimeError("[DELTA_MISSING_CHANGE_DATA] Error getting change data")))
        self.assertTrue(merge.data_gone(RuntimeError("java.io.FileNotFoundException: part-0001")))
        self.assertTrue(merge.data_gone(RuntimeError("io.delta.VersionNotFoundException: version 3")))
        self.assertTrue(merge.data_gone(RuntimeError("Cannot time travel Delta table to version 3. Available: [9]")))
        for c in ("DELTA_CHANGE_DATA_FILE_NOT_FOUND", "DELTA_LOG_FILE_NOT_FOUND", "DELTA_TRUNCATED_TRANSACTION_LOG"):
            self.assertTrue(merge.data_gone(RuntimeError(f"[{c}] ...")), c)
        # a schema change in the range, a wrong range: bugs to see, not data gone
        self.assertFalse(merge.data_gone(RuntimeError("[DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA] ...")))
        self.assertFalse(merge.data_gone(RuntimeError("[USER_RAISED_EXCEPTION] Invalid Streaming Table CDF")))
        self.assertFalse(merge.data_gone(RuntimeError("[DELTA_CONCURRENT_APPEND] ConcurrentAppendException")))
        self.assertFalse(merge.data_gone(RuntimeError("503 Service Unavailable")))

    def test_a_transient_catch_up_error_is_raised_for_a_retry(self):
        class S:
            def sql(self, q):
                if "table_changes(" in q:
                    raise RuntimeError("[DELTA_CONCURRENT_APPEND] retry")
                return R(collect=list, columns=["id", "v", "__START_AT", "__END_AT"])
        i = {"s": "east", "src": "`c`.`b`.`t`", "cols": [("id", "int"), ("v", "string"), ("__START_AT", "x"),
             ("__END_AT", "x")], "key": ["id", "__START_AT"], "now": 18, "last": 2, "tid": "x",
             "gens": merge.generations(TWO_REFRESHES), "times": {}, "start_kind": "cdc", "end_kind": "cdc",
             "plan": merge.refresh_plan(TWO_REFRESHES, 2, 18)}
        with self.assertRaises(RuntimeError):
            merge._catch_up(S(), "`c`.`s`.`all_t`", i, "division", [], "r", "all_t", {"all_t": {}})

    def test_a_failed_catch_up_falls_back_to_a_copy_that_closes(self):
        done = []

        class S:
            def sql(self, q):
                if "table_changes(" in q:
                    raise RuntimeError("[DELTA_MISSING_CHANGE_DATA] vacuumed\nmore")
                done.append(q)
                return R(collect=list, columns=["id", "v", "__START_AT", "__END_AT"])   # absent_at's read
        i = {"s": "east", "src": "`c`.`b`.`t`", "cols": [("id", "int"), ("v", "string"), ("__START_AT", "x"),
             ("__END_AT", "x")], "key": ["id", "__START_AT"], "now": 18, "last": 2, "tid": "x",
             "gens": merge.generations(TWO_REFRESHES), "times": {h.version: h.timestamp for h in TWO_REFRESHES},
             "start_kind": "cdc", "end_kind": "cdc", "plan": merge.refresh_plan(TWO_REFRESHES, 2, 18)}
        report, logged = {"all_t": {}}, []
        merge._catch_up(S(), "`c`.`s`.`all_t`", i, "division", logged, "r", "all_t", report)
        self.assertIn("catch-up failed, versions since the last read lost: [DELTA_MISSING_CHANGE_DATA] vacuumed",
                      report["all_t"]["east"]["done"])
        copy = [q for q in done if q.startswith("MERGE INTO `c`.`s`.`all_t`")]
        self.assertEqual(len(copy), 1)
        self.assertIn("VERSION AS OF 18", copy[0])
        self.assertIn("AND t.`__END_AT` IS NULL THEN UPDATE SET", copy[0])   # closes, never deletes
        self.assertNotIn("THEN DELETE", copy[0].split("WHEN NOT MATCHED BY SOURCE")[1])

    def test_a_first_read_of_scd2_closes_rather_than_deletes(self):   # a lost state, a target holding rows
        done = []

        class S:
            def sql(self, q):
                done.append(q)
                return R(collect=list, columns=[])
        i = {"s": "east", "src": "`c`.`b`.`t`", "cols": [("id", "int"), ("v", "string"), ("__START_AT", "x"),
             ("__END_AT", "x")], "key": ["id", "__START_AT"], "now": 18, "last": None, "tid": "x",
             "gens": merge.generations(TWO_REFRESHES), "times": {h.version: h.timestamp for h in TWO_REFRESHES},
             "start_kind": "cdc", "end_kind": "cdc"}
        merge._copy(S(), "`c`.`s`.`all_t`", i, "division", [], "r", "all_t", {"all_t": {}}, "copied")
        self.assertIn("AND t.`__END_AT` IS NULL THEN UPDATE SET", done[0])
        self.assertNotIn("THEN DELETE", done[0].split("WHEN NOT MATCHED BY SOURCE")[1])

    def test_an_old_target_s_unfilled_starts_are_found(self):
        self.assertEqual(merge.unfilled_sql("cdc"), "(`__START_AT` IS NULL OR `__START_AT`.__cdc_timestamp_value IS NULL)")
        self.assertEqual(merge.unfilled_sql("timestamp"), "`__START_AT` IS NULL")
        self.assertIsNone(merge.unfilled_sql(None))
        self.assertEqual(merge.start_sql("cdc", "T0"), merge.start_expr("cdc", "T0") + " AS `__START_AT`")


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
