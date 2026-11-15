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

    def __init__(self, versions=None, state=None, history=None, cols=None, keys=("n_nationkey",), targets=()):
        self.versions = versions or {"east": 5, "west": 7}
        self.state = dict(state or {})
        self.history = history or {}   # source -> [(version, operation, params, metrics)]
        self.cols = cols or {"east": COLS, "west": COLS}
        self.keys = list(keys)
        self.targets = set(targets)
        self.target_cols = {"all_nation": [("division", "string")] + COLS}
        self.done = []   # the statements that change something

    def sql(self, q):
        rows = []
        quoted = re.findall(r"`([^`]+)`", q)
        if q.startswith("SHOW SCHEMAS"):
            rows = [R(databaseName=f"shop_bronze_{s}") for s in self.versions]
        elif q.startswith("SHOW TABLES IN") and "shop_bronze_" in q:
            rows = [R(tableName="nation")]
        elif q.startswith("SHOW TABLES IN"):
            rows = [R(tableName=t) for t in self.targets]
        elif q.startswith("SHOW TBLPROPERTIES"):
            rows = [R(key=evidence.KEYS_PROPERTY, value=json.dumps(self.keys))]
        elif q.startswith("DESCRIBE TABLE"):
            schema, table = quoted[1], quoted[2]
            source = schema[len("shop_bronze_"):]
            cols = self.target_cols.get(table, []) if schema == "shop_silver" else self.cols[source]
            rows = [R(col_name=c, data_type=t) for c, t in cols] + [R(col_name="", data_type="")]
        elif q.startswith("SELECT MAX(version)"):
            rows = [R(v=self.versions[quoted[1][len("shop_bronze_"):]])]
        elif q.startswith("SELECT version, operation, operationParameters"):
            since = int(q.rsplit("version > ", 1)[1])
            history = self.history.get(quoted[1][len("shop_bronze_"):], [])
            rows = [R(version=h[0], operation=h[1], operationParameters=h[2],
                      operationMetrics=h[3] if len(h) > 3 else {}) for h in history if h[0] > since]
        elif q.startswith("SELECT target_table, source, version"):
            rows = [R(target_table=t, source=s, version=v) for (t, s), v in self.state.items()]
        elif q.startswith("CREATE TABLE IF NOT EXISTS"):
            pass
        else:
            self.done.append(q)
            if q.startswith("CREATE TABLE"):
                self.targets.add(quoted[2])
            if q.startswith(f"MERGE INTO `main`.`shop_silver`.`{STATE}`"):
                saved = r"SELECT '([^']+)' AS target_table, '([^']+)' AS source, (\d+) AS version"
                t, s, v = re.search(saved, q).groups()
                self.state[(t, s)] = int(v)
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
        self.assertTrue(spark.done[0].startswith("CREATE TABLE `main`.`shop_silver`.`all_nation` (`division` STRING, "
                                                 "`n_nationkey` int, `n_name` string) TBLPROPERTIES"))
        self.assertIn("'delta.enableChangeDataFeed' = 'true'", spark.done[0])
        m = spark.merges()
        self.assertEqual(len(m), 2)
        self.assertIn("FROM `main`.`shop_bronze_east`.`nation` VERSION AS OF 5", m[0])   # the version saved
        self.assertNotIn("table_changes", m[0])
        self.assertEqual(spark.state, {("all_nation", "east"): 5, ("all_nation", "west"): 7})

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
        self.assertEqual(spark.state[("all_nation", "east")], 9)

    def test_new_versions_without_rows_written_are_unchanged(self):   # an ingestion update touches every table
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(6, "MERGE", {}, {"numTargetRowsInserted": "0"}),
                                        (7, "DELETE", {}, {"numDeletedRows": "0"}),
                                        (8, "MERGE", {}, {"numOutputRows": "0"}), (9, "OPTIMIZE", {}, {})]},
                      targets={"all_nation"})
        report = run(spark)
        self.assertEqual(report["all_nation"]["east"], {"done": "unchanged", "version": 9})
        self.assertEqual(spark.merges(), [])
        self.assertEqual(spark.state[("all_nation", "east")], 9)   # read up to 9: not looked at again

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
        self.assertIn("DELETE FROM `main`.`shop_silver`.`all_nation` WHERE `division` = 'east'", spark.done)
        self.assertEqual(spark.state[("all_nation", "east")], 9)

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
        self.assertIn("DELETE FROM `main`.`shop_silver`.`all_nation` WHERE `division` = 'east'", spark.done)
        (m,) = spark.merges()
        self.assertNotIn("table_changes", m)

    def test_overwrite_counts_as_a_refresh(self):
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      history={"east": [(6, "WRITE", {"mode": "Overwrite"})]}, targets={"all_nation"})
        self.assertEqual(run(spark)["all_nation"]["east"]["done"], "copied")


class Shapes(unittest.TestCase):
    def test_new_bronze_column_is_added(self):
        cols = {"east": COLS + [("n_comment", "string")], "west": COLS}
        spark = Spark(versions={"east": 9, "west": 7}, state={("all_nation", "east"): 5, ("all_nation", "west"): 7},
                      cols=cols, history={"east": [(8, "MERGE", {}, {"numTargetRowsInserted": "1"})]},
                      targets={"all_nation"})
        run(spark)
        self.assertIn("ALTER TABLE `main`.`shop_silver`.`all_nation` ADD COLUMNS (`n_comment` string)", spark.done)

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
