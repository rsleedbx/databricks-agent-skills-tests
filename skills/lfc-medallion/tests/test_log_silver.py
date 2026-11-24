"""Offline checks of lfc_medallion.log_silver: the build and MERGE SQL from a change source, and positions kept per
table (fake session).

    python3 -m unittest skills/lfc-medallion/tests/test_log_silver.py
"""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.gateway_log import from_gateway  # noqa: E402
from lfc_medallion import log_silver  # noqa: E402

ENTRY = {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]}


class LogSilver(unittest.TestCase):
    def src(self):
        return from_gateway("gw", "`c`.`x`.`cdc_log`", ENTRY, "db.s.t")

    def test_build_and_merge_sql(self):
        b = log_silver.build_sql(self.src(), "`c`.`silver`.`t`")
        self.assertTrue(b.startswith("CREATE OR REPLACE TABLE `c`.`silver`.`t` AS SELECT `id`, `v` FROM ("))
        m = log_silver.merge_sql(self.src(), "`c`.`silver`.`t`", Position("gateway_cursor", "05"),
                                 Position("gateway_cursor", "09"))
        self.assertIn("ON t.`id` <=> c.`id`", m)
        self.assertLess(m.index("THEN DELETE"), m.index("THEN UPDATE SET t.`v` = c.`v`"))
        self.assertIn("WHEN NOT MATCHED AND c._op <> 'delete' THEN INSERT (`id`, `v`) VALUES (c.`id`, c.`v`)", m)
        self.assertIn("value:cursor.sequence::STRING > '05'", m)
        self.assertIn("value:cursor.sequence::STRING <= '09'", m)

    def test_apply_reads_each_tables_kept_position(self):
        seen = []

        def sql(q):
            seen.append(q)
            rows = [("db_t", "gateway_cursor", "03")] if q.startswith("SELECT target") else \
                [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 2})]
            return types.SimpleNamespace(collect=lambda: rows)
        done = log_silver.apply(types.SimpleNamespace(sql=sql), {"db_t": self.src()}, "`c`.`silver`",
                                Position("gateway_cursor", "09"), threads=1)
        self.assertEqual(done["db_t"][0], 2)
        merge = next(q for q in seen if q.startswith("MERGE INTO `c`.`silver`.`db_t`"))
        self.assertIn("> '03'", merge)                                   # from its own kept position
        self.assertIn("MERGE INTO `c`.`silver`._log_silver", seen[-1])   # then the new position, one statement


    def test_staged_reads_the_log_once_then_the_batch(self):
        seen = []

        def sql(q):
            seen.append(q)
            rows = [("a", "gateway_cursor", "03"), ("b", "gateway_cursor", "05")] if q.startswith("SELECT target") \
                else [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})]
            return types.SimpleNamespace(collect=lambda: rows)
        other = from_gateway("gw", "`c`.`x`.`cdc_log`", dict(ENTRY, id="T2"), "db.s.u")
        done = log_silver.apply_staged(types.SimpleNamespace(sql=sql), {"a": self.src(), "b": other}, "`c`.`silver`",
                                       Position("gateway_cursor", "09"), threads=1)
        stage = next(q for q in seen if q.startswith("CREATE OR REPLACE TABLE `c`.`silver`._log_silver_batch"))
        self.assertIn("CLUSTER BY (destination)", stage)
        self.assertIn("destination IN ('db.s.t', 'db.s.u')", stage)
        self.assertIn("> '03'", stage)                                   # from the oldest kept position
        merges = [q for q in seen if q.startswith("MERGE INTO `c`.`silver`.`a`") or q.startswith("MERGE INTO `c`.`silver`.`b`")]
        self.assertTrue(all("FROM `c`.`silver`._log_silver_batch" in q for q in merges))   # not the log
        self.assertIn("> '05'", next(q for q in merges if "`b`" in q[:30]))   # each from its own
        self.assertIn("_stage", done)

if __name__ == "__main__":
    unittest.main()
