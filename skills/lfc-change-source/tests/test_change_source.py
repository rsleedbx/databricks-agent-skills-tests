"""Offline checks of lfc_change_source: the contract and the Lakeflow Connect bronze adapter.

    python3 -m unittest skills/lfc-change-source/tests/test_change_source.py
"""
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_change_source.contract import Position, Stats, last_change_sql  # noqa: E402
from lfc_change_source.lfc_bronze import Bronze  # noqa: E402

V = lambda n: Position("delta_version", str(n))  # noqa: E731


class Contract(unittest.TestCase):
    def test_position_text_round_trip(self):
        p = Position.parse("hlc:1700000000000000000.0000000001")
        self.assertEqual((p.kind, str(p)), ("hlc", "hlc:1700000000000000000.0000000001"))

    def test_last_change_per_key_by_seq(self):
        q = last_change_sql("SELECT 1", ["a", "b"])
        self.assertIn("PARTITION BY `a`, `b` ORDER BY _seq DESC", q)
        self.assertIn("WHERE `__lfc_n` = 1", q)


class LfcBronze(unittest.TestCase):
    b = Bronze("lob_001", "`c`.`s`.`account`", "account", ["a_id"], ["a_id", "a_balance"])

    def test_snapshot_pinned_and_changes_with_op_and_seq(self):
        self.assertEqual(self.b.snapshot_sql(V(8)), "SELECT `a_id`, `a_balance`, 'upsert' AS _op FROM `c`.`s`.`account` VERSION AS OF 8")
        q = self.b.changes_sql(V(3), V(8))
        self.assertIn("FROM table_changes('c.s.account', 4, 8) WHERE _change_type != 'update_preimage'", q)
        self.assertIn("CASE _change_type WHEN 'delete' THEN 'delete' ELSE 'upsert' END AS _op", q)
        self.assertIn("named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END) AS _seq", q)
        with self.assertRaises(ValueError):
            self.b.changes_sql(Position("lsn", "0/16B3748"), V(8))

    def test_watermark_and_stats_from_history(self):
        seen = []
        def sql(q):
            seen.append(q)
            row = (19,) if "max(version)" in q else (78, 1024, 2)
            return types.SimpleNamespace(collect=lambda: [row])
        s = types.SimpleNamespace(sql=sql)
        self.assertEqual(self.b.watermark(s), V(19))
        self.assertEqual(self.b.stats(s, V(16)), Stats(78, 1024, 2))
        self.assertIn("FROM (DESCRIBE HISTORY `c`.`s`.`account`) WHERE version > 16", seen[-1])


if __name__ == "__main__":
    unittest.main()
