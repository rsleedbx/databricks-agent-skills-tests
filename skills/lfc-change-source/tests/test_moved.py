"""lfc_change_source.moved offline: a moved table's changes as the old log up to the cut and the new one after it,
for ranges before, across and after the cut (an event of the overlap read once, from the old log), and the checks.

    python3 -m unittest skills/lfc-change-source/tests/test_moved.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.moved import Moved  # noqa: E402

mk = lambda log: CdcLog("pg1", log, "orders", "pg1.tpch1.orders", ["id"], {"id": "INT", "v": "STRING"}, seq="lsn")  # noqa: E731
OLD, NEW = "`c`.`s`.cdc_log", "`c`.`s`.`cdc_log_orders`"
P = lambda v: Position("lsn", str(v))  # noqa: E731
m = Moved(mk(OLD), mk(NEW), P(29814640))


class MovedTest(unittest.TestCase):
    def test_across_the_cut_both_logs_split_at_it(self):
        q = m.changes_sql(P(29807000), P(29815432))
        old, new = q.split(" UNION ALL ")
        self.assertIn(f"FROM {OLD}", old)
        self.assertIn("> 29807000", old)
        self.assertIn("<= 29814640", old)       # the overlap's events from the old log only
        self.assertIn(f"FROM {NEW}", new)
        self.assertIn("> 29814640", new)        # never the new log's copy of them
        self.assertIn("<= 29815432", new)

    def test_ranges_on_one_side(self):
        before = m.changes_sql(None, P(29800000))
        self.assertNotIn(NEW, before)
        self.assertIn("<= 29800000", before)
        after = m.changes_sql(P(29814640), None)
        self.assertNotIn(f"FROM {OLD} ", after)
        self.assertIn("> 29814640", after)

    def test_the_rest_is_the_new_logs_and_mismatches_refused(self):
        self.assertEqual((m.fq, m.keys, m.destination), (NEW, ["id"], "pg1.tpch1.orders"))
        with self.assertRaises(ValueError):
            Moved(mk(OLD), CdcLog("pg1", NEW, "o", "pg1.tpch1.other", ["id"], {"id": "INT"}, seq="lsn"), P(1))
        with self.assertRaises(ValueError):
            Moved(mk(OLD), mk(NEW), Position("binlog", "1"))
        self.assertIn("WHERE _op <> 'delete'", m.snapshot_sql())


if __name__ == "__main__":
    unittest.main()
