"""lfc_routing.snapshots offline: which tables get a provisional snapshot (small, with statistics), and step 7's keep
or take (a log holding its position, a split's overlap, a trimmed log, a changed schema, none taken).

    python3 -m unittest skills/lfc-routing/tests/test_snapshots.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-change-source"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing import snapshots  # noqa: E402


class Snapshots(unittest.TestCase):
    def test_pick_small_tables_with_statistics(self):
        picked, skipped = snapshots.pick({"tables": [{"name": "region", "row_count": 5, "data_size_bytes": 8192},
                                                     {"name": "lineitem", "data_size_bytes": 5 << 30},
                                                     {"name": "hits", "row_count": 70}]})
        self.assertEqual([t for t, _ in picked], ["region"])
        self.assertEqual({t: w.split(":")[0] for t, w in skipped},
                         {"lineitem": "5,368,709,120 bytes, over 1,073,741,824", "hits": "no statistics of its size"})

    def test_keep_or_take(self):
        run = {"status": "ok", "position": "1000"}
        self.assertTrue(snapshots.keep_or_take(run, {"c.s.cdc_log": 10})["keep"])
        # a split: the old log from 10, the new one from 990 (they overlap): kept
        self.assertTrue(snapshots.keep_or_take(run, {"c.s.cdc_log": 10, "c.s.cdc_log_t": 990})["keep"])
        # only the new log, started after the snapshot: a new one
        self.assertFalse(snapshots.keep_or_take(run, {"c.s.cdc_log_t": 1500})["keep"])
        self.assertIn("schema changed", snapshots.keep_or_take(run, {"c.s.cdc_log": 10}, {"schema"})["why"])
        self.assertFalse(snapshots.keep_or_take(None, {"c.s.cdc_log": 10})["keep"])
        self.assertFalse(snapshots.keep_or_take({"status": "failed"}, {"c.s.cdc_log": 10})["keep"])


if __name__ == "__main__":
    unittest.main()
