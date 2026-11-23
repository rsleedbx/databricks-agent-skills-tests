"""Offline checks of lfc_medallion.methods (refresh techniques from an event log) and paths.fit_r2.

    python3 -m unittest skills/lfc-medallion/tests/test_methods.py
"""
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import methods, paths  # noqa: E402


class Methods(unittest.TestCase):
    def test_sql_reads_planning_and_progress(self):
        q = methods.refreshes_sql("cat.ctl.events_p", since="2026-11-21")
        for part in ("details:planning_information::string", "event_type = 'planning_information'",
                     "event_type = 'flow_progress'", "timestamp >= '2026-11-21'", "AS change_bytes", "USING (u, f)"):
            self.assertIn(part, q)

    def test_each_technique_follows_its_own_input(self):   # a recompute follows the sources, incremental the changes
        rnd, rows = random.Random(7), []
        for _ in range(200):
            full, change = 10 ** rnd.uniform(8, 11), 10 ** rnd.uniform(4, 8)
            rows.append({"technique": "COMPLETE_RECOMPUTE", "full_bytes": full, "change_bytes": change,
                         "seconds": full ** 0.3 * rnd.uniform(0.9, 1.1), "planning_ms": 1000})
            rows.append({"technique": "ROW_BASED", "full_bytes": full, "change_bytes": change,
                         "seconds": change ** 0.3 * rnd.uniform(0.9, 1.1), "planning_ms": 1000})
        got = {r["technique"]: r for r in methods.summarize(rows)}
        self.assertGreater(got["COMPLETE_RECOMPUTE"]["r2 source bytes"], 0.9)
        self.assertLess(got["COMPLETE_RECOMPUTE"]["r2 change bytes"], 0.1)
        self.assertGreater(got["ROW_BASED"]["r2 change bytes"], 0.9)
        self.assertEqual(got["ROW_BASED"]["refreshes"], 200)

    def test_fit_r2(self):
        w, r2 = paths.fit_r2([(x,) for x in range(10)], [3 + 2 * x for x in range(10)])
        self.assertAlmostEqual(w[1], 2.0)
        self.assertAlmostEqual(r2, 1.0)
        self.assertEqual(paths.fit_r2([], []), (None, None))


if __name__ == "__main__":
    unittest.main()
