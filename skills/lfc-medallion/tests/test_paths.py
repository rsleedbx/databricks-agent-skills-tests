"""Offline checks of lfc_medallion's paths (the MERGE optimizer) and engine (the warehouse adapter's values).

    python3 -m unittest skills/lfc-medallion/tests/test_paths.py
"""
import datetime as dt
import decimal
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import engine, paths  # noqa: E402


class Choose(unittest.TestCase):
    def test_few_sources_one_merge_many_split(self):   # the cardinality decides the shape
        m = paths.Model({"warehouse": (4.5, 0.23, 0.54, 0.0, True)})
        self.assertEqual(paths.choose(m, 1, engines=("warehouse",))[:2], ("warehouse", 1))
        k124 = paths.choose(m, 124, engines=("warehouse",))[1]
        self.assertTrue(5 <= k124 <= 10, k124)             # about sqrt(0.23 * 124 / 0.54) = 7
        self.assertGreater(paths.choose(m, 2500, engines=("warehouse",))[1], k124)

    def test_the_cheaper_engine_and_a_cold_start(self):
        m = paths.Model({"spark": (3.0, 1.11, 0.54, 0.0, True), "warehouse": (4.5, 0.23, 0.54, 0.0, True)}, start_s=60)
        self.assertEqual(paths.choose(m, 124, engines=("spark", "warehouse"))[0], "warehouse")
        self.assertEqual(paths.choose(m, 2, engines=("spark", "warehouse"), cold=frozenset({"warehouse"}))[0], "spark")

    def test_forced_and_unmeasured(self):
        m = paths.Model({"spark": (3.0, 1.0, 0.5, 0.0, True), "warehouse": (0.0, 0.0, 0.0, 0.0, False)})
        self.assertEqual(paths.choose(m, 50, engines=("spark", "warehouse"))[0], "spark")   # never guessed into
        self.assertEqual(paths.choose(m, 50, engines=("spark", "warehouse"), forced="warehouse:4")[:2],
                         ("warehouse", 4))
        with self.assertRaisesRegex(ValueError, "isn't available"):
            paths.choose(m, 5, engines=("spark",), forced="warehouse")
        with self.assertRaises(ValueError):
            paths.parse("gpu:2")

    def test_refit_from_runs(self):   # k = 1 and k = n runs tell planning from commits
        runs = [("warehouse", 1, n, 0, 4 + 0.25 * n) for n in (10, 60, 120)] + \
               [("warehouse", n, n, 0, 4 + 0.25 + 0.5 * n) for n in (10, 60, 120)]
        a, b, c, d, ok = paths.Model().fit(runs).lines["warehouse"]
        self.assertAlmostEqual(b, 0.25, places=2)
        self.assertAlmostEqual(c, 0.5, places=2)
        self.assertTrue(ok)

    def test_groups_even_in_order(self):
        self.assertEqual(paths.groups(list(range(7)), 3), [[0, 1, 2], [3, 4], [5, 6]])
        self.assertEqual(paths.groups([1, 2], 5), [[1], [2]])

    def test_planner_records(self):
        p = paths.Planner({"spark": object()}, paths.Model({"spark": (1.0, 1.0, 1.0, 0.0, True)}),
                          rows={("t", "a"): 10, ("t", "b"): 5})
        e, eng, k, rows, preds = p.pick("t", ["a", "b"])
        p.record("all_t", e, k, 2, rows, preds, 3.0, 0)
        self.assertEqual((e, rows, p.records[0]["sources"], p.records[0]["forced"]), ("spark", 15, 2, False))


class Values(unittest.TestCase):
    def test_types(self):
        self.assertEqual(engine.value("12", "LONG"), 12)
        self.assertEqual(engine.value("1.50", "DECIMAL"), decimal.Decimal("1.50"))
        self.assertEqual(engine.value('{"a": "1"}', "MAP"), {"a": "1"})
        self.assertEqual(engine.value("2026-11-23T04:03:31.409Z", "TIMESTAMP"),
                         dt.datetime(2026, 11, 23, 4, 3, 31, 409000, tzinfo=dt.timezone.utc))
        self.assertIsNone(engine.value(None, "INT"))
        r = engine.row_type(["version", "operation"])([3, "MERGE"])
        self.assertEqual((r.version, r[1], r.asDict()), (3, "MERGE", {"version": 3, "operation": "MERGE"}))


if __name__ == "__main__":
    unittest.main()
