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


WH = paths.Line(6.0, 0.80, 0.54, 0.155, 0.26)
SPARK = paths.Line(3.0, 1.05, 0.54, 0.30, 1.40)


class Choose(unittest.TestCase):
    def test_few_sources_one_merge_many_split(self):   # the cardinality decides the shape
        m = paths.Model({"warehouse": WH})
        self.assertEqual(paths.choose(m, 1, engines=("warehouse",))[:2], ("warehouse", 1))
        k124 = paths.choose(m, 124, engines=("warehouse",))[1]
        self.assertTrue(5 <= k124 <= 20, k124)
        k, s = m.best("warehouse", 2500)   # one unit: throughput binds; the smallest k that reaches it
        self.assertAlmostEqual(s, paths.throughput(WH, 2500, k), places=6)
        self.assertGreater(m.best("warehouse", 2500, u=16.0)[0], k)

    def test_throughput_binds_and_units_move_it(self):   # 124 sources, 3 tables at once, the 2026-11-22 harness
        m = paths.Model({"warehouse": WH})
        self.assertAlmostEqual(paths.cost(WH, 124, 1, at_once=3), 105.7, places=0)    # measured 105 s
        self.assertAlmostEqual(paths.cost(WH, 124, 7, at_once=3), 63.2, places=0)     # measured 67 s
        k1, s1 = m.best("warehouse", 124, at_once=3, u=1.0)
        k4, s4 = m.best("warehouse", 124, at_once=3, u=4.0)
        self.assertLess(s4, s1 / 2)   # four clusters: throughput no longer binds as soon
        self.assertGreater(k4, k1)
        self.assertEqual(paths.units("Small", 2), 8.0)

    def test_the_cheaper_engine_and_a_cold_start(self):
        m = paths.Model({"spark": SPARK, "warehouse": WH}, start_s=60)
        self.assertEqual(paths.choose(m, 124, engines=("spark", "warehouse"), at_once=3)[0], "warehouse")
        self.assertEqual(paths.choose(m, 2, engines=("spark", "warehouse"), cold=frozenset({"warehouse"}))[0], "spark")

    def test_forced_and_unmeasured(self):
        m = paths.Model({"spark": SPARK, "warehouse": paths.Line(0, 0, 0, 0, 0, 0, False)})
        self.assertEqual(paths.choose(m, 50, engines=("spark", "warehouse"))[0], "spark")   # never guessed into
        self.assertEqual(paths.choose(m, 50, engines=("spark", "warehouse"), forced="warehouse:4")[:2],
                         ("warehouse", 4))
        with self.assertRaisesRegex(ValueError, "isn't available"):
            paths.choose(m, 5, engines=("spark",), forced="warehouse")
        with self.assertRaises(ValueError):
            paths.parse("gpu:2")

    def test_refit_from_runs(self):   # runs made by a known line give it back
        true = paths.Line(4.0, 0.5, 0.3, 0.1, 0.4)
        shapes = [(1, n) for n in (10, 60, 120)] + [(k, 120) for k in (2, 4, 8, 16, 32)] + [(8, 60), (32, 60)]
        runs = [("warehouse", k, n, 0, paths.cost(true, n, k, at_once=3), 3, 1.0) for k, n in shapes]
        got = paths.Model({"warehouse": WH}).fit(runs, rounds=6).lines["warehouse"]
        for k, n in shapes:
            self.assertAlmostEqual(paths.cost(got, n, k, at_once=3), paths.cost(true, n, k, at_once=3), delta=1.0)
        self.assertTrue(got.measured)

    def test_groups_even_in_order(self):
        self.assertEqual(paths.groups(list(range(7)), 3), [[0, 1, 2], [3, 4], [5, 6]])
        self.assertEqual(paths.groups([1, 2], 5), [[1], [2]])

    def test_planner_records(self):
        p = paths.Planner({"spark": object()}, paths.Model({"spark": paths.Line(1.0, 1.0, 1.0, 0.1, 0.1)}),
                          rows={("t", "a"): 10, ("t", "b"): 5}, units={"spark": 1.0}, at_once=2)
        e, eng, k, rows, preds = p.pick("t", ["a", "b"])
        p.record("all_t", e, k, 2, rows, preds, 3.0, 0)
        r = p.records[0]
        self.assertEqual((e, rows, r["sources"], r["forced"], r["at_once"], r["units"]), ("spark", 15, 2, False, 2, 1.0))


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
