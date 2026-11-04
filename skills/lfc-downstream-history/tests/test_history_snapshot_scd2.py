"""Offline checks of history-snapshot-scd2.py's guards: refresh_in_progress and empty_after_rows (the notebook needs a
pipeline runtime, so only its pure functions are loaded, from its source), and that the DataFrame form the pipeline runs
(source_state, committed_since) gives the same answers on the same history (local Spark; skipped without pyspark/Java)."""
import ast
import os
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "history-snapshot-scd2.py")
NAMES = {"WRITES", "ROW_METRICS", "count", "wrote_rows", "refresh_in_progress", "empty_after_rows"}
with open(SRC) as fh:
    tree = ast.parse(fh.read())
code = ast.Module(body=[n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in NAMES) or
                        (isinstance(n, ast.Assign) and {t.id for t in n.targets if isinstance(t, ast.Name)} & NAMES)],
                  type_ignores=[])
g = {}
exec(compile(code, SRC, "exec"), g)  # noqa: S102  (the notebook's own functions)
ROWS = {"numTargetRowsInserted": "4"}
NONE = {"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0", "numOutputRows": "0"}


class RefreshInProgress(unittest.TestCase):
    def f(self, *ops):
        return g["refresh_in_progress"](list(ops))

    def test_cases(self):
        self.assertTrue(self.f((10, "MERGE", ROWS), (11, "DLT REFRESH", {})))                 # emptied, not reloaded
        self.assertFalse(self.f((11, "DLT REFRESH", {}), (12, "MERGE", ROWS)))                # reloaded
        self.assertFalse(self.f((10, "MERGE", ROWS)))                                         # no refresh
        self.assertTrue(self.f((11, "DLT REFRESH", {})))                                      # refresh, no write
        self.assertTrue(self.f((11, "DLT REFRESH", {}), (12, "SET TBLPROPERTIES", {}),
                               (13, "DLT SETUP", {}), (14, "CLUSTER BY", {})))               # metadata only
        self.assertTrue(self.f((11, "DLT REFRESH", {}), (12, "MERGE", NONE)))                 # a write of no rows
        self.assertFalse(self.f((11, "DLT REFRESH", {}), (12, "STREAMING UPDATE", None)))     # no metrics: rows

    def test_newest_refresh_counts(self):
        self.assertTrue(self.f((5, "DLT REFRESH", {}), (6, "MERGE", ROWS), (9, "DLT REFRESH", {})))


class EmptyAfterRows(unittest.TestCase):
    def test_cases(self):
        f = g["empty_after_rows"]
        self.assertTrue(f(0, [(3, "MERGE", ROWS)], False))       # emptied: refuse
        self.assertFalse(f(0, [(3, "MERGE", ROWS)], True))       # meant to be empty
        self.assertFalse(f(0, [(3, "MERGE", NONE)], False))      # never had rows
        self.assertFalse(f(4, [(3, "MERGE", ROWS)], False))      # has rows


CASES = [  # (ops, refresh in progress, had rows)
    ([(10, "MERGE", ROWS), (11, "DLT REFRESH", {})], True, True),                            # emptied, not reloaded
    ([(11, "DLT REFRESH", {}), (12, "MERGE", ROWS)], False, True),                           # reloaded
    ([(10, "MERGE", ROWS)], False, True),                                                    # no refresh
    ([(11, "DLT REFRESH", {}), (12, "SET TBLPROPERTIES", {}), (13, "CLUSTER BY", {})], True, False),  # metadata only
    ([(11, "DLT REFRESH", {}), (12, "MERGE", NONE)], True, False),                           # a write of no rows
    ([(11, "DLT REFRESH", {}), (12, "STREAMING UPDATE", None)], False, True),                # no metrics: rows
    ([(11, "DLT REFRESH", {}), (12, "MERGE", {})], False, True),                             # empty metrics: rows
    ([(11, "DLT REFRESH", {}), (12, "MERGE", {"numOutputRows": "x"})], True, False),         # unreadable count
    ([(5, "DLT REFRESH", {}), (6, "MERGE", ROWS), (9, "DLT REFRESH", {})], True, True),       # newest refresh counts
]


class DataFrameForm(unittest.TestCase):
    """source_state / committed_since on a history DataFrame built from the same ops."""

    @classmethod
    def setUpClass(cls):
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
            cls.spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
        except Exception as e:  # no pyspark, no usable Java
            raise unittest.SkipTest(f"no local Spark: {type(e).__name__}") from e
        names = {"source_state", "committed_since", "WROTE_ROWS"} | NAMES
        mod = ast.Module(body=[n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in names) or
                               (isinstance(n, ast.Assign) and {t.id for t in n.targets if isinstance(t, ast.Name)} & names)],
                         type_ignores=[])
        cls.g = {"F": F, "SRC": "src", "spark": cls}
        exec(compile(mod, SRC, "exec"), cls.g)  # noqa: S102

    @classmethod
    def sql(cls, _statement):  # the notebook's spark.sql("DESCRIBE HISTORY ...")
        return cls.history

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def load(self, ops):
        type(self).history = self.spark.createDataFrame(
            [(v, op, m, v * 1000) for v, op, m in ops],
            "version BIGINT, operation STRING, operationMetrics MAP<STRING,STRING>, ms BIGINT").selectExpr(
            "version", "operation", "operationMetrics", "timestamp_millis(ms) AS timestamp")

    def test_same_as_python_rules(self):
        for ops, in_refresh, had_rows in CASES:
            with self.subTest(ops=ops):
                self.assertEqual(g["refresh_in_progress"](ops), in_refresh)
                self.load(ops)
                self.assertEqual(self.g["source_state"](), (in_refresh, had_rows))

    def test_committed_since(self):
        self.load([(10, "MERGE", ROWS), (11, "MERGE", ROWS)])     # commits at 10 000 ms and 11 000 ms
        self.assertTrue(self.g["committed_since"](10_500))
        self.assertFalse(self.g["committed_since"](11_000))


if __name__ == "__main__":
    unittest.main()
