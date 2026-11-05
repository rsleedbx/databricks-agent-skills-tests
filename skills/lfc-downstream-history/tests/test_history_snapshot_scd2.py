"""Offline checks of history-snapshot-scd2.py's guards: refresh_in_progress and empty_after_rows (the notebook needs a
pipeline runtime, so only its pure functions are loaded, from its source), and that the DataFrame form the pipeline runs
(source_state, committed_since) gives the same answers on the same history (local Spark; skipped without pyspark/Java)."""
import ast
import os
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "history-snapshot-scd2.py")
NAMES = {"WRITES", "ROW_METRICS", "count", "wrote_rows", "reload_done", "refresh_in_progress", "empty_after_rows"}
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


class ReloadDone(unittest.TestCase):
    def test_cases(self):
        f, t = g["reload_done"], "main.s.t"
        self.assertTrue(f("Full Refresh completed for dataset: main.s.t.", t))
        self.assertTrue(f("Snapshot of main.s.t has been APPLIED.", t))
        self.assertFalse(f("Full Refresh requested for dataset: main.s.t.", t))
        self.assertFalse(f("Full Refresh completed for dataset: main.s.other.", t))


class LocalSpark(unittest.TestCase):
    """A local SparkSession and the notebook's DataFrame functions (skipped without pyspark/Java)."""

    @classmethod
    def setUpClass(cls):
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
            cls.spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
        except Exception as e:  # no pyspark, no usable Java
            raise unittest.SkipTest(f"no local Spark: {type(e).__name__}") from e
        names = {"source_state", "committed_since", "WROTE_ROWS", "part", "reload_pending"} | NAMES
        mod = ast.Module(body=[n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in names) or
                               (isinstance(n, ast.Assign) and {t.id for t in n.targets if isinstance(t, ast.Name)} & names)],
                         type_ignores=[])
        from pyspark.sql.types import StructType
        cls.g = {"F": F, "SRC": "src", "spark": cls, "StructType": StructType}
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



class DataFrameForm(LocalSpark):
    """source_state / committed_since on a history DataFrame built from the same ops."""

    def test_same_as_python_rules(self):
        for ops, in_refresh, had_rows in CASES:
            with self.subTest(ops=ops):
                self.assertEqual(g["refresh_in_progress"](ops), in_refresh)
                self.load(ops)
                self.assertEqual(self.g["source_state"](), (in_refresh, had_rows))

    def test_part(self):
        """src_start / src_start_ts from __START_AT: a struct's internal value and timestamp, or a plain column."""
        from pyspark.sql.types import StructType
        rows = {"struct": ("STRUCT<__cdc_internal_value: STRING, __cdc_timestamp_value: TIMESTAMP>",
                           "named_struct('__cdc_internal_value', '0000002a', '__cdc_timestamp_value', TIMESTAMP'2026-11-04 10:00:00')",
                           "0000002a"),
                "plain": ("TIMESTAMP", "TIMESTAMP'2026-11-04 10:00:00'", "2026-11-04 10:00:00")}
        for name, (_type, value, seq) in rows.items():
            with self.subTest(name):
                df = self.spark.sql(f"SELECT 1 AS id, {value} AS __START_AT")
                self.g["SCHEMA"] = df.schema
                self.assertEqual(isinstance(df.schema["__START_AT"].dataType, StructType), name == "struct")
                got = df.select(self.g["part"]("__START_AT", "seq").alias("s"),
                                self.g["part"]("__START_AT", "ts").cast("string").alias("t")).first()
                self.assertEqual((got.s, got.t), (seq, "2026-11-04 10:00:00"))

    def test_reload_pending(self):
        """A DLT REFRESH after the last snapshot (ms) is pending until an end event after it; none found: pending."""
        self.load([(10, "MERGE", ROWS), (11, "DLT REFRESH", {}), (12, "MERGE", ROWS)])    # refresh at 11 000 ms
        for latest, done, want in [(10_500, None, True), (10_500, 11_500, False), (10_500, 10_800, True),
                                   (11_500, None, False), (None, 12_000, False), (None, None, True)]:
            with self.subTest(latest=latest, done=done):
                self.g["completed_ms"] = lambda _since, d=done: d
                self.assertEqual(self.g["reload_pending"](latest), want)

    def test_committed_since(self):
        self.load([(10, "MERGE", ROWS), (11, "MERGE", ROWS)])     # commits at 10 000 ms and 11 000 ms
        self.assertTrue(self.g["committed_since"](10_500))
        self.assertFalse(self.g["committed_since"](11_000))


# the history table of a harness run (7 of 7): first load (T0), a sync, a gap (1 -> a2, 5 deleted), a full refresh seen
# at T2, then 4 -> d2. (id, v, src_start, src_start_ts, src_end, src_end_ts, __START_AT, __END_AT)
T0, T1, T2, T4 = 1793824367122, 1793824772154, 1793826531546, 1793828090486
S2, S3, S4, S6 = "0000002C000004480003", "0000002C000004500005", "0000002E000016F00003", "0000002C000004580003"
TS1, TS4 = "2026-11-04 20:33:15.07", "2026-11-04 21:29:19.513"
RUN = [
    (1, "a1", "", None, None, None, T0, T2), (1, "a2", "", None, None, None, T2, None),
    (2, "b1", "", None, S2, TS1, T0, T2), (2, "b2", S2, TS1, None, None, T1, T2), (2, "b2", "", None, None, None, T2, None),
    (3, "c1", "", None, S3, TS1, T0, T2),
    (4, "d1", "", None, S4, TS4, T0, None), (4, "d2", S4, TS4, None, None, T4, None),
    (5, "e1", "", None, None, None, T0, T2),
    (6, "f1", S6, TS1, None, None, T1, T2), (6, "f1", "", None, None, None, T2, None),
]
SEEN2 = "2026-11-04 21:08:51.546"   # T2
# what the run's versions table held (src_start_at, src_end_at, dropped_by_refresh)
RUN_VERSIONS = [
    (1, "a1", None, SEEN2, True), (1, "a2", SEEN2, None, False),
    (2, "b1", None, TS1, True), (2, "b2", TS1, None, False),
    (3, "c1", None, TS1, True),
    (4, "d1", None, TS4, False), (4, "d2", TS4, None, False),
    (5, "e1", None, SEEN2, True),
    (6, "f1", TS1, None, False),
]


class Versions(LocalSpark):
    """versions() on hand-built history tables (AUTO CDC FROM SNAPSHOT's output, as the run above wrote it)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "versions")
        fn = ast.FunctionDef(**{**{f: getattr(fn, f) for f in fn._fields}, "decorator_list": []})
        from pyspark.sql import Window
        cls.g.update({"Window": Window, "KEYS": ["id"], "DATA": ["v"],
                      "spark": type("S", (), {"read": type("R", (), {"table": staticmethod(lambda _n: cls.history)})})})
        exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])), SRC, "exec"), cls.g)  # noqa: S102

    def versions(self, rows, keys=("id",)):
        cols = ", ".join(f"`{k}` INT" for k in keys)
        type(self).history = self.spark.createDataFrame(rows, f"{cols}, v STRING, src_start STRING, src_start_ts STRING, "
                                                               "src_end STRING, src_end_ts STRING, __START_AT BIGINT, __END_AT BIGINT"
                                                        ).selectExpr("*", "CAST(src_start_ts AS TIMESTAMP) AS s_ts", "CAST(src_end_ts AS TIMESTAMP) AS e_ts"
                                                        ).drop("src_start_ts", "src_end_ts").withColumnsRenamed({"s_ts": "src_start_ts", "e_ts": "src_end_ts"})
        self.g["KEYS"] = list(keys)
        out = self.g["versions"]().selectExpr(*keys, "v", "CAST(src_start_at AS STRING)", "CAST(src_end_at AS STRING)", "dropped_by_refresh")
        return sorted(tuple(r) for r in out.collect())

    def test_run(self):
        self.assertEqual(self.versions(RUN), sorted(RUN_VERSIONS))

    def test_second_refresh(self):
        """A second refresh at T5 reloads 4=d2 (its CDC version is dropped) under the snapshot key, which held d1:
        d1 keeps its source end and is gone from the destination; d2 is one version, from its CDC start."""
        T5 = 1793830000000
        rows = [r for r in RUN if r[0] != 4] + [
            (4, "d1", "", None, S4, TS4, T0, T5), (4, "d2", S4, TS4, None, None, T4, T5), (4, "d2", "", None, None, None, T5, None)]
        want = [v for v in RUN_VERSIONS if v[0] != 4] + [(4, "d1", None, TS4, True), (4, "d2", TS4, None, False)]
        self.assertEqual(self.versions(rows), sorted(want))

    def test_changed_reload(self):
        """A refresh reloads a value the history never saw (1 -> a2 in the gap): a1 ends when the history saw the reload."""
        got = {(r[0], r[1]): r for r in self.versions(RUN)}
        self.assertEqual(got[(1, "a1")][3], SEEN2)
        self.assertEqual(got[(1, "a2")][2], SEEN2)

    def test_composite_keys(self):
        rows = [(k, 7, *r) for k, *r in RUN]
        want = [(k, 7, *v) for k, *v in RUN_VERSIONS]
        self.assertEqual(self.versions(rows, keys=("id", "part")), sorted(want))


if __name__ == "__main__":
    unittest.main()
