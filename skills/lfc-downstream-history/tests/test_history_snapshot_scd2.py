"""Offline checks of history-snapshot-scd2.py's guards: refresh_in_progress and empty_after_rows (the notebook needs a
pipeline runtime, so only its pure functions are loaded, from its source), and that the DataFrame form the pipeline runs
(source_state, committed_since) gives the same answers on the same history (local Spark; skipped without pyspark/Java).
Also: the ingestion events reader with a fake SDK, the order of next_snapshot's guards, and versions."""
import ast
import os
import re
import sys
import types
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "history-snapshot-scd2.py")
NAMES = {"WRITES", "ROW_METRICS", "count", "wrote_rows", "reload_done", "completed_ms", "refresh_in_progress", "empty_after_rows"}
with open(SRC) as fh:
    tree = ast.parse(fh.read())
code = ast.Module(body=[n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in NAMES) or
                        (isinstance(n, ast.Assign) and {t.id for t in n.targets if isinstance(t, ast.Name)} & NAMES)],
                  type_ignores=[])
g = {"re": re}
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
        self.assertFalse(f("Full Refresh completed for dataset: main.s.t2.", t))       # a sibling with a longer name
        self.assertFalse(f("Full Refresh completed for dataset: main.s.t_x.", t))
        self.assertFalse(f("Full Refresh completed for dataset: x.main.s.t.", t))
        self.assertTrue(f("Full Refresh completed for dataset: main.s.t", "`main`.`s`.`T`"))  # backticks, case


class CompletedMs(unittest.TestCase):
    """completed_ms with a fake databricks.sdk: events newest first, timestamps as the API gives them."""

    def run_with(self, events, pipeline_id="p1", since=None):
        def ev(ts, msg):
            return types.SimpleNamespace(timestamp=ts, message=msg)
        client = types.SimpleNamespace(
            tables=types.SimpleNamespace(get=lambda _n: types.SimpleNamespace(pipeline_id=pipeline_id)),
            pipelines=types.SimpleNamespace(list_pipeline_events=lambda **_k: iter([ev(*e) for e in events])))
        sdk = types.ModuleType("databricks.sdk"); sdk.WorkspaceClient = lambda: client
        old = {k: sys.modules.get(k) for k in ("databricks", "databricks.sdk")}
        sys.modules.update({"databricks": types.ModuleType("databricks"), "databricks.sdk": sdk})
        try:
            g["SRC"] = "main.s.t"
            return g["completed_ms"](since)
        finally:
            for k, v in old.items():
                if v is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = v

    def test_cases(self):
        done = ("2026-11-04T21:27:46.530Z", "Full Refresh completed for dataset: main.s.t.")
        other = ("2026-11-04T21:30:00.000Z", "Full Refresh completed for dataset: main.s.t2.")
        asked = ("2026-11-04T20:44:36.283Z", "Full Refresh requested for dataset: main.s.t.")
        ms = 1793827666530                                            # 2026-11-04T21:27:46.530Z
        self.assertEqual(self.run_with([other, done, asked]), ms)
        self.assertEqual(self.run_with([other, done, asked], since=ms - 1), ms)
        self.assertIsNone(self.run_with([other, done, asked], since=ms + 1))   # older than the last snapshot
        self.assertIsNone(self.run_with([other, asked]))
        with self.assertRaisesRegex(RuntimeError, "no ingestion pipeline"):
            self.run_with([done], pipeline_id=None)


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
                                   (11_500, None, False)]:
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


def rowsort(rows):
    """Rows in a fixed order; None sorts first."""
    return sorted(rows, key=lambda r: tuple("" if x is None else str(x) for x in r))


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
        return rowsort(tuple(r) for r in out.collect())

    def test_run(self):
        self.assertEqual(self.versions(RUN), rowsort(RUN_VERSIONS))

    def test_second_refresh(self):
        """A second refresh at T5 reloads 4=d2 (its CDC version is dropped) under the snapshot key, which held d1:
        d1 keeps its source end and is gone from the destination; d2 is one version, from its CDC start."""
        T5 = 1793830000000
        rows = [r for r in RUN if r[0] != 4] + [
            (4, "d1", "", None, S4, TS4, T0, T5), (4, "d2", S4, TS4, None, None, T4, T5), (4, "d2", "", None, None, None, T5, None)]
        want = [v for v in RUN_VERSIONS if v[0] != 4] + [(4, "d1", None, TS4, True), (4, "d2", TS4, None, False)]
        self.assertEqual(self.versions(rows), rowsort(want))

    def test_changed_reload(self):
        """A refresh reloads a value the history never saw (1 -> a2 in the gap): a1 ends when the history saw the reload."""
        got = {(r[0], r[1]): r for r in self.versions(RUN)}
        self.assertEqual(got[(1, "a1")][3], SEEN2)
        self.assertEqual(got[(1, "a2")][2], SEEN2)

    def test_composite_keys(self):
        rows = [(k, 7, *r) for k, *r in RUN]
        want = [(k, 7, *v) for k, *v in RUN_VERSIONS]
        self.assertEqual(self.versions(rows, keys=("id", "part")), rowsort(want))


class NextSnapshot(LocalSpark):
    """The order of next_snapshot's checks, with the helpers stubbed."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        import time
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "next_snapshot")
        cls.g.update({"time": time, "KEYS": ["id"], "DATA": ["v"], "SRC": "src", "UPDATE_START_MS": 5_000})
        exec(compile(ast.Module(body=[fn], type_ignores=[]), SRC, "exec"), cls.g)  # noqa: S102

    def call(self, latest, rows=1, check="events", allow_empty=False, since=True, pending=False, state=(False, True)):
        calls = []
        df = self.spark.sql("SELECT 1 AS id, 'a' AS v, TIMESTAMP'2026-11-04 10:00:00' AS __START_AT, "
                            "CAST(NULL AS TIMESTAMP) AS __END_AT").limit(rows)
        self.g.update({"RELOAD_CHECK": check, "ALLOW_EMPTY": allow_empty, "SCHEMA": df.schema,
                       "committed_since": lambda _l: since, "source_state": lambda: state,
                       "reload_pending": lambda _l: calls.append("pending") or pending,
                       "spark": types.SimpleNamespace(read=types.SimpleNamespace(table=lambda _n: df))})
        return self.g["next_snapshot"](latest), calls

    def test_order(self):
        self.assertIsNone(self.call(6_000)[0])                         # a snapshot already taken in this update
        self.assertIsNone(self.call(4_000, since=False)[0])            # nothing new since the last one
        with self.assertRaisesRegex(RuntimeError, "being reloaded"):
            self.call(4_000, pending=True)
        snap, calls = self.call(4_000, pending=True, check="history")  # history only: the events aren't read
        self.assertEqual(calls, [])
        self.assertGreater(snap[1], 4_000)
        _, calls = self.call(None, pending=True)                       # the first snapshot: no event check
        self.assertEqual(calls, [])
        with self.assertRaisesRegex(RuntimeError, "being reloaded"):
            self.call(4_000, state=(True, True))
        with self.assertRaisesRegex(RuntimeError, "empty after rows"):
            self.call(4_000, rows=0)
        self.assertEqual(self.call(4_000, rows=0, allow_empty=True)[0][0].count(), 0)
        self.assertEqual(self.call(4_000, rows=0, state=(False, False))[0][0].count(), 0)   # never had rows


class KnownLimits(Versions):
    """What the route gets wrong (SKILL.md, what it can't keep)."""

    @unittest.expectedFailure
    def test_value_back_then_refresh(self):
        """4: d1 (first load) -> d2 -> d1 by CDC, then a full refresh reloads 4=d1 with no source start: same key
        (4, '') and same data as the first-load row, so AUTO CDC updates that row in place (src_end isn't tracked) and
        its end is lost (inferred from AUTO CDC's rules; this case wasn't run). The right answer: three versions."""
        S5, TS5, T6 = "0000003000000001", "2026-11-04 22:00:00", 1793832000000
        rows = [(4, "d1", "", None, None, None, T0, None),              # src_end S4 overwritten by the reload
                (4, "d2", S4, TS4, S5, TS5, T4, T6), (4, "d1", S5, TS5, None, None, T6 - 1, T6)]
        self.assertEqual(self.versions(rows), rowsort([(4, "d1", None, TS4, True), (4, "d1", TS5, None, False),
                                                       (4, "d2", TS4, TS5, True)]))

    test_run = test_second_refresh = test_changed_reload = test_composite_keys = None


if __name__ == "__main__":
    unittest.main()
