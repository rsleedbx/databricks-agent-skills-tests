"""Offline checks of lfc_medallion.coltypes, the one widening rule for a column across sources: the rule (integers,
decimals, past 38 digits, dates, text, types that don't widen), schema-union.py's answers unchanged on raw values
(its widen at BEFORE, read with git), Spark types read precisely (TIMESTAMP has a time zone), what Delta widens in
place, and stack's aligned() on local Spark (skipped without pyspark) for the unions that failed or lost data before.

    python3 -m unittest skills/lfc-medallion/tests/test_coltypes.py
"""
import ast
import datetime
import itertools
import os
import subprocess
import sys
import unittest
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_medallion import coltypes as ct
from lfc_medallion.stack import aligned

BEFORE = "64d4af7"   # origin/main when coltypes.py came in: schema-union.py's own widen
REPO = subprocess.run(["git", "-C", HERE, "rev-parse", "--show-toplevel"], capture_output=True, text=True,
                      check=True).stdout.strip()


def d(p, s):
    return {"type": "decimal", "precision": p, "scale": s}


class Rule(unittest.TestCase):
    def test_widest(self):
        for types, want in ((["int", "bigint"], "BIGINT"), (["smallint", "int"], "INT"), (["tinyint", "smallint"], "SMALLINT"),
                            (["tinyint", "tinyint"], "TINYINT"), (["smallint", "decimal(4,1)"], "DECIMAL(11,1)"),
                            (["decimal(10,2)", "decimal(12,4)"], "DECIMAL(12,4)"),
                            (["decimal(10,2)", "decimal(12,1)"], "DECIMAL(13,2)"),
                            (["int", "decimal(5,2)"], "DECIMAL(12,2)"),            # an INT as decimal(10,0)
                            (["bigint", "decimal(10,2)"], "DECIMAL(22,2)"),        # typed, a BIGINT as decimal(20,0)
                            (["decimal(38,0)", "decimal(38,10)"], "DECIMAL(38,10)"),   # past 38: the scale is kept
                            (["int", "string"], "STRING"), (["date", "int"], "STRING"), (["int", "boolean"], "STRING"),
                            (["timestamp", "timestamp_ntz"], "STRING"),            # no silent time-zone shift
                            (["date", "timestamp"], "TIMESTAMP"), (["date", "timestamp_ntz"], "TIMESTAMP_NTZ"),
                            (["varchar(5)", "varchar(9)"], "STRING"), (["float", "double"], "DOUBLE"),
                            (["int", "double"], "DOUBLE")):
            self.assertEqual(ct.widest(types), want, types)

    def test_raw_values_keep_a_date_from_a_timestamp(self):   # schema-union.py: a date arrives as a count of days
        self.assertEqual(ct.widen({"type": "date"}, {"type": "timestamp"})["type"], "string")
        self.assertEqual(ct.widen({"type": "date"}, {"type": "timestamp"}, typed=True)["type"], "timestamp")
        self.assertEqual(ct.widen({"type": "float"}, {"type": "double"})["type"], "string")
        self.assertEqual(ct.widen({"type": "long"}, d(10, 2))["precision"], 21)   # raw: a long as decimal(19,0)

    def test_from_spark(self):
        self.assertEqual(ct.from_spark("timestamp"), {"type": "timestamptz"})
        self.assertEqual(ct.from_spark("TIMESTAMP_NTZ"), {"type": "timestamp"})
        self.assertEqual(ct.from_spark("decimal(12, 4)"), d(12, 4))
        self.assertEqual(ct.from_spark("varchar(20)"), {"type": "string"})
        self.assertEqual(ct.from_spark("array<int>"), {"type": "array<int>"})
        self.assertEqual(ct.from_spark("smallint"), {"type": "integer", "bits": 16})
        self.assertNotEqual(ct.from_spark("smallint"), ct.from_spark("int"))   # so a SMALLINT target widens to INT

    def test_widest_of_a_target_widens_in_place(self):   # what merge_all asks Delta for is a change it makes
        for old, src in (("int", "bigint"), ("int", "decimal(5,2)"), ("smallint", "int"), ("tinyint", "decimal(6,2)"), ("bigint", "decimal(10,2)"), ("bigint", "decimal(38,0)"),
                         ("decimal(10,2)", "decimal(12,4)"), ("date", "timestamp_ntz"), ("float", "double")):
            self.assertTrue(ct.widens_in_place(old, ct.widest([old, src])), (old, src))

    def test_widens_in_place(self):
        for old, new, ok in (("int", "bigint", True), ("int", "decimal(12,2)", True), ("decimal(10,2)", "decimal(12,4)", True),
                             ("int", "decimal(9,0)", False), ("int", "decimal(10,0)", True),     # as Delta: live 2026-11-16
                             ("bigint", "decimal(19,0)", False), ("bigint", "decimal(20,0)", True),
                             ("smallint", "int", True), ("tinyint", "smallint", True), ("int", "smallint", False),
                             ("smallint", "decimal(10,0)", True), ("smallint", "decimal(9,0)", False),
                             ("tinyint", "decimal(12,2)", True), ("tinyint", "decimal(11,2)", False),
                             ("decimal(10,2)", "decimal(12,1)", False), ("decimal(10,4)", "decimal(10,6)", False),
                             ("date", "timestamp_ntz", True), ("date", "timestamp", False), ("int", "string", False),
                             ("float", "double", True), ("bigint", "int", False)):
            self.assertEqual(ct.widens_in_place(old, new), ok, (old, new))


class SchemaUnionSame(unittest.TestCase):
    """schema-union.py now imports this widen (typed=False): its answers on every pair are the ones it gave before."""

    def test_every_pair_same(self):
        text = subprocess.run(["git", "-C", REPO, "show",
                               f"{BEFORE}:skills/zerobus-debezium-test/scripts/schema-union.py"],
                              capture_output=True, text=True, check=True).stdout
        keep = [n for n in ast.parse(text).body if (isinstance(n, ast.FunctionDef) and n.name == "widen")
                or (isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "RANK")]
        ns = {}
        exec(compile(ast.Module(body=keep, type_ignores=[]), "schema-union.py", "exec"), ns)  # noqa: S102 - this repo's own code at BEFORE
        cols = [{"type": t} for t in ("integer", "long", "string", "char", "date", "timestamp", "timestamptz",
                                      "boolean", "double", "binary")]
        cols += [d(p, s) for p, s in ((10, 2), (12, 4), (38, 0), (38, 10), (5, 0))]
        cols += [{"type": "string", "length": 5}, {"type": "char", "length": 9}, {"type": "decimal"}]
        for a, b in itertools.product(cols, repeat=2):
            self.assertEqual(ct.widen(a, b), ns["widen"](a, b), (a, b))


try:
    from pyspark.sql import SparkSession
except ImportError:   # pragma: no cover
    SparkSession = None


@unittest.skipIf(SparkSession is None, "pyspark not installed")
class AlignedOnSpark(unittest.TestCase):
    """aligned() on local Spark: the unions that failed or lost data with unionByName alone now hold every value."""

    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
                         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        except RuntimeError as e:   # Databricks Connect's pyspark: remote sessions only (the repo's test runner runs this
            raise unittest.SkipTest(f"no local Spark session: {str(e)[:80]}") from e   # with pyspark through uv)

    def stack(self, a, b):
        x, y = aligned([self.spark.sql(f"SELECT 'e' AS s, {a} AS c"), self.spark.sql(f"SELECT 'w' AS s, {b} AS c")])
        u = x.unionByName(y, allowMissingColumns=True)
        return u.schema["c"].dataType.simpleString(), [r.c for r in u.orderBy("s").collect()]

    def test_unions_that_failed_or_lost_data(self):
        self.assertEqual(self.stack("CAST(7 AS INT)", "'abc'"), ("string", ["7", "abc"]))
        self.assertEqual(self.stack("DATE'2026-01-01'", "CAST(5 AS INT)"), ("string", ["2026-01-01", "5"]))
        self.assertEqual(self.stack("CAST(1 AS INT)", "true"), ("string", ["1", "true"]))
        self.assertEqual(self.stack("CAST(1 AS DECIMAL(38,0))", "CAST(1.5 AS DECIMAL(38,10))"),
                         ("decimal(38,10)", [Decimal("1.0000000000"), Decimal("1.5000000000")]))   # was rounded to 2
        kind, rows = self.stack("TIMESTAMP'2026-01-01 10:00:00'", "TIMESTAMP_NTZ'2026-01-01 10:00:00'")
        self.assertEqual(kind, "string")   # each source's text, neither read in the other's time zone
        self.assertEqual([r[:19] for r in rows], ["2026-01-01 10:00:00", "2026-01-01 10:00:00"])

    def test_unions_that_worked_stay(self):
        self.assertEqual(self.stack("CAST(1 AS INT)", "CAST(3000000000 AS BIGINT)"), ("bigint", [1, 3000000000]))
        self.assertEqual(self.stack("DATE'2026-01-01'", "TIMESTAMP_NTZ'2026-01-01 10:00:00'"),
                         ("timestamp_ntz", [datetime.datetime(2026, 1, 1), datetime.datetime(2026, 1, 1, 10)]))

    def test_agreeing_columns_are_left_as_they_are(self):
        frames = [self.spark.sql("SELECT 1 AS k, 'a' AS v"), self.spark.sql("SELECT 2 AS k, 'b' AS v")]
        self.assertEqual([id(f) for f in aligned(frames)], [id(f) for f in frames])   # the same frames: no cast added


if __name__ == "__main__":
    unittest.main()
