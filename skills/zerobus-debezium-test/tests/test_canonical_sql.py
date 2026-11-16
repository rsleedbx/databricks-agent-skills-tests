"""Offline checks of canonical_sql.py, the one copy of the column SQL the skill's scripts share, against the copies it
replaced (origin/main before the change, read with git): the pipeline's (silver-from-schema.py) SQL text is the same
for every column; the merge job's (merge-silver.py) differs only where the pipeline's had fixes the job lacked
(MySQL UNSIGNED, 7-9 fraction digits, Oracle's TIMESTAMP(0) time, hex binary). placement.py and model-sql.py give
the same output, run as the skill runs them (python3 -I, which leaves the script's folder off sys.path).

    python3 -m unittest skills/zerobus-debezium-test/tests/test_canonical_sql.py
"""
import ast
import itertools
import os
import subprocess
import sys
import tempfile
import unittest
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "scripts")
sys.path.insert(0, SCRIPTS)
import canonical_sql as cs

BEFORE = "f8b4dbc"   # origin/main when canonical_sql.py came in: the last commit with the copies
REPO = subprocess.run(["git", "-C", HERE, "rev-parse", "--show-toplevel"], capture_output=True, text=True,
                      check=True).stdout.strip()


def old(script, names, **globs):
    """The functions `names` (and ALIAS, TEMPORAL) from `script` at BEFORE, as a dict, with globs as their globals."""
    text = subprocess.run(["git", "-C", REPO, "show", f"{BEFORE}:skills/zerobus-debezium-test/scripts/{script}"],
                          capture_output=True, text=True, check=True).stdout
    keep = [n for n in ast.parse(text).body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in ("ALIAS", "TEMPORAL") for t in n.targets))]
    ns = {"re": __import__("re"), "zlib": zlib, **globs}
    exec(compile(ast.Module(body=keep, type_ignores=[]), script, "exec"), ns)  # noqa: S102 - this repo's own code at BEFORE
    return ns


TYPES = ["integer", "int", "smallint", "tinyint", "long", "bigint", "decimal", "numeric", "float", "real", "double",
         "double precision", "boolean", "bool", "string", "varchar", "text", "char", "date", "timestamp",
         "timestamptz", "time", "timetz", "binary", "uuid"]
COLUMNS = [dict(name="c", type=t, **({"precision": p} if p is not None else {}), **({"unsigned": True} if u else {}),
                **({"scale": 2} if t in ("decimal", "numeric") and p else {}))
           for t, p, u in itertools.product(TYPES, [None, 0, 3, 6, 7, 9], [False, True])]
NAMES = ["c", "SK_TimeID", "key:a"]


class PipelineSame(unittest.TestCase):
    """silver-from-schema.py's own copies are what canonical_sql.py took: the SQL text is the same."""

    def test_every_column_same_sql(self):
        for binhex in (False, True):
            o = old("silver-from-schema.py", {"ctype", "spark_type", "decode", "typed", "path", "shard_of"},
                    BINHEX=binhex)
            for c in COLUMNS:
                self.assertEqual(cs.ctype(c), o["ctype"](c))
                self.assertEqual(cs.spark_type(c), o["spark_type"](c), c)
                for cast in ("CAST", "TRY_CAST"):
                    self.assertEqual(cs.decode(c, "s", cast), o["decode"](c, "s", cast), c)
                    self.assertEqual(cs.typed(c, "value:after:c", cast, binhex), o["typed"](c, "value:after:c", cast), c)
        for base, n in itertools.product(["value:after", "key", "v"], NAMES):
            self.assertEqual(cs.path(base, n), o["path"](base, n))
        for t in ("lineitem", "orders", "x" * 40):
            for n in (1, 3, 7):
                self.assertEqual(cs.shard_of(t, n), o["shard_of"](t, n))

    def test_snapshot_value_is_the_pipelines_snap(self):
        text = subprocess.run(["git", "-C", REPO, "show", f"{BEFORE}:skills/zerobus-debezium-test/scripts/silver-from-schema.py"],
                              capture_output=True, text=True, check=True).stdout
        self.assertIn('return f"CASE WHEN typeof({x}) = \'binary\' THEN CAST({x} AS BINARY) ELSE unhex(CAST({x} AS STRING)) END"', text)
        c = {"name": "b", "type": "binary"}
        self.assertEqual(cs.snapshot_value(c, {"b"}, True),
                         "CASE WHEN typeof(`b`) = 'binary' THEN CAST(`b` AS BINARY) ELSE unhex(CAST(`b` AS STRING)) END")
        self.assertEqual(cs.snapshot_value({"name": "x", "type": "int", "default": "7"}, set()), "CAST(7 AS INT)")
        self.assertEqual(cs.snapshot_value({"name": "x", "type": "int"}, set()), "CAST(NULL AS INT)")
        d = {"name": "d", "type": "date"}
        self.assertEqual(cs.snapshot_value(d, {"d"}), cs.decode(d, "CAST(`d` AS STRING)"))


class MergeFixed(unittest.TestCase):
    """merge-silver.py's copies differed from the pipeline's; now the same, so only the fixed cases change."""

    def test_only_the_fixed_cases_change(self):
        o = old("merge-silver.py", {"ctype", "spark_type", "decode", "typed"})
        changed = set()
        for c in COLUMNS:
            t, p = cs.ctype(c), c.get("precision")
            if cs.spark_type(c) != o["spark_type"](c):
                changed.add(("spark_type", t, "unsigned"))
                self.assertTrue(c.get("unsigned") and t in ("integer", "long"), c)
            if cs.decode(c, "s") != o["decode"](c, "s"):
                why = ("nanoseconds" if p is not None and p > 6 else "time text" if t in ("time", "timetz")
                       else "unsigned" if c.get("unsigned") else "?")
                changed.add(("decode", t, why))
                self.assertNotEqual(why, "?", c)
        self.assertEqual({w for _, _, w in changed}, {"unsigned", "nanoseconds", "time text"})
        b = {"name": "b", "type": "binary"}
        self.assertEqual(cs.typed(b, "v", "CAST", True), "unhex(v::STRING)")   # was CAST(v AS BINARY) on hex text
        self.assertEqual(cs.typed(b, "v", "CAST"), o["typed"](b, "v", "CAST"))


class ScriptsSameOutput(unittest.TestCase):
    """placement.py and model-sql.py, run with python3 -I as the skill runs them, print what they printed before."""

    def run_both(self, script, args, cwd):
        new = subprocess.run([sys.executable, "-I", os.path.join(SCRIPTS, script), *args], capture_output=True,
                             text=True, cwd=cwd, check=False)
        self.assertEqual(new.returncode, 0, new.stderr)
        with tempfile.TemporaryDirectory() as d:
            src = subprocess.run(["git", "-C", REPO, "show", f"{BEFORE}:skills/zerobus-debezium-test/scripts/{script}"],
                                 capture_output=True, text=True, check=True).stdout
            with open(os.path.join(d, script), "w") as f:
                f.write(src)
            was = subprocess.run([sys.executable, "-I", os.path.join(d, script), *args], capture_output=True,
                                 text=True, cwd=cwd, check=True)
        self.assertEqual(new.stdout, was.stdout)
        return new.stdout

    def test_placement(self):
        with tempfile.TemporaryDirectory() as d:
            tables = os.path.join(d, "tables.txt")
            with open(tables, "w") as f:
                f.write("\n".join([f"t{i:02d}" for i in range(20)] + ["orders", "lineitem"]) + "\n")
            out = self.run_both("placement.py", [os.path.join(HERE, "..", "placements", "mix20-busy-quiet.yaml"),
                                                 tables], d)
        self.assertGreater(len(out.splitlines()), 10)

    def test_model_sql_select(self):
        model = os.path.join(HERE, "..", "models", "shop.yaml")
        for table, side, dialect in itertools.product(["customers", "orders"], ["source", "databricks"],
                                                      ["pg", "mysql", "mssql"]):
            out = self.run_both("model-sql.py", [model, "select", table, "--prefix", "s1", "--side", side,
                                                 "--dialect", dialect], SCRIPTS)
            self.assertIn("SELECT", out.upper())



class FolderOnPath(unittest.TestCase):
    """How each script finds canonical_sql.py, as checked on serverless (2026-11-16): a job's script has no __file__
    (sys.argv[0] is its path), a pipeline's file has none either (cdc.code is its folder)."""

    def test_job_and_pipeline_dont_need_file(self):
        with open(os.path.join(SCRIPTS, "merge-silver.py")) as f:
            self.assertIn("os.path.dirname(os.path.abspath(sys.argv[0]))", f.read())
        with open(os.path.join(SCRIPTS, "silver-from-schema.py")) as f:
            self.assertIn('or spark.conf.get("cdc.code")', f.read())
        with open(os.path.join(SCRIPTS, "debezium-zerobus.sh")) as f:
            self.assertIn('.configuration["cdc.code"]', f.read())

if __name__ == "__main__":
    unittest.main()
