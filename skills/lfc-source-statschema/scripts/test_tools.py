"""Offline tests of statschema.tools: ddl() and ddl_to_schema() give the same text as the command-line wrappers
(scripts/ddl.py, scripts/ddl-to-schema.py) for TPC-B and TPC-H from tpc-schema.sh, with the schema given as a path, YAML
text or a dict; the arguments, input, sys.path and sqlglot's MySQL reader are restored after a call; the wrappers and canon.py still import.

  uv run --no-project --with pyyaml --with sqlglot python -m unittest test_tools.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from statschema.tools import ddl, ddl_to_schema

TPC = os.path.join(HERE, "..", "..", "lfc-source-datagen", "scripts", "tpc", "tpc-schema.sh")


def cli(script, *args, stdin=""):
    return subprocess.run([sys.executable, os.path.join(HERE, script), *args], input=stdin, capture_output=True,
                          text=True, check=True).stdout


class Tools(unittest.TestCase):
    def test_same_as_the_wrappers(self):
        for w in ("tpcb", "tpch"):
            my = subprocess.run(["bash", TPC, w, "1"], capture_output=True, text=True, check=True).stdout
            y = ddl_to_schema(my, "mysql")
            self.assertEqual(y, cli("ddl-to-schema.py", "--dialect", "mysql", stdin=my), w)
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
                f.write(y)
            try:
                for d in ("postgres", "tsql", "informix"):
                    want = cli("ddl.py", f.name, "--dialect", d)
                    self.assertIn("CREATE TABLE", want)
                    for form in (f.name, y, yaml.safe_load(y)):
                        self.assertEqual(ddl(form, d), want, f"{w} {d} {type(form).__name__}")
            finally:
                os.unlink(f.name)

    def test_state_restored(self):
        argv, stdin, path = list(sys.argv), sys.stdin, list(sys.path)
        ddl(ddl_to_schema("CREATE TABLE a (id INT PRIMARY KEY) COMMENT='lfc:rows=10';", "mysql"), "postgres")
        self.assertEqual((sys.argv, sys.stdin, sys.path), (argv, stdin, path))

    def test_sqlglot_mysql_restored(self):
        from sqlglot.dialects.mysql import MySQL
        term, factor = MySQL.Parser.TERM, MySQL.Parser.FACTOR
        y = ddl_to_schema("CREATE TABLE a (id INT PRIMARY KEY, v INT COMMENT 'lfc:v={1 + x.n * 7 % 25}') COMMENT='lfc:rows=10';", "mysql")
        ddl(y, "informix")
        self.assertIs(MySQL.Parser.TERM, term); self.assertIs(MySQL.Parser.FACTOR, factor)
        self.assertEqual(ddl_to_schema("CREATE TABLE b (id INT PRIMARY KEY, CHECK (id % 3 + 1 > 0));", "mysql"),
                         cli("ddl-to-schema.py", "--dialect", "mysql", stdin="CREATE TABLE b (id INT PRIMARY KEY, CHECK (id % 3 + 1 > 0));"))

    def test_shape_same_as_the_wrapper(self):
        from statschema.tools import model, shape
        for m in ("tpcb", "tpcc"):
            with tempfile.TemporaryDirectory() as d:
                want = cli("shape.py", model(m), "--sf", "0.01", "--expected", os.path.join(d, "e.json"))
                with open(os.path.join(d, "e.json")) as f:
                    want_e = __import__("json").load(f)
            got, got_e = shape(model(m), sf=0.01)
            self.assertEqual((got, got_e), (want, want_e), m)
            with open(model(m)) as f:
                self.assertEqual(shape(yaml.safe_load(f), sf=0.01)[0], want)
        with self.assertRaisesRegex(ValueError, "one of tpcb"):
            model("nosuch")

    def test_shape_text_stats_scale_keep_small(self):
        from statschema.tools import model, shape
        with open(model("tpcb")) as f:
            text = f.read()
        with tempfile.TemporaryDirectory() as d:
            e = os.path.join(d, "e.json")
            self.assertEqual(shape(text, sf=0.01)[0], cli("shape.py", model("tpcb"), "--sf", "0.01", "--expected", e))
            schema = {"tables": [{"name": "t", "row_count": 50, "columns": [
                {"name": "id", "type": "integer", "not_null": True, "primary_key": True},
                {"name": "v", "type": "integer"}]}, {"name": "big", "row_count": 5000, "columns": [
                {"name": "id", "type": "integer", "not_null": True, "primary_key": True}]}]}
            stats = {"tables": [{"name": "t", "row_count": 50, "columns": [
                {"name": "v", "null_fraction": 0.1, "n_distinct": 5, "most_common_values": [{"value": 3, "frequency": 0.5}]}]}]}
            sp, tp = os.path.join(d, "s.yaml"), os.path.join(d, "t.yaml")
            for path, obj in ((sp, schema), (tp, stats)):
                with open(path, "w") as f:
                    yaml.safe_dump(obj, f, sort_keys=False)
            want = cli("shape.py", sp, "--stats", tp, "--scale", "2.0", "--keep-small", "10", "--expected", e)
            with open(e) as f:
                want_e = __import__("json").load(f)
            got = shape(schema, stats=stats, scale=2, keep_small=10)
            self.assertEqual(got, (want, want_e))
            self.assertIn("lfc:rows=100", want)                           # t: 50 rows x scale 2 (above keep_small)
            self.assertNotEqual(shape(schema, stats=stats, scale=2, keep_small=10)[0], shape(schema, scale=2, keep_small=10)[0])

    def test_wheel_ships_the_models_and_model_finds_them(self):
        import shutil
        import zipfile
        if not shutil.which("uv"):
            self.skipTest("uv is needed to build the wheel")
        with tempfile.TemporaryDirectory() as d:
            skill = os.path.join(d, "skill")                            # a copy: the build leaves files where it runs
            shutil.copytree(os.path.dirname(HERE), skill, ignore=shutil.ignore_patterns("__pycache__", "build", "*.egg-info"))
            subprocess.run(["uv", "build", "-q", "--wheel", "--out-dir", os.path.join(d, "dist"), skill], check=True,
                           capture_output=True)
            whl = next(os.path.join(d, "dist", f) for f in os.listdir(os.path.join(d, "dist")) if f.endswith(".whl"))
            names = zipfile.ZipFile(whl).namelist()
            models = sorted(f[:-5] for f in os.listdir(os.path.join(skill, "schemas")) if f.endswith(".yaml"))
            self.assertEqual(len(models), 6)
            self.assertIn("statschema/tools/shape.py", names)
            for m in models:
                self.assertIn(f"statschema/_schemas/{m}.yaml", names)
            site = os.path.realpath(os.path.join(d, "site"))             # the wheel unpacked: an installed package's layout
            zipfile.ZipFile(whl).extractall(site)
            code = "import sys; sys.path.insert(0, sys.argv[1]); from statschema.tools import model; print(model('tpcb'))"
            out = subprocess.run([sys.executable, "-I", "-c", code, site],
                                 capture_output=True, text=True, check=True).stdout.strip()
            self.assertEqual(os.path.realpath(out), os.path.join(site, "statschema", "_schemas", "tpcb.yaml"))

    def test_canon_wrapper(self):
        import canon
        from statschema.tools.canon import canon as real
        self.assertIs(canon.canon, real)


if __name__ == "__main__":
    unittest.main()


SCHEMAS = os.path.join(HERE, "..", "schemas")
TPC_MODELS = ("tpcb", "tpcc", "tpch", "tpcdi", "tpcds", "tpce")
# fact and dimension counts per model, from the specifications' roles (TPC-DS: 7 fact, 17 dimension tables)
KINDS = {"tpcb": (1, 3), "tpcc": (5, 4), "tpch": (3, 5), "tpcdi": (6, 13), "tpcds": (7, 17), "tpce": (16, 17)}


class Roles(unittest.TestCase):
    """Every TPC table says its role; the model keeps it on a load and a dump; describe.py groups by kind."""

    def test_every_tpc_table_has_a_role_and_the_kinds_add_up(self):
        from statschema.core.model import ROLES
        from statschema.core.schema_io import load_canonical
        for m in TPC_MODELS:
            tables = load_canonical(os.path.join(SCHEMAS, f"{m}.yaml"))
            self.assertTrue(all(t.role in ROLES for t in tables), m)
            got = (sum(t.kind == "fact" for t in tables), sum(t.kind == "dimension" for t in tables))
            self.assertEqual(got, KINDS[m], m)

    def test_role_survives_a_dump_and_an_unknown_one_is_refused(self):
        from statschema.core.model import CanonicalTableSchema
        t = CanonicalTableSchema.from_dict({"name": "lineitem", "role": "fact", "columns": []})
        self.assertEqual((t.role, t.kind, t.to_dict()["role"]), ("fact", "fact", "fact"))
        self.assertIsNone(CanonicalTableSchema.from_dict({"name": "x", "columns": []}).kind)   # a collected schema
        with self.assertRaises(ValueError):
            CanonicalTableSchema.from_dict({"name": "x", "role": "facts", "columns": []})

    def test_describe_lists_by_kind_and_tpch_has_its_lateness(self):
        d = json.loads(cli("describe.py", os.path.join(SCHEMAS, "tpch.yaml"), "--json"))
        self.assertEqual((d["tables"], d["fact"], d["dimension"]), (8, 3, 5))
        self.assertEqual([r["table"] for r in d["by_table"] if r["kind"] == "fact"], ["partsupp", "lineitem", "orders"])
        (sig,) = d["signals"]
        self.assertEqual(sig["name"], "lineitem_lateness")
        self.assertEqual(len(sig["parameters"]), 9)   # the nine numbers of lfc-tpch-sources' <name>:<sf>:<lead times>
        self.assertIn("signals: none", cli("describe.py", os.path.join(SCHEMAS, "tpcb.yaml")))
        ds = json.loads(cli("describe.py", os.path.join(SCHEMAS, "tpcds.yaml"), "--json"))["signals"]
        self.assertEqual([s["name"] for s in ds], ["return_propensity", "web_ship_delay"])   # one retailer, both
