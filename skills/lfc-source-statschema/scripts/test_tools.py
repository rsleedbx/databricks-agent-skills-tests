"""Offline tests of statschema.tools: ddl() and ddl_to_schema() give the same text as the command-line wrappers
(scripts/ddl.py, scripts/ddl-to-schema.py) for TPC-B and TPC-H from tpc-schema.sh, with the schema given as a path, YAML
text or a dict; the arguments, input, sys.path and sqlglot's MySQL reader are restored after a call; the wrappers and
canon.py still import; the rows a spec fixes, written as YAML rules (FixedRows).

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


class FixedRows(unittest.TestCase):
    """Rows a spec fixes, written as rules in the YAML (generation: expression and the table's formulas:, shape.py):
    TPC-DS date_dim's calendar, time_dim and the demographics' products, run in SQLite (stdlib) after sqlglot's
    transpile, against Python's calendar and the spec's first and last rows; a {name} not in formulas: or a loop stops
    shape(); a foreign key column with an expression keeps it."""

    @classmethod
    def setUpClass(cls):
        import re
        import sqlite3
        import sqlglot
        from sqlglot.dialects.mysql import MySQL
        from sqlglot.tokens import TokenType
        from statschema.tools import model, shape
        text, _ = shape(model("tpcds"), sf=0.01)
        cls.T = {t["name"]: t for t in yaml.safe_load(text)["tables"]}
        term, factor = MySQL.Parser.TERM, MySQL.Parser.FACTOR   # % binds with * in MySQL (ddl.py reads it so too)
        MySQL.Parser.TERM = {k: v for k, v in term.items() if k != TokenType.MOD}
        MySQL.Parser.FACTOR = {**factor, TokenType.MOD: sqlglot.exp.Mod}
        try:
            cls.sql = {(t, c["name"]): sqlglot.transpile(re.search(r"lfc:[ve]=\{(.*)\}", c["comment"]).group(1),
                                                         read="mysql", write="sqlite")[0]
                       for t in ("date_dim", "time_dim", "customer_demographics", "household_demographics", "income_band")
                       for c in cls.T[t]["columns"] if "lfc:" in (c.get("comment") or "")}
        finally:
            MySQL.Parser.TERM, MySQL.Parser.FACTOR = term, factor
        cls.db = sqlite3.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def row(self, table, n, *cols):
        return tuple(self.db.execute(f"SELECT {self.sql[(table, c)]} FROM (SELECT {n} AS n) x").fetchone()[0] for c in cols)

    def test_date_dim_is_the_calendar(self):
        import datetime
        d0 = datetime.date(1900, 1, 2)
        days = [1, 73049, 36524, 36583] + list(range(2, 73049, 997)) + [(datetime.date(y, 2, 29) - d0).days + 1 for y in (1904, 2000, 2096)]
        for n in days:
            d = d0 + datetime.timedelta(n - 1)
            end = ((d.replace(day=28) + datetime.timedelta(4)).replace(day=1) - datetime.timedelta(1)).day
            self.assertEqual(self.row("date_dim", n, "d_year", "d_moy", "d_dom", "d_dow", "d_qoy", "d_day_name", "d_weekend", "d_last_dom"),
                             (d.year, d.month, d.day, (d.weekday() + 1) % 7, (d.month - 1) // 3 + 1, d.strftime("%A"),
                              "Y" if d.weekday() >= 5 else "N", n - d.day + end), d)
        july4 = (datetime.date(2000, 7, 4) - d0).days + 1
        self.assertEqual(self.row("date_dim", july4, "d_holiday", "d_following_holiday", "d_quarter_name"), ("Y", "N", "2000Q3"))
        self.assertEqual(self.row("date_dim", july4 + 1, "d_holiday", "d_following_holiday"), ("N", "Y"))

    def test_products_first_and_last_rows(self):
        cd = ("cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating",
              "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count")
        self.assertEqual(self.row("customer_demographics", 1, *cd), ("M", "M", "Primary", 500, "Good", 0, 0, 0))
        self.assertEqual(self.row("customer_demographics", 1920800, *cd), ("F", "U", "Unknown", 10000, "Unknown", 6, 6, 6))
        hd = ("hd_income_band_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count")
        self.assertEqual(self.row("household_demographics", 7200, *hd), (20, "Unknown", 9, 4))
        self.assertEqual(self.row("time_dim", 18 * 3600 + 1, "t_hour", "t_am_pm", "t_shift", "t_meal_time"), (18, "PM", "second", "dinner"))
        self.assertEqual(self.row("income_band", 2, "ib_lower_bound", "ib_upper_bound"), (10001, 20000))
        self.assertNotIn("builtin_generator", open(os.path.join(SCHEMAS, "tpcds.yaml")).read())
        self.assertNotIn("formulas", self.T["date_dim"])   # spent: the tags hold them
        from statschema.core.model import CanonicalTableSchema
        raw = next(x for x in yaml.safe_load(open(os.path.join(SCHEMAS, "tpcds.yaml")))["tables"] if x["name"] == "date_dim")
        back = CanonicalTableSchema.from_dict(raw).to_dict()   # a round trip through the model keeps the rules
        self.assertEqual(back["formulas"], raw["formulas"])
        self.assertEqual({c["name"]: (c.get("generation") or {}).get("expression") for c in back["columns"]},
                         {c["name"]: (c.get("generation") or {}).get("expression") for c in raw["columns"]})

    def test_bad_names_stop_and_a_foreign_key_keeps_its_expression(self):
        from statschema.tools import shape

        def schema(expr, formulas):
            return {"version": "1.0", "tables": [
                {"name": "p", "row_count": 5, "columns": [{"name": "id", "type": "integer", "primary_key": True}]},
                {"name": "c", "row_count": 10, "formulas": formulas, "columns": [
                    {"name": "id", "type": "integer", "primary_key": True},
                    {"name": "pid", "type": "integer", "generation": {"expression": expr}}],
                 "fk_constraints": [{"columns": ["pid"], "parent_table": "p", "parent_columns": ["id"]}]}]}
        with self.assertRaises(SystemExit):
            shape(schema("{nope}", {}))
        with self.assertRaises(SystemExit):
            shape(schema("{a}", {"a": "{b}", "b": "{a}"}))
        c = {t["name"]: t for t in yaml.safe_load(shape(schema("{k} + 1", {"k": "(x.n - 1) % 5"}))[0])["tables"]}["c"]
        self.assertIn("lfc:v={((x.n - 1) % 5) + 1}", next(x["comment"] for x in c["columns"] if x["name"] == "pid"))


class CompositeKeys(unittest.TestCase):
    """A composite key's columns past the first: an integer range (with a step) or list keeps its values, and the rest
    of the key is one mixed-radix number, so every column varies; each row's key is unique and lfc:n reads n back.
    Run in SQLite (stdlib) after sqlglot's transpile."""

    def test_three_columns_unique_in_range_and_read_back(self):
        import re
        import sqlite3
        import sqlglot
        from statschema.tools import shape
        sch = {"version": "1.0", "tables": [
            {"name": "item", "row_count": 6, "columns": [{"name": "i", "type": "integer", "primary_key": True}]},
            {"name": "inv", "row_count": 60, "columns": [
                {"name": "wk", "type": "integer", "primary_key": True, "generation": {"min_value": 100, "max_value": 128, "distribution_params": {"step": 7}}},
                {"name": "item", "type": "integer", "primary_key": True},
                {"name": "q", "type": "integer", "primary_key": True, "generation": {"values": [1, 2, 3, 4]}}],
             "fk_constraints": [{"columns": ["item"], "parent_table": "item", "parent_columns": ["i"]}]}]}
        t = {x["name"]: x for x in yaml.safe_load(shape(sch)[0])["tables"]}["inv"]
        f = {c["name"]: re.search(r"lfc:v=\{(.*)\}", c["comment"]).group(1) for c in t["columns"]}
        back = re.search(r"lfc:n=\{(.*)\}", t["lfc_tags"]).group(1)
        lite = lambda s: sqlglot.transpile(s, read="mysql", write="sqlite")[0]
        db = sqlite3.connect(":memory:")
        try:
            rows = [db.execute(f"SELECT {lite(f['wk'])}, {lite(f['item'])}, {lite(f['q'])} FROM (SELECT {n} AS n) x").fetchone()
                    for n in range(1, t["row_count"] + 1)]
            self.assertEqual(len(set(rows)), len(rows))                                   # unique
            self.assertEqual(sorted({r[0] for r in rows}), [100, 107, 114, 121, 128])      # every week, by the step
            self.assertTrue({r[1] for r in rows} <= set(range(1, 7)) and len({r[2] for r in rows}) > 1)   # q varies
            for n, (wk, item, q) in enumerate(rows, 1):
                got = db.execute(f"SELECT {lite(back)} FROM (SELECT {wk} AS wk, {item} AS item, {q} AS q) x").fetchone()[0]
                self.assertEqual(got, n)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
