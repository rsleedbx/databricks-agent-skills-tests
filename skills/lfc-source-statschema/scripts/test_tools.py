"""Offline tests of statschema.tools: ddl() and ddl_to_schema() give the same text as the command-line wrappers
(scripts/ddl.py, scripts/ddl-to-schema.py) for TPC-B and TPC-H from tpc-schema.sh, with the schema given as a path, YAML
text or a dict; the arguments, input, sys.path and sqlglot's MySQL reader are restored after a call; the wrappers and canon.py still import.

  uv run --no-project --with pyyaml --with sqlglot python -m unittest test_tools.py
"""
from __future__ import annotations

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

    def test_canon_wrapper(self):
        import canon
        from statschema.tools.canon import canon as real
        self.assertIs(canon.canon, real)


if __name__ == "__main__":
    unittest.main()
