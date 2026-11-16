"""Offline tests of lfc_source_datagen.sources and .tpch_sources (no database): sources parsed and refused (and
TPC-H's own checks), the SQL files split and their psql variables filled (casts kept), each model's DDL (tpc_schema for
tpch and tpcb, statschema's shape for the others), a new source's steps in order on a fake DB-API connection (guarded
DDL, the reader's grants before the fill, the checks qualified and marked once, the model's steps with search_path,
the rename last, the publication over every source), a resumed copy skipping its marked checks, a finished source
kept, broken rules stopping before the rename, and the command line refusing bad arguments before connecting.

  uv run --no-project --with 'psycopg[binary]' --with pyyaml --with sqlglot python -m unittest test_sources.py
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest
from contextlib import redirect_stderr
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.join(HERE, "..", "..", "lfc-source-statschema", "scripts"),
          os.path.join(HERE, "..", "..", "lfc-source-contract", "scripts")):
    sys.path.insert(0, p)
from lfc_source_datagen import sources as src
from lfc_source_datagen import tpch_sources as tp


class Db:
    """A fake source: answers the catalog questions build() asks; logs every statement."""
    def __init__(self, existing=(), checked=False, bad=0):
        self.existing, self.checked, self.bad, self.log = set(existing), checked, bad, []

    def connect(self):
        db = self

        class Cur:
            def __init__(self):
                self.rows = []

            def execute(self, q, params=None):
                db.log.append(q)
                self.rows = db.answer(q, params)

            def fetchall(self):
                return list(self.rows)

        class Con:
            def cursor(self):
                return Cur()

            def close(self):
                pass
        return Con()

    def answer(self, q, params):
        if q == "SELECT current_database()":
            return [("orders",)]
        if "FROM pg_namespace WHERE nspname = %s" in q and "obj_description" not in q:
            return [(1,)] if params[0] in self.existing else []
        if "obj_description" in q:
            return [(src.CHECKED if self.checked else None,)]
        if q.startswith("SELECT 'bad"):
            return [("bad: x", self.bad)]
        if q.startswith("SELECT '"):
            return [("info", 1)]
        if "lineitem" in q and "PERCENTILE_CONT" in q:
            return [(100, 12.5, 3.0)]
        return []


def build_tpch(db, sources, **k):
    with mock.patch.object(src, "_fill") as fill, mock.patch.object(src.cdc, "publication") as pub:
        rows = tp.tpch_sources(db.connect, sources, log=lambda m: db.log.append(f"LOG {m}"), **k)
    return rows, fill, pub


class Parse(unittest.TestCase):
    def test_forms(self):
        want = [("a", 0.05, "1,0,1,1,5,0,0,1,2"), ("b", 0.1, "")]
        self.assertEqual(src.parse({"a": (0.05, "1,0,1,1,5,0,0,1,2"), "b": 0.1}), want)
        self.assertEqual(src.parse(["a:0.05:1,0,1,1,5,0,0,1,2", "b:0.1"]), want)
        self.assertEqual(src.parse(want), want)                               # its own output

    def test_refused(self):
        for bad in (["Bad:1"], ["a:x"], ["a:0"], ["a:0.03", "a:0.03"], []):
            with self.assertRaises(ValueError, msg=bad):
                src.parse(bad)
        for bad in (["a:0.01"], ["a:0.03:1,2"], ["a:0.03:1,0,0,1,5,0,0,1,2"]):
            with self.assertRaises(ValueError, msg=bad):
                tp.check(src.parse(bad))


class SqlFiles(unittest.TestCase):
    def test_verify_split(self):
        for m in ("tpch", "tpcb"):
            text = src.read_sql(f"{m}-verify.sql")
            self.assertEqual(len(src.statements(text)), len(re.findall(r"^SELECT '", text, re.MULTILINE)), m)

    def test_lead_times_and_labels(self):
        steps = tp.steps("anonymous")("a", "1,2,3,4,5,6,7,8,9")
        body = "\n".join(steps)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")
        self.assertIn("CASE WHEN 'anonymous' = 'spec'", body)
        self.assertIn(")::int AS ship_d", body)
        self.assertIn("od + 1, l_shipdate", body); self.assertIn("% 5 = 0 THEN 4", body)
        self.assertIn("WHEN 'AIR' THEN 6", body); self.assertIn("ELSE 7 END", body); self.assertIn("'SHIP' THEN 8", body)
        self.assertEqual(len(tp.steps("spec")("a", "")), 6)                    # labels only
        with self.assertRaises(ValueError):
            tp.steps("other")

    def test_model_ddl(self):
        ddl, tables = src.model_ddl("tpcb", 1)
        self.assertEqual(sorted(tables), ["account", "branch", "history", "teller"])
        self.assertIn('CREATE TABLE "branch"', ddl)
        ddl, tables = src.model_ddl("tpcc", 0.01)                             # statschema's model, shaped
        self.assertIn("warehouse", tables); self.assertIn('CREATE TABLE "warehouse"', ddl)


class Build(unittest.TestCase):
    def test_new_source_steps_in_order(self):
        db = Db()
        rows, fill, pub = build_tpch(db, ["a:0.03:1,0,1,1,5,0,0,1,2"], login="lfc_cdc", labels="anonymous")
        q = db.log
        ddl = next(i for i, s in enumerate(q) if s.startswith("DO ") and 'CREATE TABLE "lineitem"' in s)
        grant = q.index('GRANT SELECT ON ALL TABLES IN SCHEMA "tpch_0001" TO "lfc_cdc"')
        verify = [s for s in q if s.startswith("SELECT 'bad")]
        mark = q.index(f"COMMENT ON SCHEMA \"tpch_0001\" IS '{src.CHECKED}'")
        path = q.index('SET search_path = "tpch_0001"')
        lead = next(i for i, s in enumerate(q) if s.startswith("WITH d AS"))
        rename = next(i for i, s in enumerate(q) if 'ALTER SCHEMA "tpch_0001" RENAME TO "a"' in s)
        self.assertTrue(ddl < grant < mark < path < lead < rename)
        self.assertTrue(q[ddl].startswith("DO ") and 'SET LOCAL search_path = "tpch_0001"' in q[ddl])
        self.assertTrue(verify and not [s for s in verify if "DATEDIFF(" in s])
        self.assertTrue(all('"tpch_0001".lineitem' in s or "lineitem" not in s for s in verify))
        self.assertIn("COMMENT ON SCHEMA", q[rename])                          # the mark cleared with the rename
        self.assertEqual(fill.call_args.args[1:], ("tpch_0001", 4))
        self.assertEqual(pub.call_args.args[1:], ("orders_pub", ["a"]))
        self.assertEqual(rows, [{"source": "a", "lines": 100, "late_pct": 12.5, "median_days": 3.0}])

    def test_resume_skips_marked_checks_and_kept_source(self):
        db = Db(checked=True)
        build_tpch(db, ["a:0.03"])
        self.assertFalse([s for s in db.log if s.startswith("SELECT 'bad")])
        db = Db(existing={"a"})
        _, fill, _ = build_tpch(db, ["a:0.03"])
        fill.assert_not_called()
        self.assertIn("LOG sources: orders.a exists: kept", db.log)
        self.assertFalse([s for s in db.log if s.startswith("DO ")])

    def test_broken_rules_stop_before_the_rename(self):
        db = Db(bad=3)
        with self.assertRaisesRegex(RuntimeError, "spec rules broken"):
            build_tpch(db, ["a:0.03"])
        self.assertFalse([s for s in db.log if "RENAME" in s])


class CommandLine(unittest.TestCase):
    def test_bad_arguments_before_connecting(self):
        with mock.patch.object(src, "connector", side_effect=AssertionError("connected")), \
             redirect_stderr(io.StringIO()) as err:
            self.assertEqual(tp.main(["--lakebase", "p", "db", "Bad:1"]), 2)
            self.assertEqual(tp.main(["--lakebase", "p", "db", "a:0.01"]), 2)
            self.assertEqual(src.main(["--lakebase", "p", "--model", "tpcc", "db", "a:x"]), 2)
        self.assertIn("lower-case letters", err.getvalue()); self.assertIn("0.03 or more", err.getvalue())


if __name__ == "__main__":
    unittest.main()
