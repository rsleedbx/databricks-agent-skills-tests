"""Offline tests of lfc_source_datagen.tpch_sources (no database): sources parsed and refused, SQL files split and their
psql variables filled (casts kept), the publication kept, created or altered on a fake connection (by schema on 15+,
by table before), a failed ALTER explained, a new source's steps (DDL, verify rewritten, rename, labels, lead times, grants;\nbroken rules stop before the rename), the fill wrapper's exit codes and cleanup, the slot, and the command line refusing\nbad arguments before connecting.

  uv run --no-project --with 'psycopg[binary]' --with pyyaml --with sqlglot python -m unittest test_tpch_sources.py
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
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-source-statschema", "scripts"))   # statschema.tools
from lfc_source_datagen import tpch_sources as ts


def render(q):
    """psycopg.sql objects as text without a connection."""
    return q if isinstance(q, str) else q.as_string(None)


class FakeCursor:
    def __init__(self, db):
        self.db, self.rows = db, []

    def execute(self, q, args=None):
        text = render(q)
        self.db.log.append((text, args))
        self.rows = self.db.answer(text, args)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeDb:
    """Every schema exists (no fill); answers for the publication and slot queries."""
    def __init__(self, version=170000, pub=None, slot=False, alter_fails=False, schemas_exist=True, bad=0):
        self.version, self.pub, self.slot, self.alter_fails, self.log = version, pub, slot, alter_fails, []
        self.schemas_exist, self.bad = schemas_exist, bad

    def answer(self, q, args):
        if "FROM pg_database" in q:
            return [(1,)]
        if "FROM information_schema.schemata" in q or "table_schema = %s" in q:
            return [(1 if self.schemas_exist else 0,)]
        if q.startswith("SELECT 'bad"):
            return [("bad: x", self.bad, 0)]
        if q.startswith("SELECT '"):
            return [("info", 5, 25)]
        if "server_version_num" in q:
            return [(str(self.version),)]
        if "pg_publication_namespace" in q:
            return [(self.pub,)]
        if "FROM information_schema.tables" in q:
            return [(s, "lineitem") for s in args[0]]
        if "FROM pg_publication_tables" in q:
            return [tuple(x.split(".")) for x in (self.pub or "").split(",") if x]
        if "FROM pg_publication WHERE" in q:
            return [(1 if self.pub else 0,)]
        if q.startswith("ALTER PUBLICATION") and self.alter_fails:
            raise RuntimeError("must be superuser to add or set schemas")
        if "lineitem JOIN" in q:
            return [(100, 12.5, 3.0)]
        if "COUNT(*) FROM pg_replication_slots" in q:
            return [(1 if self.slot else 0,)]
        if "slot_name, active" in q:
            return [(args[0], False, "1 kB")]
        return []

    def connect(self, database=None):
        db = self

        class C:
            def cursor(self):
                return FakeCursor(db)

            def close(self):
                pass
        return C()

    def sql(self):
        return [q for q, _ in self.log]


def run(db, *a, **k):
    with mock.patch.object(ts.Pg, "connect", lambda self, database=None: db.connect(database)):
        return ts.tpch_sources(ts.Pg(host="h", password="p"), *a, log=lambda m: db.log.append((m, None)), **k)


class Sources(unittest.TestCase):
    def test_forms(self):
        want = [("a", 0.05, "1,0,1,1,5,0,0,1,2"), ("b", 0.1, "")]
        self.assertEqual(ts._sources({"a": (0.05, "1,0,1,1,5,0,0,1,2"), "b": 0.1}), want)
        self.assertEqual(ts._sources(["a:0.05:1,0,1,1,5,0,0,1,2", "b:0.1"]), want)

    def test_refused(self):
        for bad in (["Bad:1"], ["a:x"], ["a:0.01"], ["a:0.03:1,2"], ["a:0.03:1,0,0,1,5,0,0,1,2"], ["a:0.03", "a:0.03"], []):
            with self.assertRaises(ValueError, msg=bad):
                ts._sources(bad)


class SqlFiles(unittest.TestCase):
    def test_verify_split(self):
        text = ts._read("tpch-verify.sql")
        self.assertEqual(len(ts._statements(text)), len(re.findall(r"^SELECT '", text, re.MULTILINE)))

    def test_lead_times_vars(self):
        out = ts._psql_vars(ts._read("tpch-lead-times.sql"), dict(zip(ts.LEAD, ["1", "2", "3", "4", "5", "6", "7", "8", "9"])))
        body = "\n".join(x for x in out.splitlines() if not x.startswith("--"))
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")
        self.assertIn(")::int AS ship_d", body)
        self.assertIn("od + 1, l_shipdate", body)                       # commit = 1
        self.assertIn("% 5 = 0 THEN 4", body)                           # slow_every 5, slow_nation 4
        self.assertIn("WHEN 'AIR' THEN 6", body); self.assertIn("ELSE 7 END", body); self.assertIn("'SHIP' THEN 8", body)

    def test_labels_vars(self):
        out = ts._psql_vars(ts._read("tpch-labels.sql"), {"names": "anonymous"})
        self.assertIn("CASE WHEN 'anonymous' = 'spec'", out)
        self.assertEqual(len(ts._statements(out)), 6)


class Publication(unittest.TestCase):
    def test_created_by_schema(self):
        db = FakeDb()
        rows = run(db, "db", ["a:0.03", "b:0.03"], login="lfc_cdc")
        self.assertIn('CREATE PUBLICATION "db_pub" FOR TABLES IN SCHEMA "a", "b"', db.sql())
        self.assertEqual(rows[0], {"source": "a", "lines": 100, "late_pct": 12.5, "median_days": 3.0})
        self.assertIn("tpch-sources: db.a exists: kept", db.sql())

    def test_kept_any_order(self):
        db = FakeDb(pub="a,b")
        run(db, "db", ["b:0.03", "a:0.03"])
        self.assertFalse([q for q in db.sql() if q.startswith(("ALTER PUBLICATION", "CREATE PUBLICATION"))])

    def test_before_15_by_table(self):
        db = FakeDb(version=140000)
        run(db, "db", ["a:0.03", "b:0.03"], publication="p")
        self.assertIn('CREATE PUBLICATION "p" FOR TABLE "a"."lineitem", "b"."lineitem"', db.sql())
        db = FakeDb(version=140000, pub="a.lineitem,b.lineitem")
        run(db, "db", ["a:0.03", "b:0.03"])
        self.assertFalse([q for q in db.sql() if "PUBLICATION" in q and q.startswith(("ALTER", "CREATE"))])

    def test_altered_and_failed(self):
        db = FakeDb(pub="a,c")
        run(db, "db", ["a:0.03", "b:0.03"])
        self.assertIn('ALTER PUBLICATION "db_pub" SET TABLES IN SCHEMA "a", "b"', db.sql())
        with self.assertRaisesRegex(RuntimeError, "changing it failed"):
            run(FakeDb(pub="a", alter_fails=True), "db", ["a:0.03", "b:0.03"])

    def test_slot(self):
        for exists, created in ((False, True), (True, False)):
            db = FakeDb(slot=exists)
            with mock.patch.object(ts.Pg, "connect", lambda self, database=None, db=db: db.connect(database)):
                r = ts.slot(ts.Pg(host="h", password="p"), "db", "s1")
            self.assertEqual(r["slot"], "s1")
            self.assertEqual(any("pg_create_logical_replication_slot" in q for q in db.sql()), created)
        db = FakeDb(slot=True)
        with mock.patch.object(ts.Pg, "connect", lambda self, database=None: db.connect(database)):
            ts.slot(ts.Pg(host="h", password="p"), "db", "s1", drop=True)
        self.assertTrue(any("pg_drop_replication_slot" in q for q in db.sql()))


class NewSource(unittest.TestCase):
    def build(self, db, *a, **k):
        with mock.patch.object(ts, "_fill") as fill:
            run(db, *a, **k)
        return fill

    def test_build_steps(self):
        db = FakeDb(schemas_exist=False)
        fill = self.build(db, "db", ["a:0.03:1,2,3,4,5,6,7,8,9"], login="lfc_cdc", labels="anonymous")
        q = db.sql()
        self.assertEqual(fill.call_args.args[1:3], ("db", "tpch_0001"))
        self.assertTrue(any(s.startswith('CREATE TABLE "lineitem"') for s in q))         # the spec DDL, run in the copy
        verify = [s for s in q if s.startswith("SELECT 'bad")]
        self.assertTrue(verify); self.assertFalse([s for s in q if "DATEDIFF(" in s])   # rewritten for Postgres
        self.assertIn('ALTER SCHEMA "tpch_0001" RENAME TO "a"', q)
        self.assertTrue(any("CASE WHEN 'anonymous' = 'spec'" in s for s in q))
        lead = [s for s in q if s.startswith("WITH d AS")]
        self.assertTrue(lead and "od + 1, l_shipdate" in lead[0] and "WHEN 'AIR' THEN 6" in lead[0])
        for g in ('GRANT CONNECT ON DATABASE "db" TO "lfc_cdc"', 'GRANT USAGE ON SCHEMA "a" TO "lfc_cdc"',
                  'GRANT SELECT ON ALL TABLES IN SCHEMA "a" TO "lfc_cdc"'):
            self.assertIn(g, q)
        self.assertLess(q.index('ALTER SCHEMA "tpch_0001" RENAME TO "a"'), q.index(lead[0]))

    def test_no_lead_times_no_login(self):
        db = FakeDb(schemas_exist=False)
        self.build(db, "db", ["a:0.03"])
        self.assertFalse([s for s in db.sql() if s.startswith(("WITH d AS", "GRANT"))])

    def test_broken_rules_stop_before_the_rename(self):
        db = FakeDb(schemas_exist=False, bad=3)
        with self.assertRaisesRegex(RuntimeError, "TPC-H rules broken"):
            self.build(db, "db", ["a:0.03"])
        self.assertFalse([s for s in db.sql() if s.startswith("ALTER SCHEMA")])


class Fill(unittest.TestCase):
    def stub(self, d, body):
        with open(os.path.join(d, "fill.py"), "w") as f:
            f.write(body)

    def test_exit_codes_and_cleanup(self):
        import tempfile
        pg = ts.Pg(host="h", password="secret-pw")
        with tempfile.TemporaryDirectory() as d, mock.patch.object(ts, "PLANNER", d):
            self.stub(d, "import sys\nopen(sys.argv[sys.argv.index('--password-file') + 1]).read()\n"
                         "print('PWFILE', sys.argv[sys.argv.index('--password-file') + 1]); print('filled'); sys.exit(0)\n")
            argv, path = list(sys.argv), list(sys.path)
            out = ts._fill(pg, "db", "tpch_0001", 2)
            self.assertIn("filled", out)
            pw = out.split("PWFILE ")[1].split()[0]
            self.assertFalse(os.path.exists(pw)); self.assertEqual((sys.argv, sys.path), (argv, path))
            self.stub(d, "import sys\nprint('level 1: boom'); sys.exit(3)\n")
            with self.assertRaisesRegex(RuntimeError, "fill of db.tpch_0001 failed:\n.*boom"):
                ts._fill(pg, "db", "tpch_0001", 2)
            self.stub(d, "import sys\nsys.exit('level 2: no rows added')\n")   # fill.py's sys.exit(<message>)
            with self.assertRaisesRegex(RuntimeError, "no rows added"):
                ts._fill(pg, "db", "tpch_0001", 2)
            self.assertEqual((sys.argv, sys.path), (argv, path))


class CommandLine(unittest.TestCase):
    def test_bad_arguments_before_connecting(self):
        with mock.patch.object(ts, "lakebase", side_effect=AssertionError("connected")), redirect_stderr(io.StringIO()) as err:
            self.assertEqual(ts.main(["--lakebase", "p", "db", "Bad:1"]), 2)
        self.assertIn("lower-case letters", err.getvalue())


if __name__ == "__main__":
    unittest.main()
