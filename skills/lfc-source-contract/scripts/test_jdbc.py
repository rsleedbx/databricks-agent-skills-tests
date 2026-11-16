"""Offline tests of lfc_source_contract.jdbc: the Maven driver and allow-list in a connection request, the URLs (TLS on),
guarded() as a DO block that a rerun leaves alone, and JdbcSql's reads and writes on a fake spark (one partition,
the statement as sessionInitStatement, the connection named).

  python3 -m unittest test_jdbc.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from lfc_source_contract import jdbc


class FakeReader:
    def __init__(self, log, rows):
        self.log, self.rows, self.opts = log, rows, {}

    def format(self, f):
        self.opts["format"] = f
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def load(self):
        self.log.append(dict(self.opts))
        return self

    def collect(self):
        return self.rows


class FakeSpark:
    def __init__(self, rows=()):
        self.log, self.rows = [], list(rows)

    @property
    def read(self):
        return FakeReader(self.log, self.rows)


class Jdbc(unittest.TestCase):
    def test_body(self):
        b = jdbc.connection_body("c", "postgres", "jdbc:postgresql://h:5432/d?sslmode=require", "w", "PW", comment="x")
        self.assertEqual(b["connection_type"], "JDBC")
        self.assertEqual(b["environment_settings"], {"java_dependencies": ["org.postgresql:postgresql:42.7.4"]})
        self.assertEqual(b["options"]["externalOptionsAllowList"], "dbtable,query,sessionInitStatement,numPartitions")
        self.assertEqual((b["options"]["user"], b["options"]["password"], b["comment"]), ("w", "PW", "x"))
        with self.assertRaises(ValueError):
            jdbc.connection_body("c", "nosuch", "u", "w", "p")

    def test_urls_keep_tls_on(self):
        self.assertTrue(jdbc.url("postgres", "h", 5432, "d").endswith("?sslmode=require"))
        self.assertIn("sslMode=REQUIRED", jdbc.url("mysql", "h", 3306, "d"))
        self.assertIn("encrypt=true", jdbc.url("sqlserver", "h", 1433, "d"))

    def test_guarded(self):
        s = jdbc.guarded("SELECT 1 FROM pg_publication WHERE pubname = 'p';", "CREATE SCHEMA s;", "CREATE TABLE s.t (v text DEFAULT 'a')")
        self.assertTrue(s.startswith("DO $lfc_sql_do$ BEGIN"))
        self.assertIn("IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'p') THEN", s)
        self.assertIn("EXECUTE $lfc_sql$CREATE SCHEMA s$lfc_sql$;", s)
        self.assertIn("EXECUTE $lfc_sql$CREATE TABLE s.t (v text DEFAULT 'a')$lfc_sql$;", s)
        self.assertTrue(s.endswith("END $lfc_sql_do$"))
        with self.assertRaises(ValueError):
            jdbc.guarded("SELECT 1", "SELECT $lfc_sql$x$lfc_sql$")
        with self.assertRaises(ValueError):
            jdbc.guarded("SELECT 1")

    def test_execute_and_query(self):
        sp = FakeSpark(rows=[(3, "a")])
        db = jdbc.JdbcSql(sp, "conn")
        db.execute("UPDATE t SET v = 1")
        self.assertEqual(sp.log[0], {"format": "jdbc", "databricks.connection": "conn", "query": "SELECT 1 AS lfc_one",
                                     "numPartitions": "1", "sessionInitStatement": "UPDATE t SET v = 1"})
        self.assertEqual(db.query("SELECT n, v FROM t"), [(3, "a")])
        self.assertEqual(sp.log[1], {"format": "jdbc", "databricks.connection": "conn", "query": "SELECT n, v FROM t"})


if __name__ == "__main__":
    unittest.main()
