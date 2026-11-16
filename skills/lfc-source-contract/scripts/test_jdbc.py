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
        self.log, self.rows, self.sqls = [], list(rows), []

    def sql(self, q, args=None):
        self.sqls.append((q, args))
        return FakeReader([], self.rows)

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
        db.query("SELECT 1", init="SET search_path = s")
        self.assertEqual(sp.log[2]["sessionInitStatement"], "SET search_path = s")

    def test_dbapi_reads_through_remote_query_writes_through_jdbc(self):
        sp = FakeSpark(rows=[('[{"f1": 5, "f2": "it\'s"}]',)])
        con = jdbc.dbapi(sp, "w_jdbc", read_connection="r_pg", database="orders")
        cur = con.cursor()
        cur.execute("SELECT count(*), %s FROM s.t WHERE x LIKE 'a%%'", ("it's",))
        self.assertEqual(cur.fetchall(), [(5, "it's")])
        q, args = sp.sqls[0]
        self.assertEqual(q, "SELECT * FROM remote_query(:connection, database => :database, query => :query)")
        self.assertEqual((args["connection"], args["database"]), ("r_pg", "orders"))
        self.assertIn("FROM (SELECT count(*), 'it''s' FROM s.t WHERE x LIKE 'a%') q", args["query"])
        self.assertEqual(sp.log, [])                                   # no read through the JDBC connection
        cur.execute("SET search_path = s")
        cur.execute("UPDATE t SET v = 1")
        self.assertEqual(sp.log[0]["sessionInitStatement"], "SET search_path = s;\nUPDATE t SET v = 1")
        cur.execute("WITH d AS (SELECT 1) UPDATE t SET v = 2")       # a CTE then a write: a write
        self.assertEqual(sp.log[1]["sessionInitStatement"], "SET search_path = s;\nWITH d AS (SELECT 1) UPDATE t SET v = 2")
        with self.assertRaises(ValueError):
            jdbc.dbapi(sp, "w", read_connection="r")

    def test_dbapi_without_a_read_connection(self):
        sp = FakeSpark(rows=[('[{"f1": 1}]',)])
        cur = jdbc.dbapi(sp, "w_jdbc").cursor()
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))
        self.assertTrue(sp.log[0]["query"].startswith("SELECT coalesce(json_agg"))
        self.assertEqual(jdbc.ident('a"b'), '"a""b"')


if __name__ == "__main__":
    unittest.main()
