"""size_query.remote_query: the statement size.py and a notebook run (offline)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import size_query as sq


class RemoteQuery(unittest.TestCase):
    def test_postgres_names_its_database_and_has_no_comments(self):
        q = sq.remote_query("c1", "postgres", database="d1")
        self.assertTrue(q.startswith("SELECT * FROM remote_query('c1', database => 'd1', query => \""))
        self.assertNotIn("\n", q)
        self.assertNotIn("__DB__", q)

    def test_engines_and_their_arguments(self):
        self.assertIn("service_name => 'PDB1'", sq.remote_query("c1", "oracle", service="PDB1"))
        self.assertEqual(sq.remote_query("c1", "mariadb"), sq.remote_query("c1", "mysql"))   # the MySQL query
        self.assertIn("remote_query('c1', query =>", sq.remote_query("c1", "mysql", file="mysql-process"))
        for engine, kw, msg in (("postgres", {}, "--database"), ("oracle", {}, "--service"), ("db2", {}, "engine:"),
                                ("mysql", {"file": "nope"}, "no query")):
            with self.assertRaises(sq.SizeQueryError) as e:
                sq.remote_query("c1", engine, **kw)
            self.assertIn(msg, str(e.exception))


if __name__ == "__main__":
    unittest.main()
