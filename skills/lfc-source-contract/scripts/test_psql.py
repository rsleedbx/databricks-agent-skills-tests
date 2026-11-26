"""lfc_source_contract.psql offline: parameters as literals, rows split on tabs, a wrong parameter count refused, and
cdc.publication through it (created, then a table added) on a fake shell.

    python3 -m unittest skills/lfc-source-contract/tests/test_psql.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_source_contract import cdc, psql  # noqa: E402


class Psql(unittest.TestCase):
    def test_parameters_and_rows(self):
        seen = []
        cur = psql.cursor(lambda c: seen.append(c) or "a\tb\n\nc\td\n", "psql -d x")
        cur.execute("SELECT 1 WHERE n = %s AND v = %s", ("it's", 3))
        self.assertIn("WHERE n = 'it''s' AND v = 3;", seen[0])
        self.assertTrue(seen[0].startswith("psql -d x -X -At"))
        self.assertEqual(cur.fetchall(), [("a", "b"), ("c", "d")])
        with self.assertRaises(ValueError):
            cur.execute("SELECT %s", (1, 2))

    def test_publication_through_it(self):
        state = {"pub": False, "members": []}

        def run(cmd):
            if "server_version_num" in cmd:
                return "160000\n"
            if "FROM pg_class c JOIN pg_namespace" in cmd and "ANY(" in cmd:
                return "s\ta\ns\tb\n"
            if "SELECT 1 FROM pg_publication WHERE pubname" in cmd and "DO" not in cmd:
                return "1\n" if state["pub"] else ""
            if "pg_publication_rel" in cmd and "UNION" in cmd:
                return "".join(f"s\t{m}\n" for m in state["members"])
            if "CREATE PUBLICATION" in cmd:
                state["pub"], state["members"] = True, ["a"]
            if "ADD TABLE" in cmd:
                state["members"].append("b")
            return ""
        cur = psql.cursor(run, "psql")
        self.assertEqual(cdc.publication(cur, "p", ["s"], log=lambda m: None, only=["a"]), "created")
        self.assertEqual(cdc.publication(cur, "p", ["s"], log=lambda m: None), "added 1")
        self.assertEqual(cdc.publication(cur, "p", ["s"], log=lambda m: None), "kept")


if __name__ == "__main__":
    unittest.main()
