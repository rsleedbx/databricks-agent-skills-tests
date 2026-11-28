"""size.py offline: the arguments (the usage, unknown options, the engine's --database), a warehouse answer written as
tables.csv (header, NULL as empty), an empty or failed answer, MySQL's PROCESS fallback, and the DuckDB report's call.
The warehouse and the workspace login are fakes: nothing runs against a workspace, and duckdb is a patched call."""
from __future__ import annotations

import contextlib
import csv
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import size as sz
from lfc_change_source.engine import row_type


class FakeWarehouse:
    """Each sql() call takes the next answer in order: an exception to raise, or (names, rows) to return."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.statements = []

    def sql(self, text):
        self.statements.append(text)
        ans = self.answers.pop(0)
        if isinstance(ans, Exception):
            raise ans
        names, rows = ans
        make = row_type(names)
        return mock.Mock(collect=mock.Mock(return_value=[make(r) for r in rows]))


def workspace(fake):
    """Patches: the warehouse pick returns the fake, and the login is a placeholder."""
    return (mock.patch.object(sz, "Warehouse", mock.Mock(pick=mock.Mock(return_value=fake))),
            mock.patch.object(sz.Rest, "connect", return_value=object()))


class Args(unittest.TestCase):
    def test_usage_options_and_engine_arguments(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(sz.main([]), 2)
            self.assertEqual(sz.main(["p", "c", "postgres", "--bogus"]), 2)
            self.assertEqual(sz.main(["p", "c", "postgres"]), 2)   # PostgreSQL needs --database
        self.assertIn("usage: size.py", err.getvalue())
        self.assertIn("--database is needed for postgres", err.getvalue())


class Answers(unittest.TestCase):
    def test_csv_has_a_header_and_empty_nulls(self):
        d = tempfile.mkdtemp()
        fake = FakeWarehouse((["table_schema", "rows_est"], [("public", 5), ("public", None)]))
        p1, p2 = workspace(fake)
        with p1, p2:
            self.assertIsNone(sz.query_to_csv(None, "SELECT 1", os.path.join(d, "t.csv")))
        with open(os.path.join(d, "t.csv")) as f:
            self.assertEqual(list(csv.reader(f)), [["table_schema", "rows_est"], ["public", "5"], ["public", ""]])

    def test_errors_and_an_empty_answer(self):
        d = tempfile.mkdtemp()
        fake = FakeWarehouse(RuntimeError("SCHEMA_NOT_FOUND: BOOM"), ([""], []))
        p1, p2 = workspace(fake)
        with p1, p2:
            self.assertEqual(sz.query_to_csv(None, "a", os.path.join(d, "a")), "SCHEMA_NOT_FOUND: BOOM")
            self.assertEqual(sz.query_to_csv(None, "b", os.path.join(d, "b")), sz.NO_ROWS)
        self.assertFalse(os.path.exists(os.path.join(d, "b")))


class Main(unittest.TestCase):
    def test_mysql_without_process_falls_back_and_says_so(self):
        d = tempfile.mkdtemp()
        fake = FakeWarehouse(RuntimeError("PERMISSION_DENIED: PROCESS privilege"), (["table_schema"], [("db1",)]))
        p1, p2 = workspace(fake)
        with p1, p2, mock.patch.object(sz.subprocess, "run") as run, \
                contextlib.redirect_stdout(io.StringIO()) as out:
            rc = sz.main(["p", "c", "mysql", "--out", d])
        self.assertEqual(rc, 0)
        self.assertEqual(len(fake.statements), 2)   # the PROCESS variant, then the plain one
        self.assertIn("no PROCESS privilege", out.getvalue())
        self.assertIn("per-table detail: " + os.path.join(d, "tables.csv"), out.getvalue())
        cmd = run.call_args[0][0]
        self.assertEqual(cmd[:3], ["duckdb", "-box", "-c"])
        self.assertIn("read_csv('" + os.path.join(d, "tables.csv") + "'", cmd[3])
        self.assertIn("LIMIT 15", cmd[3])

    def test_a_failed_query_says_so_and_exits_1(self):
        d = tempfile.mkdtemp()
        p1, p2 = workspace(FakeWarehouse(RuntimeError("NO_ACCESS: denied")))
        with p1, p2, contextlib.redirect_stdout(io.StringIO()) as out:
            rc = sz.main(["p", "c", "postgres", "--database", "db", "--out", d])
        self.assertEqual(rc, 1)
        self.assertIn("query failed: NO_ACCESS: denied", out.getvalue())


if __name__ == "__main__":
    unittest.main()
