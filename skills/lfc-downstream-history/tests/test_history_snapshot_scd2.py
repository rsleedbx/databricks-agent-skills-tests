"""Offline checks of history-snapshot-scd2.py's guards: refresh_in_progress and empty_after_rows (the notebook needs a
pipeline runtime, so only its pure functions are loaded, from its source)."""
import ast
import os
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "history-snapshot-scd2.py")
NAMES = {"WRITES", "ROW_METRICS", "wrote_rows", "refresh_in_progress", "empty_after_rows"}
with open(SRC) as fh:
    tree = ast.parse(fh.read())
code = ast.Module(body=[n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in NAMES) or
                        (isinstance(n, ast.Assign) and {t.id for t in n.targets if isinstance(t, ast.Name)} & NAMES)],
                  type_ignores=[])
g = {}
exec(compile(code, SRC, "exec"), g)  # noqa: S102  (the notebook's own functions)
ROWS = {"numTargetRowsInserted": "4"}
NONE = {"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0", "numOutputRows": "0"}


class RefreshInProgress(unittest.TestCase):
    def f(self, *ops):
        return g["refresh_in_progress"](list(ops))

    def test_cases(self):
        self.assertTrue(self.f((10, "MERGE", ROWS), (11, "DLT REFRESH", {})))                 # emptied, not reloaded
        self.assertFalse(self.f((11, "DLT REFRESH", {}), (12, "MERGE", ROWS)))                # reloaded
        self.assertFalse(self.f((10, "MERGE", ROWS)))                                         # no refresh
        self.assertTrue(self.f((11, "DLT REFRESH", {})))                                      # refresh, no write
        self.assertTrue(self.f((11, "DLT REFRESH", {}), (12, "SET TBLPROPERTIES", {}),
                               (13, "DLT SETUP", {}), (14, "CLUSTER BY", {})))               # metadata only
        self.assertTrue(self.f((11, "DLT REFRESH", {}), (12, "MERGE", NONE)))                 # a write of no rows
        self.assertFalse(self.f((11, "DLT REFRESH", {}), (12, "STREAMING UPDATE", None)))     # no metrics: rows

    def test_newest_refresh_counts(self):
        self.assertTrue(self.f((5, "DLT REFRESH", {}), (6, "MERGE", ROWS), (9, "DLT REFRESH", {})))


class EmptyAfterRows(unittest.TestCase):
    def test_cases(self):
        f = g["empty_after_rows"]
        self.assertTrue(f(0, [(3, "MERGE", ROWS)], False))       # emptied: refuse
        self.assertFalse(f(0, [(3, "MERGE", ROWS)], True))       # meant to be empty
        self.assertFalse(f(0, [(3, "MERGE", NONE)], False))      # never had rows
        self.assertFalse(f(4, [(3, "MERGE", ROWS)], False))      # has rows


if __name__ == "__main__":
    unittest.main()
