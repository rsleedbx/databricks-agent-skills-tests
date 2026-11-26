"""lfc_introspection.history offline: rows from the documents, what changed between runs, which requests are
pending, run_pending's statements on a fake session (a run recorded, a failed one too).

    python3 -m unittest skills/lfc-introspection/tests/test_history.py
"""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_introspection import history  # noqa: E402

S1 = {"tables": [{"name": "orders", "columns": [{"name": "id", "type": "integer", "native_type": "int", "primary_key": True},
                                                {"name": "amt", "type": "decimal", "native_type": "numeric(10,2)"}]},
                 {"name": "region", "columns": [{"name": "r", "type": "integer", "native_type": "int"}]}]}
T1 = {"tables": [{"name": "orders", "row_count": 1000}, {"name": "region", "row_count": 5}]}
S2 = {"tables": [{"name": "orders", "columns": [{"name": "id", "type": "integer", "native_type": "int", "primary_key": True},
                                                {"name": "amt", "type": "decimal", "native_type": "numeric(12,2)"},
                                                {"name": "note", "type": "string", "native_type": "text"}]},
                 {"name": "hits", "columns": [{"name": "url", "type": "string", "native_type": "text"}]}]}
T2 = {"tables": [{"name": "orders", "row_count": 4000}, {"name": "hits", "row_count": 9}]}


class History(unittest.TestCase):
    def test_rows(self):
        cols, stats = history.rows(S1, T1)
        self.assertEqual(cols[0], {"tbl": "orders", "ordinal": 1, "col": "id", "type": "integer", "native_type": "int",
                                   "not_null": False, "pk": True})
        self.assertEqual(len(cols), 3)
        self.assertEqual(stats[1], {"tbl": "region", "row_count": 5, "data_size_bytes": None})

    def test_changes(self):
        self.assertEqual(history.changes(None, (S1, T1)), [])
        self.assertEqual(history.changes((S1, T1), (S1, T1)), [])
        self.assertEqual(history.changes((S1, T1), (S2, T2)),
                         ["table added: hits", "table dropped: region", "orders: column added: note",
                          "orders.amt: type numeric(10,2) -> numeric(12,2)", "orders: rows 1,000 -> 4,000"])

    def test_pending(self):
        reqs = [{"n": 2, "source": "a"}, {"n": 1, "source": "a"}, {"n": 1, "source": "b"}, {"n": None, "source": "c"},
                {"n": 3, "source": None}]   # half-filled rows (from a dashboard) are skipped, not fatal
        self.assertEqual(history.pending(reqs, [{"run": 1, "source": "a"}]), [{"n": 1, "source": "b"}, {"n": 2, "source": "a"}])

    def test_run_pending_records_runs_and_failures(self):
        seen = []
        state = {"runs": []}

        class R:
            def __init__(self, rows):
                self.rows = rows

            def collect(self):
                return self.rows

        class Fake:
            def sql(self, q):
                seen.append(q)
                if "FROM `c`.`s`.introspection_request" in q:
                    return R([{"n": 1, "source": "good", "engine": "postgres", "database": "d", "src_schema": "app"},
                              {"n": 1, "source": "bad", "engine": "mysql", "database": None, "src_schema": "x"}])
                if "SELECT run, source" in q:
                    return R(state["runs"])
                return R([])

        def fake_introspect(session, src, engine, db, sch, via=None):
            if src == "bad":
                raise RuntimeError("no route to host")
            return S1, T1
        out = history.run_pending(Fake(), "`c`.`s`", introspect=fake_introspect)
        self.assertEqual([(o["source"], o["status"]) for o in out], [("good", "ok"), ("bad", "failed")])
        self.assertIn("no route to host", out[1]["error"])
        ins = [q for q in seen if q.startswith("INSERT INTO `c`.`s`.introspection_runs")]
        self.assertEqual(len(ins), 2)
        self.assertIn(json.dumps(S1), ins[0])   # the documents kept, as classify reads them
        self.assertTrue(any(q.startswith("INSERT INTO `c`.`s`.introspection_schema") for q in seen))

    def test_ddl(self):
        d = history.ddl("`c`.`s`")
        self.assertEqual(len(d), 4)
        self.assertIn("enableChangeDataFeed", d[0])


if __name__ == "__main__":
    unittest.main()
