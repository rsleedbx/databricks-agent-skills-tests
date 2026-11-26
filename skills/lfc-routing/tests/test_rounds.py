"""lfc_routing.rounds offline: a round from the tables on a fake session: the latest introspection of two tenants, the
log's counts against the last round's, an override, the repeating tables as one row each, routing_plan and
routing_observed written for round n.

    python3 -m unittest skills/lfc-routing/tests/test_rounds.py
"""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-medallion", "lfc-change-source", "lfc-introspection"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing import rounds  # noqa: E402

DOC = {"tables": [{"name": "orders", "columns": [{"name": "id", "type": "integer", "primary_key": True}]},
                  {"name": "hits", "columns": [{"name": "url", "type": "string"}]}]}
STATS = {"tables": [{"name": "orders", "row_count": 100, "data_size_bytes": 8192}, {"name": "hits", "row_count": 5}]}


class Session:
    def __init__(self):
        self.seen = []

    def sql(self, q):
        self.seen.append(q)
        rows = []
        if "FROM `c`.`s`.routing_iteration" in q:
            rows = [{"n": 1}, {"n": 2}]
        elif "SELECT run FROM `c`.`s`.introspection_runs" in q:
            rows = [{"run": 7}]
        elif "SELECT schema_json" in q:
            rows = [{"schema_json": json.dumps(DOC), "stats_json": json.dumps(STATS)}]
        elif "FROM `c`.`s`.routing_observed o" in q:
            rows = [{"source": "t1", "tbl": "orders", "events": 100, "at": 1000.0},
                    {"source": "t2", "tbl": "orders", "events": 100, "at": 1000.0}]
        elif "GROUP BY destination" in q:
            pre = "t1.app." if "t1.app.%" in q else "t2.app."
            rows = [{"destination": pre + "orders", "n": 100 + 50_000}]
        elif "FROM `c`.`s`.routing_decisions" in q:
            rows = [{"tbl": "hits", "override_cell": "merge_round"}]

        class R:
            def collect(self):
                return rows
        return R()


class Rounds(unittest.TestCase):
    def test_a_round_from_the_tables(self):
        s = Session()
        srcs = {"t1": {"log": "`c`.`s`.cdc_log", "prefix": "t1.app."}, "t2": {"log": "`c`.`s`.cdc_log", "prefix": "t2.app."}}
        plan = rounds.run_round(s, "`c`.`s`", srcs, now=1100.0)
        self.assertEqual(plan["round"], 2)
        self.assertEqual(plan["introspection_runs"], {"t1": 7, "t2": 7})
        self.assertEqual(sorted(plan["tables"]), ["hits", "orders"])          # 2 tenants x 2 tables: 2 rows
        self.assertEqual(plan["tables"]["orders"]["route"]["class"], "small_fast")   # 2 x 50,000 events in 100 s
        self.assertEqual(plan["tables"]["hits"]["route"]["silver"], "merge_round")   # the override
        self.assertEqual(plan["tables"]["hits"]["key"], "all_columns")
        ins = [q for q in s.seen if q.startswith("INSERT INTO `c`.`s`.routing_plan")]
        self.assertEqual(len(ins), 1)
        self.assertIn("(2, 'hits'", ins[0])
        self.assertIn('{"t1": 7, "t2": 7}', ins[0].replace("\\'", "'"))   # the introspection runs it came from
        obs = [q for q in s.seen if q.startswith("INSERT INTO `c`.`s`.routing_observed")]
        self.assertIn("(2, 't1', 'orders', '`c`.`s`.cdc_log', 50100, 1100.000)", obs[0])
        self.assertTrue(any(q.startswith("DELETE FROM `c`.`s`.routing_plan WHERE round = 2") for q in s.seen))


if __name__ == "__main__":
    unittest.main()
