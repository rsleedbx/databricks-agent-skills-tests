"""lfc_routing.classify, the box from files: keys (a keyless table's every column), the observed rate over the
activity's, a person's decisions, the round. Offline: plain data, no database.

    python3 -m unittest skills/lfc-routing/tests/test_classify.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-medallion", "lfc-change-source"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing.classify import classify  # noqa: E402

SCHEMA = {"tables": [
    {"name": "customer", "row_count": 1000,
     "columns": [{"name": "id", "type": "integer", "primary_key": True}, {"name": "name", "type": "string"}]},
    {"name": "orders", "row_count": 50_000_000,
     "columns": [{"name": "id", "type": "integer", "primary_key": True}, {"name": "cust", "type": "integer"}],
     "fk_constraints": [{"columns": ["cust"], "parent_table": "customer", "parent_columns": ["id"]}]},
    {"name": "audit_log", "row_count": 10,
     "columns": [{"name": "at", "type": "timestamp"}, {"name": "msg", "type": "string"}]},
]}
STATS = {"tables": [{"name": "orders", "data_size_bytes": 2 << 40}, {"name": "customer", "data_size_bytes": 1 << 20},
                    {"name": "audit_log", "data_size_bytes": 4096}]}


class Classify(unittest.TestCase):
    def test_keys_are_the_primary_key_else_every_column(self):
        t = classify(SCHEMA, STATS)["tables"]
        self.assertEqual((t["orders"]["keys"], t["orders"]["key"]), (["id"], "primary_key"))
        self.assertEqual((t["audit_log"]["keys"], t["audit_log"]["key"]), (["at", "msg"], "all_columns"))

    def test_sizes_and_no_rates_give_slow_classes(self):
        t = classify(SCHEMA, STATS)["tables"]
        self.assertEqual(t["orders"]["route"]["class"], "big_slow")
        self.assertEqual(t["customer"]["route"]["class"], "small_slow")

    def test_the_observed_rate_replaces_the_activity(self):
        act = {"orders": {"inserts": 10, "updates": 0, "deletes": 0}}
        quiet = classify(SCHEMA, STATS, act, activity_seconds=100)["tables"]["orders"]["route"]["class"]
        busy = classify(SCHEMA, STATS, act, activity_seconds=100,
                        observed={"round": 4, "tables": {"orders": {"events_per_s": 5000}}})
        self.assertEqual((quiet, busy["tables"]["orders"]["route"]["class"]), ("big_slow", "big_fast"))
        self.assertEqual(busy["round"], 4)

    def test_decisions_choose_class_method_snapshot_and_role(self):
        d = {"tables": {"customer": {"class": "small_fast", "snapshot_by": "transport"},
                        "orders": {"method": "merge_round", "role": "fact"},
                        "nope": {"role": "dimension"}}}
        p = classify(SCHEMA, STATS, decisions=d)
        c, o = p["tables"]["customer"]["route"], p["tables"]["orders"]
        self.assertEqual((c["class"], c["snapshot_by"]), ("small_fast", "transport"))
        self.assertEqual((o["route"]["silver"], o["role"]), ("merge_round", "fact"))
        self.assertEqual(p["problems"], ["review: no table nope in the schema"])

    def test_the_schema_role_lines_are_never_read(self):
        marked = {"tables": [dict(t, role="queue") for t in SCHEMA["tables"]]}
        self.assertEqual(classify(marked, STATS), classify(SCHEMA, STATS))


if __name__ == "__main__":
    unittest.main()
