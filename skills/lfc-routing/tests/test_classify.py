"""lfc_routing.classify, the box from files: keys (a keyless table's every column), the observed rate over the
activity's, a person's decisions, the round. Offline: plain data, no database.

    python3 -m unittest skills/lfc-routing/tests/test_classify.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-medallion", "lfc-change-source", "lfc-log-silver"):
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


class Observed(unittest.TestCase):
    """flowthrough.observed, the document classify reads: two rounds of counts, a table first seen, then the plan."""

    def test_two_rounds_of_counts_make_the_rates_classify_reads(self):
        from lfc_log_silver.flowthrough import observed
        prev = {"orders": ({}, 1000, 100.0), "customer": ({}, 10, 100.0)}
        doc = observed(prev, {"orders": 501_000, "customer": 10, "audit_log": 5}, 200.0,
                       {"orders": "`c`.`s`.`cdc_log`"}, round_=2)
        t = doc["tables"]
        self.assertEqual((doc["round"], doc["seconds"]), (2, 100.0))
        self.assertEqual((t["orders"]["events_per_s"], t["customer"]["events_per_s"], t["audit_log"]["events_per_s"]),
                         (5000.0, 0.0, 0.0))
        self.assertEqual((t["orders"]["log"], t["audit_log"]["events"]), ("`c`.`s`.`cdc_log`", 5))
        plan = classify(SCHEMA, STATS, observed=doc)
        self.assertEqual((plan["round"], plan["tables"]["orders"]["route"]["class"]), (2, "big_fast"))


class Tables(unittest.TestCase):
    """lfc_routing.tables: routing_decisions rows -> decisions -> classify -> routing_plan rows."""

    def test_overrides_through_classify_into_plan_rows(self):
        from lfc_routing import tables
        rows = [{"tbl": "orders", "override_cell": "merge_round", "override_log": None, "override_role": ""},
                {"tbl": "orders", "override_log": "c.s.cdc_log_orders"},        # a later row adds the log
                {"tbl": "customer", "override_class": None}]                    # nothing set: the heuristic
        d = tables.decisions(rows)
        self.assertEqual(d, {"tables": {"orders": {"method": "merge_round", "log": "c.s.cdc_log_orders"}}})
        out = {r["tbl"]: r for r in tables.plan_rows(classify(SCHEMA, STATS, decisions=d), round_=3)}
        self.assertEqual((out["orders"]["cell"], out["orders"]["log"], out["orders"]["round"]),
                         ("merge_round", "c.s.cdc_log_orders", 3))
        self.assertIn("log chosen", out["orders"]["why"])
        self.assertEqual((out["audit_log"]["key_kind"], out["audit_log"]["keys"]), ("all_columns", ["at", "msg"]))
        self.assertEqual(set(out["customer"]), set(tables.PLAN_COLUMNS))

    def test_ddl_and_the_round_asked_for(self):
        from lfc_routing import tables
        d = tables.ddl("`c`.`s`")
        self.assertEqual([x.split()[5] for x in d], ["`c`.`s`.routing_plan", "`c`.`s`.routing_decisions",
                                                     "`c`.`s`.routing_iteration"])
        self.assertIn("enableChangeDataFeed", d[2])
        self.assertEqual((tables.latest_round([]), tables.latest_round([{"n": 1}, {"n": 4}])), (0, 4))


if __name__ == "__main__":
    unittest.main()
