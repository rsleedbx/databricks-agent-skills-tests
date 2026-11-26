"""lfc_routing.shapes offline: 1,000 tenants of the same 2 tables are 2 groups, a deviating tenant a variant, the
members' rows summed, foreign keys pointing at groups, classify once a group with the members' rates added.

    python3 -m unittest skills/lfc-routing/tests/test_shapes.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-medallion", "lfc-change-source"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing import shapes  # noqa: E402

CUST = {"name": "customer", "columns": [{"name": "id", "type": "integer", "primary_key": True}, {"name": "name", "type": "string"}]}
ORD = {"name": "orders", "columns": [{"name": "id", "type": "integer", "primary_key": True}, {"name": "cust", "type": "integer"}],
       "fk_constraints": [{"columns": ["cust"], "parent_table": "customer", "parent_columns": ["id"]}]}
ODD = {"name": "orders", "columns": ORD["columns"] + [{"name": "extra", "type": "string"}]}


def tenants(n, odd=()):
    return {f"t{i:04d}": ({"tables": [CUST, ODD if i in odd else ORD]},
                          {"tables": [{"name": "customer", "row_count": 10, "data_size_bytes": 100},
                                      {"name": "orders", "row_count": 1000}]}) for i in range(n)}


class Shapes(unittest.TestCase):
    def test_a_thousand_tenants_are_two_groups(self):
        g = shapes.group({k: v[0] for k, v in tenants(1000).items()})
        self.assertEqual(sorted(g), ["customer", "orders"])
        self.assertEqual(len(g["orders"]["members"]), 1000)
        self.assertFalse(g["orders"]["variant"])

    def test_a_group_keeps_its_tables_own_name(self):
        docs = {"t1": {"tables": [dict(CUST, name="Customer")]}, "t2": {"tables": [dict(CUST, name="customer")]},
                "t3": {"tables": [dict(CUST, name="Customer")]}}
        g = shapes.group(docs)
        self.assertEqual(list(g), ["Customer"])           # grouped lower-case, named as most members spell it
        self.assertEqual(len(g["Customer"]["members"]), 3)
        tie = shapes.group({"a": {"tables": [dict(CUST, name="customer")]}, "b": {"tables": [dict(CUST, name="Customer")]}})
        self.assertEqual(list(tie), ["Customer"])         # a tie: by the spelling, whichever source sorts first

    def test_a_deviating_tenant_is_a_variant(self):
        g = shapes.group({k: v[0] for k, v in tenants(5, odd={3}).items()})
        var = [k for k, v in g.items() if v["variant"]]
        self.assertEqual(len(var), 1)
        self.assertTrue(var[0].startswith("orders~"))
        self.assertEqual(g[var[0]]["members"], [("t0003", "orders")])
        self.assertEqual(len(g["orders"]["members"]), 4)

    def test_rows_summed_and_foreign_keys_to_groups(self):
        src = tenants(3)
        g = shapes.group({k: v[0] for k, v in src.items()})
        s, t = shapes.representative({k: v[0] for k, v in src.items()}, {k: v[1] for k, v in src.items()}, g)
        st = {x["name"]: x for x in t["tables"]}
        self.assertEqual((st["orders"]["row_count"], st["customer"]["data_size_bytes"]), (3000, 300))
        self.assertNotIn("data_size_bytes", st["orders"])   # no member knew it
        o = next(x for x in s["tables"] if x["name"] == "orders")
        self.assertEqual(o["fk_constraints"][0]["parent_table"], "customer")

    def test_classify_once_a_group_with_the_members_rates_added(self):
        src = tenants(100)
        obs = {"round": 2, "tables": {f"t{i:04d}.orders": {"events_per_s": 0.5} for i in range(100)}}
        p = shapes.classify_many(src, observed=obs)
        self.assertEqual(sorted(p["tables"]), ["customer", "orders"])
        self.assertEqual(p["tables"]["orders"]["route"]["class"], "auto_fast")   # 100 x 0.5 = 50 a second; size unknown
        self.assertEqual(len(p["groups"]["orders"]["members"]), 100)
        self.assertEqual(p["round"], 2)


if __name__ == "__main__":
    unittest.main()
