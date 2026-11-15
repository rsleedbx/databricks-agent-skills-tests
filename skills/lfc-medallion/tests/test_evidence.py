"""Offline checks of lfc_medallion's evidence with a fake spark: bronze (keys from Lakeflow Connect's table property,
columns up to the first section, row counts, history after the snapshot, foreign keys inferred from names), the
source through a UC connection (remote_query on PostgreSQL's catalog), the plan's roles and routes, and hints.

    python3 -m unittest skills/lfc-medallion/tests/test_evidence.py
"""
import json
import os
import re
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import evidence, table_plan

R = types.SimpleNamespace
# a TPC-H-shaped bronze schema: table -> (key, [(column, type)], rows, history [(operation, metrics)])
SNAP = ("MERGE", {"numTargetRowsInserted": "100"})
TPCH = {
    "region": (["r_regionkey"], [("r_regionkey", "int"), ("r_name", "string")], 5, [SNAP]),
    "nation": (["n_nationkey"], [("n_nationkey", "int"), ("n_name", "string"), ("n_regionkey", "int")], 25,
               [SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "25"})]),
    "supplier": (["s_suppkey"], [("s_suppkey", "int"), ("s_name", "string"), ("s_nationkey", "int")], 500, [SNAP]),
    "customer": (["c_custkey"], [("c_custkey", "int"), ("c_name", "string"), ("c_nationkey", "int")], 7500, [SNAP]),
    "part": (["p_partkey"], [("p_partkey", "int"), ("p_name", "string")], 10000, [SNAP]),
    "partsupp": (["ps_partkey", "ps_suppkey"], [("ps_partkey", "int"), ("ps_suppkey", "int"), ("ps_availqty", "int"),
                                                ("ps_supplycost", "decimal(15,2)")], 40000, [SNAP]),
    "orders": (["o_orderkey"], [("o_orderkey", "int"), ("o_custkey", "int"), ("o_totalprice", "decimal(15,2)"),
                                ("o_orderdate", "date")], 75000, [SNAP, ("MERGE", {"numTargetRowsInserted": "1"})]),
    "lineitem": (["l_orderkey", "l_linenumber"],
                 [("l_orderkey", "int"), ("l_linenumber", "int"), ("l_partkey", "int"), ("l_suppkey", "int"),
                  ("l_quantity", "decimal(15,2)"),
                  ("l_extendedprice", "decimal(15,2)")], 300000,
                 [("CREATE TABLE", {}), SNAP, ("DELETE", {}), ("MERGE", {"numTargetRowsInserted": "4"}),
                  ("MERGE", {"numTargetRowsMatchedUpdated": "2", "numTargetRowsDeleted": "1"})]),
}


class BronzeSpark:
    """Answers the queries bronze_schema runs, for one schema `sales_bronze_east` holding TPCH."""

    def __init__(self, tables=TPCH):
        self.t = tables
        self.queries = []

    def sql(self, q):
        self.queries.append(q)
        quoted = re.findall(r"`([^`]+)`", q)
        name = quoted[-1] if len(quoted) == 3 else None   # `catalog`.`schema`.`table`
        if q.startswith("SHOW TABLES"):
            rows = [R(tableName=n) for n in self.t]
        elif q.startswith("SHOW TBLPROPERTIES"):
            rows = [R(key="delta.enableChangeDataFeed", value="true"),
                    R(key=evidence.KEYS_PROPERTY, value=json.dumps(self.t[name][0]))]
        elif q.startswith("DESCRIBE TABLE"):
            rows = [R(col_name=c, data_type=t) for c, t in self.t[name][1]] + \
                   [R(col_name="", data_type=""), R(col_name="# Clustering Information", data_type=""),
                    R(col_name=self.t[name][1][0][0], data_type="int")]
        elif q.startswith("SELECT COUNT(*)"):
            rows = [R(n=self.t[name][2])]
        elif "DESCRIBE HISTORY" in q:
            rows = [R(version=i, operation=op, operationMetrics=json.dumps(m))
                    for i, (op, m) in enumerate(self.t[name][3])]
        else:
            raise AssertionError(q)
        return R(collect=lambda: rows)


class Bronze(unittest.TestCase):
    def setUp(self):
        self.s = evidence.bronze_schema(BronzeSpark(), "main", "sales_bronze_east")
        self.by = {t["name"]: t for t in self.s["tables"]}

    def test_keys_from_the_table_property(self):
        pk = {n: [c["name"] for c in t["columns"] if c.get("primary_key")] for n, t in self.by.items()}
        self.assertEqual(pk["lineitem"], ["l_orderkey", "l_linenumber"])
        self.assertEqual(pk["nation"], ["n_nationkey"])

    def test_columns_stop_at_the_first_section(self):
        self.assertEqual([c["name"] for c in self.by["region"]["columns"]], ["r_regionkey", "r_name"])
        self.assertEqual(self.by["orders"]["columns"][2]["type"], "decimal")

    def test_activity_skips_the_snapshot(self):
        # the 4 inserts before the first update count as snapshot: a lower bound
        self.assertEqual(self.s["activity"]["lineitem"], {"inserts": 0, "updates": 2, "deletes": 1})
        self.assertEqual(self.s["activity"]["nation"], {"inserts": 0, "updates": 25, "deletes": 0})
        self.assertEqual(self.s["activity"]["region"], {"inserts": 0, "updates": 0, "deletes": 0})

    def test_foreign_keys_inferred_from_names(self):
        fks = {(n, tuple(fk["columns"]), fk["parent_table"])
               for n, t in self.by.items() for fk in t.get("fk_constraints") or []}
        self.assertTrue({("nation", ("n_regionkey",), "region"), ("supplier", ("s_nationkey",), "nation"),
                         ("orders", ("o_custkey",), "customer"), ("lineitem", ("l_orderkey",), "orders"),
                         ("lineitem", ("l_suppkey",), "supplier"), ("partsupp", ("ps_suppkey",), "supplier"),
                         ("lineitem", ("l_partkey", "l_suppkey"), "partsupp")} <= fks, fks)
        self.assertNotIn(("lineitem", ("l_linenumber",), "orders"), fks)


class HiddenProperty(BronzeSpark):
    """SHOW TBLPROPERTIES without the keys property (as on a workspace); the Unity Catalog API has it."""

    def sql(self, q):
        r = super().sql(q)
        if q.startswith("SHOW TBLPROPERTIES"):
            rows = [x for x in r.collect() if x.key != evidence.KEYS_PROPERTY]
            return R(collect=lambda: rows)
        return r


class FakeWorkspace:
    def __init__(self, tables=TPCH):
        self.asked = []
        self.tables = self
        self.t = tables

    def get(self, full_name):
        self.asked.append(full_name)
        return R(properties={evidence.KEYS_PROPERTY: json.dumps(self.t[full_name.split(".")[-1]][0])})


class KeysFromTheApi(unittest.TestCase):
    def test_api_when_show_tblproperties_hides_it(self):
        w = FakeWorkspace()
        s = evidence.bronze_schema(HiddenProperty(), "main", "sales_bronze_east", workspace=w)
        lineitem = next(t for t in s["tables"] if t["name"] == "lineitem")
        pk = [c["name"] for c in lineitem["columns"] if c.get("primary_key")]
        self.assertEqual(pk, ["l_orderkey", "l_linenumber"])
        self.assertIn("main.sales_bronze_east.lineitem", w.asked)


class History(unittest.TestCase):
    """Where the snapshot ends: several insert-only batches, and a full refresh that starts a new one."""

    def activity(self, history):
        t = {"t": (["k"], [("k", "int")], 10, history)}
        return evidence.bronze_schema(BronzeSpark(t), "main", "s")["activity"]["t"]

    def test_snapshot_in_several_batches(self):
        h = [("MERGE", {"numTargetRowsInserted": "50"}), ("MERGE", {"numTargetRowsInserted": "50"}),
             ("MERGE", {"numTargetRowsMatchedUpdated": "3"}), ("MERGE", {"numTargetRowsInserted": "2"})]
        self.assertEqual(self.activity(h), {"inserts": 2, "updates": 3, "deletes": 0})

    def test_full_refresh_starts_over(self):
        h = [("MERGE", {"numTargetRowsInserted": "50"}), ("MERGE", {"numTargetRowsMatchedUpdated": "3"}),
             ("CREATE OR REPLACE TABLE", {}), ("MERGE", {"numTargetRowsInserted": "50"}),
             ("MERGE", {"numTargetRowsDeleted": "1"})]
        self.assertEqual(self.activity(h), {"inserts": 0, "updates": 0, "deletes": 1})

    def test_inserts_only_is_no_change_evidence(self):
        h = [("MERGE", {"numTargetRowsInserted": "50"}), ("MERGE", {"numTargetRowsInserted": "5"})]
        self.assertEqual(self.activity(h), {"inserts": 0, "updates": 0, "deletes": 0})


class InferByName(unittest.TestCase):
    def test_full_key_name_before_the_bare_one(self):   # ORM keys: customer_id, product_id both strip to "id"
        tables = [{"name": "customer", "columns": [{"name": "customer_id", "primary_key": True}]},
                  {"name": "product", "columns": [{"name": "product_id", "primary_key": True}]},
                  {"name": "orders", "columns": [{"name": "order_id", "primary_key": True}, {"name": "customer_id"},
                                                 {"name": "product_id"}]}]
        evidence.infer_foreign_keys(tables)
        fks = {(tuple(f["columns"]), f["parent_table"]) for f in tables[2]["fk_constraints"]}
        self.assertEqual(fks, {(("customer_id",), "customer"), (("product_id",), "product")})


class Plan(unittest.TestCase):
    def test_roles_and_routes(self):
        p = table_plan(BronzeSpark(), "main", "sales_bronze_east")
        t = p["tables"]
        self.assertEqual((t["lineitem"]["role"], t["lineitem"]["route"]), ("fact", "pipeline"))
        self.assertEqual(t["orders"]["role"], "fact")
        self.assertEqual((t["partsupp"]["role"], t["partsupp"]["route"]), ("bridge", "merge"))
        for n in ("supplier", "customer", "nation", "region", "part"):
            self.assertIn(t[n]["role"], ("dimension", "lookup"), (n, t[n]["why"]))
            self.assertEqual(t[n]["route"], "merge")
        self.assertIn("inferred from names", t["lineitem"]["why"])
        self.assertEqual(p["evidence"], "bronze main.sales_bronze_east")

    def test_hint_wins(self):
        hints = {"nation": "lookup", "orders": {"role": "log"}}
        p = table_plan(BronzeSpark(), "main", "sales_bronze_east", hints=hints)
        self.assertEqual((p["tables"]["nation"]["role"], p["tables"]["nation"]["decided_by"]), ("lookup", "hint"))
        self.assertEqual((p["tables"]["orders"]["silver"], p["tables"]["orders"]["route"]), (["append"], "pipeline"))
        self.assertIn("guessed", p["tables"]["orders"])

    def test_bad_hints(self):
        with self.assertRaisesRegex(ValueError, "no such table"):
            table_plan(BronzeSpark(), "main", "sales_bronze_east", hints={"nope": "fact"})
        with self.assertRaisesRegex(ValueError, "isn't one of"):
            table_plan(BronzeSpark(), "main", "sales_bronze_east", hints={"nation": "reference"})

    def test_routes_override(self):
        routes = {"dimension": "pipeline", "lookup": "pipeline", "bridge": "pipeline"}
        p = table_plan(BronzeSpark(), "main", "sales_bronze_east", routes=routes)
        self.assertTrue(all(t["route"] == "pipeline" for t in p["tables"].values()))


class Connection(unittest.TestCase):
    """The source's catalog through remote_query: declared keys and foreign keys, pg_stat_user_tables."""

    class Spark:
        def __init__(self):
            self.queries = []

        def sql(self, q):
            self.queries.append(q)
            if "information_schema.columns" in q:
                rows = [R(table_name="nation", column_name="n_nationkey", data_type="integer"),
                        R(table_name="nation", column_name="n_name", data_type="character"),
                        R(table_name="supplier", column_name="s_suppkey", data_type="integer"),
                        R(table_name="supplier", column_name="s_nationkey", data_type="integer"),
                        R(table_name="supplier", column_name="s_acctbal", data_type="numeric")]
            elif "pg_constraint" in q:
                rows = [R(table_name="nation", constraint_type="PRIMARY KEY", constraint_name="nation_pkey",
                          column_name="n_nationkey", parent_table=None),
                        R(table_name="supplier", constraint_type="PRIMARY KEY", constraint_name="supplier_pkey",
                          column_name="s_suppkey", parent_table=None),
                        R(table_name="supplier", constraint_type="FOREIGN KEY", constraint_name="supplier_fk1",
                          column_name="s_nationkey", parent_table="nation")]
            elif "pg_stat_user_tables" in q:   # nation: only its load, which is no evidence
                rows = [R(relname="nation", n_live_tup=25, n_tup_ins=25, n_tup_upd=0, n_tup_del=0),
                        R(relname="supplier", n_live_tup=500, n_tup_ins=500, n_tup_upd=3, n_tup_del=0)]
            else:
                raise AssertionError(q)
            return R(collect=lambda: rows)

    def test_declared_keys_and_activity(self):
        spark = self.Spark()
        s = evidence.connection_schema(spark, "pg_conn", "sales", "east")
        by = {t["name"]: t for t in s["tables"]}
        self.assertEqual(by["supplier"]["fk_constraints"], [{"columns": ["s_nationkey"], "parent_table": "nation"}])
        self.assertTrue(by["nation"]["columns"][0]["primary_key"])
        self.assertEqual(by["supplier"]["columns"][2]["type"], "numeric")
        self.assertEqual(s["activity"]["supplier"]["updates"], 3)
        self.assertNotIn("nation", s["activity"])
        start = "SELECT * FROM remote_query('pg_conn', database => 'sales'"
        self.assertTrue(all(q.startswith(start) for q in spark.queries))
        self.assertIn("table_schema = \\'east\\'", spark.queries[0])   # PostgreSQL's 'east', inside Spark's string

    def test_plan_through_the_connection(self):
        p = table_plan(self.Spark(), "main", "sales_bronze_east", connection="pg_conn", database="sales",
                       source_schema="east")
        self.assertEqual(p["evidence"], "connection pg_conn")
        self.assertEqual(p["tables"]["nation"]["route"], "merge")
        self.assertNotIn("inferred", p["tables"]["supplier"]["why"])


if __name__ == "__main__":
    unittest.main()
