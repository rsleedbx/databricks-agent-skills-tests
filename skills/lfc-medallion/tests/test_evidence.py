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
        if "information_schema.tables" in q:   # every table, plus a view the plan must leave out
            rows = [R(table_name=n, table_type="STREAMING_TABLE") for n in self.t] + \
                   [R(table_name="recent_orders", table_type="VIEW"),
                    R(table_name="__materialization_mat_1a2b_lineitem_1", table_type="MANAGED"),
                    R(table_name="event_log_ac7b7c4c_5e82_4203_90fe_ca87989b9f72", table_type="MANAGED")]
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
            rows = [R(version=i, operation=h[0], operationMetrics=json.dumps(h[1]),
                      operationParameters=json.dumps(h[2] if len(h) > 2 else {}))
                    for i, h in enumerate(self.t[name][3])]
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


class NotKeysOfOthers(unittest.TestCase):
    def test_own_key_and_unprefixed_columns(self):
        tables = [{"name": "a", "columns": [{"name": "id", "primary_key": True}, {"name": "external_id"}]},
                  {"name": "b", "columns": [{"name": "id", "primary_key": True}]}]
        evidence.infer_foreign_keys(tables)
        self.assertEqual([t.get("fk_constraints") for t in tables], [None, None])   # no a.id -> b, no external_id -> b


class Plan(unittest.TestCase):
    def test_roles_and_routes(self):
        p = table_plan(BronzeSpark(), "main", "sales_bronze_east")
        t = p["tables"]
        self.assertNotIn("recent_orders", t)   # a view: no history, not planned
        self.assertEqual(sorted(t), sorted(TPCH))   # nor the pipeline's backing tables
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
            if "information_schema.columns" in q:   # out of order: sorted by ord
                rows = [R(table_name="supplier", column_name="s_acctbal", data_type="numeric", ord=3),
                        R(table_name="nation", column_name="n_nationkey", data_type="integer", ord=1),
                        R(table_name="nation", column_name="n_name", data_type="character", ord=2),
                        R(table_name="supplier", column_name="s_suppkey", data_type="integer", ord=1),
                        R(table_name="supplier", column_name="s_nationkey", data_type="integer", ord=2)]
            elif "pg_constraint" in q:
                rows = [R(table_name="nation", constraint_type="PRIMARY KEY", constraint_name="nation_pkey",
                          column_name="n_nationkey", parent_table=None, ord=1),
                        R(table_name="supplier", constraint_type="PRIMARY KEY", constraint_name="supplier_pkey",
                          column_name="s_suppkey", parent_table=None, ord=1),
                        R(table_name="supplier", constraint_type="FOREIGN KEY", constraint_name="supplier_fk1",
                          column_name="s_nationkey", parent_table="nation", ord=1)]
            elif "pg_stat_user_tables" in q:   # nation: only its load, which is no evidence
                rows = [R(table_name="nation", live=25, ins=25, upd=0, **{"del": 0}),
                        R(table_name="supplier", live=500, ins=500, upd=3, **{"del": 0})]
            else:
                raise AssertionError(q)
            return R(collect=lambda: rows)

    def test_declared_keys_and_activity(self):
        spark = self.Spark()
        s = evidence.connection_schema(spark, "pg_conn", "sales", "east", "POSTGRESQL")
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
                       source_schema="east", engine="POSTGRESQL")
        self.assertEqual(p["evidence"], "connection pg_conn (POSTGRESQL)")
        self.assertEqual(p["tables"]["nation"]["route"], "merge")
        self.assertNotIn("inferred", p["tables"]["supplier"]["why"])


class Engines(unittest.TestCase):
    """Each engine's catalog queries: the same columns, no ORDER BY (remote_query runs them as subqueries, and SQL
    Server refuses ORDER BY there), the schema quoted; activity optional where the engine can fall back to rows."""

    def test_every_engine_has_its_queries(self):
        for engine, q in evidence._engine_sql("'east'").items():
            self.assertTrue({"columns", "keys", "activity"} <= set(q), engine)
            for kind, sql in q.items():
                self.assertNotIn("ORDER BY", sql.upper(), (engine, kind))
        sqlserver = evidence._engine_sql("'east'")["SQLSERVER"]["activity"]   # rows summed apart from the counters
        self.assertIn("SUM(rows) AS live FROM sys.partitions", sqlserver)
        self.assertNotIn("MAX(p.rows)", sqlserver)
        for engine, q in evidence._engine_sql("'east'").items():
            for kind, sql in q.items():
                self.assertIn("'east'", sql, (engine, kind))
                self.assertIn(" AS ORD" if kind in ("columns",) else "", sql.upper())

    def test_quote_and_backslash(self):
        self.assertEqual(evidence.sql_string("o'b\\x"), "'o''b\\\\x'")

    def test_no_counters_privilege_gives_row_counts(self):
        class Spark(Connection.Spark):
            def sql(self, q):
                if "table_io_waits" in q:
                    raise RuntimeError("SELECT command denied to user for table 'table_io_waits_summary_by_table'")
                if "information_schema.tables" in q and "table_rows" in q:
                    return R(collect=lambda: [R(table_name="nation", live=25), R(table_name="supplier", live=500)])
                return super().sql("pg_constraint" if "key_column_usage" in q else q)
        s = evidence.connection_schema(Spark(), "my_conn", "east", "east", "MYSQL")
        self.assertEqual(s["activity"], {})
        self.assertEqual({t["name"]: t.get("row_count") for t in s["tables"]}, {"nation": 25, "supplier": 500})
        self.assertIn("no counters", s["evidence"])

    def test_mysql_schema_is_the_database(self):
        spark = Connection.Spark()
        with self.assertRaises(AssertionError):   # the fake answers PostgreSQL's key query only; the first query counts
            table_plan(spark, "main", "sales_bronze_east", connection="my", database="shop", engine="MYSQL")
        self.assertIn("table_schema = \\'shop\\'", spark.queries[0])

    def test_no_tables_found_is_an_error(self):
        class Empty(Connection.Spark):
            def sql(self, q):
                return R(collect=list)
        with self.assertRaisesRegex(ValueError, "no tables in database shop, schema nope"):
            table_plan(Empty(), "main", "b", connection="c", database="shop", source_schema="nope", engine="POSTGRESQL")

    def test_engine_from_the_connection_and_fallback(self):
        class W:
            def __init__(self, kind):
                self.connections = self
                self.kind = kind

            def get(self, name):
                return R(connection_type=R(value=self.kind))
        self.assertEqual(evidence.connection_engine("c", W("SQLSERVER")), "SQLSERVER")
        p = table_plan(BronzeSpark(), "main", "sales_bronze_east", connection="snow", workspace=W("SNOWFLAKE"))
        self.assertIn("connection type SNOWFLAKE: no catalog queries for it, bronze used", p["evidence"])
        self.assertEqual(p["tables"]["lineitem"]["role"], "fact")


class Situations(unittest.TestCase):
    def test_appends_and_overwrite(self):   # an append-only destination: WRITE / STREAMING UPDATE; an overwrite resets
        h = [("WRITE", {"numOutputRows": "100"}, {"mode": "Append"}), ("UPDATE", {"numUpdatedRows": "2"}),
             ("STREAMING UPDATE", {"numOutputRows": "7"}), ("WRITE", {"numOutputRows": "50"}, {"mode": "Overwrite"}),
             ("DELETE", {"numDeletedRows": "1"})]
        t = {"t": (["k"], [("k", "int")], 10, h)}
        self.assertEqual(evidence.bronze_schema(BronzeSpark(t), "main", "s")["activity"]["t"],
                         {"inserts": 0, "updates": 0, "deletes": 1})
        self.assertEqual(evidence.bronze_schema(BronzeSpark({"t": (["k"], [("k", "int")], 10, h[:3])}), "main", "s")
                         ["activity"]["t"], {"inserts": 7, "updates": 2, "deletes": 0})

    def test_system_columns_left_out(self):   # SCD type 2 and file sources add these
        cols = [("k", "int"), ("v", "string"), ("__START_AT", "timestamp"), ("__END_AT", "timestamp"),
                ("_rescued_data", "string")]
        s = evidence.bronze_schema(BronzeSpark({"t": (["k"], cols, 3, [SNAP])}), "main", "s")
        self.assertEqual([c["name"] for c in s["tables"][0]["columns"]], ["k", "v"])

    def test_orm_table_id(self):   # Rails and most ORMs: orders.customer_id -> customers.id
        tables = [{"name": "customers", "columns": [{"name": "id", "primary_key": True}, {"name": "name"}]},
                  {"name": "categories", "columns": [{"name": "id", "primary_key": True}]},
                  {"name": "orders", "columns": [{"name": "id", "primary_key": True}, {"name": "customer_id"},
                                                 {"name": "category_id"}, {"name": "external_id"}]}]
        evidence.infer_foreign_keys(tables)
        fks = {(tuple(f["columns"]), f["parent_table"]) for f in tables[2]["fk_constraints"]}
        self.assertEqual(fks, {(("customer_id",), "customers"), (("category_id",), "categories")})

    def test_no_row_counts(self):
        spark = BronzeSpark()
        s = evidence.bronze_schema(spark, "main", "sales_bronze_east", count_rows=False)
        self.assertFalse(any("COUNT(*)" in q for q in spark.queries))
        self.assertTrue(all("row_count" not in t for t in s["tables"]))


class Busy(unittest.TestCase):
    """The measured activity overrides the role's route: a much-changed table goes to the pipeline."""

    def plan(self, history, rows=1000, **kw):
        t = {"solo": (["pk"], [("pk", "int"), ("dt", "timestamp")], rows, history)}
        return table_plan(BronzeSpark(t), "main", "s", **kw)["tables"]["solo"]

    def test_busy_dimension_to_the_pipeline(self):
        t = self.plan([SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "1900", "numTargetRowsDeleted": "200"})])
        self.assertEqual((t["role"], t["route"]), ("dimension", "pipeline"))
        self.assertIn("busy (2100 changes recorded, 2.1 per row)", t["why"])

    def test_one_bulk_update_isnt_busy(self):   # every row changed once (a relabel at setup)
        t = self.plan([SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "7500"})], rows=7500)
        self.assertEqual(t["route"], "merge")

    def test_quiet_dimension_stays(self):
        t = self.plan([SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "32"})])
        self.assertEqual(t["route"], "merge")

    def test_no_row_counts_no_override(self):
        history = [SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "7500"})]
        spark = BronzeSpark({"solo": (["pk"], [("pk", "int")], 10, history)})
        t = table_plan(spark, "main", "s", count_rows=False)["tables"]["solo"]
        self.assertEqual(t["route"], "merge")
        self.assertIn("rows not counted: busy not checked", t["why"])

    def test_through_the_connection_the_load_isnt_change(self):
        class Spark(Connection.Spark):
            def __init__(self, ins):
                super().__init__()
                self.ins = ins

            def sql(self, q):
                if "pg_stat_user_tables" in q:
                    return R(collect=lambda: [R(table_name="nation", live=1000, ins=self.ins, upd=0, **{"del": 0}),
                                              R(table_name="supplier", live=500, ins=500, upd=3, **{"del": 0})])
                return super().sql(q)
        kw = {"connection": "pg_conn", "database": "sales", "source_schema": "east", "engine": "POSTGRESQL"}
        quiet = table_plan(Spark(1000), "main", "s", **kw)["tables"]["nation"]
        self.assertEqual(quiet["route"], "merge")   # only its load
        busy = table_plan(Spark(4000), "main", "s", **kw)["tables"]["nation"]
        self.assertEqual(busy["route"], "pipeline")
        self.assertIn("busy (3000 changes recorded, 3.0 per row)", busy["why"])

    def test_threshold_and_hint(self):
        h = [SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "50"})]
        self.assertEqual(self.plan(h, rows=10, busy_changes=40)["route"], "pipeline")
        self.assertEqual(self.plan(h, rows=10, busy_changes=40, busy_per_row=10)["route"], "merge")
        busy = [SNAP, ("MERGE", {"numTargetRowsMatchedUpdated": "5000"})]
        self.assertEqual(self.plan(busy, hints={"solo": "lookup"})["route"], "merge")   # a hint's route stands


if __name__ == "__main__":
    unittest.main()


class SplitTables(unittest.TestCase):
    def test_one_rule_for_every_split(self):
        from lfc_medallion import split_tables
        roles = {"history": "log", "account": "dimension", "partsupp": "bridge", "nation": "lookup", "x": "",
                 "orders": "fact", "new_order": "queue"}
        self.assertEqual(split_tables(roles), (["history", "x", "orders", "new_order"],
                                               ["account", "partsupp", "nation"]))   # a role-less table is busy
        self.assertEqual(split_tables({"partsupp": "bridge"}, routes={"bridge": "pipeline"}), (["partsupp"], []))
