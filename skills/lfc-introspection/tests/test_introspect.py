"""lfc_introspection.introspect on a fake session: the three catalog statements (columns, keys, sizes) through
remote_query, each engine's type text (lengths, decimals, a timestamp's digits, MySQL's whole column_type), a
composite foreign key with its action, a keyless table, rows and bytes from the size query. Offline.

    uv run --no-project --with pyyaml --with sqlglot python -m unittest skills/lfc-introspection/tests/test_introspect.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_introspection.introspect import introspect, native  # noqa: E402

COLS = [("orders", 1, "o_id", "integer", None, 32, 0, None, "NO", None),
        ("orders", 2, "o_c_id", "integer", None, 32, 0, None, "NO", None),
        ("orders", 3, "o_w_id", "integer", None, 32, 0, None, "NO", None),
        ("orders", 4, "total", "numeric", None, 12, 2, None, "YES", "0"),
        ("orders", 5, "at", "timestamp without time zone", None, None, None, 3, "YES", None),
        ("customer", 2, "c_w_id", "integer", None, 32, 0, None, "NO", None),
        ("customer", 1, "c_id", "integer", None, 32, 0, None, "NO", None),
        ("customer", 3, "name", "character varying", 40, None, None, None, "YES", None),
        ("hits", 1, "url", "text", None, None, None, None, "YES", None)]
KEYS = [("P", "orders", "orders_pkey", "o_id", 1, None, None, None, None),
        ("P", "customer", "customer_pkey", "c_w_id", 2, None, None, None, None),
        ("P", "customer", "customer_pkey", "c_id", 1, None, None, None, None),
        ("F", "orders", "orders_c_fk", "o_w_id", 2, "customer", "c_w_id", "CASCADE", "NO ACTION"),
        ("F", "orders", "orders_c_fk", "o_c_id", 1, "customer", "c_id", "CASCADE", "NO ACTION")]
SIZES = [{"table_schema": "app", "table_name": "orders", "rows_est": 5000, "row_bytes": 400_000, "lob_bytes": 0},
         {"table_schema": "app", "table_name": "customer", "rows_est": None, "rows_alt": 100, "row_bytes": 8192},
         {"table_schema": "other", "table_name": "hits", "rows_est": 9},
         {"table_schema": "app", "table_name": "hits", "rows_est": 70, "row_bytes": 16384, "lob_bytes": 8192}]


class Fake:
    def __init__(self):
        self.seen = []

    def sql(self, q):
        self.seen.append(q)
        rows = (COLS if "information_schema.columns" in q else KEYS if ("pg_constraint" in q or "referential_constraints" in q) else SIZES)

        class R:
            def collect(self):
                return rows
        return R()


class Introspect(unittest.TestCase):
    def setUp(self):
        self.fake = Fake()
        self.schema, self.stats = introspect(self.fake, "conn1", "postgres", database="db1", schema="app",
                                             via="remote_query")
        self.t = {t["name"]: t for t in self.schema["tables"]}

    def test_three_statements_through_the_connection(self):
        self.assertEqual(len(self.fake.seen), 3)
        self.assertTrue(all(q.startswith("SELECT * FROM remote_query('conn1', database => 'db1'") for q in self.fake.seen))
        self.assertTrue(all("'app'" in q for q in self.fake.seen[1:]))   # the schema filled in (not the size query)

    def test_types_keys_and_a_keyless_table(self):
        o = {c["name"]: c for c in self.t["orders"]["columns"]}
        self.assertEqual((o["total"]["type"], o["total"]["precision"], o["total"]["scale"]), ("decimal", 12, 2))
        self.assertEqual(o["at"]["native_type"], "timestamp(3) without time zone")
        self.assertTrue(o["o_id"]["primary_key"])
        c = self.t["customer"]
        self.assertEqual([x["name"] for x in c["columns"]], ["c_id", "c_w_id", "name"])   # by ordinal
        self.assertEqual([x["name"] for x in c["columns"] if x.get("primary_key")], ["c_id", "c_w_id"])
        self.assertFalse(any(x.get("primary_key") for x in self.t["hits"]["columns"]))

    def test_a_composite_foreign_key_with_its_action(self):
        self.assertEqual(self.t["orders"]["fk_constraints"],
                         [{"columns": ["o_c_id", "o_w_id"], "parent_table": "customer", "parent_columns": ["c_id", "c_w_id"],
                           "fk_distribution": "uniform", "on_delete": "CASCADE"}])

    def test_rows_and_bytes_from_the_size_query_of_this_schema(self):
        s = {t["name"]: t for t in self.stats["tables"]}
        self.assertEqual((s["orders"]["row_count"], s["orders"]["data_size_bytes"]), (5000, 400_000))
        self.assertEqual(s["customer"]["row_count"], 100)                   # rows_alt when rows_est is unknown
        self.assertEqual((s["hits"]["row_count"], s["hits"]["data_size_bytes"]), (70, 24576))   # not schema other's

    def test_the_same_reads_through_spark_jdbc(self):
        seen = []

        class Reader:
            def __init__(self):
                self.opts = {}

            def format(self, f):
                self.opts["format"] = f
                return self

            def option(self, k, v):
                self.opts[k] = v
                return self

            def load(self):
                seen.append(dict(self.opts))
                q = self.opts["query"]
                rows = COLS if "information_schema.columns" in q else KEYS if "pg_constraint" in q else SIZES

                class D:
                    def collect(self):
                        return rows
                return D()

        class Spark:
            @property
            def read(self):
                return Reader()
        schema, _ = introspect(Spark(), "jdbc1", "postgres", database="db1", schema="app")   # via="jdbc", the default
        self.assertEqual(schema, self.schema)
        self.assertEqual([(s["format"], s["databricks.connection"]) for s in seen], [("jdbc", "jdbc1")] * 3)
        self.assertFalse(any("remote_query" in s["query"] for s in seen))
        self.assertTrue(all(not s["query"].endswith(";") for s in seen))   # Spark wraps it in a subquery

    def test_native_type_text(self):
        self.assertEqual(native("sqlserver", "nvarchar", -1, None, None, None), "nvarchar")   # (max)
        self.assertEqual(native("sqlserver", "datetime2", None, None, None, 7), "datetime2(7)")
        self.assertEqual(native("mysql", "decimal(10,2) unsigned", None, None, None, None), "decimal(10,2) unsigned")


if __name__ == "__main__":
    unittest.main()
