"""Offline checks of lfc_medallion's discover and stack with a fake spark and a fake pipelines module: sources from the
schema names or a list, each source's tables, the UNION ALL each view runs, a table only some sources have, quoting.

    python3 -m unittest skills/lfc-medallion/tests/test_stack.py
"""
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_medallion import sources, stack_all, tables, union_sql

SCHEMAS = ["shop_bronze_east", "shop_bronze_west", "shop_bronze_north", "shop_silver", "other"]
TABLES = {"shop_bronze_east": ["orders", "lines", "nation"], "shop_bronze_west": ["orders", "lines", "nation"],
          "shop_bronze_north": ["orders", "lines"]}


class FakeSpark:
    """Answers SHOW SCHEMAS ... LIKE '<prefix>*' and SHOW TABLES IN `c`.`s`; any other query is returned as given."""

    def __init__(self):
        self.queries = []

    def sql(self, q):
        self.queries.append(q)
        if q.startswith("SHOW SCHEMAS"):
            like = q.split("LIKE '")[1].rstrip("*'")
            return types.SimpleNamespace(collect=lambda: [types.SimpleNamespace(databaseName=s) for s in SCHEMAS
                                                          if s.startswith(like)])
        if q.startswith("SHOW TABLES"):
            schema = q.split("`.`")[1].rstrip("`")
            return types.SimpleNamespace(collect=lambda: [types.SimpleNamespace(tableName=t) for t in TABLES[schema]])
        return q


class FakeDp:
    """Records each materialized view's name and comment, and the query its function runs."""

    def __init__(self):
        self.views = {}

    def materialized_view(self, name, comment):
        def register(fn):
            self.views[name] = (comment, fn())
            return fn
        return register


class Discover(unittest.TestCase):
    def test_from_schema_names(self):
        self.assertEqual(sources(FakeSpark(), "main", "shop_bronze_"), ["east", "north", "west"])

    def test_list_restricts(self):
        self.assertEqual(sources(FakeSpark(), "main", "shop_bronze_", "west, east,"), ["west", "east"])
        self.assertEqual(sources(FakeSpark(), "main", "shop_bronze_", ["east"]), ["east"])

    def test_none_found_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "no sources"):
            sources(FakeSpark(), "main", "missing_")

    def test_tables(self):
        held = tables(FakeSpark(), "main", "shop_bronze_", ["east", "north"])
        self.assertEqual(held, {"east": {"orders", "lines", "nation"}, "north": {"orders", "lines"}})


class Stack(unittest.TestCase):
    def test_union_sql(self):
        self.assertEqual(union_sql("main", "shop_bronze_", "orders", ["east", "west"], "division"),
                         "SELECT 'east' AS `division`, * FROM `main`.`shop_bronze_east`.`orders` UNION ALL "
                         "SELECT 'west' AS `division`, * FROM `main`.`shop_bronze_west`.`orders`")

    def test_quote_in_a_source_name(self):
        self.assertIn("SELECT 'o''brien' AS", union_sql("c", "p_", "t", ["o'brien"]))

    def test_every_table_one_view(self):
        dp = FakeDp()
        defined = stack_all(dp, FakeSpark(), "main", "shop_bronze_", source_column="division")
        self.assertEqual(defined, {"all_lines": ["east", "north", "west"], "all_nation": ["east", "west"],
                                   "all_orders": ["east", "north", "west"]})
        comment, query = dp.views["all_nation"]
        self.assertEqual(query, union_sql("main", "shop_bronze_", "nation", ["east", "west"], "division"))
        self.assertIn("(n:1)", comment)

    def test_each_view_keeps_its_own_query(self):   # the loop's closures don't all end up with the last table's query
        dp = FakeDp()
        stack_all(dp, FakeSpark(), "main", "shop_bronze_")
        self.assertIn("`orders`", dp.views["all_orders"][1])
        self.assertNotIn("`lines`", dp.views["all_orders"][1])

    def test_name_pattern_and_only(self):
        dp = FakeDp()
        defined = stack_all(dp, FakeSpark(), "main", "shop_bronze_", only="east", name="n1_{table}")
        self.assertEqual(sorted(defined), ["n1_lines", "n1_nation", "n1_orders"])
        self.assertTrue(all(v == ["east"] for v in defined.values()))


if __name__ == "__main__":
    unittest.main()
