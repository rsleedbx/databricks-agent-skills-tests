"""Offline checks of lfc_log_silver.silver (fake session): the six cells (view, merge, auto_cdc x a snapshot in the
log or in place) each read the snapshot and the changes, MERGE's SQL, positions kept per table, only_changed, a round
by method, and the AUTO CDC lists.

    python3 -m unittest skills/lfc-log-silver/tests/test_log_silver.py
"""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_change_source import routing  # noqa: E402
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.gateway_log import from_gateway  # noqa: E402
from lfc_log_silver import silver  # noqa: E402

ENTRY = {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]}
LOG = "`c`.`x`.`cdc_log`"
SNAP = "SELECT `C1`::INT AS `id`, `C2`::STRING AS `v` FROM read_files('/V/g/snapshot/T1/9/', format => 'parquet')"


def src(name="t"):
    return from_gateway("gw", LOG, dict(ENTRY, id=name), f"db.s.{name}")


def session(seen, kept=(), newest=()):
    def sql(q):
        seen.append(q)
        if q.startswith("SELECT target"):
            rows = list(kept)
        elif q.startswith("SELECT destination, max("):
            rows = list(newest)
        elif "information_schema" in q:
            rows = []
        elif q.startswith("SELECT count(*)"):
            rows = [(0,)]
        else:
            rows = [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})]
        return types.SimpleNamespace(collect=lambda: rows)
    return types.SimpleNamespace(sql=sql)


class SixCells(unittest.TestCase):
    def test_every_cell_reads_the_snapshot_and_the_changes(self):
        at = Position("gateway_cursor", "04")
        for in_place in (False, True):
            t = silver.Table(src(), "merge", snapshot_sql=SNAP if in_place else None, position=at if in_place else None,
                             snapshot_files={"path": "/V/g/snapshot/T1/9/", "exprs": ["`C1`::INT AS `id`"]} if in_place
                             else None)
            for method in silver.METHODS:
                t.method = method
                where = "in place" if in_place else "in log"
                if method == "auto_cdc":
                    item = silver.auto_cdc_tables({"t": t})[0][0]
                    self.assertEqual("snapshot" in item, in_place, where)          # a one-time flow reads it
                    self.assertIn("value:data.`C2`::STRING AS `v`", item["exprs"])  # and the flow reads the log
                    continue
                q = (silver.view_sql if method == "view" else silver.build_sql)(t, "`c`.`s`.`t`")
                self.assertIn("FROM `c`.`x`.`cdc_log` WHERE destination = 'db.s.t'", q, (method, where))   # changes
                if in_place:
                    self.assertIn("read_files('/V/g/snapshot/T1/9/'", q, method)                         # snapshot
                    self.assertIn("value:cursor.sequence::STRING > '04'", q, method)                     # after it
                else:
                    self.assertNotIn("read_files", q, method)   # the snapshot rows are the log's read events

    def test_from_route(self):
        p = routing.Policy(LOG, chosen={"a": "big_slow", "b": "small_slow"})
        routes = routing.plan({"a": (0, 0), "b": (0, 0)}, p)
        t = silver.from_route(src(), routes["a"])
        self.assertEqual((t.method, t.only_changed), ("merge", True))
        self.assertEqual((silver.from_route(src(), routes["b"]).method, silver.from_route(src(), routes["b"]).group),
                         ("shared", "0"))


class Shared(unittest.TestCase):
    def test_one_scan_one_merge_a_group_and_typed_views(self):
        tables = {n: silver.Table(src(n), "shared", group="0") for n in "ab"}
        q = silver.shared_changes_sql([t.src for t in tables.values()], Position("gateway_cursor", "05"),
                                      Position("gateway_cursor", "09"))
        self.assertEqual(q.count("FROM `c`.`x`.`cdc_log`"), 1)                               # one scan
        self.assertIn("SELECT destination, key::STRING AS k, value:data AS data", q)        # nothing a table
        self.assertNotIn("CASE destination", q)
        self.assertIn("value:cursor.sequence::STRING > '05'", q)
        v = silver.shared_view_sql(tables["a"], "`c`.`s`", "`c`.`s`.`a`")
        self.assertIn("data:`C1`::INT AS `id`, data:`C2`::STRING AS `v` FROM `c`.`s`.`_shared_0` "
                      "WHERE destination = 'db.s.a'", v)                                      # typed as the source
        seen = []
        silver.run(session(seen), tables, "`c`.`s`", Position("gateway_cursor", "09"), threads=1, first=True)
        self.assertEqual(sum(q.startswith("CREATE OR REPLACE TABLE `c`.`s`.`_shared_0`") for q in seen), 1)
        self.assertEqual(sum(q.startswith("CREATE OR REPLACE VIEW") for q in seen), 2)
        seen.clear()
        got = silver.run(session(seen, kept=[("_shared_0", "gateway_cursor", "05")]), tables, "`c`.`s`",
                         Position("gateway_cursor", "12"), threads=1)
        merges = [q for q in seen if q.startswith("MERGE INTO `c`.`s`.`_shared_0`")]
        self.assertEqual((len(merges), got["shared"]["groups"], got["shared"]["done"]), (1, 1, 1))  # one MERGE
        self.assertIn("ON t.destination = c.destination AND t.k = c.k", merges[0])
        self.assertIn("> '05'", merges[0])

    def test_a_debezium_member_reads_its_after_image(self):
        from lfc_change_source.cdc_log import CdcLog
        d = CdcLog("pg1", LOG, "t", "pg1.public.t", ["id"], {"id": "BIGINT", "total": "DECIMAL(12,2)"}, seq="lsn")
        self.assertEqual(d.row_sql(), "value:after")
        self.assertEqual(d.stored_values(), {"id": "CAST(data:`id` AS BIGINT)", "total": "CAST(data:`total` AS DECIMAL(12,2))"})
        with self.assertRaises(ValueError):                                                  # one transport a group
            silver.shared_changes_sql([src("a"), d], None, None)

    def test_a_group_needs_keys_one_log_and_its_snapshot_in_the_log(self):
        keyless = from_gateway("gw", LOG, dict(ENTRY, id="k", keys=[]), "db.s.k")
        with self.assertRaises(ValueError):
            silver.shared_changes_sql([src("a"), keyless], None, None)
        with self.assertRaises(ValueError):
            silver.run(session([]), {"a": silver.Table(src("a"), "shared", snapshot_sql=SNAP)}, "`c`.`s`",
                       Position("gateway_cursor", "1"), first=True)


class Merge(unittest.TestCase):
    def test_merge_sql(self):
        m = silver.merge_sql(src(), "`c`.`silver`.`t`", Position("gateway_cursor", "05"),
                             Position("gateway_cursor", "09"))
        self.assertIn("ON t.`id` <=> c.`id`", m)
        self.assertLess(m.index("THEN DELETE"), m.index("THEN UPDATE SET t.`v` = c.`v`"))
        self.assertIn("WHEN NOT MATCHED AND c._op <> 'delete' THEN INSERT (`id`, `v`) VALUES (c.`id`, c.`v`)", m)
        self.assertIn("value:cursor.sequence::STRING > '05'", m)
        self.assertIn("value:cursor.sequence::STRING <= '09'", m)

    def test_apply_from_each_kept_position_and_only_changed(self):
        seen = []
        s = session(seen, kept=[("a", "gateway_cursor", "03"), ("b", "gateway_cursor", "05"),
                                ("c", "gateway_cursor", "05")], newest=[("db.s.b", "07")])
        tables = {"a": silver.Table(src("a")), "b": silver.Table(src("b"), only_changed=True),
                  "c": silver.Table(src("c"), only_changed=True)}
        done = silver.apply(s, tables, "`c`.`silver`", Position("gateway_cursor", "09"), threads=1)
        self.assertEqual(sorted(done), ["a", "b"])                              # c: nothing past 05
        merge_a = next(q for q in seen if q.startswith("MERGE INTO `c`.`silver`.`a`"))
        self.assertIn("> '03'", merge_a)
        self.assertIn("MERGE INTO `c`.`silver`._log_silver", seen[-1])          # the new positions, one statement


class Round(unittest.TestCase):
    def test_first_round_then_a_round(self):
        tables = {"v": silver.Table(src("v"), "view"), "m": silver.Table(src("m")),
                  "q": silver.Table(src("q"), "auto_cdc")}
        seen = []
        got = silver.run(session(seen), tables, "`c`.`silver`", Position("gateway_cursor", "09"), threads=1, first=True)
        self.assertEqual({m: (v["tables"], v["done"]) for m, v in got.items()},
                         {"shared": (0, 0), "view": (1, 1), "merge": (1, 1), "auto_cdc": (1, 0)})
        self.assertTrue(any(q.startswith("CREATE OR REPLACE VIEW `c`.`silver`.`v`") for q in seen))
        self.assertTrue(any(q.startswith("CREATE OR REPLACE TABLE `c`.`silver`.`m`") for q in seen))
        seen.clear()
        got = silver.run(session(seen, kept=[("m", "gateway_cursor", "05")]), tables, "`c`.`silver`",
                         Position("gateway_cursor", "12"), threads=1)
        self.assertEqual(got["view"]["done"], 0)                                 # views: no work a round
        self.assertTrue(any(q.startswith("MERGE INTO `c`.`silver`.`m`") for q in seen))
        with self.assertRaises(ValueError):
            silver.run(session([]), {"x": silver.Table(src(), "merge_round")}, "`c`.`s`", Position("gateway_cursor", "1"))

    def test_auto_cdc_lists(self):
        tables = {n: silver.Table(src(n), "auto_cdc") for n in "abc"}
        lists = silver.auto_cdc_tables(tables, per_pipeline=2)
        self.assertEqual([len(x) for x in lists], [2, 1])
        self.assertEqual(lists[0][0]["seq"], "value:cursor.sequence::STRING")
        self.assertEqual(lists[0][0]["seq_type"], "STRING")   # the snapshot flow's position takes this type


if __name__ == "__main__":
    unittest.main()
