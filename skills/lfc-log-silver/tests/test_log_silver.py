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
SNAP = "`c`.`snap`.`t`"   # the snapshot contract: a Delta table with the source's columns


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
        elif q.startswith("SELECT snapshot_table, target") or q.startswith("SELECT max(") or q.startswith("DESCRIBE"):
            rows = []   # no snapshot ids kept or in the log
        else:
            rows = [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})]
        return types.SimpleNamespace(collect=lambda: rows)
    return types.SimpleNamespace(sql=sql)


class SixCells(unittest.TestCase):
    def test_every_cell_reads_the_snapshot_and_the_changes(self):
        at = Position("gateway_cursor", "04")
        for in_place in (False, True):
            t = silver.Table(src(), "merge", snapshot_table=SNAP if in_place else None, position=at if in_place else None)
            for method in silver.METHODS:
                t.method = method
                where = "in place" if in_place else "in log"
                if method == "auto_cdc":
                    item = silver.auto_cdc_tables({"t": t})[0][0]
                    self.assertEqual("snapshot" in item, in_place, where)          # a flow streams it
                    if in_place:
                        self.assertEqual((item["snapshot"]["table"], item["snapshot"]["columns"]), (SNAP, ["id", "v"]))
                    self.assertIn("value:data.`C2`::STRING AS `v`", item["exprs"])  # and the flow reads the log
                    continue
                q = (silver.view_sql if method == "view" else silver.build_sql)(t, "`c`.`s`.`t`")
                self.assertIn("FROM `c`.`x`.`cdc_log` WHERE destination = 'db.s.t'", q, (method, where))   # changes
                if in_place:
                    self.assertIn(f"FROM {SNAP}", q, method)                                             # snapshot
                    self.assertIn("value:cursor.sequence::STRING > '04'", q, method)                     # after it
                else:
                    self.assertNotIn(SNAP, q, method)   # the snapshot rows are the log's read events

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
                      "WHERE destination = 'db.s.a' AND NOT coalesce(deleted, false)", v)     # typed as the source
        self.assertNotIn("UNION ALL", v)                                                     # no snapshot table
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
        self.assertIn("t.deleted = c._op = 'delete'", merges[0])                             # a delete: its marker
        self.assertIn("> '05'", merges[0])

    def test_a_debezium_member_reads_its_after_image(self):
        from lfc_change_source.cdc_log import CdcLog
        d = CdcLog("pg1", LOG, "t", "pg1.public.t", ["id"], {"id": "BIGINT", "total": "DECIMAL(12,2)"}, seq="lsn")
        self.assertEqual(d.row_sql(), "value:after")
        self.assertEqual(d.stored_values(), {"id": "CAST(data:`id` AS BIGINT)", "total": "CAST(data:`total` AS DECIMAL(12,2))"})
        with self.assertRaises(ValueError):                                                  # one transport a group
            silver.shared_changes_sql([src("a"), d], None, None)

    def test_a_group_needs_keys_and_one_log(self):
        keyless = from_gateway("gw", LOG, dict(ENTRY, id="k", keys=[]), "db.s.k")
        with self.assertRaises(ValueError):
            silver.shared_changes_sql([src("a"), keyless], None, None)

    def test_a_member_view_reads_its_snapshot_table_for_keys_the_group_has_not_changed(self):
        t = silver.Table(src("a"), "shared", snapshot_table=SNAP, group="0")
        v = silver.shared_view_sql(t, "`c`.`s`", "`c`.`s`.`a`")
        self.assertIn(f"UNION ALL SELECT s.`id`, s.`v` FROM {SNAP} s WHERE NOT EXISTS (SELECT 1 FROM "
                      "`c`.`s`.`_shared_0` x WHERE x.destination = 'db.s.a' AND parse_json(x.k)[0]::INT <=> s.`id`)", v)
        seen = []
        silver.run(session(seen), {"a": t}, "`c`.`s`", Position("gateway_cursor", "09"), threads=1, first=True)
        build = next(q for q in seen if q.startswith("CREATE OR REPLACE TABLE `c`.`s`.`_shared_0`"))
        self.assertIn("_op = 'delete' AS deleted", build)                                    # deletes kept as markers
        self.assertNotIn(SNAP, build)                                                        # the snapshot isn't copied


class NewerSnapshot(unittest.TestCase):
    def test_a_new_snapshot_is_a_new_table(self):
        tables = {n: silver.Table(src(n), "merge", snapshot_table=x) for n, x in
                  (("a", "`c`.`s2`.`a`"), ("b", "`c`.`s1`.`b`"), ("c", "`c`.`s1`.`c`"), ("d", "`c`.`s1`.`d`"))}
        kept = {"a": "`c`.`s1`.`a`", "b": "`c`.`s1`.`b`", "c": None}   # d: never built
        self.assertEqual(silver.renewed(tables, kept), ["a", "c"])      # a: a new table; c: built from the log alone


class SchemaChange(unittest.TestCase):
    def test_a_table_whose_columns_differ_from_its_source(self):
        rows = [("same", "id", "int"), ("same", "v", "string"), ("wider", "id", "int"), ("wider", "v", "string"),
                ("retyped", "id", "bigint"), ("retyped", "v", "string")]
        sess = types.SimpleNamespace(sql=lambda q: types.SimpleNamespace(collect=lambda: rows))
        tables = {n: silver.Table(src(n)) for n in ("same", "retyped", "new")}
        wide = from_gateway("gw", LOG, dict(ENTRY, columns=ENTRY["columns"] + [("w", "INT", "C3")]), "db.s.wider")
        tables["wider"] = silver.Table(wide)
        self.assertEqual(sorted(silver.schema_changed(sess, tables, "`c`.`s`")), ["retyped", "wider"])   # new: not built


class FlowThrough(unittest.TestCase):
    def test_a_busy_table_leaves_its_group_for_a_merge_of_its_own(self):
        import json
        from lfc_log_silver import flowthrough
        policy = routing.Policy(LOG, fast_events_per_s=1)
        sources = {n: src(n) for n in "abc"}
        shared = {"class": "small_slow", "snapshot": "log", "log": LOG, "shards": 1, "silver": "shared", "group": "0", "why": ""}
        prev = [(n, json.dumps(shared), 0, 1000.0) for n in "abc"]
        seen = []

        def sql(q):
            seen.append(q)
            rows = (prev if q.startswith("SELECT name, route") else
                    [("db.s.a", 5000)] if q.startswith("SELECT destination, count(*)") else
                    [] if q.startswith(("SELECT target", "SELECT snapshot", "SELECT max(", "DESCRIBE", "SELECT table_name"))
                    or "information_schema" in q else
                    [(0,)] if q.startswith("SELECT count(*)") else
                    [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})])
            return types.SimpleNamespace(collect=lambda: rows)
        got = flowthrough.step(types.SimpleNamespace(sql=sql), "`c`.`s`", sources, Position("gateway_cursor", "09"),
                               policy, now=1010.0)        # a: 5,000 events in 10 s; b, c: none
        self.assertEqual(got["routes"], {"merge_round": 1, "shared": 2})
        self.assertEqual([(m["table"], m["from"], m["to"]) for m in got["moves"]], [("a", "shared", "merge_round")])
        self.assertIn("of its group's changes", got["moves"][0]["why"])
        self.assertTrue(any(q.startswith("DELETE FROM `c`.`s`.`_shared_0` WHERE destination = 'db.s.a'") for q in seen))
        self.assertTrue(any(q.startswith("DROP VIEW IF EXISTS `c`.`s`.`a`") for q in seen))
        self.assertTrue(any(q.startswith("CREATE OR REPLACE TABLE `c`.`s`.`a`") for q in seen))   # built from the log
        self.assertFalse(any(q.startswith("CREATE OR REPLACE TABLE `c`.`s`.`b`") for q in seen))  # b stays shared

    def test_observe_keeps_views_and_recreates_one_when_its_snapshot_is_ready(self):
        from unittest import mock

        from lfc_log_silver import flowthrough
        policy = routing.Policy(LOG, fast_events_per_s=1)
        sources = {n: src(n) for n in "ab"}
        state, seen = {}, []

        def sql(q):
            seen.append(q)
            rows = [("db.s.a", 50)] if q.startswith("SELECT destination, count(*)") else []
            return types.SimpleNamespace(collect=lambda: rows)
        sess = types.SimpleNamespace(sql=sql)
        with mock.patch.object(flowthrough, "load", lambda *_: dict(state)), \
                mock.patch.object(flowthrough, "save", lambda _s, _c, routes, got, at: state.update(
                    {n: (r, got.get(n, 0), at) for n, r in routes.items()})):
            got = flowthrough.observe(sess, "`c`.`s`", sources, policy, now=1000.0)
            self.assertEqual(got["views"], ["a", "b"])                                    # views over the log
            self.assertFalse(any(q.startswith("CREATE OR REPLACE TABLE") for q in seen))  # nothing stored
            seen.clear()
            got = flowthrough.observe(sess, "`c`.`s`", sources, policy, ready={"a": SNAP}, now=1010.0)
            self.assertEqual(got["views"], ["a"])                                         # a's snapshot is ready
            self.assertTrue(any(q.startswith("CREATE OR REPLACE VIEW `c`.`s`.`a`") and f"FROM {SNAP}" in q for q in seen))
            self.assertEqual((state["a"][0]["silver"], state["a"][0]["proposed"]["silver"]), ("view", "shared"))
            got = flowthrough.observe(sess, "`c`.`s`", sources, policy, ready={"a": SNAP}, now=1020.0)
            self.assertEqual(got["views"], [])                                            # nothing changed: no DDL
            state["a"] = ({**state["a"][0], "silver": "merge_round", "why": ""}, 50, 1020.0)   # step() pinned a
            got = flowthrough.observe(sess, "`c`.`s`", sources, policy, ready={"a": SNAP, "b": SNAP}, now=1030.0)
            self.assertEqual((got["views"], got["pinned"], state["a"][0]["silver"]), (["b"], 1, "merge_round"))


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
