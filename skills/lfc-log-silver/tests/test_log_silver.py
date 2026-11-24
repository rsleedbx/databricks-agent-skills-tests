"""Offline checks of lfc_log_silver.silver: the build and MERGE SQL from a change source, and positions kept per
table (fake session).

    python3 -m unittest skills/lfc-log-silver/tests/test_log_silver.py
"""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.gateway_log import from_gateway  # noqa: E402
from lfc_log_silver import silver as log_silver  # noqa: E402

ENTRY = {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]}


class LogSilver(unittest.TestCase):
    def src(self):
        return from_gateway("gw", "`c`.`x`.`cdc_log`", ENTRY, "db.s.t")

    def test_build_and_merge_sql(self):
        b = log_silver.build_sql(self.src(), "`c`.`silver`.`t`")
        self.assertTrue(b.startswith("CREATE OR REPLACE TABLE `c`.`silver`.`t` AS SELECT `id`, `v` FROM ("))
        m = log_silver.merge_sql(self.src(), "`c`.`silver`.`t`", Position("gateway_cursor", "05"),
                                 Position("gateway_cursor", "09"))
        self.assertIn("ON t.`id` <=> c.`id`", m)
        self.assertLess(m.index("THEN DELETE"), m.index("THEN UPDATE SET t.`v` = c.`v`"))
        self.assertIn("WHEN NOT MATCHED AND c._op <> 'delete' THEN INSERT (`id`, `v`) VALUES (c.`id`, c.`v`)", m)
        self.assertIn("value:cursor.sequence::STRING > '05'", m)
        self.assertIn("value:cursor.sequence::STRING <= '09'", m)

    def test_apply_reads_each_tables_kept_position(self):
        seen = []

        def sql(q):
            seen.append(q)
            rows = [("db_t", "gateway_cursor", "03")] if q.startswith("SELECT target") else \
                [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 2})]
            return types.SimpleNamespace(collect=lambda: rows)
        done = log_silver.apply(types.SimpleNamespace(sql=sql), {"db_t": self.src()}, "`c`.`silver`",
                                Position("gateway_cursor", "09"), threads=1)
        self.assertEqual(done["db_t"][0], 2)
        merge = next(q for q in seen if q.startswith("MERGE INTO `c`.`silver`.`db_t`"))
        self.assertIn("> '03'", merge)                                   # from its own kept position
        self.assertIn("MERGE INTO `c`.`silver`._log_silver", seen[-1])   # then the new position, one statement


    def test_staged_reads_the_log_once_then_the_batch(self):
        seen = []

        def sql(q):
            seen.append(q)
            rows = [("a", "gateway_cursor", "03"), ("b", "gateway_cursor", "05")] if q.startswith("SELECT target") \
                else [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})]
            return types.SimpleNamespace(collect=lambda: rows)
        other = from_gateway("gw", "`c`.`x`.`cdc_log`", dict(ENTRY, id="T2"), "db.s.u")
        done = log_silver.apply_staged(types.SimpleNamespace(sql=sql), {"a": self.src(), "b": other}, "`c`.`silver`",
                                       Position("gateway_cursor", "09"), threads=1)
        stage = next(q for q in seen if q.startswith("CREATE OR REPLACE TABLE `c`.`silver`._log_silver_batch"))
        self.assertIn("CLUSTER BY (destination)", stage)
        self.assertIn("destination IN ('db.s.t', 'db.s.u')", stage)
        self.assertIn("> '03'", stage)                                   # from the oldest kept position
        merges = [q for q in seen if q.startswith("MERGE INTO `c`.`silver`.`a`") or q.startswith("MERGE INTO `c`.`silver`.`b`")]
        self.assertTrue(all("FROM `c`.`silver`._log_silver_batch" in q for q in merges))   # not the log
        self.assertIn("> '05'", next(q for q in merges if "`b`" in q[:30]))   # each from its own
        self.assertIn("_stage", done)

    def test_run_by_method(self):   # views once, merge_round always, merge_changed only when changed, auto_cdc left out
        from lfc_change_source import routing
        seen = []

        def sql(q):
            seen.append(q)
            if q.startswith("SELECT target"):
                rows = [(n, "gateway_cursor", "05") for n in ("a", "b", "c")]
            elif q.startswith("SELECT destination, max("):
                rows = [("db.s.b", "07")]                     # only b changed after its kept 05
            else:
                rows = [types.SimpleNamespace(asDict=lambda: {"num_affected_rows": 1})]
            return types.SimpleNamespace(collect=lambda: rows)
        src = {n: from_gateway("gw", "`c`.`x`.`cdc_log`", dict(ENTRY, id=n), f"db.s.{n}") for n in "abcde"}
        p = routing.Policy("`c`.`x`.`cdc_log`", silver_chosen={"a": "merge_round", "b": "merge_changed",
                                                                  "c": "merge_changed", "d": "view", "e": "auto_cdc"})
        routes = {n: routing.log_for(n, 10, 0, p) for n in src}
        got = log_silver.run(types.SimpleNamespace(sql=sql), src, routes, "`c`.`silver`", Position("gateway_cursor", "09"),
                             threads=1)
        self.assertEqual({m: (v["tables"], v["merged"]) for m, v in got.items()},
                         {"view": (1, 0), "merge_round": (1, 1), "merge_changed": (2, 1), "auto_cdc": (1, 0)})
        merged = sorted(q.split("`c`.`silver`.`")[1].split("`")[0] for q in seen if q.startswith("MERGE INTO `c`.`silver`.`"))
        self.assertEqual(merged, ["a", "b"])
        self.assertFalse(any(q.startswith("CREATE OR REPLACE VIEW") for q in seen))   # views only on the first round
        tables = log_silver.auto_cdc_tables({n: src[n] for n in "abc"}, per_pipeline=2)
        self.assertEqual([len(x) for x in tables], [2, 1])
        self.assertEqual(tables[0][0]["seq"], "value:cursor.sequence::STRING")
        self.assertIn("value:data.`C1`::INT AS `id`", tables[0][0]["exprs"])

    def test_build_keeps_a_direct_table_at_its_snapshot(self):
        seen = []
        s = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(collect=lambda: []))
        log_silver.build(s, {"a": self.src()}, "`c`.`silver`", Position("gateway_cursor", "09"), threads=1,
                         at={"a": Position("gateway_cursor", "04")})
        self.assertIn("('a', 'gateway_cursor', '04', 0,", seen[-1])   # its snapshot's position, not the run's

if __name__ == "__main__":
    unittest.main()
