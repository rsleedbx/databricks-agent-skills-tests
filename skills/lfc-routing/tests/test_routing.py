"""lfc-routing: each table's class, log, shards, snapshot method and silver cell, offline."""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "scripts"), os.path.join(HERE, "..", "..", "lfc-change-source", "scripts")]

class Routing(unittest.TestCase):
    def test_four_classes_by_size_and_rate(self):
        from lfc_routing import routing
        p = routing.Policy(shared_log="`m`.`c`.`cdc_log`", fast_events_per_s=1, own_log_events_per_s=10_000)
        got = routing.plan({"db.s.region": (4096, 0.01), "db.s.carts": (64 << 20, 50.0),
                            "db.s.ledger": (5 << 40, 0.2), "db.s.orders": (5 << 40, 250_000),
                            "db.s.queue": (100, 20_000)}, p)
        self.assertEqual({t: (r.cls, r.log, r.shards, r.silver) for t, r in got.items()}, {
            "db.s.region": ("small_slow", "`m`.`c`.`cdc_log`", 1, "shared"),
            "db.s.carts": ("small_fast", "`m`.`c`.`cdc_log`", 1, "merge_round"),   # most of its group's changes
            "db.s.ledger": ("big_slow", "`m`.`c`.`cdc_log`", 1, "merge_changed"),
            "db.s.orders": ("big_fast", "`m`.`c`.`cdc_log_orders`", 3, "auto_cdc"),   # 250,000 / 100,000
            "db.s.queue": ("small_fast", "`m`.`c`.`cdc_log_queue`", 1, "merge_round")})   # busy: own log

    def test_suggest_from_statistics_says_why(self):
        from lfc_routing import routing
        got = routing.suggest({"region": {"bytes": 2048, "events_per_s": 0.0},
                               "lineitem": {"bytes": 80 << 30, "events_per_s": 0.5},
                               "ticks": {"bytes": None, "events_per_s": 150_000.0},
                               "orders": {"bytes": None, "events_per_s": None}}, routing.Policy("shared"))
        self.assertEqual({t: (r["class"], r["log"], r["shards"], r["silver"]) for t, r in got.items()}, {
            "region": ("small_slow", "shared", 1, "shared"),
            "lineitem": ("big_slow", "shared", 1, "merge_changed"),
            "ticks": ("auto_fast", "own", 2, "merge_round"),           # its size decides: small_fast or big_fast
            "orders": ("auto_slow", "shared", 1, "shared")})
        self.assertIn("80.0 GiB snapshot", got["lineitem"]["why"])
        self.assertIn("change rate unknown", got["orders"]["why"])

    def test_small_tables_in_groups_and_the_rules_out(self):
        from lfc_routing import routing
        p = routing.Policy("`m`.`c`.`cdc_log`", shared_group_size=2, own_table={"db.s.d": "streaming reads"},
                           shared_max_reads_per_day=100)
        tables = {f"db.s.{t}": (4096, 0.0) for t in "abcdef"}
        tables["db.s.e"] = (4096, 5.0)                                      # all of its group's changes
        tables.update({f"db.s.q{i}": (4096, 1.0) for i in range(20)})      # 5% each
        got = routing.plan(tables, p, reads_per_day={"db.s.f": 500})
        self.assertEqual({t: (r.silver, r.group) for t, r in got.items() if t in ("db.s.a", "db.s.b", "db.s.c")},
                         {"db.s.a": ("shared", "0"), "db.s.b": ("shared", "0"), "db.s.c": ("shared", "1")})
        self.assertEqual((got["db.s.d"].silver, got["db.s.d"].why), ("view", "needs its own table: streaming reads"))
        self.assertEqual(got["db.s.e"].silver, "merge_round")                # over shared_max_share
        self.assertIn("20% of its group's changes", got["db.s.e"].why)
        self.assertEqual(got["db.s.f"].silver, "view")                       # read often
        self.assertEqual(got["db.s.q3"].silver, "shared")                    # fast, but 4% of the changes
        self.assertEqual(routing.log_for("db.s.x", 4096, 20_000, p).silver, "merge_round")   # its own log: no group
        tr = routing.transitions({"silver": "shared"}, {"silver": "merge_round"})
        self.assertIn("build its own table from the log once", tr[0][1])

    def test_cutover_reads_each_log_for_its_positions(self):   # shared up to P, its own after, no new snapshot
        from lfc_change_source import land
        from lfc_routing import routing
        from lfc_change_source.cdc_log import CdcLog
        fq = routing.history_fq([("`m`.`c`.`cdc_log`", None), ("`m`.`c`.`cdc_log_t`", "00000000000000000042")],
                                "gateway_cursor")
        self.assertIn("FROM `m`.`c`.`cdc_log` WHERE operation = 'resolved' OR "
                      "(value:cursor.sequence::STRING <= '00000000000000000042')", fq)
        self.assertIn("FROM `m`.`c`.`cdc_log_t` WHERE operation = 'resolved' OR "
                      "(value:cursor.sequence::STRING > '00000000000000000042')", fq)
        self.assertEqual(routing.history_fq([("`m`.`c`.`cdc_log`", None)], "lsn"), "`m`.`c`.`cdc_log`")
        q = CdcLog("s", fq, "t", "db.s.t", ["id"], {"id": "INT"}, seq="gateway_cursor").changes_sql(None, None)
        self.assertIn(f"FROM {fq} WHERE destination = 'db.s.t'", q)
        seen = []

        def sql(q):   # one change file, listed (LIST), never read before
            seen.append(q)
            rows = ([("/V/g/cdc/1/", "1/", 0, 0)] if q == "LIST '/V/g/cdc/'" else
                    [("/V/g/cdc/1/0", "0", 9, 0)] if q == "LIST '/V/g/cdc/1/'" else [])
            return types.SimpleNamespace(collect=lambda: rows)
        s = types.SimpleNamespace(sql=sql)
        land.gateway(s, "/V/g", "`m`.`c`.`cdc_log_t`", "gw", only=["db.s.t"])
        land.gateway(s, "/V/g", "`m`.`c`.`cdc_log`", "gw", only=["db.s.u"])
        ins = [q for q in seen if q.startswith("INSERT INTO `m`.`c`.`cdc_log") and "read_files(" in q]
        self.assertIn("event:table_id.name::STRING)) IN ('db.s.t')", ins[0])   # the moved table: its own log
        self.assertIn("event:table_id.name::STRING)) IN ('db.s.u')", ins[1])   # every other table: listed

    def test_transitions_name_what_a_route_change_takes(self):
        from lfc_routing import routing
        same = {"class": "small_slow", "log": "shared", "shards": 1, "silver": "view"}
        self.assertEqual(routing.transitions(same, same), [])
        hot = {"class": "big_fast", "log": "own", "shards": 3, "silver": "auto_cdc"}
        self.assertEqual([a for a, _ in routing.transitions(same, hot)], ["cutover", "silver", "reshard"])
        busy = dict(same, **{"class": "small_fast", "silver": "merge_round"})
        self.assertIn("build the table from the log once", dict(routing.transitions(same, busy))["silver"])
        self.assertIn("no new snapshot", dict(routing.transitions(same, hot))["cutover"])

    def test_chosen_by_name_and_rates_from_counts(self):
        from lfc_routing import routing
        p = routing.Policy(shared_log="`m`.`c`.`cdc_log`", chosen={"db.s.region": "big_fast"},
                           silver_chosen={"db.s.nation": "merge_round"})
        self.assertEqual(routing.log_for("db.s.region", 10, 0, p).cls, "big_fast")
        self.assertEqual(routing.log_for("db.s.nation", 10, 0, p).silver, "merge_round")   # a view by class, chosen
        with self.assertRaises(ValueError):
            routing.classify(1, 1, routing.Policy("x", chosen={"t": "huge"}), "t")
        self.assertEqual(routing.rates({"a": 100}, {"a": 700, "b": 30}, 60), {"a": 10.0, "b": 0.5})



if __name__ == "__main__":
    unittest.main()
