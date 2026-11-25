"""Offline checks of lfc_fanin.fanin: names quoted part by part, what each connector type refuses, which errors
mean "not written yet", and that a connector type can be added.

    python3 -m unittest skills/lfc-fanin/tests/test_fanin.py
"""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_fanin import fanin  # noqa: E402

SPARK = types.SimpleNamespace(sql=lambda *a, **k: types.SimpleNamespace(collect=lambda: []))


class Names(unittest.TestCase):
    def test_quoted_part_by_part(self):
        self.assertEqual(fanin.quoted("catalog-zb.s.cdc_log"), "`catalog-zb`.`s`.`cdc_log`")
        self.assertEqual(fanin.quoted("`c`.`s`.`t`"), "`c`.`s`.`t`")

    def test_not_yet_only_a_missing_volume_folder(self):
        self.assertTrue(fanin.not_yet(Exception("[PATH_NOT_FOUND] Path does not exist: dbfs:/Volumes/c/s/v/orders")))
        self.assertFalse(fanin.not_yet(Exception("[TABLE_OR_VIEW_NOT_FOUND] `c`.`s`.`cdc_log` cannot be found")))


class Refuses(unittest.TestCase):
    def test_dbz_needs_log_volume_and_source_type(self):
        for d, why in (({}, "change_log"), ({"change_log": "c.s.l", "snapshot_volume": "/Volumes/c/s/v"}, "source_type"),
                       ({"change_log": "c.s.l", "snapshot_volume": "/tmp/x", "source_type": "SQLSERVER"}, "snapshot_volume")):
            with self.assertRaises(ValueError) as e:
                fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "fanin_dbz_zbus", **d})
            self.assertIn(why, str(e.exception))

    def test_crdb_tables_with_keys_and_their_own_folders(self):
        base = {"connector_type": "fanin_crdb_cdf", "changefeed_path": "/Volumes/c/s/v"}
        t = {"source_catalog": "shop", "source_schema": "public", "source_table": "orders"}
        for objs, why in (([{"schema": {"source_catalog": "shop", "source_schema": "public"}}], "no schema objects"),
                          ([{"table": t}], "primary_keys"),
                          ([{"table": {**t, "primary_keys": ["id"]}},
                            {"table": {**t, "source_schema": "x", "primary_keys": ["id"]}}], "share a changefeed folder")):
            with self.assertRaises(ValueError) as e:
                fanin.create(None, SPARK, "f", "c", "s", {**base, "objects": objs})
            self.assertIn(why, str(e.exception))

    def test_unknown_type_and_an_added_one(self):
        with self.assertRaises(ValueError):
            fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "nope"})
        seen = []
        fanin.CONNECTORS["probe"] = (lambda w, s, p, d: p["parts"].update(x=1), lambda w, s, p: seen.append(p) or {"x": 1})
        try:
            p = fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "probe"})
            self.assertEqual((p["parts"], fanin.land(None, SPARK, p)), ({"x": 1}, {"x": 1}))
        finally:
            del fanin.CONNECTORS["probe"]


class Page(unittest.TestCase):
    def test_a_page_only_for_a_test_that_passed(self):
        import json
        import tempfile
        sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
        import fanin_page
        with tempfile.TemporaryDirectory() as d:
            w = lambda n, v: open(os.path.join(d, n), "w").write(v if isinstance(v, str) else json.dumps(v))  # noqa: E731
            w("pg-skill.json", {"skill_exit": 0, "skill_pass": 23, "skill_fail": 0})
            r = lambda n, eq: {"round": n, "fanin_job": "TERMINATED SUCCESS", "job_s": 300, "compare": {"tables": 2, "equal": eq}}  # noqa: E731
            w("pg-rounds.jsonl", json.dumps(r(1, 2)) + "\n" + json.dumps(r(2, 2)) + "\n")
            w("pg-times.json", {"skill": [{"total_s": 270, "running_s": 145}, {"total_s": 190, "running_s": 120}],
                                "fanin": [{"total_s": 260, "running_s": 140}, {"total_s": 185, "running_s": 118}]})
            w("pg-skill.md", "- Date (UTC): 2026-11-25\n- Source: PostgreSQL PostgreSQL 16.15 (Debian) on x86, image docker.io/library/postgres:16 (sha256:ab)\n"
                              "- Debezium: quay.io/debezium/server:3.7.0.Final (sha256:cd), PostgreSQL connector\n")
            folder, text = fanin_page.page(d, "pg")
            self.assertEqual(folder, "postgresql")
            self.assertIn("2026-11-25: PostgreSQL 16.15 (docker.io/library/postgres:16)", text)
            self.assertIn("all 2 tables (1 tables and their `_history`) equal to the skill's, every round", text)
            self.assertIn("| 2 | 190 (120) | 185 (118) | 300 |", text)
            self.assertIn("engine --source pg --source-type POSTGRESQL", text)
            w("pg-rounds.jsonl", json.dumps(r(1, 2)) + "\n" + json.dumps(r(2, 1)) + "\n")   # round 2 differs
            with self.assertRaises(SystemExit):
                fanin_page.page(d, "pg")


class Harness(unittest.TestCase):
    def test_no_terminal_input_lists_not_splitting_names_and_a_passed_round(self):
        import tempfile
        import time
        sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
        import fanin_test
        t0 = time.time()
        self.assertEqual(fanin_test.run(["cat"]), 0)                        # no input: returns at once, not waiting
        self.assertLess(time.time() - t0, 5)
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "log")
            fanin_test.run([sys.executable, "-c", "import sys; print(sys.argv[1])", "a.b.t1,a.b.t3"], log)
            self.assertEqual(open(log).read().strip(), "a.b.t1,a.b.t3")    # one argument, commas and all
        os.environ.pop("FANIN_SCHEMA", None)
        self.assertEqual(fanin_test.names("pg", "robertlee_pg_tpch_dbz_1795627011"), ("fanin-pg-1795627011", "fanin_pg_1795627011"))
        os.environ["CATALOG"] = "c"
        env = fanin_test.bundle_env("n", "run_s", "fan_s", "POSTGRESQL")
        self.assertEqual((env["BUNDLE_VAR_change_log"], env["BUNDLE_VAR_snapshot_volume"]), ("c.run_s.cdc_log", "/Volumes/c/run_s/snapshot"))
        ok = {"fanin_job": "TERMINATED SUCCESS", "compare": {"tables": 16, "equal": 16}}
        self.assertTrue(fanin_test.round_passed(ok))
        self.assertFalse(fanin_test.round_passed({**ok, "compare": {"tables": 16, "equal": 15}}))
        self.assertFalse(fanin_test.round_passed({**ok, "fanin_job": "INTERNAL_ERROR FAILED"}))
        self.assertFalse(fanin_test.round_passed({**ok, "compare": {"tables": 0, "equal": 0}}))


if __name__ == "__main__":
    unittest.main()
