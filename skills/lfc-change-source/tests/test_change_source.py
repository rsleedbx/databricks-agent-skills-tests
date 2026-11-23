"""Offline checks of lfc_change_source: the contract and the Lakeflow Connect bronze adapter.

    python3 -m unittest skills/lfc-change-source/tests/test_change_source.py
"""
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_change_source.contract import Position, Stats, last_change_sql  # noqa: E402
from lfc_change_source.lfc_bronze import Bronze  # noqa: E402

V = lambda n: Position("delta_version", str(n))  # noqa: E731


class Contract(unittest.TestCase):
    def test_position_text_round_trip(self):
        p = Position.parse("hlc:1700000000000000000.0000000001")
        self.assertEqual((p.kind, str(p)), ("hlc", "hlc:1700000000000000000.0000000001"))

    def test_last_change_per_key_by_seq(self):
        q = last_change_sql("SELECT 1", ["a", "b"])
        self.assertIn("PARTITION BY `a`, `b` ORDER BY _seq DESC", q)
        self.assertIn("WHERE `__lfc_n` = 1", q)


class LfcBronze(unittest.TestCase):
    b = Bronze("lob_001", "`c`.`s`.`account`", "account", ["a_id"], ["a_id", "a_balance"])

    def test_snapshot_pinned_and_changes_with_op_and_seq(self):
        self.assertEqual(self.b.snapshot_sql(V(8)), "SELECT `a_id`, `a_balance`, 'upsert' AS _op FROM `c`.`s`.`account` VERSION AS OF 8")
        q = self.b.changes_sql(V(3), V(8))
        self.assertIn("FROM table_changes('c.s.account', 4, 8) WHERE _change_type != 'update_preimage'", q)
        self.assertIn("CASE _change_type WHEN 'delete' THEN 'delete' ELSE 'upsert' END AS _op", q)
        self.assertIn("named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END) AS _seq", q)
        with self.assertRaises(ValueError):
            self.b.changes_sql(Position("lsn", "0/16B3748"), V(8))

    def test_commit_time_positions(self):   # read from a saved time, to the latest commit: no version lookup
        from lfc_change_source.lfc_bronze import TS, nothing_newer
        at = Position(TS, "2026-11-23T20:52:43.005Z")
        self.assertIn("TIMESTAMP AS OF TIMESTAMP'2026-11-23T20:52:43.005Z'", self.b.snapshot_sql(at))
        q = self.b.changes_sql(at, None)
        self.assertIn("table_changes('c.s.account', TIMESTAMP'2026-11-23T20:52:43.005Z' + INTERVAL 1 MICROSECOND)", q)
        self.assertIn("_commit_timestamp", q)   # what a consumer saves after reading by time
        with self.assertRaises(ValueError):
            self.b.changes_sql(at, V(9))   # one kind a range
        self.assertTrue(nothing_newer(RuntimeError("[DELTA_TIMESTAMP_GREATER_THAN_COMMIT] Timestamp ... is after")))
        self.assertFalse(nothing_newer(RuntimeError("[DELTA_CHANGE_DATA_FILE_NOT_FOUND]")))

    def test_watermark_and_stats_from_history(self):
        seen = []
        def sql(q):
            seen.append(q)
            row = (19,) if "max(version)" in q else (78, 1024, 2)
            return types.SimpleNamespace(collect=lambda: [row])
        s = types.SimpleNamespace(sql=sql)
        self.assertEqual(self.b.watermark(s), V(19))
        self.assertEqual(self.b.stats(s, V(16)), Stats(78, 1024, 2))
        self.assertIn("FROM (DESCRIBE HISTORY `c`.`s`.`account`) WHERE version > 16", seen[-1])


class Conformance(unittest.TestCase):
    def test_scenario_truth(self):
        from lfc_change_source import conformance
        self.assertEqual(conformance.expected(), {1: "a2", 2: None, 3: "c2", 4: "d", 5: None})
        self.assertLess(conformance._order({"v": "9", "o": "1"}), conformance._order({"v": "10", "o": "0"}))

    def test_every_adapter_has_the_contract_and_build(self):
        import inspect
        from lfc_change_source import ADAPTERS
        for name, mod in ADAPTERS.items():
            self.assertTrue(callable(getattr(mod, "build", None)), name)
            classes = [c for _, c in inspect.getmembers(mod, inspect.isclass) if c.__module__ == mod.__name__]
            self.assertTrue(any(all(hasattr(c, m) for m in ("snapshot_sql", "changes_sql", "watermark", "stats"))
                                for c in classes), name)


class Engine(unittest.TestCase):
    def test_phases_from_either_sdk_shape(self):   # an older SDK (a job's environment) returns ListQueriesResponse
        from lfc_change_source.engine import Warehouse
        m = types.SimpleNamespace(compilation_time_ms=2000, execution_time_ms=5000,
                                  query_compilation_start_timestamp=3000, read_bytes=10, read_files_count=1)
        q = types.SimpleNamespace(query_id="s1", is_final=True, query_start_time_ms=1000, metrics=m)
        for ret in ([q], types.SimpleNamespace(res=[q])):
            w = types.SimpleNamespace(query_history=types.SimpleNamespace(list=lambda ret=ret, **k: ret))
            self.assertEqual(Warehouse("x", w).phases(["s1"], wait_s=1)["s1"]["queue_s"], 2.0)


class Samples(unittest.TestCase):
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")

    def test_gateway_sample_columns_map_through_its_schema(self):
        import json
        g = os.path.join(self.here, "gateway")
        schema = json.load(open(os.path.join(g, "target_schema.0.json")))
        cols = {c["uoid"]: c["name"] for c in schema["schemas"][0]["tables"][0]["columns"]}
        lines = [json.loads(x) for x in open(os.path.join(g, "cdc_history_update.jsonl")) if x.strip()]
        self.assertEqual([x["operation"] for x in lines], ["DELETE", "INSERT"])   # an update of a keyless table
        self.assertEqual(len({x["cursor"]["sequence"] for x in lines}), 1)        # at one position
        for x in lines:
            self.assertEqual(set(x["data"]) - set(cols), set())                 # every column id has a name


if __name__ == "__main__":
    unittest.main()
