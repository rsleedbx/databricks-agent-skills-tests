"""lfc_routing.provisional offline: a cdc_request started from the latest introspection's tables, a second one
with a table added restarts it, a source with no introspection is a failed run. A fake session and producer.

    python3 -m unittest skills/lfc-routing/tests/test_provisional.py
"""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-change-source", "lfc-introspection"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing import provisional  # noqa: E402

DOC = {"tables": [{"name": "orders", "columns": []}, {"name": "region", "columns": []}]}


class Session:
    def __init__(self, reqs, doc=DOC):
        self.reqs, self.runs, self.doc, self.seen = reqs, [], doc, []

    def sql(self, q):
        self.seen.append(q)
        rows = []
        if "FROM `c`.`s`.cdc_request" in q:
            rows = self.reqs
        elif "SELECT run, source FROM `c`.`s`.cdc_runs" in q:
            rows = self.runs
        elif "JOIN" in q and "introspection_request" in q:
            rows = [{"run": 4, "src_schema": "tpch1"}] if self.doc else []
        elif "SELECT schema_json" in q:
            rows = [{"schema_json": json.dumps(self.doc), "stats_json": "{}"}]
        elif q.startswith("INSERT INTO `c`.`s`.cdc_runs"):
            n = int(q.split("VALUES (")[1].split(",")[0])
            self.runs.append({"run": n, "source": "pg1"})

        class R:
            def collect(self):
                return rows
        return R()


class Fake:
    log = []

    def __init__(self, spec):
        self.spec = spec

    def start(self):
        Fake.log.append(("start", self.spec.tables, self.spec.log))

    def stop(self):
        Fake.log.append(("stop",))


class Provisional(unittest.TestCase):
    def setUp(self):
        Fake.log = []

    def test_start_then_restart_with_a_table_added(self):
        s = Session([{"n": 1, "source": "pg1", "log": "c.s.cdc_log", "producer": "dbz1"}])
        out = provisional.run_cdc_requests(s, "`c`.`s`", make=Fake, running=lambda src: None)
        self.assertEqual([(o["action"], o["status"], o["tables"], o["introspection_run"]) for o in out], [("start", "ok", 2, 4)])
        self.assertEqual(Fake.log, [("start", ("tpch1.orders", "tpch1.region"), "c.s.cdc_log")])
        from lfc_change_source.producer import Spec
        s.doc = {"tables": DOC["tables"] + [{"name": "nation", "columns": []}]}
        s.reqs.append({"n": 2, "source": "pg1", "log": "c.s.cdc_log", "producer": "dbz1"})
        running = Spec("dbz1", "c.s.cdc_log", ("tpch1.orders", "tpch1.region"))
        out = provisional.run_cdc_requests(s, "`c`.`s`", make=Fake, running=lambda src: running)
        self.assertEqual([(o["action"], o["added"]) for o in out], [("restart", ["tpch1.nation"])])
        self.assertEqual(Fake.log[-2:], [("stop",), ("start", ("tpch1.nation", "tpch1.orders", "tpch1.region"), "c.s.cdc_log")])
        self.assertEqual(provisional.run_cdc_requests(s, "`c`.`s`", make=Fake, running=lambda src: running), [])

    def test_no_introspection_is_a_failed_run(self):
        s = Session([{"n": 1, "source": "pg1", "log": "c.s.cdc_log", "producer": None}], doc=None)
        out = provisional.run_cdc_requests(s, "`c`.`s`", make=Fake, running=lambda src: None)
        self.assertEqual(out[0]["status"], "failed")
        self.assertIn("no introspection run yet", out[0]["error"])
        self.assertEqual(Fake.log, [])


if __name__ == "__main__":
    unittest.main()
