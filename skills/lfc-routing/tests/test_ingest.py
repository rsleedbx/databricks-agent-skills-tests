"""lfc_routing.ingest offline: an ingestion_start row carried out through a landing producer (a gateway) on a fake
session: the latest plan's logs (a table with a log of its own), each log landing only its tables, the producer's
snapshots recorded, keep or take against each log's first position, a table to take asked again, a run recorded once.

    python3 -m unittest skills/lfc-routing/tests/test_ingest.py
"""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-change-source", "lfc-medallion"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_routing import ingest  # noqa: E402

SHARED, OWN = "`c`.`s`.cdc_log", "`c`.`s`.`cdc_log_d`"


class R:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class Session:
    def __init__(self):
        self.seen, self.runs, self.snaps = [], [], []

    def sql(self, q):
        self.seen.append(q)
        if "FROM `c`.`s`.ingestion_start" in q:
            return R([{"n": 1, "source": "src"}])
        if "SELECT run FROM `c`.`s`.ingestion_runs" in q:
            return R(self.runs)
        if "FROM `c`.`s`.routing_plan" in q:
            return R([{"round": 3, "tbl": "a", "log": "shared", "cell": "shared", "snapshot_by": "transport"},
                      {"round": 3, "tbl": "b", "log": "shared", "cell": "merge_round", "snapshot_by": "transport"},
                      {"round": 3, "tbl": "d", "log": OWN, "cell": "shared", "snapshot_by": "transport"}])
        if q.startswith("INSERT INTO `c`.`s`.snapshot_runs"):
            self.snaps.append(q)
        if "FROM `c`.`s`.snapshot_runs" in q:
            return R([{"tbl": "a", "status": "ok", "position": ""}, {"tbl": "d", "status": "ok", "position": ""}])
        if "min(value:cursor.sequence" in q:   # each log's first position for its tables
            return R([{"destination": "db.s.a", "p": "00000000000000000001"}] if f"FROM {SHARED} " in q else
                     [{"destination": "db.s.d", "p": "00000000000000000001"}])
        if q.startswith("INSERT INTO `c`.`s`.ingestion_runs"):
            self.runs.append({"run": 1})
        return R([])


class Gateway:
    def __init__(self):
        self.landed, self.asked = [], []

    def land(self, log, only):
        self.landed.append((log, sorted(only)))
        return len(only)

    def copy_snapshots(self, into):
        return [{"tbl": "a", "table": f"{into}.`snap_a`", "rows": 10, "position": "", "kind": "gateway_cursor"}]

    def resnapshot(self, t):
        self.asked.append(t)


class Ingest(unittest.TestCase):
    def test_a_plan_carried_out_through_a_gateway(self):
        s, gw = Session(), Gateway()
        out = ingest.run_ingestion(s, "`c`.`s`", "src", gw, shared_log=SHARED, prefix="db.s.", snapshot_schema="`c`.`snap`")
        self.assertEqual([(r["status"], r["plan_round"]) for r in out], [("ok", 3)])
        self.assertEqual({(log, tuple(t)) for log, t in gw.landed}, {(SHARED, ("db.s.a", "db.s.b")), (OWN, ("db.s.d",))})
        self.assertEqual(len(s.snaps), 1)                                  # the gateway's snapshot recorded
        v = out[0]["snapshots"]
        self.assertTrue(v["a"]["keep"] and v["d"]["keep"])                 # before every change in their logs
        self.assertFalse(v["b"]["keep"])                                   # none taken yet
        self.assertEqual(gw.asked, ["b"])
        self.assertEqual(json.loads(json.dumps(out[0]["logs"]))[OWN]["tables"], ["d"])
        self.assertEqual(ingest.run_ingestion(s, "`c`.`s`", "src", gw, shared_log=SHARED, prefix="db.s."), [])   # once


if __name__ == "__main__":
    unittest.main()
