"""lfc_change_source.land's filtered landing offline (a gateway's split): files listed with LIST, only the ones in
neither the ledger nor the log read, every one read recorded (with rows or none), a file being written left for the
next round, so a file is read once a log however many rounds run.

    python3 -m unittest skills/lfc-change-source/tests/test_land_filtered.py
"""
import os
import sys
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_change_source import land  # noqa: E402

ROOT = "/Volumes/c/s/stg/gw"
LOG = "`c`.`s`.`cdc_log_d`"


class R:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class Session:
    def __init__(self, files):
        self.files, self.ledger, self.logged, self.seen = files, [], [], []   # files: {path: modified ms}

    def sql(self, q):
        self.seen.append(q)
        if q.startswith(f"LIST '{ROOT}/cdc/'"):
            return R([(f"{ROOT}/cdc/100/", "100/", 0, 0)])
        if q.startswith(f"LIST '{ROOT}/cdc/100/'"):
            return R([(p, p.rsplit("/", 1)[1], 9, m) for p, m in sorted(self.files.items())])
        if "UNION SELECT DISTINCT file" in q:
            return R([(f,) for f in self.ledger] + [("dbfs:" + f,) for f in self.logged])
        if q.startswith(f"INSERT INTO {land.ledger(LOG)}"):
            self.ledger += [x.split("'")[3] for x in q.split("), (")]
        if q.startswith(f"INSERT INTO {LOG}"):
            return R([{"num_affected_rows": 3, "num_inserted_rows": 3}])
        return R([])

    def reads(self):
        return [f for q in self.seen if q.startswith(f"INSERT INTO {LOG}")
                for f in [x.split("'")[1] for x in q.split("read_files(")[1:]]]


class Filtered(unittest.TestCase):
    def test_a_file_is_read_once_a_log(self):
        old = int(time.time() * 1000) - 60_000
        s = Session({f"{ROOT}/cdc/100/0": old, f"{ROOT}/cdc/100/1": old})
        land.gateway(s, ROOT, LOG, "gw", only=["db.s.d"])
        self.assertEqual(s.reads(), [f"{ROOT}/cdc/100/0", f"{ROOT}/cdc/100/1"])
        self.assertEqual(sorted(s.ledger), [f"{ROOT}/cdc/100/0", f"{ROOT}/cdc/100/1"])   # with rows or none
        s.seen = []
        land.gateway(s, ROOT, LOG, "gw", only=["db.s.d"])                                  # a second round
        self.assertEqual(s.reads(), [])

    def test_a_file_being_written_waits_and_one_the_log_has_is_not_read(self):
        now = int(time.time() * 1000)
        s = Session({f"{ROOT}/cdc/100/0": now - 60_000, f"{ROOT}/cdc/100/1": now, f"{ROOT}/cdc/100/2": now - 60_000})
        s.logged = [f"{ROOT}/cdc/100/2"]   # inserted from before a stop, not in the ledger yet
        land.gateway(s, ROOT, LOG, "gw", only=["db.s.d"])
        self.assertEqual(s.reads(), [f"{ROOT}/cdc/100/0"])
        self.assertIn("lower(", [q for q in s.seen if q.startswith(f"INSERT INTO {LOG}")][0])

    def test_the_ledger_beside_the_log(self):
        self.assertEqual(land.ledger("`c`.`s`.cdc_log"), "`c`.`s`.`cdc_log__scanned`")
        self.assertEqual(land.ledger(LOG), "`c`.`s`.`cdc_log_d__scanned`")


if __name__ == "__main__":
    unittest.main()
