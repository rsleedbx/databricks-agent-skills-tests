"""lfc_log_silver.planned offline, the findings of the live gateway runs: cursor families (a change tracking version
and a ~v CDC cursor never order as text), a shared group a log and family, a gateway snapshot with no position,
upto's bound below each log's newest position (one CT version spans change files), run_families' rounds.

    python3 -m unittest skills/lfc-log-silver/tests/test_planned.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-log-silver", "lfc-change-source"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_log_silver import planned  # noqa: E402


def src(name, log="`c`.`s`.cdc_log", keys=("id",)):
    return CdcLog("gw", log, name, f"db.s.{name}", list(keys), {"id": "INT", "v": "STRING"}, seq="gateway_cursor")


class R:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class Planned(unittest.TestCase):
    def test_families(self):
        self.assertEqual(planned.family("00000000000000070778"), "ct")
        self.assertEqual(planned.family("~v00001-0000014900006EB00021-00000000000000000010"), "cdc")
        self.assertLess("00000000000000099999", "~v00001")   # why: as text every version sorts below every ~v

    def test_tables_a_group_a_log_and_family_and_a_gateway_snapshot(self):
        rows = [{"tbl": "a", "cell": "shared", "log": "`c`.`s`.cdc_log"}, {"tbl": "b", "cell": "shared", "log": "`c`.`s`.cdc_log"},
                {"tbl": "d", "cell": "shared", "log": "`c`.`s`.`cdc_log_d`"}, {"tbl": "m", "cell": "merge_round", "log": "`c`.`s`.cdc_log"}]
        logs = {r["tbl"]: r["log"] for r in rows}
        snaps = {"a": {"snapshot_table": "`c`.`s`.`snap_a`", "position": ""}, "m": {"snapshot_table": "x", "position": "00000000000000000005"}}
        fams = {"db.s.a": "ct", "db.s.b": "cdc", "db.s.d": "ct"}
        t = planned.tables(rows, snaps, lambda n, log: src(n, logs[n]), fams=fams)
        self.assertEqual({n: x.group for n, x in t.items()}, {"a": "cdc_log_ct", "b": "cdc_log_cdc", "d": "cdc_log_d_ct", "m": ""})
        self.assertIsNone(t["a"].position)                       # a gateway snapshot: every change applies over it
        self.assertEqual(t["m"].position.value, "00000000000000000005")
        self.assertEqual(t["m"].method, "merge")

    def test_upto_is_below_each_logs_newest_and_the_smallest_of_the_logs(self):
        seen, tops = [], {"`c`.`s`.l1": "00000000000000000009", "`c`.`s`.l2": "00000000000000000007"}

        class S:
            def sql(self, q):
                seen.append(q)
                return R([(next(v for k, v in tops.items() if f"FROM {k} " in q),)])
        self.assertEqual(planned.upto(S(), list(tops), destinations=["db.s.a"]), "00000000000000000007")
        self.assertTrue(all("< (SELECT max(" in q and "destination IN ('db.s.a')" in q for q in seen))

    def test_upto_none_when_a_log_holds_none(self):
        class S:
            def sql(self, q):
                return R([(None,)])
        self.assertIsNone(planned.upto(S(), ["`c`.`s`.l1"]))

    def test_run_families_a_round_a_family_with_its_own_bound(self):
        calls = []
        orig_upto, orig_run = planned.upto, planned.run
        planned.upto = lambda session, logs, kind_sql=None, destinations=None: "B-" + ",".join(sorted(destinations))
        planned.run = lambda session, schema, ts, to, first=None: calls.append((sorted(ts), to.value)) or {}
        try:
            t = {"a": src("a"), "b": src("b")}
            from lfc_log_silver import silver
            tables_ = {n: silver.Table(s_, "merge") for n, s_ in t.items()}
            out = planned.run_families(None, "`c`.`s`", tables_, {"db.s.a": "ct", "db.s.b": "cdc"})
        finally:
            planned.upto, planned.run = orig_upto, orig_run
        self.assertEqual(sorted(calls), [(["a"], "B-db.s.a"), (["b"], "B-db.s.b")])
        self.assertEqual(sorted(out), ["cdc", "ct"])


if __name__ == "__main__":
    unittest.main()
