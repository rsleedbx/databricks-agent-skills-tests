"""The cell rules of skills/lfc-test-results/scripts/coverage_page.py, on hand-made records.

    python3 -m unittest bin/test_coverage_page.py
"""
import importlib.util
import unittest
from pathlib import Path

_p = Path(__file__).resolve().parents[1] / "skills/lfc-test-results/scripts/coverage_page.py"
_spec = importlib.util.spec_from_file_location("coverage_page", _p)
cp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cp)


def rec(date, outcome, engine="mysql", test="scdc", cloud="aws", order=0, **kw):
    return dict(date=date, outcome=outcome, test=test, subject={"engine": engine}, workspace={"cloud": cloud},
                _order=("f", order), **kw)


class StatusTest(unittest.TestCase):
    def test_pass_holds_over_newer_blocked_and_stop(self):
        self.assertEqual(cp.status([rec("2026-10-16", "pass"), rec("2026-10-17", "blocked"),
                                    rec("2026-10-18", "expected-stop")]), "pass")

    def test_newer_failure_overrides_pass(self):
        self.assertEqual(cp.status([rec("2026-10-16", "pass"), rec("2026-10-17", "fail")]), "fail")

    def test_same_day_rerun_pass_after_fail(self):   # a later line on the same date wins
        self.assertEqual(cp.status([rec("2026-10-17", "fail", order=1), rec("2026-10-17", "pass", order=2)]), "pass")

    def test_only_stops_is_not_offered(self):
        self.assertEqual(cp.status([rec("2026-10-15", "expected-stop")]), "not-offered")

    def test_newest_non_stop_outcome_otherwise(self):
        self.assertEqual(cp.status([rec("2026-10-16", "blocked"), rec("2026-10-25", "partial"),
                                    rec("2026-10-26", "expected-stop")]), "partial")

    def test_blocked_never_hides_a_failure(self):
        self.assertEqual(cp.status([rec("2026-10-16", "pass"), rec("2026-10-17", "fail"), rec("2026-10-18", "blocked")]), "fail")
        self.assertEqual(cp.status([rec("2026-10-17", "fail"), rec("2026-10-18", "blocked")]), "fail")
        self.assertEqual(cp.status([rec("2026-10-17", "blocked"), rec("2026-10-18", "expected-stop")]), "blocked")

    def test_no_records(self):
        self.assertIsNone(cp.status([]))


class CellTest(unittest.TestCase):
    def test_clouds_and_serverless_gateway(self):
        gw = {"gateway": {"ran_on": "serverless"}}
        recs = [rec("2026-10-22", "pass", engine="postgres", compute=gw), rec("2026-10-17", "blocked", engine="postgres", cloud="azure")]
        self.assertEqual(cp.cell(recs, "postgres", "scdc", "nope"), "✅ AWS (serverless gateway); ⛔ Azure")

    def test_by_hand_has_no_prompt_link(self):
        recs = [rec("2026-11-01", "pass", engine="lakebase", notes="run by hand, not by a skill prompt")]
        self.assertEqual(cp.cell(recs, "lakebase", "scdc", "lakebase"), "✅ AWS, by hand")

    def test_prompt_link_when_the_file_exists(self):
        recs = [rec("2026-10-17", "pass", engine="sqlserver", test="qbc")]
        self.assertEqual(cp.cell(recs, "sqlserver", "qbc", "sqlserver"), "[✅ AWS](../prompts/lfc-matrix/qbc-sqlserver.txt)")

    def test_connection_counts_a_query_based_pass(self):
        recs = [rec("2026-10-15", "pass", engine="lakebase", test="qbc", cloud="azure")]
        self.assertEqual(cp.cell(recs, "lakebase", "connection", "lakebase"), "✅ Azure")

    def test_dml_round_trip(self):
        recs = [rec("2026-10-15", "pass", engine="lakebase", test="qbc", cloud="azure"),
                rec("2026-10-15", "pass", engine="lakebase", test="dml", cloud="azure")]
        self.assertIn("with a DML round trip", cp.cell(recs, "lakebase", "qbc", "nope"))

    def test_no_run_and_docs_not_offered(self):
        self.assertEqual(cp.cell([], "mysql", "icdc", "nope"), "–")
        self.assertTrue(cp.cell([], "oracle", "scdc", "nope").startswith("not offered ([docs]("))


if __name__ == "__main__":
    unittest.main()
