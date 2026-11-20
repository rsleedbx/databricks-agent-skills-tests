"""classify.py's guess never reads the role: it's scored against: TPC-E's plan is the same with the schema's role: lines
and without them; --truth schema scores against those lines and --review schema corrects the plan with them.

  uv run --no-project --with pyyaml python -m unittest skills/cdc-table-plan/tests/test_classify.py
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CLASSIFY = HERE.parent / "scripts" / "classify.py"
TPCE = HERE.parents[1] / "lfc-source-statschema" / "schemas" / "tpce.yaml"


def classify(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(CLASSIFY), *args], capture_output=True, text=True, check=False)


class Blind(unittest.TestCase):
    def test_the_plan_is_the_same_without_the_roles(self):
        d = yaml.safe_load(TPCE.read_text())
        for t in d["tables"]:
            t.pop("role", None)
        with tempfile.TemporaryDirectory() as tmp:
            bare = Path(tmp) / "tpce.yaml"
            bare.write_text(yaml.safe_dump(d))
            with_roles, without = classify(str(TPCE), "--model", "tpce"), classify(str(bare), "--model", "tpce")
            self.assertEqual(with_roles.returncode, 0, with_roles.stderr)
            self.assertEqual(with_roles.stdout, without.stdout)
            self.assertEqual(classify(str(bare), "--truth", "schema").returncode, 2)   # no roles to compare with

    def test_review_schema_corrects_the_plan_with_the_role_lines(self):
        r = classify(str(TPCE), "--model", "tpce", "--review", "schema")
        self.assertEqual(r.returncode, 0, r.stderr)
        plan = yaml.safe_load(r.stdout)["tables"]
        want = {t["name"]: t["role"] for t in yaml.safe_load(TPCE.read_text())["tables"]}
        self.assertEqual({n: t["role"] for n, t in plan.items()}, want)   # the 5 misses take the spec's role
        self.assertEqual(sum(t.get("decided_by") == "docs" for t in plan.values()), 5)

    def test_truth_schema_scores_against_the_role_lines(self):
        r = classify(str(TPCE), "--truth", "schema")
        self.assertIn("tpce: 27 of 32 roles as expected", r.stdout)


if __name__ == "__main__":
    unittest.main()
