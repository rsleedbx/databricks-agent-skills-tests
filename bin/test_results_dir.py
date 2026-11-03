"""Where the result scripts write and read (skills/lfc-test-results/scripts/results_dir.py): in a checkout, with
LFC_TESTS_RESULTS, and in a copy laid out like an installed plugin (under .claude/plugins/cache/, HOME a temp folder).

    python3 -m unittest bin/test_results_dir.py
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "skills" / "lfc-test-results" / "scripts"
RECORD = {"date": "2026-11-02", "run": "2026-11-02-results-dir-test", "test": "qbc", "host": "laptop-podman",
          "subject": {"engine": "postgres", "version": "16"}, "outcome": "pass", "result": "10 / 10"}


def run(script: Path, *args: str, env: dict, stdin: str = "") -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(script), *args], input=stdin, capture_output=True, text=True,
                          env=env, timeout=60, check=False)


class ResultsDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.env = {k: v for k, v in os.environ.items() if k != "LFC_TESTS_RESULTS"}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def dirs(self, scripts: Path, env: dict) -> dict:
        code = ("import json, sys; sys.path.insert(0, sys.argv[1]); import results_dir as r; "
                "print(json.dumps({'write': str(r.write_dir('runs')), 'read': [str(d) for d in r.read_dirs('runs')]}))")
        out = subprocess.run([sys.executable, "-c", code, str(scripts)], capture_output=True, text=True, env=env,
                             timeout=60, check=True)
        return json.loads(out.stdout)

    def plugin_copy(self, version: str | None = "0.1.0", folder: str = "0.1.0") -> tuple[Path, dict]:
        """A copy of the scripts as an installed plugin has them, with one shipped run and one shipped finding."""
        home = self.tmp / "home"
        root = home / ".claude/plugins/cache/lfc-tests/lfc-tests" / folder
        shutil.copytree(SCRIPTS, root / "skills/lfc-test-results/scripts", ignore=shutil.ignore_patterns("__pycache__"))
        (root / ".claude-plugin").mkdir(parents=True)
        (root / ".claude-plugin/plugin.json").write_text(json.dumps({"name": "lfc-tests", **({"version": version} if version else {})}))
        (root / "results/runs").mkdir(parents=True)
        shipped = {**RECORD, "run": "2026-10-01-shipped", "date": "2026-10-01", "subject": {"engine": "mysql", "version": "8.4"}}
        (root / "results/runs/2026-10-01-shipped.jsonl").write_text(json.dumps(shipped) + "\n")
        (root / "results/findings").mkdir(parents=True)
        f = min((REPO / "results/findings").glob("*.json"))
        shutil.copy(f, root / "results/findings" / f.name)
        return root, {**self.env, "HOME": str(home)}

    def test_a_checkout_reads_and_writes_its_own_results(self):
        d = self.dirs(SCRIPTS, self.env)
        self.assertEqual(d["write"], str(REPO / "results/runs"))
        self.assertEqual(d["read"], [str(REPO / "results/runs")])

    def test_lfc_tests_results_moves_new_results_and_reads_both(self):
        d = self.dirs(SCRIPTS, {**self.env, "LFC_TESTS_RESULTS": str(self.tmp / "res")})
        self.assertEqual(d["write"], str(self.tmp / "res/runs"))
        self.assertEqual(d["read"], [str(REPO / "results/runs"), str(self.tmp / "res/runs")])

    def test_a_plugin_without_a_version_is_recorded_by_its_commit(self):
        # no version in plugin.json: Claude Code names the install folder after the commit (12 hex digits)
        root, env = self.plugin_copy(version=None, folder="0da54f5a1b2c")
        out = run(root / "skills/lfc-test-results/scripts/record.py", env=env, stdin=json.dumps(RECORD))
        self.assertEqual(out.returncode, 0, out.stderr)
        mine = Path(env["HOME"]) / ".lfc/tests/results/runs" / f"{RECORD['run']}.jsonl"
        self.assertEqual(json.loads(mine.read_text())["skills"], {"databricks-agent-skills-tests": "plugin 0da54f5a1b2c"})

    def test_a_plugin_copy_records_outside_itself_and_recalls_both(self):
        root, env = self.plugin_copy()
        out = run(root / "skills/lfc-test-results/scripts/record.py", env=env, stdin=json.dumps(RECORD))
        self.assertEqual(out.returncode, 0, out.stderr)
        mine = Path(env["HOME"]) / ".lfc/tests/results/runs" / f"{RECORD['run']}.jsonl"
        self.assertTrue(mine.exists())
        self.assertFalse((root / "results/runs" / mine.name).exists())             # nothing into the copy
        self.assertEqual(json.loads(mine.read_text())["skills"], {"databricks-agent-skills-tests": "plugin 0.1.0"})
        out = run(root / "skills/lfc-test-results/scripts/recall.py", "--all", "--json", env=env).stdout
        recs = [json.loads(line) for line in out.splitlines() if line.strip()]              # one record per line
        self.assertEqual(sorted(r["run"] for r in recs), ["2026-10-01-shipped", RECORD["run"]])

    def test_a_plugin_copys_own_finding_wins_over_the_shipped_one(self):
        root, env = self.plugin_copy()
        finding = root / "skills/lfc-test-results/scripts/finding.py"
        shipped = json.loads(next((root / "results/findings").glob("*.json")).read_text())
        mine = {**shipped, "title": "the user's own title"}
        out = run(finding, "add", env=env, stdin=json.dumps(mine))
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertTrue((Path(env["HOME"]) / ".lfc/tests/results/findings" / f"{shipped['id']}.json").exists())
        rows = [r for r in run(finding, "--with-test", env=env).stdout.splitlines() if r.startswith(shipped["id"] + "\t")]
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].endswith("\tthe user's own title"))


if __name__ == "__main__":
    unittest.main()
