#!/usr/bin/env python3
"""test_workload_env.py: offline checks of the workload contract (contract 1): workload_env.py on the fixtures (the valid
file with and without bundle_cmd, each refusal with its reason, a secret never printed, the exports set by eval). The
fixtures name absolute paths, so each is copied to a temp folder with __FX__ replaced by the fixtures folder. Also runs
test_validate_model.py. Prints "all passed" or the failures.
    python3 -I test_workload_env.py
"""
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
FX = os.path.join(HERE, "fixtures")
WE = os.path.join(HERE, "workload_env.py")


class WorkloadEnv(unittest.TestCase):
    def setUp(self):
        self.T = tempfile.mkdtemp(prefix="test-workload-env-")

    def tearDown(self):
        shutil.rmtree(self.T, ignore_errors=True)

    def render(self, name):
        """The fixture with its absolute paths, in the temp folder; its path."""
        with open(os.path.join(FX, name)) as f:
            text = f.read().replace("__FX__", FX)
        path = os.path.join(self.T, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    def we(self, path, env=None):
        e = dict(os.environ)
        e.update(env or {})
        return subprocess.run([sys.executable, "-I", WE] + ([path] if path else []), capture_output=True, text=True,
                              env=e, check=False)

    def exports(self, path, env=None):
        """The exports of a valid file as {name: value}, and the unset names (as eval would leave them)."""
        p = self.we(path, env)
        self.assertEqual(p.returncode, 0, p.stderr)
        values, unset = {}, set()
        for line in p.stdout.splitlines():
            words = shlex.split(line)
            if words[0] == "export":
                k, v = words[1].split("=", 1)
                values[k] = v
            else:
                unset.add(words[1])
        return values, unset

    def refused(self, fixture, *words):
        p = self.we(self.render(fixture))
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertEqual(p.stdout, "")
        for w in words:
            self.assertIn(w, p.stderr)
        return p

    # --- the checks, one per test
    def test_usage(self):
        p = self.we(None)
        self.assertEqual(p.returncode, 2)
        self.assertIn("workload_env.py <workload.json>", p.stderr)

    def test_valid_exports(self):
        v, unset = self.exports(self.render("wl-valid.json"))
        self.assertEqual(v["WL_MODEL"], "tpcb")
        self.assertEqual(v["WL_SCALE"], "0.01")
        self.assertEqual(v["WL_DIALECT"], "tsql")
        self.assertEqual(v["WL_TABLES"], "branch teller account history")
        self.assertEqual(v["WL_NTABLES"], "4")
        self.assertEqual(v["WL_DDL"], os.path.join(FX, "model.sql"))
        self.assertEqual(v["WL_FILL"], f"python3 {FX}/fake-fill.py ms")
        self.assertEqual(v["WL_FILL_IN_BUNDLE"], "python3 fake-fill.py ms")
        self.assertEqual(v["WL_BUNDLE"], f"python3 {FX}/fake-bundle.py")
        self.assertIn("WL_VERIFY", unset)

    def test_no_bundle_unsets(self):
        v, unset = self.exports(self.render("wl-no-bundle.json"), {"WL_BUNDLE": "stale"})
        self.assertIn("WL_BUNDLE", unset)
        self.assertNotIn("WL_BUNDLE", v)
        self.assertEqual(v["WL_NTABLES"], "4")

    def test_verify_exported(self):
        v, _ = self.exports(self.render("wl-verify.json"))
        self.assertEqual(v["WL_VERIFY"], os.path.join(FX, "model.sql"))
        self.assertEqual(v["WL_DIALECT"], "postgres")
        _, unset = self.exports(self.render("wl-valid.json"), {"WL_VERIFY": "stale"})
        self.assertIn("WL_VERIFY", unset)

    def test_unreadable(self):
        p = self.we(os.path.join(self.T, "none.json"))
        self.assertEqual(p.returncode, 2)

    def test_contract_2(self):
        self.refused("wl-contract-2.json", "contract 2", "reads contract 1 only")

    def test_no_contract(self):
        self.refused("wl-no-contract.json", "missing contract")

    def test_not_json(self):
        self.refused("wl-not-json.json", "not a JSON object")

    def test_no_model(self):
        self.refused("wl-no-model.json", "missing model")

    def test_bad_model(self):
        self.refused("wl-bad-model.json", "model must be a lowercase name")

    def test_bad_scale(self):
        self.refused("wl-bad-scale.json", "scale must be a number above 0")

    def test_relative_ddl(self):
        self.refused("wl-relative-ddl.json", "ddl must be an absolute path")

    def test_no_ddl_file(self):
        self.refused("wl-no-ddl.json", "is not a file")

    def test_empty_tables(self):
        self.refused("wl-empty-tables.json", "tables must be a non-empty list")

    def test_bad_table(self):
        self.refused("wl-bad-table.json", "not a plain identifier")

    def test_dup_table(self):
        self.refused("wl-dup-table.json", "listed twice")

    def test_quoted_fill(self):
        self.refused("wl-quote-fill.json", "fill_cmd must be words")

    def test_fill_missing(self):
        self.refused("wl-fill-missing.json", "is not a file")

    def test_secret_not_printed(self):
        p = self.refused("wl-secret.json", "a secret")
        self.assertNotIn("hunter2", p.stderr)

    def test_verify_missing(self):
        self.refused("wl-verify-missing.json", "verify", "is not a file")

    def test_eval_round_trip(self):
        """The exports, eval'd by a shell, set the names a consumer reads."""
        path = self.render("wl-valid.json")
        script = f'eval "$({shlex.quote(sys.executable)} -I {shlex.quote(WE)} {shlex.quote(path)})" && printf %s "$WL_TABLES|$WL_NTABLES"'
        p = subprocess.run(["sh", "-c", script], capture_output=True, text=True, check=False)
        self.assertEqual(p.stdout, "branch teller account history|4", p.stderr)

    def test_validate_model_fixtures(self):
        """The model-format checks (test_validate_model.py) pass too."""
        p = subprocess.run([sys.executable, "-I", os.path.join(HERE, "test_validate_model.py")], capture_output=True,
                           text=True, check=False)
        self.assertEqual(p.returncode, 0, p.stderr)


if __name__ == "__main__":
    unittest.main()
