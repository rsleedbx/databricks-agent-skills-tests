"""Offline tests for the runners and the hook (run.py, bin/run_batch.py, bin/run_matrix.py, bin/plugin_session.py)
and the suite driver's helpers: the prompt fill and the tag lines, the gate's arguments, the plugin cache, the
batch's tool allowlist, the matrix's batch files, the hook's exports."""
from __future__ import annotations

import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import plugin_session  # noqa: E402
import run_batch  # noqa: E402
import run_matrix  # noqa: E402
import test_scripts  # noqa: E402
import run as runner  # noqa: E402  (the repo's run.py)


class Env(unittest.TestCase):
    """Restores os.environ after each test (the functions read it)."""

    def setUp(self):
        self._env = dict(os.environ)
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env)
        shutil.rmtree(self.tmp, ignore_errors=True)


class Runner(Env):
    def test_fill_replaces_set_variables_and_stops_on_unset(self):
        os.environ["LFC_X"] = "7"
        self.assertEqual(runner.fill("a ${LFC_X} b"), "a 7 b")
        with self.assertRaises(ValueError) as cm:
            runner.fill("${LFC_NOPE_UNSET_1}")
        self.assertEqual(str(cm.exception), "LFC_NOPE_UNSET_1")
        self.assertEqual(runner.fill_lenient("${LFC_NOPE_UNSET_2} ${LFC_X}"), "${LFC_NOPE_UNSET_2} 7")

    def test_split_line_keeps_later_bars(self):
        self.assertEqual(runner.split_line("tag|a|b"), ("tag", "a|b"))
        self.assertEqual(runner.split_line("tag+|follow"), ("tag+", "follow"))

    def test_gate_args_name_the_tag_and_its_follow_ups(self):
        prompts = self.tmp / "lakeflow-x.txt"
        prompts.write_text("one|first\none+|more ${LFC_X}\ntwo|second\n")
        os.environ["LFC_X"] = "v"
        args = runner.gate_args(prompts, "one", "first", Path("/repo/skills"), "DEFAULT", "claude 1.2")
        self.assertEqual(args[0], "lakeflow-x:one")
        self.assertEqual(args[1:3], ["--value", "DEFAULT"])
        self.assertIn("one+|more v", args)   # the follow-up, filled (not its tag line)
        self.assertEqual(args[-1], "/repo/skills")

    def test_patch_cache_copies_skill_md_and_references(self):
        repo = self.tmp / "repo"
        (repo / "skills/databricks-foo/references").mkdir(parents=True)
        (repo / "skills/databricks-foo/SKILL.md").write_text("new")
        (repo / "skills/databricks-foo/references/r.md").write_text("ref")
        cache = self.tmp / "cache" / "skills"
        (cache / "databricks-foo").mkdir(parents=True)
        runner.patch_cache(repo, str(cache))
        self.assertEqual((cache / "databricks-foo" / "SKILL.md").read_text(), "new")
        self.assertEqual((cache / "databricks-foo" / "references" / "r.md").read_text(), "ref")

    def test_cache_root_finds_the_databricks_skills_folder(self):
        home = self.tmp / "home"
        (home / ".claude/plugins/cache/mkt/databricks/1.0/skills/databricks-x").mkdir(parents=True)
        os.environ["HOME"] = str(home)
        self.assertEqual(runner.cache_root(), str(home / ".claude/plugins/cache/mkt/databricks/1.0/skills"))


class Batch(Env):
    def test_allowed_tools_name_the_connection_files_per_path(self):
        rules = run_batch.allowed_tools("/w").split(",")
        self.assertIn("Bash(databricks:*)", rules)
        self.assertIn("Bash(rm -f /w/conn.json)", rules)
        self.assertIn("Bash(rm /private/w/scratch/conn.json)", rules)

    def test_connection_args_for_set_connection_variables(self):
        for k in list(os.environ):
            if k.startswith("LFC_") and k.endswith("_CONNECTION"):
                del os.environ[k]
        os.environ["LFC_PG_CONNECTION"] = "pgc"
        os.environ["LFC_EMPTY_CONNECTION"] = ""
        self.assertEqual(run_batch.connection_args(), ["--connection", "pgc"])

    def test_last_result_line(self):
        log = self.tmp / "x.log"
        log.write_text("a passed\nb\nc FAILED 2\n")
        self.assertEqual(run_batch.last_result_line(log), "c FAILED 2")
        log.write_text("nothing\n")
        self.assertEqual(run_batch.last_result_line(log), "")


class Matrix(Env):
    def test_batch_files_and_skips(self):
        for k in ("LFC_ORACLE_CONNECTION", "LFC_SKIP_LAKEBASE"):
            os.environ.pop(k, None)
        self.assertEqual([f.name for f in run_matrix.batch_files("dml")], ["dml-lakebase.txt"])
        self.assertEqual(len(run_matrix.batch_files("qbc")), 4)
        os.environ["LFC_ORACLE_CONNECTION"] = "o"
        self.assertEqual(run_matrix.batch_files("icdc")[-1].name, "icdc-oracle.txt")
        os.environ["LFC_SKIP_LAKEBASE"] = "1"
        self.assertNotIn("qbc-lakebase.txt", [f.name for f in run_matrix.batch_files("qbc")])
        self.assertEqual([f.name for f in run_matrix.batch_files("dml")], [])
        with self.assertRaises(ValueError):
            run_matrix.batch_files("nope")


class Hook(Env):
    def test_exports_and_the_no_settings_message(self):
        env_file = self.tmp / "env.sh"
        os.environ["CLAUDE_ENV_FILE"] = str(env_file)
        os.environ["LFC_TESTS_SETTINGS"] = str(self.tmp / "missing.env")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(plugin_session.main([]), 0)
        exports = env_file.read_text()
        self.assertIn("export LFC_TESTS=", exports)
        self.assertIn('export PATH=', exports)
        self.assertIn('"$PATH"', exports)
        self.assertIn("no test settings saved yet", out.getvalue())

    def test_missing_sdk_is_reported_not_raised(self):
        from unittest import mock
        with mock.patch("importlib.util.find_spec", side_effect=ModuleNotFoundError("databricks")):
            self.assertTrue(plugin_session.sdk_missing())
        self.assertIsInstance(plugin_session.sdk_missing(), bool)

    def test_saved_settings_name_their_keys(self):
        settings = self.tmp / "settings.env"
        settings.write_text("REGION=eastus\nSETTINGS_CHECKED=2026-11-01\n")
        os.environ.pop("CLAUDE_ENV_FILE", None)
        os.environ["LFC_TESTS_SETTINGS"] = str(settings)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            plugin_session.main([])
        self.assertIn("(REGION; checked", out.getvalue())


class SuiteHelpers(Env):
    def test_a_missing_program_or_a_raising_function_fails_the_check(self):
        os.environ["TEST_TIMINGS"] = str(self.tmp / "timings.tsv")
        s = test_scripts.Suite()
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertFalse(s.run_check("no such program", ["lfc-no-such-program-xyz"]))
                self.assertFalse(s.run_check("raises", lambda: 1 / 0))
            self.assertEqual(s.fails, 2)
            self.assertIn("FAIL  no such program", out.getvalue())
        finally:
            shutil.rmtree(s.T, ignore_errors=True)

    def test_awk_sum_and_zero_test(self):
        self.assertEqual(test_scripts._awk_sum(["a|x|2.5", "b|y|1", "short"], "|"), "3.5")
        self.assertEqual(test_scripts._awk_sum(["a\tb\t4"], "\t"), "4")
        self.assertEqual(test_scripts._awk_sum([], "|"), "0")
        self.assertTrue(test_scripts._is_zero("0.0"))
        self.assertFalse(test_scripts._is_zero("abc"))
        self.assertFalse(test_scripts._is_zero(""))


if __name__ == "__main__":
    unittest.main()
