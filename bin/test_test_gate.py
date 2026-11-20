"""Offline tests for bin/test_gate.py: which paths a check depends on, and the $T group."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_gate as g


class GateTest(unittest.TestCase):
    def test_inputs_widen_a_skill_to_its_code_references_and_a_bin_tool_to_the_repo(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for n in ("a", "b", "c", "x"):
                (root / "skills" / n).mkdir(parents=True)
            (root / "bin").mkdir()
            (root / "bin/tool.sh").write_text("")
            graph = {"a": {"b", "bin/tool.sh"}, "b": {"c"}, "c": set(), "x": {"a"}}
            old_r, g.R = g.R, root
            try:
                got = {str(p.relative_to(root)) for p in g.inputs([f"uv run {root}/skills/a/scripts/run.py"], lambda: graph)}
                self.assertEqual(got, {"skills/a", "skills/b", "skills/c", "bin/tool.sh"})   # a, what its code uses
                (root / "defaults.yaml").write_text("")
                (root / "README.md").write_text("")
                got = {str(p.relative_to(root)) for p in g.inputs([f"uv run {root}/skills/a/x.py"], lambda: graph)}
                self.assertIn("defaults.yaml", got)        # repo-root config a skill's code may read
                self.assertNotIn("README.md", got)         # prose isn't an input
                (root / "baseline.txt").write_text("skills/x-results/references/y\n")   # a root file naming results/
                got = {str(p.relative_to(root)) for p in g.inputs([f"uv run {root}/skills/a/x.py"], lambda: graph)}
                self.assertNotIn("results", got)           # only a skill's own code reading results/ adds it
                (root / "bin/vendored/scripts").mkdir(parents=True)
                (root / "bin/vendored/scripts/v.py").write_text("")
                (root / "skills/a/run.py").write_text('sys.path.insert(0, "bin/vendored/scripts")\n')
                got = {str(p.relative_to(root)) for p in g.inputs([f"uv run {root}/skills/a/x.py"], lambda: graph)}
                self.assertIn("bin/vendored/scripts", got)  # a bin subfolder its code names
                self.assertEqual(g.inputs([f"python3 {root}/bin/check_layers.py"], lambda: graph), [root])
                self.assertEqual(g.inputs(["test 4 -eq 4"], lambda: graph), [])   # no repo path: always runs
            finally:
                g.R = old_r

    def test_the_temp_group_skips_only_with_the_inputs_of_its_last_pass_unchanged(self):
        import os
        with tempfile.TemporaryDirectory() as d:
            os.environ["TEST_SKIP_DIR"] = d
            again = os.environ.pop("TEST_SKIP_AGAIN", None)   # this test checks skipping itself
            try:
                old_r, g.R = g.R, Path(d)
                script, acc, dep = Path(d) / "t.sh", Path(d) / "acc", Path(d) / "dep.py"
                script.write_text("check ...\n")
                dep.write_text("v1\n")
                self.assertEqual(g.group_decide("grp", script, []), (False, "run -new"))   # never passed: runs
                acc.write_text(f"{dep}\n")
                g.group_record("grp", script, [], acc, ok=True)
                self.assertTrue(g.group_decide("grp", script, [])[0])                     # unchanged: skips
                dep.write_text("v2\n")
                self.assertFalse(g.group_decide("grp", script, [])[0])                    # an input changed
                g.group_record("grp", script, [], acc, ok=True)
                script.write_text("check ... edited\n")
                self.assertFalse(g.group_decide("grp", script, [])[0])                    # the script changed
                self.assertEqual(json.loads(g.group_file().read_text())["paths"], ["dep.py"])   # relative to the root
            finally:
                g.R = old_r
                os.environ.pop("TEST_SKIP_DIR", None)
                if again is not None:
                    os.environ["TEST_SKIP_AGAIN"] = again


if __name__ == "__main__":
    unittest.main()
