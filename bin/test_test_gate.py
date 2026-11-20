"""Offline tests for bin/test_gate.py: which paths a check depends on, and the $T group."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_gate as g  # noqa: E402


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
                self.assertEqual(g.inputs([f"python3 {root}/bin/check_layers.py"], lambda: graph), [root])
                self.assertEqual(g.inputs(["test 4 -eq 4"], lambda: graph), [])   # no repo path: always runs
            finally:
                g.R = old_r

    def test_temp_group_reads_the_skills_of_the_lines_that_use_T(self):
        with tempfile.TemporaryDirectory() as d:
            s = Path(d) / "t.sh"
            s.write_text('SS=$SK/skill-one/scripts; DG=$SK/skill-two/scripts\n'
                         'check "a" sh -c "uv run $SS/x.py > $T/a.yaml"\n'
                         'check "b" uv run "$DG/y.py"\n'
                         'check "c" cat "$SK/skill-three/z" "$T/a.yaml"\n')
            got = [Path(p).name for p in g.temp_group(s)]
            self.assertEqual(got, ["skill-one", "skill-three"])   # b uses no $T: gated on its own


if __name__ == "__main__":
    unittest.main()
