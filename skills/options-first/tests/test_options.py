"""test_options.py: checks options.py on sample files: ranking (status, then effort, then breaks), the pick, check
failures and index. Runs options.py through uv, as the skill's own usage does. Run:
python3 -m unittest -q skills/options-first/tests/test_options.py"""
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
OPTIONS = os.path.join(HERE, "..", "scripts", "options.py")
SAMPLE = """topic: Sample topic
question: Which way?
options:
  - {name: Custom code, setup: {effort: 5, what: lots}, operate: {effort: 4, what: you own it}, status: tested}
  - {name: Built-in setting, setup: {effort: 1, what: one flag}, operate: {effort: 1, what: none}, status: not-supported}
  - {name: Small pipeline, setup: {effort: 2, what: one pipeline}, operate: {effort: 2, what: scans}, breaks: [refresh], status: tested}
  - {name: Smaller untested, setup: {effort: 1, what: tiny}, operate: {effort: 1, what: none}, status: untested}
"""


def opts(*args):
    p = subprocess.run(["uv", "run", "-q", OPTIONS, *args], capture_output=True, text=True, timeout=300, check=False)
    return p.returncode, p.stdout, p.stderr


class OptionsTest(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.t, "a"))
        self.good = os.path.join(self.t, "a", "options.yaml")
        with open(self.good, "w") as fh:
            fh.write(SAMPLE)

    def tearDown(self):
        shutil.rmtree(self.t, ignore_errors=True)

    def row_of(self, out, name):
        for line in out.splitlines():
            if line.startswith("| ") and f"| {name} |" in line:
                return int(line.split("|")[1].strip())
        raise AssertionError(f"no row for {name}")

    def test_render_ranks_tested_simplest_first_untested_after_not_supported_last(self):
        out = opts("render", self.good)[1]
        self.assertLess(self.row_of(out, "Small pipeline"), self.row_of(out, "Custom code"))
        self.assertGreater(self.row_of(out, "Smaller untested"), self.row_of(out, "Custom code"))
        self.assertEqual(self.row_of(out, "Built-in setting"), 4)

    def test_pick_is_the_simplest_tested(self):
        self.assertIn("Pick: **Small pipeline** (tested).", opts("render", self.good)[1])

    def test_check_passes_a_valid_file(self):
        self.assertEqual(opts("check", self.good)[0], 0)

    def test_check_fails_a_bad_file(self):
        bad = os.path.join(self.t, "b.yaml")
        with open(bad, "w") as fh:
            fh.write("topic: Broken\noptions:\n  - {name: x, setup: {effort: 9}, operate: {effort: 1}, status: maybe}\n")
        rc, _, err = opts("check", bad)
        self.assertEqual(rc, 1)
        for text in ("missing question", "effort must be 1 to 5", "status must be"):
            self.assertIn(text, err)

    def test_a_comma_in_a_flow_what_is_caught(self):
        bad = os.path.join(self.t, "c.yaml")
        with open(bad, "w") as fh:
            fh.write("topic: t\nquestion: q\noptions:\n  - {name: x, setup: {effort: 1, what: one pipeline, two lines}, "
                     "operate: {effort: 1}, status: tested}\n")
        rc, _, err = opts("check", bad)
        self.assertEqual(rc, 1)
        self.assertIn("unexpected keys", err)

    def test_bad_yaml_is_a_problem_line_not_a_traceback(self):
        bad = os.path.join(self.t, "d.yaml")
        with open(bad, "w") as fh:
            fh.write("topic: t\nquestion: q\noptions:\n  - name: x\n    recovery: none: lost\n")
        rc, _, err = opts("check", bad)
        self.assertEqual(rc, 1)
        self.assertIn("not valid YAML", err)
        self.assertNotIn("Traceback", err)

    def test_index_lists_the_topic_and_its_pick(self):
        out = opts("index", self.t)[1]
        self.assertIn("| Sample topic | 4 | Small pipeline | a/options.yaml |", out)


if __name__ == "__main__":
    unittest.main()
