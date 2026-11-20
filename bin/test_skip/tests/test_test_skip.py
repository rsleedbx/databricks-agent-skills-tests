# Vendored copy of shared unit 'test_skip': edit the master, then run `reuse-first vendor sync` (any other edit shows as drift).
"""Offline tests for test_skip."""
import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "test_skip.py"
spec = importlib.util.spec_from_file_location("test_skip", SCRIPT)
ts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ts)


class TestSkip(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.TemporaryDirectory()
        os.environ["TEST_SKIP_DIR"] = self.d.name
        os.environ.pop("TEST_SKIP_AGAIN", None)
        self.src = Path(self.d.name) / "src"
        (self.src / "pkg").mkdir(parents=True)
        (self.src / "pkg/a.py").write_text("x = 1\n")

    def tearDown(self):
        self.d.cleanup()

    def test_a_pass_skips_until_a_file_or_value_changes(self):
        fp = ts.fingerprint([self.src], ["v1"])
        self.assertFalse(ts.should_skip("r", "k", fp)[0])           # never run
        ts.record("r", "k", fp, ok=True)
        self.assertTrue(ts.should_skip("r", "k", fp)[0])            # unchanged since a pass
        self.assertNotEqual(ts.fingerprint([self.src], ["v2"]), fp)  # a value changed
        (self.src / "pkg/a.py").write_text("x = 2\n")
        self.assertNotEqual(ts.fingerprint([self.src], ["v1"]), fp)  # a file changed
        (self.src / "pkg/b.py").write_text("")
        self.assertNotEqual(ts.fingerprint([self.src], ["v1"]), fp)  # a file added

    def test_a_failure_never_skips_and_again_forces_a_run(self):
        fp = ts.fingerprint([self.src])
        ts.record("r", "k", fp, ok=False)
        self.assertFalse(ts.should_skip("r", "k", fp)[0])
        ts.record("r", "k", fp, ok=True)
        self.assertFalse(ts.should_skip("r", "k", fp, again=True)[0])
        os.environ["TEST_SKIP_AGAIN"] = "1"
        self.assertFalse(ts.should_skip("r", "k", fp)[0])

    def test_caches_dont_count_and_a_missing_path_does(self):
        fp = ts.fingerprint([self.src])
        (self.src / "pkg/__pycache__").mkdir()
        (self.src / "pkg/__pycache__/a.cpython-310.pyc").write_bytes(b"\0")
        self.assertEqual(ts.fingerprint([self.src]), fp)
        self.assertNotEqual(ts.fingerprint([self.src, self.src / "gone"]), fp)

    def test_the_cli_exits_0_to_skip(self):
        run = lambda *a: subprocess.run([sys.executable, str(SCRIPT), *a], capture_output=True, text=True)
        self.assertEqual(run("skip", "r", "k", str(self.src)).returncode, 1)
        self.assertEqual(run("record", "r", "k", "pass", str(self.src)).returncode, 0)
        out = run("skip", "r", "k", str(self.src))
        self.assertEqual(out.returncode, 0)
        self.assertIn("unchanged since", out.stdout)
        run("forget", "r", "k")
        self.assertEqual(run("skip", "r", "k", str(self.src)).returncode, 1)


if __name__ == "__main__":
    unittest.main()
