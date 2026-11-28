"""test_remove_podman_desktop.py: the report-only run changes nothing, and the removal refuses paths it must never
remove (an empty path, the root, the home folder). Run: python3 -m unittest -q skills/podman-desktop-remove/tests/test_remove_podman_desktop.py"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
import remove_podman_desktop as rpd


class RemoveTest(unittest.TestCase):
    def test_refuses_root_home_and_empty(self):
        rpd.CHANGING["yes"] = True
        try:
            for bad in ("", "/", rpd.HOME):
                with self.assertRaises(ValueError):
                    rpd.do_remove(bad)
        finally:
            rpd.CHANGING["yes"] = False

    def test_without_yes_only_says_what_would_run(self):
        rpd.CHANGING["yes"] = False
        missing = os.path.join(rpd.HOME, "no-such-podman-desktop-test-path")
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            rpd.do_remove(missing)
        self.assertEqual(buf.getvalue(), f"would run: rm -rf {missing}\n")
        self.assertFalse(os.path.exists(missing))


if __name__ == "__main__":
    unittest.main()
