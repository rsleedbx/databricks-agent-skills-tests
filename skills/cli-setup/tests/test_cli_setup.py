"""test_cli_setup.py: check_tools.py's group filter, and cloud_identity.py's messages when az can't be run (PATH without
it, so no cloud call is made). Run: python3 -m unittest -q skills/cli-setup/tests/test_cli_setup.py"""
import contextlib
import io
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
import check_tools
import cloud_identity
import laptop


class CliSetupTest(unittest.TestCase):
    def test_unknown_group_checks_nothing_and_passes(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(check_tools.main(["no-such-group"]), 0)
        self.assertEqual(out.getvalue(), "")

    def test_az_missing_gives_the_login_fix(self):
        with mock.patch.dict(os.environ, {"PATH": "/nonexistent-dir"}):
            self.assertFalse(laptop.have("az"))
            msg = cloud_identity.require("sub-1", "tenant-1")
        self.assertIn("az login --tenant tenant-1", msg)

    def test_json_out_is_none_for_a_missing_command(self):
        self.assertIsNone(laptop.json_out(["no-such-command-for-test"]))


if __name__ == "__main__":
    unittest.main()
