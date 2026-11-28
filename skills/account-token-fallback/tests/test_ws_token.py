"""test_ws_token.py: checks ws_token.py and profiles_check.py with fake databricks and az on PATH (Python scripts, so no
shell file is needed): a command's exit status passes through, a hung command is stopped after its timeout (laptop.run),
a deleted workspace (exit 3, no login asked), a host that doesn't resolve (exit 4), an expired account login (named
first), an account token the workspace takes (exit 0, the config file printed, mode 600), another error with "does not
exist" in it (exit 1, not a deleted workspace), and profiles_check's status per profile.

Run: python3 -m unittest -q skills/account-token-fallback/tests/test_ws_token.py   (laptop only: no CLI is called for real)
"""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "scripts")
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run

FAKE_DATABRICKS = '''#!/usr/bin/env python3
import json, os, sys
a = sys.argv[1:]
if a[:2] == ["auth", "profiles"]:
    print(json.dumps({"profiles": [
        {"name": "ws1", "host": "https://adb-1.2.azuredatabricks.net", "account_id": "acct-1", "valid": False},
        {"name": "acct-azure-1", "host": "https://accounts.azuredatabricks.net", "account_id": "acct-1", "valid": False}]}))
elif a[:2] == ["auth", "token"]:
    print(json.dumps({"access_token": os.environ.get("FK_ACCT", "")}))
elif a[:2] == ["current-user", "me"]:
    if not os.environ.get("DATABRICKS_CONFIG_FILE"):
        print("Error: refresh token is invalid", file=sys.stderr); sys.exit(1)
    cfg = open(os.environ["DATABRICKS_CONFIG_FILE"]).read()
    if os.environ.get("FK_ME") == "ok" and "token = good" in cfg:
        print(json.dumps({"userName": "u"})); sys.exit(0)
    print(os.environ.get("FK_ME") or "Error: invalid access token", file=sys.stderr); sys.exit(1)
else:
    print("fake databricks: " + " ".join(a), file=sys.stderr); sys.exit(2)
'''

FAKE_AZ = '''#!/usr/bin/env python3
import os, sys
if sys.argv[1:3] == ["account", "get-access-token"] and os.environ.get("FK_AZ"):
    print(os.environ["FK_AZ"])
'''


def write_exe(path: str, text: str) -> None:
    with open(path, "w") as fh:
        fh.write(text)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class WsTokenTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bin = os.path.join(self.tmp, "bin")
        os.mkdir(self.bin)
        write_exe(os.path.join(self.bin, "databricks"), FAKE_DATABRICKS)
        write_exe(os.path.join(self.bin, "az"), FAKE_AZ)
        self.cfg = os.path.join(self.tmp, "ws1.cfg")
        self.env = mock.patch.dict(os.environ, {"PATH": self.bin + os.pathsep + os.environ.get("PATH", ""),
                                               "FK_AZ": "", "FK_ACCT": "", "FK_ME": ""})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def wst(self, **fake):
        os.environ.update(fake)
        p = subprocess.run([sys.executable, os.path.join(SCRIPTS, "ws_token.py"), "ws1", "--cfg", self.cfg],  # noqa: PLW1510
                           capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout, p.stderr

    def test_deleted_workspace_exit_3_no_login_asked(self):
        rc, _, err = self.wst(FK_AZ="bad", FK_ACCT="good",
                                FK_ME="Error: Did not find workspace with specified org ID: 1")
        self.assertEqual(rc, 3, err)
        self.assertIn("no longer exists", err)
        self.assertNotIn("auth login", err)
        self.assertFalse(os.path.exists(self.cfg))

    def test_expired_account_login_too_names_it_first(self):
        rc, _, err = self.wst(FK_AZ="bad", FK_ACCT="", FK_ME="Error: invalid access token")
        self.assertEqual(rc, 1, err)
        self.assertIn("log in to the account", err)
        self.assertIn("auth login --profile acct-azure-1; or to this workspace only", err)

    def test_account_token_the_workspace_takes_prints_the_config_mode_600(self):
        rc, out, err = self.wst(FK_AZ="bad", FK_ACCT="good", FK_ME="ok")
        self.assertEqual(rc, 0, err)
        self.assertEqual(out.strip(), self.cfg)
        self.assertEqual(stat.S_IMODE(os.stat(self.cfg).st_mode), 0o600)
        self.assertNotIn("good", out + err)   # the token is never printed

    def test_host_that_does_not_resolve_exit_4_network_first(self):
        rc, _, err = self.wst(FK_AZ="", FK_ACCT="good",
                                FK_ME="Error: dial tcp: lookup adb-1.2.azuredatabricks.net: no such host")
        self.assertEqual(rc, 4, err)
        self.assertIn("Check the network first", err)
        self.assertNotIn("no longer exists", err)

    def test_does_not_exist_in_another_error_is_a_login_not_a_deleted_workspace(self):
        rc, _, err = self.wst(FK_AZ="bad", FK_ACCT="good", FK_ME="Error: User does not exist in the workspace")
        self.assertEqual(rc, 1, err)
        self.assertNotIn("no longer exists", err)
        self.assertIn("auth login", err)


class LaptopRunTest(unittest.TestCase):
    def test_exit_status_passes_through(self):
        self.assertEqual(run([sys.executable, "-c", "pass"])[0], 0)
        self.assertEqual(run([sys.executable, "-c", "import sys; sys.exit(3)"])[0], 3)

    def test_hung_command_is_stopped(self):
        rc, _, err = run([sys.executable, "-c", "import time; time.sleep(5)"], timeout=1)
        self.assertEqual(rc, 124)
        self.assertIn("timed out", err)

    def test_missing_command_fails(self):
        self.assertEqual(run(["no-such-command-x"])[0], 127)


class ProfilesCheckTest(unittest.TestCase):
    def test_statuses_from_a_saved_profiles_file(self):
        data = {"profiles": [
            {"name": "ok-ws", "host": "https://dbc-1.cloud.databricks.com", "account_id": "a1", "valid": True,
             "cloud": "aws"},
            {"name": "exp-ws", "host": "https://dbc-2.cloud.databricks.com", "account_id": "a1", "valid": False,
             "cloud": "aws"},
            {"name": "lone", "host": "https://dbc-3.cloud.databricks.com", "account_id": "a2", "valid": False,
             "cloud": "aws"},
            {"name": "az-ws", "host": "https://adb-4.azuredatabricks.net", "account_id": "a3", "valid": False,
             "cloud": "azure"},
            {"name": "acct", "host": "https://accounts.cloud.databricks.com", "account_id": "a1", "valid": True},
        ]}
        import profiles_check
        rows, need = profiles_check.classify(data["profiles"])
        status = {r["profile"]: r["status"] for r in rows}
        self.assertEqual(status, {"ok-ws": "ok", "exp-ws": "try", "lone": "no-account-profile", "az-ws": "try"})
        self.assertIn("a2", need)


if __name__ == "__main__":
    unittest.main()
