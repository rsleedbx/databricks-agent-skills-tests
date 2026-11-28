"""test_settings.py: settings.py offline on a temporary settings file (LFC_TESTS_SETTINGS), with fake databricks, az, aws and
gcloud on PATH (Python scripts): set/get/unset with values that need quoting, file and folder modes, require (missing
keys, no CLI call, the 30-day note), env (eval-able by bash), workspaces (numbered by cloud, saved ones marked, login
state), azure (saved and default marked), setup (the resource-group suggestions, AWS, GCP, a saved group that's gone, no
az user), checked, defaults, remove-after.

Run: python3 -m unittest -q skills/lfc-test-settings/tests/test_settings.py
"""
from __future__ import annotations

import datetime as dt
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS = os.path.join(HERE, "..", "scripts", "settings.py")

FAKE_DATABRICKS = '''#!/usr/bin/env python3
import json, os, sys
open(os.environ["FAKE_LOG"], "a").write("databricks " + " ".join(sys.argv[1:]) + "\\n")
print(json.dumps({"profiles": [
    {"name": "ws-b", "host": "https://adb-2.2.azuredatabricks.net", "valid": False, "account_id": "acct-az"},
    {"name": "ws-a", "host": "https://dbc-1.cloud.databricks.com", "valid": True, "account_id": "acct-aws"},
    {"name": "ws-c", "host": "https://dbc-3.cloud.databricks.com", "valid": False, "account_id": "acct-aws"},
    {"name": "acct", "host": "https://accounts.cloud.databricks.com", "valid": True, "account_id": "acct-aws"}]}))
'''

FAKE_AZ = '''#!/usr/bin/env python3
import json, os, sys
open(os.environ["FAKE_LOG"], "a").write("az " + " ".join(sys.argv[1:]) + "\\n")
a = sys.argv[1:]
if a[:2] == ["account", "list"]:
    print(json.dumps([{"name": "sub-two", "id": "22", "tenantId": "t2", "isDefault": True},
                      {"name": "sub-one", "id": "11", "tenantId": "t1", "isDefault": False}]))
elif a[:2] == ["account", "show"]:
    if not os.environ.get("FAKE_ME"):
        sys.exit(1)
    print(os.environ["FAKE_ME"])
elif a[:2] == ["group", "show"]:
    if "rg-mine" in a:
        print("centralus")
    else:
        sys.exit(3)
elif a[:2] == ["group", "list"]:
    print(json.dumps([
        {"name": "rg-mine", "location": "centralus", "tags": {"Owner": "me@x.com"}},
        {"name": "rg-made", "location": "eastus", "tags": {"Creator": "ME@X.COM"}},
        {"name": "rg-other", "location": "eastus", "tags": {"Owner": "other@x.com"}},
        {"name": "rg-untagged", "location": "eastus"}]))
'''

FAKE_AWS = '''#!/usr/bin/env python3
import sys
a = " ".join(sys.argv[1:])
if a == "configure list-profiles":
    print("p1")
elif a == "configure get sso_account_id --profile p1":
    print("111122223333")
elif a == "configure get region --profile p1":
    print("us-east-2")
else:
    sys.exit(1)
'''

FAKE_GCLOUD = '''#!/usr/bin/env python3
import sys
if " ".join(sys.argv[1:]).startswith("config configurations list"):
    sys.stdout.write("default\\tproj-a\\tus-central1\\tTrue\\nother\\t\\t\\tFalse\\n")
'''


def write_exe(path: str, text: str) -> None:
    with open(path, "w") as fh:
        fh.write(text)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class SettingsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bin = os.path.join(self.tmp, "bin")
        os.mkdir(self.bin)
        for name, text in (("databricks", FAKE_DATABRICKS), ("az", FAKE_AZ), ("aws", FAKE_AWS),
                           ("gcloud", FAKE_GCLOUD)):
            write_exe(os.path.join(self.bin, name), text)
        self.settings = os.path.join(self.tmp, "cfg", "settings.env")
        self.log = os.path.join(self.tmp, "calls")
        open(self.log, "w").close()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def st(self, *args, me: str = "", check: bool = False):
        env = dict(os.environ, PATH=self.bin + os.pathsep + os.environ.get("PATH", ""),
                   LFC_TESTS_SETTINGS=self.settings, FAKE_LOG=self.log, FAKE_ME=me)
        p = subprocess.run([sys.executable, SETTINGS, *args], capture_output=True, text=True, env=env, timeout=120, check=False)
        if check:
            self.assertEqual(p.returncode, 0, p.stderr)
        return p

    def out(self, *args, me: str = "") -> str:
        return self.st(*args, me=me, check=True).stdout

    def calls(self) -> str:
        with open(self.log) as fh:
            return fh.read()

    def test_set_get_round_trip_with_quoting(self):
        self.st("set", "OWNER=a b", "TAGS=x='y' $z", check=True)
        self.assertEqual(self.out("get", "OWNER"), "a b\n")
        self.assertEqual(self.out("get", "TAGS"), "x='y' $z\n")

    def test_modes_file_600_folder_700(self):
        self.st("set", "OWNER=a", check=True)
        self.assertEqual(stat.S_IMODE(os.stat(self.settings).st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(os.stat(os.path.dirname(self.settings)).st_mode), 0o700)

    def test_set_replaces_a_key_one_line_each(self):
        self.st("set", "SOURCE_HOST=vm", check=True)
        self.st("set", "SOURCE_HOST=podman", check=True)
        self.assertEqual(self.out("get", "SOURCE_HOST"), "podman\n")
        with open(self.settings) as fh:
            self.assertEqual(sum(1 for line in fh if line.startswith("SOURCE_HOST=")), 1)

    def test_set_refuses_a_key_that_is_not_capitals(self):
        p = self.st("set", "bad-key=1")
        self.assertEqual(p.returncode, 2)
        text = ""
        if os.path.exists(self.settings):
            with open(self.settings) as fh:
                text = fh.read()
        self.assertNotIn("bad-key", text)

    def test_unset_drops_the_key_and_get_exits_1(self):
        self.st("set", "TAGS=t", check=True)
        self.st("unset", "TAGS", check=True)
        self.assertEqual(self.st("get", "TAGS").returncode, 1)

    def test_require_lists_missing_keys_and_makes_no_cli_call(self):
        self.st("set", "OWNER=me", check=True)
        p = self.st("require", "OWNER", "AZ_SUBSCRIPTION", "DATABRICKS_PROFILE")
        self.assertEqual(p.returncode, 1)
        self.assertEqual(p.stdout.split(), ["AZ_SUBSCRIPTION", "DATABRICKS_PROFILE"])
        self.assertEqual(self.calls(), "")

    def test_require_all_there_exits_0_and_an_old_check_is_noted(self):
        self.st("set", "OWNER=me", "SETTINGS_CHECKED=2020-01-01", check=True)
        p = self.st("require", "OWNER")
        self.assertEqual(p.returncode, 0)
        self.assertIn("last checked 2020-01-01", p.stderr)

    def test_checked_is_today_and_require_is_quiet(self):
        self.st("checked", check=True)
        today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        self.assertEqual(self.out("get", "SETTINGS_CHECKED"), today + "\n")
        self.st("set", "OWNER=me", check=True)
        p = self.st("require", "OWNER")
        self.assertEqual(p.stderr, "")

    def test_env_evals_back_to_the_values_in_bash(self):
        self.st("set", "OWNER=a b", "SOURCE_HOST=podman", check=True)
        env_out = self.out("env")
        r = subprocess.run(["bash", "-c", 'eval "$1"; printf "%s|%s" "$OWNER" "$SOURCE_HOST"', "x", env_out],  # noqa: PLW1510
                           capture_output=True, text=True)
        self.assertEqual(r.stdout, "a b|podman")

    def test_workspaces_numbered_by_cloud_then_name_with_saved_marked(self):
        self.st("set", "WORKSPACES=ws-c", "DATABRICKS_PROFILE=ws-a", check=True)
        rows = self.out("workspaces").splitlines()
        self.assertEqual([r.split("\t")[2] for r in rows], ["ws-a", "ws-c", "ws-b"])
        self.assertTrue(rows[0].startswith("1*\taws\tws-a\tlogin ok"))
        self.assertTrue(rows[1].startswith("2*\taws\tws-c\texpired, covered by account login acct"))
        self.assertTrue(rows[2].startswith("3\tazure\tws-b\texpired; the Azure CLI may cover it"))
        self.assertNotIn("acct", [r.split("\t")[2] for r in rows])   # the account profile is left out

    def test_azure_sorted_by_name_saved_and_default_marked(self):
        self.st("set", "AZ_SUBSCRIPTION=11", check=True)
        self.assertEqual(self.out("azure"), "sub-one\t11\tt1\tsaved\nsub-two\t22\tt2\tdefault\n")

    def test_setup_suggests_only_the_users_groups_and_shows_the_rest(self):
        self.st("set", "AZ_RESOURCE_GROUP=rg-mine", "AZ_SUBSCRIPTION=11", check=True)
        out = self.st("setup", me="me@x.com", check=True).stdout
        self.assertIn("saved: rg-mine, exists, in centralus", out)
        self.assertIn("  rg-mine  centralus", out)
        self.assertIn("  rg-made  eastus", out)
        self.assertNotIn("rg-other", out)
        self.assertNotIn("rg-untagged", out)
        self.assertIn("p1\t111122223333\tus-east-2", out)
        self.assertIn("sub-one\t11\tt1\tsaved", out)
        self.assertIn("ws-a", out)
        self.assertIn("default\tproj-a\tus-central1\tactive", out)
        self.assertIn("other\t?\t?", out)
        self.assertIn("== defaults", out)

    def test_setup_says_a_gone_group_and_suggests_none_without_an_az_user(self):
        self.st("set", "AZ_RESOURCE_GROUP=rg-gone", "AZ_SUBSCRIPTION=11", check=True)
        out = self.st("setup", me="", check=True).stdout
        self.assertIn("saved: rg-gone, NOT found", out)
        self.assertIn("no groups suggested", out)
        self.assertNotIn("rg-untagged", out)

    def test_defaults_one_line_each_then_exceptions(self):
        lines = self.out("defaults").splitlines()
        self.assertTrue(lines and all(x.startswith("- ") for x in lines))
        self.assertTrue(any(x.startswith("- (exception)") for x in lines))

    def test_remove_after_default_and_set(self):
        today = dt.datetime.now(dt.timezone.utc).date()
        self.assertEqual(self.out("remove-after").strip(), (today + dt.timedelta(days=7)).isoformat())
        self.st("set", "REMOVE_AFTER_DAYS=30", check=True)
        self.assertEqual(self.out("remove-after").strip(), (today + dt.timedelta(days=30)).isoformat())


if __name__ == "__main__":
    unittest.main()
