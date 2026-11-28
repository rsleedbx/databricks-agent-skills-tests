"""test_delete.py: sqldb.py's and mi.py's delete, offline, with a fake az on PATH (fake_az.py). FA_TAGS: the resource's
tags as JSON; FA_SHOW=missing: not found; FA_SHOW=error: an expired login. delete removes only a resource tagged
CreatedBy=<this skill>, or one made before that tag (purpose=lfc-test, this owner); not found exits 3, a failed lookup 1,
another's 2, with nothing deleted.

    python3 -I -m unittest -q test_delete.py     (from this folder, or with its path)
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fake_az

MINE = "lfc-source-azure-sql"
SCRIPTS = ("sqldb.py", "mi.py")
BODY = r'''
if line.startswith("account show"):
    print("tenant-1")   # the subscription check every command makes first
elif " show " in line:
    mode = os.environ.get("FA_SHOW", "")
    if mode == "missing":
        sys.stderr.write("ERROR: (ResourceNotFound) not found\n"); sys.exit(3)
    if mode == "error":
        sys.stderr.write("ERROR: AADSTS700082: expired\n"); sys.exit(1)
    print(os.environ.get("FA_TAGS", "null"))
'''


class DeleteTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.t, True)
        os.makedirs(os.path.join(self.t, "bin"))
        fake_az.install(os.path.join(self.t, "bin"), BODY)
        self.base = {**{k: v for k, v in os.environ.items() if not k.startswith(("FA_", "LFC_"))},
                     "PATH": os.path.join(self.t, "bin") + os.pathsep + os.environ.get("PATH", ""),
                     "FA_LOG": os.path.join(self.t, "log"), "LFC_TESTS_SETTINGS": os.path.join(self.t, "settings.env"),
                     "RG": "rg1", "SUBSCRIPTION": "sub1", "SERVER": "srv1", "MI_NAME": "mi1", "VNET": "vnet1",
                     "DB": "db1", "LOCATION": "centralus", "OWNER": "jane"}

    def delete(self, script: str, tags: str | None = None, show: str | None = None):
        """(exit code, output, whether a delete was called)."""
        env = dict(self.base)
        if tags is not None:
            env["FA_TAGS"] = tags
        if show is not None:
            env["FA_SHOW"] = show
        open(env["FA_LOG"], "w").close()
        p = subprocess.run([sys.executable, "-I", os.path.join(HERE, script), "delete"], env=env,
                           capture_output=True, text=True, check=False)
        with open(env["FA_LOG"]) as f:
            log = f.read()
        return p.returncode, p.stdout + p.stderr, " delete " in log

    def test_its_own_is_deleted(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, _, deleted = self.delete(s, tags=f'{{"CreatedBy": "{MINE}"}}')
                self.assertEqual(rc, 0)
                self.assertTrue(deleted)

    def test_made_before_the_tag_for_this_owner_is_deleted(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, _, deleted = self.delete(s, tags='{"purpose": "lfc-test", "owner": "jane"}')
                self.assertEqual(rc, 0)
                self.assertTrue(deleted)

    def test_another_skills_is_not_deleted(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, _, deleted = self.delete(
                    s, tags='{"CreatedBy": "lfc-source-eventhubs", "purpose": "lfc-test", "owner": "jane"}')
                self.assertEqual(rc, 2)
                self.assertFalse(deleted)

    def test_another_owners_is_not_deleted(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, _, deleted = self.delete(s, tags='{"purpose": "lfc-test", "owner": "someone-else"}')
                self.assertEqual(rc, 2)
                self.assertFalse(deleted)

    def test_untagged_is_not_deleted(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, _, deleted = self.delete(s, tags="null")
                self.assertEqual(rc, 2)
                self.assertFalse(deleted)

    def test_not_found_exits_3_and_deletes_nothing(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, out, deleted = self.delete(s, show="missing")
                self.assertEqual(rc, 3)
                self.assertFalse(deleted)
                self.assertIn("nothing to delete", out)

    def test_failed_lookup_exits_1_with_the_error(self):
        for s in SCRIPTS:
            with self.subTest(script=s):
                rc, out, deleted = self.delete(s, show="error")
                self.assertEqual(rc, 1)
                self.assertFalse(deleted)
                self.assertIn("AADSTS", out)


if __name__ == "__main__":
    unittest.main()
