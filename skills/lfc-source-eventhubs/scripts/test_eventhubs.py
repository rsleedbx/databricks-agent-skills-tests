"""test_eventhubs.py: eventhubs.py with a fake az on PATH (fake_az.py, from lfc-source-azure-sql). It logs every call.
FA_GROUP=1: the group exists. FA_NS=<tag>: the namespace exists with that CreatedBy tag ("none": no tag); unset: not found.
Checks that the group is never created, re-tagged or deleted, that a failed lookup isn't read as "nothing there", that a
namespace this script didn't make is left alone by up and down, and that its own is created tagged and deleted. The
source contract it writes is read back with lfc-source-contract's source-env.sh.

    python3 -I -m unittest -q test_eventhubs.py     (from this folder, or with its path)
"""
from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-source-azure-sql", "scripts"))
import fake_az

EVENTHUBS = os.path.join(HERE, "eventhubs.py")
SOURCE_ENV = os.path.join(HERE, "..", "..", "lfc-source-contract", "scripts", "source-env.sh")
BODY = r'''
import json
if line.startswith("group show"):
    sys.exit(0 if os.environ.get("FA_GROUP") else 1)
if "eventhubs namespace show" in line:
    if os.environ.get("FA_LOOKUP_FAILS"):
        sys.stderr.write("ERROR: AADSTS700082: The refresh token has expired\n"); sys.exit(1)
    if os.environ.get("FA_OTHER_SUB"):
        sys.stderr.write("ERROR: (ResourceGroupNotFound) Resource group 'rg1' could not be found.\n"); sys.exit(3)
    ns = os.environ.get("FA_NS", "")
    if not ns:
        sys.stderr.write("ERROR: (ResourceNotFound) The Resource was not found\n"); sys.exit(3)
    print(json.dumps({} if ns == "none" else {"CreatedBy": ns}))
if "authorization-rule keys list" in line:
    print("Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=kafka-test;SharedAccessKey=k")
'''
FA_KEYS = ("FA_GROUP", "FA_NS", "FA_LOOKUP_FAILS", "FA_OTHER_SUB", "AZ_SUBSCRIPTION")


class EventhubsTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.t, True)
        os.makedirs(os.path.join(self.t, "bin"))
        fake_az.install(os.path.join(self.t, "bin"), BODY)
        self.log = os.path.join(self.t, "log")
        self.pw = os.path.join(self.t, "pw")
        self.base = {**{k: v for k, v in os.environ.items() if k not in FA_KEYS and not k.startswith("FA_")},
                     "PATH": os.path.join(self.t, "bin") + os.pathsep + os.environ.get("PATH", ""), "FA_LOG": self.log}

    def eh(self, *args: str, **fa: str):
        """(exit code, output) of eventhubs.py with these arguments and fake-az settings; the calls are in self.calls()."""
        env = dict(self.base)
        env.update(fa)
        open(self.log, "w").close()
        p = subprocess.run([sys.executable, "-I", EVENTHUBS, *args], env=env, capture_output=True, text=True, check=False)
        return p.returncode, p.stdout + p.stderr

    def calls(self) -> list[str]:
        with open(self.log) as f:
            return f.read().splitlines()

    def test_up_group_missing_stops_and_never_creates_it(self):
        rc, out = self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--apply")
        self.assertEqual(rc, 2)
        self.assertIn("never creates one", out)
        self.assertFalse(any("group create" in c or "namespace create" in c for c in self.calls()))

    def test_up_a_namespace_it_did_not_make_is_left_alone(self):
        rc, _ = self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--apply", FA_GROUP="1", FA_NS="none")
        self.assertEqual(rc, 2)
        self.assertFalse(any(x in c for c in self.calls() for x in ("namespace create", "eventhub create", "rule create")))

    def test_up_new_is_created_tagged_and_the_password_file_is_private(self):
        rc, _ = self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--tags", "Owner=a RemoveAfter=2026-11-09",
                        "--apply", FA_GROUP="1")
        calls = self.calls()
        self.assertEqual(rc, 0)
        creates = [c for c in calls if "namespace create" in c]
        self.assertTrue(any("--tags CreatedBy=lfc-source-eventhubs Owner=a RemoveAfter=2026-11-09" in c for c in creates))
        self.assertFalse(any(c.startswith(("group create", "group update", "group delete")) for c in calls))
        self.assertEqual(stat.S_IMODE(os.stat(self.pw).st_mode), 0o600)

    def test_up_new_writes_a_valid_kafka_source_contract_with_no_secret(self):
        self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--apply", FA_GROUP="1")
        sj = os.path.join(self.t, "ns1.source.json")
        p = subprocess.run(["bash", SOURCE_ENV, sj], capture_output=True, text=True, check=False)
        self.assertEqual(p.returncode, 0, p.stderr)
        vals = {}
        for line in p.stdout.splitlines():
            k, v = shlex.split(line)[1].split("=", 1)
            vals[k] = v
        self.assertEqual(vals.get("SRC_ENGINE"), "kafka")
        self.assertEqual(vals.get("SRC_HOST"), "ns1.servicebus.windows.net")
        self.assertEqual(vals.get("SRC_TOPIC"), "tag-observations")
        self.assertEqual(vals.get("SRC_PASSWORD_FILE"), self.pw)
        with open(sj) as f:
            self.assertNotIn("SharedAccessKey=", f.read())

    def test_up_its_own_from_before_is_reused_not_created_again(self):
        rc, out = self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--apply",
                          FA_GROUP="1", FA_NS="lfc-source-eventhubs")
        self.assertEqual(rc, 0)
        self.assertIn("reused", out)
        self.assertFalse(any("namespace create" in c for c in self.calls()))

    def test_az_subscription_goes_on_every_call(self):
        self.eh("up", "rg1", "ns1", "centralus", "--password-file", self.pw, "--apply", FA_GROUP="1", AZ_SUBSCRIPTION="sub-1")
        calls = self.calls()
        self.assertTrue(calls)
        self.assertTrue(all("--subscription sub-1" in c for c in calls))

    def test_down_a_namespace_it_did_not_make_is_not_deleted(self):
        rc, _ = self.eh("down", "rg1", "ns1", "--apply", FA_NS="none")
        self.assertEqual(rc, 2)
        self.assertFalse(any("delete" in c for c in self.calls()))

    def test_down_its_own_namespace_is_deleted_and_never_the_group(self):
        rc, _ = self.eh("down", "rg1", "ns1", "--apply", FA_NS="lfc-source-eventhubs")
        calls = self.calls()
        self.assertEqual(rc, 0)
        self.assertTrue(any("namespace delete" in c for c in calls))
        self.assertFalse(any("group delete" in c for c in calls))

    def test_down_under_the_legacy_tag_is_deleted(self):
        rc, _ = self.eh("down", "rg1", "ns1", "--apply", FA_NS="lfc-kafka-freshness-test")
        self.assertEqual(rc, 0)
        self.assertTrue(any("namespace delete" in c for c in self.calls()))

    def test_down_nothing_there_is_rc_0_and_no_delete(self):
        rc, _ = self.eh("down", "rg1", "ns1", "--apply")
        self.assertEqual(rc, 0)
        self.assertFalse(any("delete" in c for c in self.calls()))

    def test_down_failed_lookup_is_rc_1_with_the_error_and_no_delete(self):
        rc, out = self.eh("down", "rg1", "ns1", "--apply", FA_LOOKUP_FAILS="1", FA_NS="lfc-source-eventhubs")
        self.assertEqual(rc, 1)
        self.assertFalse(any("delete" in c for c in self.calls()))
        self.assertIn("AADSTS", out)

    def test_down_group_not_in_this_subscription_is_not_nothing_to_delete(self):
        rc, out = self.eh("down", "rg1", "ns1", "--apply", FA_OTHER_SUB="1")
        self.assertEqual(rc, 1)
        self.assertFalse(any("delete" in c for c in self.calls()))
        self.assertIn("check the subscription", out)


if __name__ == "__main__":
    unittest.main()
