"""lfc_source_contract.slot offline: the ssh words (your key, or the admin login as an owner), ssh's own failures tried
again, a refused key not, words checked before ssh, the contract with and without a connection. End to end through a fake
ssh: lfc-source-cloud-vm's test-slot-source.sh."""
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lfc_source_contract import slot

UP = {"contract": 1, "name": "pg1", "engine": "postgres", "host": "203.0.113.10", "port": 30000, "user": "lfc_user",
      "password": "Lfc1pw", "database": "postgres"}


def done(rc, out="", err=""):
    return subprocess.CompletedProcess([], rc, out, err)


class Words(unittest.TestCase):
    def test_own_key_or_admin_as_owner(self):
        s = slot.Slot("203.0.113.10", "pg1", key="/k")
        self.assertEqual(s.words()[-1], "slot@203.0.113.10")
        a = slot.Slot("203.0.113.10", "pg1", owner="alice", admin_key="/a")
        self.assertEqual(a.words()[-5:], ["azureuser@203.0.113.10", "sudo", "/usr/local/sbin/lfc-slot", "--owner", "alice"])
        self.assertEqual(a.words()[1:3], ["-i", "/a"])

    def test_bad_words_refused_before_ssh(self):
        for bad in (lambda: slot.Slot("h;x", "pg1"), lambda: slot.Slot("h", "a b"),
                    lambda: slot.Slot("h", "pg1", owner="x;y", admin_key="/a")):
            with self.assertRaises(slot.Fail) as x:
                bad()
            self.assertEqual(x.exception.rc, 2)
        with mock.patch.object(slot.subprocess, "run") as run, self.assertRaises(slot.Fail):
            slot.Slot("h", "pg1").run(["sql", "pg1", "$(id)"])
        run.assert_not_called()


class Retry(unittest.TestCase):
    def test_ssh_failure_retried_then_answer(self):
        s = slot.Slot("h", "pg1")
        with mock.patch.object(slot.subprocess, "run", side_effect=[done(255, err="timed out"), done(0, "1\n")]) as run, \
                mock.patch.object(slot.time, "sleep"), mock.patch("sys.stderr"):
            self.assertEqual(s.sql("SELECT 1;"), "1\n")
        self.assertEqual(run.call_count, 2)

    def test_refused_key_not_retried(self):
        s = slot.Slot("h", "pg1")
        with mock.patch.object(slot.subprocess, "run", return_value=done(255, err="Permission denied")) as run:
            with self.assertRaises(RuntimeError):
                s.sql("SELECT 1;")
        self.assertEqual(run.call_count, 1)


class SqlArgs(unittest.TestCase):
    def test_database_first_client_flags_ignored(self):
        env = {"SLOT_HOST": "h", "SLOT_NAME": "pg1"}
        with mock.patch.dict(os.environ, env), mock.patch.object(slot.Slot, "run", return_value=done(0)) as run, \
                mock.patch.object(slot.sys, "stdin", mock.Mock(read=lambda: "SELECT 1;")):
            slot.slot_sql(["tpch", "-F", "|"])
            slot.slot_sql([])
            slot.slot_sql(["tpch", "--env"])
        self.assertEqual(run.call_args_list[0].args[0], ["sql", "pg1", "tpch"])
        self.assertEqual(run.call_args_list[1].args[0], ["sql", "pg1"])
        self.assertEqual(run.call_args_list[2].args[0], ["info", "pg1"])


class Contract(unittest.TestCase):
    def test_connection_keeps_the_password_out(self):
        s = slot.Slot("203.0.113.10", "pg1")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SLOT_KEY", None)
            c = slot.contract(UP, s, connection="me_pg1")
            p = slot.contract(UP, s)
        self.assertNotIn("password", c)
        self.assertEqual(c["connection"], "me_pg1")
        self.assertEqual(p["password"], "Lfc1pw")
        self.assertTrue(c["sql_cmd"].startswith("env SLOT_HOST=203.0.113.10 SLOT_NAME=pg1 python3 -I "))
        self.assertTrue(c["sql_cmd"].endswith("slot.py sql"))
        self.assertEqual(c["database"], "postgres")


if __name__ == "__main__":
    unittest.main()
