"""lfc_vm offline: the limits file, the env file, the usage hours, the lock's bound, the engine table, and the admin
password change when the env file can't be replaced. The commands against podman are in test-slot-limits.sh and
test-slot-masters.sh (fakes on PATH)."""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lfc_vm import engines, slots
from lfc_vm.host import Fail, Lock


class Files(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def test_limit_owner_then_default_then_fallback(self):
        p = os.path.join(self.d, "limits")
        Path(p).write_text("default max_running=1 hours_per_week=29\nalice max_running=2 max_running_x=9\n")
        self.assertEqual(slots.limit(p, "alice", "max_running", "5"), "2")
        self.assertEqual(slots.limit(p, "alice", "hours_per_week", ""), "29")
        self.assertEqual(slots.limit(p, "carol", "max_running", "5"), "1")
        self.assertEqual(slots.limit(p + ".none", "carol", "max_running", "5"), "5")

    def test_env_file(self):
        p = os.path.join(self.d, "env")
        Path(p).write_text("# a comment\nexport HOST=10.0.0.4\nROOT='/mnt/x'\nCPUS_PER_DB=1  # personal VM\n")
        self.assertEqual(slots.read_env_file(p), {"HOST": "10.0.0.4", "ROOT": "/mnt/x", "CPUS_PER_DB": "1"})

    def test_lock_waits_then_fails(self):
        p = os.path.join(self.d, "lock")
        with Lock(p, 5), self.assertRaises(Fail) as x, Lock(p, 0.6):
            pass
        self.assertIn("still holds", x.exception.msg)
        with Lock(p, 0.1):      # released: free again
            pass

    def test_lock_not_inherited_by_a_child(self):
        p = os.path.join(self.d, "lock")
        with Lock(p, 1):
            child = subprocess.Popen(["sleep", "5"])
        try:
            with Lock(p, 0.5):  # the child still runs, and doesn't hold it
                pass
        finally:
            child.kill()


class Logs(unittest.TestCase):
    def test_logs_reads_the_containers_stderr_too(self):
        from lfc_vm.host import Host
        d = tempfile.mkdtemp()
        Path(d, "podman").write_text("#!/bin/sh\necho out\necho 'DATABASE IS READY TO USE' >&2\n")
        os.chmod(os.path.join(d, "podman"), 0o755)
        with mock.patch.dict(os.environ, {"PATH": d + os.pathsep + os.environ["PATH"]}):
            self.assertIn("DATABASE IS READY TO USE", Host().logs("c"))


class Stream(unittest.TestCase):
    def test_sql_streams_stdin_with_oracles_head_and_tail(self):
        from lfc_vm.host import Host
        d = tempfile.mkdtemp()
        Path(d, "podman").write_text("#!/bin/sh\ncat > \"$FAKE_OUT\"\n")
        os.chmod(os.path.join(d, "podman"), 0o755)
        out, src = os.path.join(d, "got"), os.path.join(d, "in")
        Path(src).write_bytes(b"SELECT 1 FROM dual;\n\xff")
        env = {"PATH": d + os.pathsep + os.environ["PATH"], "FAKE_OUT": out}
        with mock.patch.dict(os.environ, env), open(src, "rb") as f, \
                mock.patch.object(sys, "stdin", mock.Mock(buffer=f)):
            self.assertEqual(Host().exec_stream("c", ["sqlplus"], head=b"H\n", tail=b"\nT\n"), 0)
        self.assertEqual(Path(out).read_bytes(), b"H\nSELECT 1 FROM dual;\n\xff\nT\n")


class Settled(unittest.TestCase):
    def test_a_container_up_over_30_minutes_needs_no_log_line(self):
        h = mock.Mock()
        sl = slots.Slots("alice", mock.Mock(), host=h)
        old = (slots.dt.datetime.now(slots.dt.timezone.utc) - slots.dt.timedelta(hours=2))
        h.inspect.return_value = old.strftime("%Y-%m-%d %H:%M:%S.123456789 +0000 UTC")   # podman's StartedAt
        self.assertTrue(sl.settled("c"))
        h.inspect.return_value = slots.dt.datetime.now(slots.dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertFalse(sl.settled("c"))
        h.inspect.return_value = ""
        self.assertFalse(sl.settled("c"))


class Usage(unittest.TestCase):
    def test_hours_clip_to_since_and_count_an_open_up(self):
        ev = [{"ts": "2026-11-01T00:00:00Z", "owner": "a", "name": "x", "event": "up"},
              {"ts": "2026-11-01T03:00:00Z", "owner": "a", "name": "x", "event": "park"},
              {"ts": "2026-11-01T05:00:00Z", "owner": "a", "name": "x", "event": "up"},
              {"ts": "2026-11-01T06:00:00Z", "owner": "b", "name": "y", "event": "up"}]
        r = slots.usage_report(ev, "2026-11-01T01:00:00Z", "2026-11-01T06:30:00Z", "a", True)
        self.assertEqual(r, [{"owner": "a", "since": "2026-11-01", "hours": 3.5,
                              "databases": [{"name": "x", "hours": 3.5}]},
                             {"owner": "b", "since": "2026-11-01", "hours": 0.5,
                              "databases": [{"name": "y", "hours": 0.5}]}])
        self.assertEqual([o["owner"] for o in slots.usage_report(ev, "2026-11-01T01:00:00Z",
                                                                  "2026-11-01T06:30:00Z", "a", False)], ["a"])
        self.assertEqual(slots.tenth(2.0), 2)
        self.assertEqual(slots.tenth(0.25), 0.3)


class Engines(unittest.TestCase):
    def test_every_engine_has_a_login_and_a_probe(self):
        for e in engines.ENGINES.values():
            self.assertIn("lfc_user", engines.login_sql(e, "p"))
            self.assertTrue(engines.probe(e).endswith("\n"))
            self.assertEqual(bool(e.admin_key), bool(e.env_new), e.name)

    def test_login_password_quoted(self):
        self.assertIn("PASSWORD 'a''b'", engines.login_sql(engines.get("postgres"), "a'b"))

    def test_oracle_input_stops_at_an_error(self):
        self.assertEqual(engines.admin_input(engines.get("oracle"), "SELECT 1 FROM dual;"),
                         "whenever sqlerror exit failure\nSELECT 1 FROM dual;\nexit\n")

    def test_unknown_engine(self):
        with self.assertRaises(KeyError):
            engines.get("sqlite")


class Rotate(unittest.TestCase):
    def test_env_file_not_replaced_keeps_the_new_password(self):
        d = Path(tempfile.mkdtemp())
        env = d / "pg5.env"
        env.write_text("POSTGRES_PASSWORD=old\n")
        sl = slots.Slots("alice", mock.Mock(), host=mock.Mock())
        sl.rotate_change = lambda e, c, old, new: True
        real = os.replace

        def replace(src, dst):
            if str(dst) == str(env):
                raise OSError("read-only")
            return real(src, dst)

        with mock.patch.object(slots.os, "replace", replace), mock.patch.object(slots, "say") as said:
            self.assertFalse(sl.rotate_admin(engines.get("postgres"), "c", env))
        kept = [p for p in d.iterdir() if p.name.startswith("pg5.env.") and p.name != "pg5.env.pending"]
        self.assertEqual(len(kept), 1)
        self.assertIn("the new admin password is in", said.call_args[0][0])
        self.assertNotEqual(slots.read_env_file(str(kept[0]))["POSTGRES_PASSWORD"], "old")
        self.assertEqual(env.read_text(), "POSTGRES_PASSWORD=old\n")


if __name__ == "__main__":
    unittest.main()
