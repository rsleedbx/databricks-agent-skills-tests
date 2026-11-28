"""lfc-source-podman offline: the host helpers (command forms, the report), the AG names and env file, the ag scripts'
refusals and their container CLI calls (a fake podman that records its arguments), the Lima and ssh.config parsing."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import shutil
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ag
from ag import Ag
from podman_host import Fail, Host, Report


def load(file: str):
    """A script whose file name has dashes (ag-down.py): loaded by path, its main() and helpers."""
    spec = importlib.util.spec_from_file_location(file.replace("-", "_").removesuffix(".py"), HERE / file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


FAKE_PODMAN = """#!/usr/bin/env python3
import json, os, sys
stdin = sys.stdin.read() if os.environ.get("FAKE_READ") else ""
with open(os.environ["FAKE_LOG"], "a") as f:
    f.write(json.dumps({"argv": sys.argv[1:], "stdin": stdin}) + "\\n")
sys.exit(int(os.environ.get("FAKE_RC", "0")))
"""


class HostHelpers(unittest.TestCase):
    def test_host_forms(self):
        self.assertEqual(Host("").argv("id -un"), ["bash", "-c", "id -un"])
        self.assertEqual(Host("limactl shell vm1 --").argv("echo hi"),
                         ["limactl", "shell", "vm1", "--", "bash", "-c", "echo hi"])
        # ssh joins its words into one remote command: the command goes quoted, as one word
        self.assertEqual(Host("ssh -i k u@h").argv("echo 'a b'"),
                         ["ssh", "-i", "k", "u@h", "bash -c 'echo '\"'\"'a b'\"'\"''"])

    def test_report_counts_fails_and_pads(self):
        rep = Report(width=10)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rep.row("PASS", "step", "ok")
            rep.row("FAIL", "other", "no")
        self.assertEqual(rep.fails, 1)
        self.assertEqual(buf.getvalue(), "PASS  step       ok\nFAIL  other      no\n")

    def test_fail_carries_message_and_code(self):
        e = Fail("bad", 2)
        self.assertEqual((e.msg, e.rc), ("bad", 2))


class AgNames(unittest.TestCase):
    def test_valid_name(self):
        self.assertTrue(ag.valid_name("lfc-ag_1"))
        self.assertFalse(ag.valid_name(""))
        self.assertFalse(ag.valid_name("a b"))
        self.assertFalse(ag.valid_name("a;b"))

    def test_defaults_and_overrides(self):
        a = Ag({"HOME": "/h"})
        self.assertEqual((a.name, a.dir, a.pm), ("lfc-ag", "/h/lfc-ag", "podman"))
        b = Ag({"HOME": "/h", "AG_NAME": "x", "AG_DIR": "/d", "PODMAN": "docker"})
        self.assertEqual((b.name, b.dir, b.pm), ("x", "/d", "docker"))

    def test_env_file_parse_and_missing(self):
        with tempfile.TemporaryDirectory() as d:
            Path(d, "ag.env").write_text("SA_PW=a1\nLFC_PW=b2\nCERT_PW=c3\n")
            self.assertEqual(Ag({"AG_DIR": d}).env(), {"SA_PW": "a1", "LFC_PW": "b2", "CERT_PW": "c3"})
        with self.assertRaises(Fail):
            Ag({"AG_DIR": "/no/such/dir"}).env()


class AgUp(unittest.TestCase):
    def test_passwords_and_the_env_file(self):
        up = load("ag-up.py")
        self.assertRegex(up.password(), r"^Lf[A-Za-z0-9]{20}#9$")
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ag.env")
            up.make_env(path)
            first = Path(path).read_text()
            up.make_env(path)   # kept: never replaced once made
            self.assertEqual(Path(path).read_text(), first)
            self.assertEqual(sorted(k for k, _ in (x.split("=", 1) for x in first.splitlines())),
                             ["CERT_PW", "LFC_PW", "SA_PW"])
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)

    def test_the_sql_it_writes(self):
        up = load("ag-up.py")
        script = up.db_script("lfcag", ["t1", "t2"], "3000")
        self.assertIn("CREATE TABLE dbo.t1", script)
        self.assertIn("SELECT TOP (3000)", script)
        self.assertTrue(script.rstrip().endswith("WITH INIT, FORMAT;"))
        ag_sql = up.ag_primary_sql("lfc-ag", "lfcag", "lfcagp", "lfcags")
        self.assertIn("N'lfcags' WITH (ENDPOINT_URL = N'tcp://lfcags:5022'", ag_sql)
        self.assertIn("CLUSTER_TYPE = NONE", ag_sql)
        self.assertIn("'$(LFC_PW)'", up.login_sid_sql("lfc_user"))   # sqlcmd substitutes it, not the shell

    def test_refusals_before_any_container_call(self):
        up = load("ag-up.py")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(up.main([]), 1)
        with self.assertRaises(Fail) as e:
            up.main(["/no/such/utility.sql"])
        self.assertEqual(e.exception.rc, 2)
        with tempfile.NamedTemporaryFile() as util, mock.patch.dict(os.environ, {"AG_NAME": "a b"}), \
                self.assertRaises(Fail) as e:
            up.main([util.name])
        self.assertIn("names: letters", e.exception.msg)


class FakePodman:
    """A fake container CLI on disk: each call's argv (and stdin, when FAKE_READ is set) goes to a log; exit FAKE_RC."""

    def __init__(self, d: str):
        self.path = os.path.join(d, "podman")
        self.log = os.path.join(d, "calls.jsonl")
        with open(self.path, "w") as f:
            f.write(FAKE_PODMAN)
        os.chmod(self.path, stat.S_IRWXU)

    def calls(self):
        if not os.path.exists(self.log):
            return []
        with open(self.log) as f:
            return [json.loads(x) for x in f if x.strip()]


class AgScripts(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.fake = FakePodman(self.d)
        self.env = {"PODMAN": self.fake.path, "FAKE_LOG": self.fake.log, "HOME": self.d}
        self.ag_down, self.ag_sql = load("ag-down.py"), load("ag-sql.py")

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def state_dir(self, name: str, env_text: str | None = None) -> str:
        path = os.path.join(self.d, name)
        os.makedirs(path)
        if env_text is not None:
            Path(path, "ag.env").write_text(env_text)
        return path

    def test_ag_down_removes_the_containers_and_the_state_dir(self):
        state = self.state_dir("state")
        Path(state, "x").write_text("data")
        buf = io.StringIO()
        with mock.patch.dict(os.environ, {**self.env, "AG_NAME": "t-ag", "AG_DIR": state}), redirect_stdout(buf):
            self.assertEqual(self.ag_down.main([]), 0)
        argvs = [c["argv"] for c in self.fake.calls()]
        self.assertIn(["rm", "-f", "t-ag-p"], argvs)
        self.assertIn(["rm", "-f", "t-ag-s"], argvs)
        self.assertIn(["network", "rm", "t-ag-net"], argvs)
        self.assertIn(["unshare", "rm", "-rf", state], argvs)
        self.assertIn("removed t-ag-p\n", buf.getvalue())
        self.assertIn(f"removed {state}\n", buf.getvalue())

    def test_ag_down_refuses_a_top_level_dir_and_keeps_with_keep_data(self):
        with mock.patch.dict(os.environ, {**self.env, "AG_NAME": "t-ag", "AG_DIR": "/"}):
            with self.assertRaises(Fail) as e:
                self.ag_down.main([])
            self.assertEqual(e.exception.rc, 2)
            self.assertEqual(self.ag_down.main(["--keep-data"]), 0)
        self.assertNotIn(["unshare", "rm", "-rf", "/"], [c["argv"] for c in self.fake.calls()])

    def test_ag_down_refuses_a_bad_name(self):
        with mock.patch.dict(os.environ, {**self.env, "AG_NAME": "a b"}), self.assertRaises(Fail) as e:
            self.ag_down.main([])
        self.assertEqual(e.exception.rc, 2)

    def test_ag_sql_sqlcmd_mode_passes_the_password_and_the_flags(self):
        state = self.state_dir("st", "SA_PW=s3cret\n")
        with mock.patch.dict(os.environ, {**self.env, "AG_NAME": "t-ag", "AG_DIR": state}):
            self.assertEqual(self.ag_sql.main(["p", "--sqlcmd", "-Q", "select 1"]), 0)
        argv = self.fake.calls()[-1]["argv"]
        self.assertEqual(argv[:5], ["exec", "-i", "-e", "SQLCMDPASSWORD=s3cret", "t-ag-p"])
        self.assertEqual(argv[-2:], ["-Q", "select 1"])   # the flags follow the sqlcmd words, as given

    def test_ag_sql_xe_query_takes_the_database_since_and_kind(self):
        state = self.state_dir("st2", "SA_PW=x\n")
        with mock.patch.dict(os.environ, {**self.env, "AG_NAME": "t-ag", "AG_DIR": state, "FAKE_READ": "1"}):
            self.assertEqual(self.ag_sql.main(["s", "lfcag", "--xe", "2026-11-01T10:00:00", "writes"]), 0)
            with self.assertRaises(Fail) as e:
                self.ag_sql.main(["s", "--xe", "bad;since"])
            self.assertEqual(e.exception.rc, 2)
        last = self.fake.calls()[-1]
        self.assertEqual(last["argv"][last["argv"].index("-d") + 1], "lfcag")
        self.assertIn("WHERE ts >= '2026-11-01T10:00:00'", last["stdin"])
        self.assertIn("('writes' = 'errors'", last["stdin"])

    def test_ag_sql_node_and_usage(self):
        self.assertEqual(self.ag_sql.main([]), 1)
        self.assertEqual(self.ag_sql.main(["q"]), 2)


class EnsureHelpers(unittest.TestCase):
    def test_lima_list_parse(self):
        el = load("ensure-lima-podman.py")
        self.assertEqual(el.instances("vm1 Running\nvm2 Stopped\nvm1 Stale\nbare\n"),
                         {"vm1": "Running", "vm2": "Stopped", "bare": ""})

    def test_ssh_config_settings(self):
        es = load("ensure-podman-socket.py")
        with tempfile.TemporaryDirectory() as d:
            cfg = os.path.join(d, "ssh.config")
            Path(cfg).write_text('Host lima-x\n  Port 60022\n  User me\n  IdentityFile "/k/id"\n  Port 1\n')
            self.assertEqual(es.ssh_settings(cfg), ("60022", "me", "/k/id"))
        self.assertEqual(es.ssh_settings("/no/such/config"), ("", "", ""))


if __name__ == "__main__":
    unittest.main()
