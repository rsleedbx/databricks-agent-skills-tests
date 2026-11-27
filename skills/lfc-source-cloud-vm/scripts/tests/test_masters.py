"""lfc_vm.masters offline: the SAS file's checks, the engine beside a master, the JuiceFS staging wait, a build's
refusals before it starts anything, and the masters words through the cli. Export and import against a Blob-style server
are in test-slot-masters.sh."""
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lfc_vm import cli, masters
from lfc_vm.host import Fail


class Sas(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.p = os.path.join(self.d, "sas")

    def test_private_file_only(self):
        Path(self.p).write_text("?sv=1&sig=x\n")
        os.chmod(self.p, 0o644)
        with self.assertRaises(Fail) as x:
            masters.Blob("https://a/b", self.p)
        self.assertIn("chmod 600", x.exception.msg)
        os.chmod(self.p, 0o600)
        self.assertEqual(masters.Blob("https://a/b/", self.p).sas, "sv=1&sig=x")

    def test_empty_or_missing(self):
        with self.assertRaises(Fail):
            masters.Blob("https://a/b", self.p)
        Path(self.p).write_text(" \n")
        os.chmod(self.p, 0o600)
        with self.assertRaises(Fail) as x:
            masters.Blob("https://a/b", self.p)
        self.assertIn("empty", x.exception.msg)


class EngineOf(unittest.TestCase):
    def test_file_then_setting_then_refused(self):
        d = tempfile.mkdtemp()
        os.makedirs(f"{d}/masters/m1")
        Path(d, "masters", "m1.engine").write_text("mysql\n")
        self.assertEqual(masters.engine_of(d, "m1"), "mysql")
        with mock.patch.dict(os.environ, {"MASTER_ENGINES": "m2=oracle,m3=db2"}):
            self.assertEqual(masters.engine_of(d, "m2"), "oracle")
        with self.assertRaises(Fail):
            masters.engine_of(d, "m4")


class Staging(unittest.TestCase):
    def test_a_file_in_rawstaging_waits(self):
        d = tempfile.mkdtemp()
        os.makedirs(f"{d}/vol/rawstaging/chunks")
        self.assertFalse(masters.staging(d))
        Path(d, "vol/rawstaging/chunks/0_1").write_text("x")
        self.assertTrue(masters.staging(d))


class BuildRefusals(unittest.TestCase):
    def test_unknown_engine_missing_env_existing_master(self):
        d = tempfile.mkdtemp()
        with self.assertRaises(Fail) as x:
            masters.Build("sqlite", "m", [])
        self.assertEqual(x.exception.rc, 2)
        env = {"SECRETS": f"{d}/s", "OUT": f"{d}/out", "DATA": f"{d}/data"}
        with mock.patch.dict(os.environ, env):
            b = masters.Build("postgres", "m", [])
            self.assertEqual(b.wl, list(masters.WORKLOADS))
            with self.assertRaises(Fail) as x:
                b.run()
            self.assertIn("missing", x.exception.msg)
            os.makedirs(f"{d}/s")
            Path(d, "s", "postgres.env").write_text("POSTGRES_PASSWORD=x\n")
            os.makedirs(f"{d}/out/m")
            with self.assertRaises(Fail) as x:
                b.run()
            self.assertIn("exists", x.exception.msg)


class Cli(unittest.TestCase):
    def test_masters_words_and_usage(self):
        err = io.StringIO()
        with redirect_stderr(err), mock.patch.object(sys, "argv", ["lfc-vm"]):
            self.assertEqual(cli.main(["masters"]), 2)
            self.assertEqual(cli.main(["masters", "export"]), 1)       # no MASTERS_URL
        self.assertIn("masters: MASTERS_URL=", err.getvalue())


if __name__ == "__main__":
    unittest.main()
