"""test_sync.py: sync.py and inventory.py on a fake Rest (no workspace): the TLS options per row, the empty-column split,
created / updated / a failed read, a missing password file, the failure classes, and inventory's failed-list line.
Run: python3 -m unittest -q skills/lfc-connection-sync/tests/test_sync.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "scripts")
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-rest", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "secret-handling", "scripts"))
import inventory
import sync
from lfc_rest import Rest


class Fake:
    """A Rest whose connection exists (or not), recording each write."""

    def __init__(self, exists: bool, fail_write: str = ""):
        self.exists, self.fail_write, self.writes = exists, fail_write, []

    def __call__(self, method, path, query, body, **kw):
        if method == "GET":
            if not self.exists:
                raise RuntimeError("RESOURCE_DOES_NOT_EXIST: connection not found")
            return {"name": path.rsplit("/", 1)[-1]}
        if self.fail_write:
            raise RuntimeError(self.fail_write)
        self.writes.append((method, path, body))
        return {}


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.pwfile = os.path.join(self.dir, "pg.pw")
        with open(self.pwfile, "w") as fh:
            fh.write("secret-value\n")
        self.keep = mock.patch.object(sync.subprocess, "run")
        self.run = self.keep.start()
        self.run.return_value = mock.Mock(stdout="  kept catalog\n", stderr="", returncode=0)

    def tearDown(self):
        self.keep.stop()

    def row(self, **over):
        base = {"profile": "ws", "name": "conn1", "connection_type": "POSTGRESQL", "host": "h", "port": "5432",
                "user": "u", "password_file": self.pwfile, "extra": ""}
        base.update(over)
        return base

    def test_tls_default_and_a_verified_certificate_replaces_it(self):
        opts = sync.options_for(self.row(), "pw")
        self.assertEqual(opts["trustServerCertificate"], "true")
        opts = sync.options_for(self.row(extra='{"userProvidedServerCertificate": "PEM"}'), "pw")
        self.assertNotIn("trustServerCertificate", opts)

    def test_parse_keeps_empty_columns(self):
        path = os.path.join(self.dir, "t.tsv")
        with open(path, "w") as fh:
            fh.write(f"# comment\nws\tc\tMYSQL\th\t3306\t\t{self.pwfile}\n")
        rows = list(sync.parse_rows(path))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user"], "")
        self.assertEqual(rows[0]["password_file"], self.pwfile)

    def test_missing_connection_is_created_and_keep_catalogs_lines_follow(self):
        api = Rest(Fake(exists=False))
        out = sync.sync_row(api, self.row())
        self.assertEqual(out.splitlines()[0], "ws conn1: created")
        self.assertIn("  kept catalog", out)
        self.assertEqual(api._do.writes[0][:2], ("POST", sync.CONNECTIONS))

    def test_existing_connection_is_updated(self):
        api = Rest(Fake(exists=True))
        self.assertEqual(sync.sync_row(api, self.row()).splitlines()[0], "ws conn1: updated")
        self.assertEqual(api._do.writes[0][0], "PATCH")

    def test_a_refused_write_is_classed(self):
        api = Rest(Fake(exists=True, fail_write="PERMISSION_DENIED: user does not have CREATE CONNECTION"))
        out = sync.sync_row(api, self.row())
        self.assertIn("FAILED no_create_connection", out)

    def test_missing_password_file_fails_the_row_without_a_call(self):
        api = Rest(Fake(exists=True))
        out = sync.sync_row(api, self.row(password_file=os.path.join(self.dir, "none.pw")))
        self.assertIn("FAILED missing_password", out)
        self.assertEqual(api._do.writes, [])

    def test_failure_classes(self):
        self.assertEqual(sync.failure_class("invalid access token"), "auth")
        self.assertEqual(sync.failure_class("quota exceeded"), "connection_limit")
        self.assertEqual(sync.failure_class("odd"), "other")


class InventoryTest(unittest.TestCase):
    def test_failed_list_prints_the_login_line(self):
        with mock.patch.object(inventory.Rest, "connect", side_effect=RuntimeError("no login")), \
                mock.patch("builtins.print") as p:
            self.assertEqual(inventory.main(["ws-x"]), 0)
        p.assert_called_with("ws-x\t(failed to list: check the profile's login)")


if __name__ == "__main__":
    unittest.main()
