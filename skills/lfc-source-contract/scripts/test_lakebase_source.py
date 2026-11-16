"""Offline tests of lakebase-source.py with a mocked workspace and database: a new login and connection, an existing
login and connection (ALTER, update), password logins turned on only when off, no read-write endpoint, the connection
looked up before the password changes, a failed connection update, a missing or new project, and that the password is
never printed or written to the contract.

  uv run --with databricks-sdk --with 'psycopg[binary]' python -m unittest test_lakebase_source.py
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import EndpointType

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("lakebase_source", os.path.join(HERE, "lakebase-source.py"))
ls = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ls)


def workspace(*, project=True, native=True, connection=False):
    w = mock.MagicMock()
    if project:
        w.postgres.get_project.return_value.status.enable_pg_native_login = native
        w.postgres.get_project.return_value.status.default_branch = "projects/p/branches/production"
    else:
        w.postgres.get_project.side_effect = NotFound("no project")
    ro = mock.MagicMock(); ro.status.endpoint_type = EndpointType.ENDPOINT_TYPE_READ_ONLY
    rw = mock.MagicMock(); rw.name = "projects/p/branches/production/endpoints/primary"
    rw.status.endpoint_type = EndpointType.ENDPOINT_TYPE_READ_WRITE; rw.status.hosts.host = "ep-1.example.com"
    w.postgres.list_endpoints.return_value = [ro, rw]
    w.postgres.generate_database_credential.return_value.token = "oauth-token"
    w.current_user.me.return_value.user_name = "owner@example.com"
    if not connection:
        w.connections.get.side_effect = NotFound("no connection")
    return w


def database(role_exists):
    pg = mock.MagicMock()
    pg.__enter__.return_value = pg
    pg.execute.return_value.fetchone.return_value = (1,) if role_exists else None
    return pg


class LakebaseSource(unittest.TestCase):
    def run_main(self, w, pg, *args):
        out = io.StringIO()
        with mock.patch.object(ls, "WorkspaceClient", return_value=w), \
             mock.patch.object(ls.psycopg, "connect", return_value=pg) as connect, \
             mock.patch.object(ls.secrets, "token_urlsafe", return_value="SECRET-PW"), \
             mock.patch.object(sys, "argv", ["lakebase-source.py", *args]), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            rc = ls.main()
        return rc, out.getvalue(), connect

    def statements(self, pg):
        return [c.args[0].as_string(None) if hasattr(c.args[0], "as_string") else c.args[0] for c in pg.execute.call_args_list]

    def test_new_login_and_connection(self):
        w, pg = workspace(), database(role_exists=False)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "source.json")
            rc, out, connect = self.run_main(w, pg, "--project", "p", "--role", "lfc_cdc", "--connection", "c1",
                                             "--database", "orders", "--profile", "prof", "--contract", path)
            self.assertEqual(rc, 0)
            with open(path) as f:
                text = f.read()
            contract = json.loads(text)
        self.assertTrue(any(s.startswith('CREATE ROLE "lfc_cdc" WITH LOGIN REPLICATION PASSWORD') for s in self.statements(pg)))
        self.assertEqual(connect.call_args.kwargs["host"], "ep-1.example.com")        # the read-write endpoint
        self.assertEqual(connect.call_args.kwargs["password"], "oauth-token")
        w.connections.create.assert_called_once()
        self.assertEqual(w.connections.create.call_args.kwargs["options"]["password"], "SECRET-PW")
        w.postgres.update_project.assert_not_called()                                   # password logins already on
        self.assertEqual(contract["user"], "lfc_cdc"); self.assertEqual(contract["database"], "orders")
        self.assertEqual(contract["connection"], "c1"); self.assertEqual(contract["host"], "ep-1.example.com")
        self.assertTrue(contract["sql_cmd"].startswith("env LB_PROJECT=p LB_PROFILE=prof "))
        self.assertTrue(contract["sql_cmd"].endswith("lakebase-sql.sh"))
        self.assertNotIn("SECRET-PW", text); self.assertNotIn("SECRET-PW", out); self.assertNotIn("oauth-token", out)

    def test_existing_login_and_connection_native_off(self):
        w, pg = workspace(native=False, connection=True), database(role_exists=True)
        rc, out, _ = self.run_main(w, pg, "--project", "p", "--role", "lfc_cdc", "--connection", "c1")
        self.assertEqual(rc, 0)
        self.assertTrue(any(s.startswith('ALTER ROLE "lfc_cdc"') for s in self.statements(pg)))
        w.connections.update.assert_called_once(); w.connections.create.assert_not_called()
        w.postgres.update_project.assert_called_once()
        self.assertIn("password logins turned on", out); self.assertNotIn("SECRET-PW", out)

    def test_no_read_write_endpoint(self):
        w, pg = workspace(), database(role_exists=False)
        w.postgres.list_endpoints.return_value = w.postgres.list_endpoints.return_value[:1]      # the read-only one only
        rc, out, connect = self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c")
        self.assertEqual(rc, 2); self.assertIn("no read-write endpoint", out)
        connect.assert_not_called(); w.postgres.generate_database_credential.assert_not_called()
        w.connections.create.assert_not_called(); w.connections.update.assert_not_called()

    def test_connection_lookup_fails_before_the_password_changes(self):
        w, pg = workspace(), database(role_exists=True)
        w.connections.get.side_effect = PermissionError("no USE CONNECTION")
        with self.assertRaises(PermissionError):
            self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c")
        pg.execute.assert_not_called()

    def test_failed_connection_update_says_rerun(self):
        w, pg = workspace(connection=True), database(role_exists=True)
        w.connections.update.side_effect = RuntimeError("network")
        rc, out, _ = self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c")
        self.assertEqual(rc, 1); self.assertIn("has a new password but connection c wasn't updated", out)
        self.assertIn("rerun", out); self.assertNotIn("SECRET-PW", out)

    def test_missing_project(self):
        w, pg = workspace(project=False), database(role_exists=False)
        rc, out, connect = self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c")
        self.assertEqual(rc, 2); self.assertIn("--create-project", out)
        connect.assert_not_called(); w.postgres.create_project.assert_not_called()

    def test_create_project(self):
        w, pg = workspace(project=False), database(role_exists=False)
        w.postgres.create_project.return_value.wait.return_value.status.enable_pg_native_login = True
        w.postgres.create_project.return_value.wait.return_value.status.default_branch = "production"
        rc, _out, _ = self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c", "--create-project")
        self.assertEqual(rc, 0)
        spec = w.postgres.create_project.call_args.args[0].spec
        self.assertTrue(spec.enable_pg_native_login); self.assertEqual(spec.pg_version, 17)
        w.postgres.list_endpoints.assert_called_with("projects/p/branches/production")   # a bare branch id expanded


if __name__ == "__main__":
    unittest.main()
