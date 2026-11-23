"""Offline tests of lakebase-source.py with a mocked workspace and database: a new login and connection, an existing
login and connection (ALTER, update), password logins turned on only when off, no read-write endpoint, the connection
looked up before the password changes, a failed connection update, a missing or new project, setup() called as a notebook would
(its result and errors), and that the password is never printed or written to the contract.

  uv run --no-project --with databricks-sdk --with 'psycopg[binary]' python -m unittest test_lakebase_source.py
"""
from __future__ import annotations

import contextlib
import importlib
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
sys.path.insert(0, HERE)
from lfc_source_contract import lakebase as ls


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
        self.assertEqual(spec.default_endpoint_settings.suspend_timeout_duration.seconds, 300)   # not Lakebase's 24 h
        w.postgres.list_endpoints.assert_called_with("projects/p/branches/production")   # a bare branch id expanded

    def test_suspend_settings(self):
        self.assertEqual(ls.suspend_settings(600).suspend_timeout_duration.seconds, 600)
        self.assertTrue(ls.suspend_settings(0).no_suspension)
        with self.assertRaises(ls.SetupError):
            ls.suspend_settings(30)                                                     # Lakebase's minimum is 60

    def test_entry_point_and_wrapper(self):
        with open(os.path.join(HERE, "pyproject.toml")) as f:
            line = next(x for x in f if x.startswith("lfc-lakebase-source"))
        module, func = line.split("=", 1)[1].strip().strip('"').split(":")
        self.assertIs(getattr(importlib.import_module(module), func), ls.main)
        with open(os.path.join(HERE, "lakebase-source.py")) as f:
            self.assertIn("from lfc_source_contract.lakebase import main", f.read())

    def test_contract_needs_a_checkout(self):
        w, pg = workspace(), database(role_exists=False)
        with mock.patch.object(ls.os.path, "exists", return_value=False), tempfile.TemporaryDirectory() as d:
            rc, out, _ = self.run_main(w, pg, "--project", "p", "--role", "r", "--connection", "c", "--profile", "x",
                                       "--contract", os.path.join(d, "s.json"))
            self.assertEqual(rc, 2); self.assertIn("needs a checkout", out)
            self.assertFalse(os.path.exists(os.path.join(d, "s.json")))

    def test_setup_from_a_notebook(self):
        w, pg = workspace(), database(role_exists=False)
        logs = []
        with mock.patch.object(ls, "WorkspaceClient", return_value=w) as wc, \
             mock.patch.object(ls.psycopg, "connect", return_value=pg), \
             mock.patch.object(ls.secrets, "token_urlsafe", return_value="SECRET-PW"):
            r = ls.setup("p", "lfc_cdc", "c1", log=logs.append)
        wc.assert_called_once_with()                                   # no profile: the workspace's own login
        self.assertEqual(r, {"host": "ep-1.example.com", "connection": "c1", "connection_created": True,
                             "login_created": True})
        self.assertNotIn("SECRET-PW", " ".join(logs) + str(r))

    def test_setup_with_a_jdbc_connection(self):
        w, pg = workspace(), database(role_exists=False)
        w.postgres.list_endpoints.return_value[1].status.hosts.host = "ep-1.example.com"
        pws = iter(["READER-PW", "WRITER-PW"])
        logs = []
        with mock.patch.object(ls, "WorkspaceClient", return_value=w), mock.patch.object(ls.psycopg, "connect", return_value=pg), \
             mock.patch.object(ls.secrets, "token_urlsafe", side_effect=lambda n: next(pws)):
            r = ls.setup("p", "lfc_cdc", "c1", log=logs.append, database="orders", writer="lfc_writer", jdbc_connection="c1_jdbc")
        q = self.statements(pg)
        self.assertTrue(any(s.startswith('CREATE ROLE "lfc_writer" WITH LOGIN REPLICATION PASSWORD') for s in q))
        self.assertIn('CREATE DATABASE "orders"', q)
        self.assertIn('GRANT CONNECT, CREATE ON DATABASE "orders" TO "lfc_writer"', q)
        self.assertIn('GRANT CONNECT ON DATABASE "orders" TO "lfc_cdc"', q)
        jc = next(c for c in w.connections.create.call_args_list if c.args[0] == "c1_jdbc")
        self.assertEqual(jc.kwargs["connection_type"], ls.ConnectionType.JDBC)
        self.assertEqual(jc.kwargs["environment_settings"].java_dependencies, ["org.postgresql:postgresql:42.7.4"])
        o = jc.kwargs["options"]
        self.assertEqual((o["user"], o["password"], o["url"]), ("lfc_writer", "WRITER-PW", "jdbc:postgresql://ep-1.example.com:5432/orders?sslmode=require"))
        reader = next(c for c in w.connections.create.call_args_list if c.args[0] == "c1")
        self.assertEqual(reader.kwargs["options"]["password"], "READER-PW")
        self.assertEqual((r["jdbc_connection"], r["jdbc_created"], r["writer_created"]), ("c1_jdbc", True, True))
        text = " ".join(logs) + str(r)
        self.assertNotIn("WRITER-PW", text); self.assertNotIn("READER-PW", text)

    def jdbc_setup(self, w, pg):
        pws = iter(["READER-PW", "WRITER-PW"])
        with mock.patch.object(ls, "WorkspaceClient", return_value=w), mock.patch.object(ls.psycopg, "connect", return_value=pg), \
             mock.patch.object(ls.secrets, "token_urlsafe", side_effect=lambda n: next(pws)):
            return ls.setup("p", "lfc_cdc", "c1", log=lambda m: None, database="orders", writer="lfc_writer",
                            jdbc_connection="c1_jdbc")

    def test_setup_jdbc_update_path(self):
        w, pg = workspace(connection=True), database(role_exists=True)
        r = self.jdbc_setup(w, pg)
        q = self.statements(pg)
        self.assertTrue(any(s.startswith('ALTER ROLE "lfc_writer" WITH LOGIN REPLICATION PASSWORD') for s in q))
        self.assertNotIn('CREATE DATABASE "orders"', q)                     # the database is there
        w.connections.create.assert_not_called()
        up = next(c for c in w.connections.update.call_args_list if c.args[0] == "c1_jdbc")
        self.assertEqual(up.kwargs["options"]["password"], "WRITER-PW")
        self.assertEqual(up.kwargs["environment_settings"].java_dependencies, ["org.postgresql:postgresql:42.7.4"])
        self.assertEqual((r["jdbc_created"], r["writer_created"]), (False, False))

    def test_setup_jdbc_connection_failure_says_rerun(self):
        w, pg = workspace(), database(role_exists=False)
        w.connections.create.side_effect = lambda name, **k: (_ for _ in ()).throw(RuntimeError("boom")) if name == "c1_jdbc" else None
        with self.assertRaises(ls.SetupError) as e:
            self.jdbc_setup(w, pg)
        self.assertEqual(e.exception.code, 1)
        self.assertIn("lfc_writer has a new password but connection c1_jdbc wasn't created", str(e.exception))
        self.assertNotIn("WRITER-PW", str(e.exception))

    def test_reader_failure_leaves_the_writer_alone(self):
        w, pg = workspace(), database(role_exists=False)
        w.connections.create.side_effect = RuntimeError("reader boom")
        with self.assertRaises(ls.SetupError) as e:
            self.jdbc_setup(w, pg)
        self.assertIn("connection c1 wasn't created", str(e.exception))
        self.assertFalse([s for s in self.statements(pg) if '"lfc_writer"' in s])   # the writer's password never changed

    def test_setup_jdbc_needs_writer_and_its_own_login(self):
        for kw in ({"jdbc_connection": "j"}, {"jdbc_connection": "j", "writer": "r", "database": "d"}):
            with self.assertRaises(ls.SetupError):
                ls.setup("p", "r", "c", log=lambda m: None, **kw)

    def test_endpoint_and_owner_login(self):
        w = workspace()
        self.assertEqual(ls.endpoint(w, "p"), ("projects/p/branches/production/endpoints/primary", "ep-1.example.com"))
        w.postgres.list_endpoints.assert_called_with("projects/p/branches/production")
        ls.endpoint(w, "p", branch="dev")
        w.postgres.list_endpoints.assert_called_with("projects/p/branches/dev")
        with mock.patch.object(ls, "WorkspaceClient", return_value=w):
            self.assertEqual(ls.owner_login("p"), {"host": "ep-1.example.com", "user": "owner@example.com",
                                                   "password": "oauth-token"})
        w.postgres.list_endpoints.return_value = w.postgres.list_endpoints.return_value[:1]
        with self.assertRaises(ls.SetupError):
            ls.endpoint(w, "p")

    def test_setup_errors(self):
        w = workspace(project=False)
        with mock.patch.object(ls, "WorkspaceClient", return_value=w), self.assertRaises(ls.SetupError) as e:
            ls.setup("p", "r", "c", log=lambda m: None)
        self.assertEqual(e.exception.code, 2)
        w, pg = workspace(connection=True), database(role_exists=True)
        w.connections.update.side_effect = RuntimeError("network")
        with mock.patch.object(ls, "WorkspaceClient", return_value=w), mock.patch.object(ls.psycopg, "connect", return_value=pg), \
             mock.patch.object(ls.secrets, "token_urlsafe", return_value="SECRET-PW"), self.assertRaises(ls.SetupError) as e:
            ls.setup("p", "r", "c", log=lambda m: None)
        self.assertEqual(e.exception.code, 1); self.assertNotIn("SECRET-PW", str(e.exception))


if __name__ == "__main__":
    unittest.main()
