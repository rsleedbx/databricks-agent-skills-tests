"""lfc_source_contract.lakebase: a Lakebase project as Lakeflow Connect's PostgreSQL source, in one step. The same
code runs from a checkout (scripts/lakebase-source.py, through uv) and as a workspace job (this package installed
from the repo's archive URL, entry point lfc-lakebase-source; no Git credentials needed).

  uv run lakebase-source.py --project P --role R --connection C [--database D] [--branch B] [--profile X]
                            [--create-project [--pg-version 17] [--suspend-seconds 300]] [--contract source.json]

From a notebook cell (the workspace's own login):

    from lfc_source_contract.lakebase import setup
    setup("my-project", role="lfc_cdc", connection="my_pg_conn")   # {host, connection, connection_created, login_created}

It makes everything the connector needs, each step kept when already done:
  1. the project (only with --create-project; otherwise a missing project is an error), its compute suspended after
     5 idle minutes (--suspend-seconds; Lakebase's own default is 24 hours, so it would run all day; 0 = never);
  2. password logins on the project (enable_pg_native_login), which the UC connection's login needs;
  3. the connection's login R, WITH LOGIN REPLICATION and a new random password (an existing login gets a new one);
  4. the Unity Catalog POSTGRESQL connection C from it (an existing one is updated, not recreated, so a gateway that
     names it keeps the same name);
  5. with --contract, the source contract (contract 1) for the tests: host, login, database, connection, and
     lakebase_sql.py as its sql_cmd (needs --profile; a job run has no CLI profile, so no sql_cmd then).

The password goes only into the login and the connection; it is never printed or written to the contract. Who may read
through the connection is its USE CONNECTION grant. It runs from a laptop (--profile) or as a serverless job (the job's
own login). The administrator steps run as you, the project owner, with an OAuth database credential. Rerunning rotates
the password in both places. Tables, grants on them and the publication come after (lfc-source-datagen's
lfc_source_datagen.sources).
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ConnectionType, EnvironmentSettings
from databricks.sdk.service.postgres import (
    EndpointType,
    Project,
    ProjectDefaultEndpointSettings,
    ProjectSpec,
)
from google.protobuf.duration_pb2 import Duration
from psycopg import sql

from . import jdbc


class SetupError(Exception):
    """A step that stopped: .code 2 when nothing changed, 1 when the login has a new password the connection lacks."""
    def __init__(self, message: str, code: int = 2):
        super().__init__(message)
        self.code = code


def endpoint(w, project: str, branch: str | None = None, proj=None) -> tuple[str, str]:
    """The project's read-write endpoint on the branch (default: the project's default branch): (its name, host)."""
    name = f"projects/{project}"
    proj = proj or w.postgres.get_project(name)
    br = f"{name}/branches/{branch}" if branch else proj.status.default_branch
    if not br.startswith("projects/"):
        br = f"{name}/branches/{br}"
    ep = next((e for e in w.postgres.list_endpoints(br)
               if e.status and e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE), None)
    if ep is None:
        raise SetupError(f"no read-write endpoint on {br}")
    return ep.name, ep.status.hosts.host


def suspend_settings(seconds: int) -> ProjectDefaultEndpointSettings:
    """A new project's compute settings: suspended after seconds idle (Lakebase's minimum is 60), or never with 0."""
    if seconds == 0:
        return ProjectDefaultEndpointSettings(no_suspension=True)
    if seconds < 60:
        raise SetupError(f"suspend_seconds: 0 (never) or at least 60, not {seconds}")
    return ProjectDefaultEndpointSettings(suspend_timeout_duration=Duration(seconds=seconds))


def owner_login(project: str, branch: str | None = None, profile: str | None = None) -> dict:
    """You, the project owner, at its read-write endpoint: {host, user, password}, the password an OAuth database
    credential (about an hour). For administrator SQL from the laptop; a notebook writes through a JDBC connection."""
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    ep, host = endpoint(w, project, branch)
    return {"host": host, "user": w.current_user.me().user_name,
            "password": w.postgres.generate_database_credential(endpoint=ep).token}


def _owner(w, ep: str, host: str):
    """A psycopg connection to databricks_postgres as you, the owner (autocommit: CREATE DATABASE needs it)."""
    return psycopg.connect(host=host, port=5432, dbname="databricks_postgres", user=w.current_user.me().user_name,
                           password=w.postgres.generate_database_credential(endpoint=ep).token, sslmode="require",
                           autocommit=True)


def _login(pg, name: str, pw: str) -> bool:
    """Create the login (LOGIN REPLICATION) with this password, or give it the password; True when it existed."""
    had = pg.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,)).fetchone()
    verb = sql.SQL("ALTER") if had else sql.SQL("CREATE")
    pg.execute(sql.SQL("{} ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(verb, sql.Identifier(name), sql.Literal(pw)))
    return bool(had)


def setup(project: str, role: str, connection: str, branch: str | None = None, profile: str | None = None,
          create_project: bool = False, pg_version: int = 17, log=print, database: str | None = None,
          writer: str | None = None, jdbc_connection: str | None = None, suspend_seconds: int = 300) -> dict:
    """Steps 1 to 4 (the module's docstring) from a notebook cell, a job or the laptop; returns {host, connection,
    connection_created, login_created} (and jdbc_connection, jdbc_created, writer_created with the JDBC options).
    The password goes only into the login and the connection. Raises SetupError.

    With jdbc_connection, writer and database: also the database (created when missing), the writer login (LOGIN
    REPLICATION, CONNECT and CREATE on the database: it makes the schemas, tables, publication and slot it owns) and
    the TYPE JDBC connection holding it (lfc_source_contract.jdbc: the Maven driver, sessionInitStatement allowed), so a
    notebook writes to the source through JdbcSql with no password in its code."""
    if jdbc_connection and not (writer and database):
        raise SetupError("jdbc_connection needs writer and database")
    if writer and writer == role:
        raise SetupError("writer: its own login, not the connection's reader (USE CONNECTION runs any SQL it may)")
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    name = f"projects/{project}"
    try:
        proj = w.postgres.get_project(name)
    except NotFound:
        if not create_project:
            raise SetupError(f"no project {project} (add --create-project to create it)") from None
        spec = ProjectSpec(display_name=project, pg_version=pg_version, enable_pg_native_login=True,
                           default_endpoint_settings=suspend_settings(suspend_seconds))
        proj = w.postgres.create_project(Project(spec=spec), project_id=project).wait()
        idle = f"suspends after {suspend_seconds} s idle" if suspend_seconds else "never suspends"
        log(f"lakebase-source: project {project} created (PostgreSQL {pg_version}, compute {idle})")
    if not (proj.status and proj.status.enable_pg_native_login):
        w.postgres.update_project(name, Project(spec=ProjectSpec(enable_pg_native_login=True)),
                                  FieldMask(["spec.enable_pg_native_login"])).wait()
        log(f"lakebase-source: password logins turned on for {project}")

    ep, host = endpoint(w, project, branch, proj)

    # look the connections up before a password changes, so a permission error stops here with nothing changed
    def have(name):
        try:
            w.connections.get(name)
            return True
        except NotFound:
            return False
    have_connection = have(connection)
    have_jdbc = have(jdbc_connection) if jdbc_connection else False

    password = secrets.token_urlsafe(32)
    wpassword = secrets.token_urlsafe(32) if jdbc_connection else None
    with _owner(w, ep, host) as pg:
        exists = _login(pg, role, password)
    options = {"host": host, "port": "5432", "user": role, "password": password}
    try:
        if have_connection:
            w.connections.update(connection, options=options)
        else:
            w.connections.create(connection, connection_type=ConnectionType.POSTGRESQL, options=options,
                                 comment=f"Lakeflow Connect source: Lakebase project {project}")
    except Exception as e:  # noqa: BLE001 (any failure here leaves the two out of step) the login has the new password and the connection doesn't: say so, and how to fix it
        raise SetupError(f"login {role} has a new password but connection {connection} wasn't "
                         f"{'updated' if have_connection else 'created'} ({type(e).__name__}: {e}); rerun when that's "
                         "fixed, which sets a new password on both", code=1) from None
    finally:
        del password, options
    wexists = None
    if jdbc_connection:   # the writer's password changes only now, after the reader's connection holds its new one
        with _owner(w, ep, host) as pg:
            if not pg.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone():
                pg.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                log(f"lakebase-source: database {database} created")
            wexists = _login(pg, writer, wpassword)
            pg.execute(sql.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(sql.Identifier(database),
                                                                                   sql.Identifier(writer)))
            pg.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(role)))
        b = jdbc.connection_body(jdbc_connection, "postgres", jdbc.url("postgres", host, 5432, database), writer, wpassword,
                                 comment=f"Writes to the Lakeflow Connect source: Lakebase project {project}, database {database}")
        env = EnvironmentSettings(java_dependencies=b["environment_settings"]["java_dependencies"])
        try:
            if have_jdbc:
                w.connections.update(jdbc_connection, options=b["options"], environment_settings=env)
            else:
                w.connections.create(jdbc_connection, connection_type=ConnectionType.JDBC, options=b["options"],
                                     environment_settings=env, comment=b["comment"])
        except Exception as e:  # noqa: BLE001 (the writer has the new password and its connection doesn't)
            raise SetupError(f"login {writer} has a new password but connection {jdbc_connection} wasn't "
                             f"{'updated' if have_jdbc else 'created'} ({type(e).__name__}: {e}); rerun when that's "
                             "fixed, which sets a new password on both", code=1) from None
        finally:
            del wpassword, b
        log(f"lakebase-source: JDBC connection {jdbc_connection} {'updated' if have_jdbc else 'created'}; login "
            f"{writer} ({'new password' if wexists else 'created'}, LOGIN REPLICATION, CREATE on {database})")
    log(f"lakebase-source: connection {connection} {'updated' if have_connection else 'created'}; login {role} "
        f"({'new password' if exists else 'created'}, LOGIN REPLICATION) on {project}, host {host}")
    r = {"host": host, "connection": connection, "connection_created": not have_connection, "login_created": not exists}
    if jdbc_connection:
        r |= {"jdbc_connection": jdbc_connection, "jdbc_created": not have_jdbc, "writer_created": not wexists}
    return r


def main(argv: list[str] | None = None) -> int:
    """The command line; argv (default sys.argv[1:]) lets a notebook call it in its own process, where the SDK has
    the notebook's login (a subprocess there has none)."""
    a = argparse.ArgumentParser(prog="lakebase-source", description=__doc__.split("\n")[0])
    a.add_argument("--project", required=True, help="Lakebase project id")
    a.add_argument("--role", required=True, help="the connection's Postgres login")
    a.add_argument("--connection", required=True, help="Unity Catalog connection name")
    a.add_argument("--database", default="databricks_postgres", help="the database the contract names")
    a.add_argument("--branch", help="branch id (default: the project's default branch)")
    a.add_argument("--profile", help="Databricks CLI profile (default: the environment's login, e.g. a job's)")
    a.add_argument("--create-project", action="store_true", help="create the project when it doesn't exist")
    a.add_argument("--pg-version", type=int, default=17, help="PostgreSQL version of a new project")
    a.add_argument("--suspend-seconds", type=int, default=300,
                   help="a new project's compute suspends after this many idle seconds (default 300; 0 = never)")
    a.add_argument("--contract", help="write the source contract here")
    a.add_argument("--writer", help="a writer login for the JDBC connection (with --jdbc-connection)")
    a.add_argument("--jdbc-connection", help="also a TYPE JDBC connection for writes, holding --writer, on --database")
    x = a.parse_args(argv)
    if x.contract and not x.profile:
        a.error("--contract needs --profile (its sql_cmd runs lakebase_sql.py with that profile)")
    try:
        r = setup(x.project, x.role, x.connection, x.branch, x.profile, x.create_project, x.pg_version,
                  database=x.database if x.jdbc_connection else None, writer=x.writer, jdbc_connection=x.jdbc_connection,
                  suspend_seconds=x.suspend_seconds)
    except SetupError as e:
        print(f"lakebase-source: {e}", file=sys.stderr)
        return e.code

    if x.contract:
        sql_sh = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakebase_sql.py")
        if not os.path.exists(sql_sh):
            print("lakebase-source: --contract needs a checkout (its sql_cmd is the checkout's lakebase_sql.py)",
                  file=sys.stderr)
            return 2
        env = f"env LB_PROJECT={x.project} LB_PROFILE={x.profile}"
        if x.branch:
            env += f" LB_BRANCH={x.branch}"
        contract = {"contract": 1, "name": x.project, "engine": "postgres", "host": r["host"], "port": 5432,
                    "user": x.role, "database": x.database, "connection": x.connection,
                    "sql_cmd": f"{env} {sql_sh}"}
        with open(x.contract, "w") as f:
            json.dump(contract, f, indent=1)
            f.write("\n")
        print(f"lakebase-source: contract {x.contract} (check it: source-env.sh {x.contract})")
    return 0
