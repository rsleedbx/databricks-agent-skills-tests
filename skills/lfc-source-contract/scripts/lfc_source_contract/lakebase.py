"""lfc_source_contract.lakebase: a Lakebase project as Lakeflow Connect's PostgreSQL source, in one step. The same
code runs from a checkout (scripts/lakebase-source.py, through uv) and as a workspace job (this package installed
from the repo's archive URL, entry point lfc-lakebase-source; no Git credentials needed).

  uv run lakebase-source.py --project P --role R --connection C [--database D] [--branch B] [--profile X]
                            [--create-project [--pg-version 17]] [--contract source.json]

From a notebook cell (the workspace's own login):

    from lfc_source_contract.lakebase import setup
    setup("my-project", role="lfc_cdc", connection="my_pg_conn")   # {host, connection, connection_created, login_created}

It makes everything the connector needs, each step kept when already done:
  1. the project (only with --create-project; otherwise a missing project is an error);
  2. password logins on the project (enable_pg_native_login), which the UC connection's login needs;
  3. the connection's login R, WITH LOGIN REPLICATION and a new random password (an existing login gets a new one);
  4. the Unity Catalog POSTGRESQL connection C from it (an existing one is updated, not recreated, so a gateway that
     names it keeps the same name);
  5. with --contract, the source contract (contract 1) for the tests: host, login, database, connection, and
     lakebase-sql.sh as its sql_cmd (needs --profile; a job run has no CLI profile, so no sql_cmd then).

The password goes only into the login and the connection; it is never printed or written to the contract. Who may read
through the connection is its USE CONNECTION grant. It runs from a laptop (--profile) or as a serverless job (the job's
own login). The administrator steps run as you, the project owner, with an OAuth database credential. Rerunning rotates
the password in both places. Tables, grants on them and the publication come after (lfc-source-datagen's
tpch-sources.sh).
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
from databricks.sdk.service.catalog import ConnectionType
from databricks.sdk.service.postgres import EndpointType, Project, ProjectSpec
from psycopg import sql


class SetupError(Exception):
    """A step that stopped: .code 2 when nothing changed, 1 when the login has a new password the connection lacks."""
    def __init__(self, message: str, code: int = 2):
        super().__init__(message)
        self.code = code


def setup(project: str, role: str, connection: str, branch: str | None = None, profile: str | None = None,
          create_project: bool = False, pg_version: int = 17, log=print) -> dict:
    """Steps 1 to 4 (the module's docstring) from a notebook cell, a job or the laptop; returns {host, connection,
    connection_created, login_created}. The password goes only into the login and the connection. Raises SetupError."""
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    name = f"projects/{project}"
    try:
        proj = w.postgres.get_project(name)
    except NotFound:
        if not create_project:
            raise SetupError(f"no project {project} (add --create-project to create it)") from None
        spec = ProjectSpec(display_name=project, pg_version=pg_version, enable_pg_native_login=True)
        proj = w.postgres.create_project(Project(spec=spec), project_id=project).wait()
        log(f"lakebase-source: project {project} created (PostgreSQL {pg_version})")
    if not (proj.status and proj.status.enable_pg_native_login):
        w.postgres.update_project(name, Project(spec=ProjectSpec(enable_pg_native_login=True)),
                                  FieldMask(["spec.enable_pg_native_login"])).wait()
        log(f"lakebase-source: password logins turned on for {project}")

    br = f"{name}/branches/{branch}" if branch else proj.status.default_branch
    if not br.startswith("projects/"):
        br = f"{name}/branches/{br}"
    endpoints = [e for e in w.postgres.list_endpoints(br)
                 if e.status and e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE]
    if not endpoints:
        raise SetupError(f"no read-write endpoint on {br}")
    endpoint, host = endpoints[0].name, endpoints[0].status.hosts.host

    # look the connection up before the password changes, so a permission error stops here with nothing changed
    try:
        w.connections.get(connection)
        have_connection = True
    except NotFound:
        have_connection = False

    password = secrets.token_urlsafe(32)
    token = w.postgres.generate_database_credential(endpoint=endpoint).token
    with psycopg.connect(host=host, port=5432, dbname="databricks_postgres", user=w.current_user.me().user_name,
                         password=token, sslmode="require", autocommit=True) as pg:
        exists = pg.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
        verb = sql.SQL("ALTER") if exists else sql.SQL("CREATE")
        pg.execute(sql.SQL("{} ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
            verb, sql.Identifier(role), sql.Literal(password)))
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
        del password, options, token
    log(f"lakebase-source: connection {connection} {'updated' if have_connection else 'created'}; login {role} "
        f"({'new password' if exists else 'created'}, LOGIN REPLICATION) on {project}, host {host}")
    return {"host": host, "connection": connection, "connection_created": not have_connection,
            "login_created": not exists}


def main() -> int:
    a = argparse.ArgumentParser(prog="lakebase-source", description=__doc__.split("\n")[0])
    a.add_argument("--project", required=True, help="Lakebase project id")
    a.add_argument("--role", required=True, help="the connection's Postgres login")
    a.add_argument("--connection", required=True, help="Unity Catalog connection name")
    a.add_argument("--database", default="databricks_postgres", help="the database the contract names")
    a.add_argument("--branch", help="branch id (default: the project's default branch)")
    a.add_argument("--profile", help="Databricks CLI profile (default: the environment's login, e.g. a job's)")
    a.add_argument("--create-project", action="store_true", help="create the project when it doesn't exist")
    a.add_argument("--pg-version", type=int, default=17, help="PostgreSQL version of a new project")
    a.add_argument("--contract", help="write the source contract here")
    x = a.parse_args()
    if x.contract and not x.profile:
        a.error("--contract needs --profile (its sql_cmd runs lakebase-sql.sh with that profile)")
    try:
        r = setup(x.project, x.role, x.connection, x.branch, x.profile, x.create_project, x.pg_version)
    except SetupError as e:
        print(f"lakebase-source: {e}", file=sys.stderr)
        return e.code

    if x.contract:
        sql_sh = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lakebase-sql.sh")
        if not os.path.exists(sql_sh):
            print("lakebase-source: --contract needs a checkout (its sql_cmd is the checkout's lakebase-sql.sh)",
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
