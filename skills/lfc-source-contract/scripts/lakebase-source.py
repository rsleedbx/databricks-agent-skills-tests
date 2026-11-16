#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.81", "psycopg[binary]>=3.2"]
# ///
"""lakebase-source.py: a Lakebase project as Lakeflow Connect's PostgreSQL source, in one step.

  uv run lakebase-source.py --project P --role R --connection C [--database D] [--branch B] [--profile X]
                            [--create-project [--pg-version 17]] [--contract source.json]

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


def main() -> int:
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
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

    w = WorkspaceClient(profile=x.profile) if x.profile else WorkspaceClient()
    name = f"projects/{x.project}"
    try:
        project = w.postgres.get_project(name)
    except NotFound:
        if not x.create_project:
            print(f"lakebase-source: no project {x.project} (add --create-project to create it)", file=sys.stderr)
            return 2
        spec = ProjectSpec(display_name=x.project, pg_version=x.pg_version, enable_pg_native_login=True)
        project = w.postgres.create_project(Project(spec=spec), project_id=x.project).wait()
        print(f"lakebase-source: project {x.project} created (PostgreSQL {x.pg_version})")
    if not (project.status and project.status.enable_pg_native_login):
        w.postgres.update_project(name, Project(spec=ProjectSpec(enable_pg_native_login=True)),
                                  FieldMask(["spec.enable_pg_native_login"])).wait()
        print(f"lakebase-source: password logins turned on for {x.project}")

    branch = f"{name}/branches/{x.branch}" if x.branch else project.status.default_branch
    if not branch.startswith("projects/"):
        branch = f"{name}/branches/{branch}"
    endpoints = [e for e in w.postgres.list_endpoints(branch)
                 if e.status and e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE]
    if not endpoints:
        print(f"lakebase-source: no read-write endpoint on {branch}", file=sys.stderr)
        return 2
    endpoint, host = endpoints[0].name, endpoints[0].status.hosts.host

    password = secrets.token_urlsafe(32)
    token = w.postgres.generate_database_credential(endpoint=endpoint).token
    with psycopg.connect(host=host, port=5432, dbname="databricks_postgres", user=w.current_user.me().user_name,
                         password=token, sslmode="require", autocommit=True) as pg:
        exists = pg.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (x.role,)).fetchone()
        verb = sql.SQL("ALTER") if exists else sql.SQL("CREATE")
        pg.execute(sql.SQL("{} ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
            verb, sql.Identifier(x.role), sql.Literal(password)))
    options = {"host": host, "port": "5432", "user": x.role, "password": password}
    try:
        w.connections.get(x.connection)
        w.connections.update(x.connection, options=options)
        done = "updated"
    except NotFound:
        w.connections.create(x.connection, connection_type=ConnectionType.POSTGRESQL, options=options,
                             comment=f"Lakeflow Connect source: Lakebase project {x.project}")
        done = "created"
    del password, options, token
    print(f"lakebase-source: connection {x.connection} {done}; login {x.role} "
          f"({'new password' if exists else 'created'}, LOGIN REPLICATION) on {x.project}, host {host}")

    if x.contract:
        here = os.path.dirname(os.path.abspath(__file__))
        env = f"env LB_PROJECT={x.project} LB_PROFILE={x.profile}"
        if x.branch:
            env += f" LB_BRANCH={x.branch}"
        contract = {"contract": 1, "name": x.project, "engine": "postgres", "host": host, "port": 5432,
                    "user": x.role, "database": x.database, "connection": x.connection,
                    "sql_cmd": f"{env} {os.path.join(here, 'lakebase-sql.sh')}"}
        with open(x.contract, "w") as f:
            json.dump(contract, f, indent=1)
            f.write("\n")
        print(f"lakebase-source: contract {x.contract} (check it: source-env.sh {x.contract})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
