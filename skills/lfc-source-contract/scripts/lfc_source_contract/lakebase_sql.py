#!/usr/bin/env python3
"""lakebase_sql.py [database]: a contract's sql_cmd for a Lakebase (Postgres) source: SQL from stdin, run as the project
owner (your Databricks login) through psql, so a Lakebase project stands in for a customer's PostgreSQL in every test
that takes a source contract. No password on the command line: the owner's OAuth database credential comes from
lakebase.owner_login (through the workspace REST client) and goes to psql in PGPASSWORD.
  Needs LB_PROJECT (the Lakebase project id) and LB_PROFILE (the CLI profile of its workspace). Optional LB_BRANCH
  (default: the project's default branch). The database argument picks the database; `postgres` (the server-level name
  lib.sh passes) means Lakebase's default database, databricks_postgres. Output: rows tab-separated, no headers, as
  sql_client.py. Arguments after the database go to psql after these, so a caller can change them (fill.py reads |
  between fields: PSQL="lakebase_sql.py <db> -F |").
  Each call renews an expired workspace login from your account login when it can (account-token-fallback ws-token.sh,
  run as a subprocess; nothing when the profile's own login works).
  In a contract: "sql_cmd": "env LB_PROJECT=lfc-demo LB_PROFILE=<profile> /abs/path/lakebase_sql.py"
Replaces lakebase-sql.sh (kept as a two-line shim: saved contracts name that path). Standard library plus lakebase.py.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
SKILLS = HERE.parents[3]   # skills/: the account-token-fallback skill's ws-token.sh lives beside this repo's skills
WS_TOKEN = SKILLS / "account-token-fallback" / "scripts" / "ws-token.sh"


def renew_login(profile: str) -> None:
    """Point the SDK at a renewed workspace login when the fallback can make one (no change when it can't)."""
    if not WS_TOKEN.is_file():
        return
    res = subprocess.run(["bash", str(WS_TOKEN), profile], capture_output=True, text=True, check=False)
    cfg = res.stdout.strip()
    if res.returncode == 0 and cfg:
        os.environ["DATABRICKS_CONFIG_FILE"] = cfg


def main(argv: list) -> int:
    project = os.environ.get("LB_PROJECT", "")
    profile = os.environ.get("LB_PROFILE", "")
    if not project or not profile:
        print("lakebase_sql.py: set LB_PROJECT to the Lakebase project id and LB_PROFILE to the workspace profile",
              file=sys.stderr)
        return 1
    db = argv[0] if argv and argv[0] else "databricks_postgres"
    if db == "postgres":
        db = "databricks_postgres"
    branch = os.environ.get("LB_BRANCH") or None
    renew_login(profile)
    if str(SKILLS / "lfc-source-contract" / "scripts") not in sys.path:
        sys.path.insert(0, str(HERE.parents[2]))   # the scripts folder, so lfc_source_contract imports
    from lfc_source_contract.lakebase import owner_login

    cred = owner_login(project, branch, profile)
    env = dict(os.environ, PGPASSWORD=cred["password"], PGSSLMODE="require")
    cmd = ["psql", "-h", cred["host"], "-U", cred["user"], "-d", db, "-v", "ON_ERROR_STOP=1", "-qtAX", "-F", "\t"]
    os.execvpe("psql", cmd + argv[1:], env)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
