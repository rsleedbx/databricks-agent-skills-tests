#!/usr/bin/env python3
"""sql_client.py [database [psql arguments]]: run SQL from stdin with the default client for the contract's engine.
source_env.py sets SRC_SQL to this file when a contract has no sql_cmd. It connects as the contract's user with its
password, and the password reaches the client only in an environment variable (SQLCMDPASSWORD, PGPASSWORD, MYSQL_PWD),
or in the SQL for sqlplus.
  Needs SRC_CONTRACT (the contract file; source_env.py sets it). The argument is the database (SQL Server, PostgreSQL,
  MySQL, MariaDB); it overrides the contract's database. Oracle takes the contract's service and ignores the argument.
  PostgreSQL, CockroachDB, YugabyteDB: arguments after the database go to psql after the defaults (fill.py: -F '|').
  Output: rows tab-separated, no headers (psql -At, sqlcmd -h -1, mysql -N -B). The exit status is the client's.
  Engines: sqlserver (sqlcmd -C), postgres, cockroachdb, yugabytedb (psql), mysql, mariadb, oracle (sqlplus).
  db2 and informix have no default client: their contract needs sql_cmd.
Replaces sql-client.sh (kept as a two-line shim: saved contracts name that path). Standard library only.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys


def contract_fields(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)

    def val(key: str) -> str:
        v = d.get(key)
        return "" if v is None or v is False else str(v)

    port = d.get("port")
    return {"ENG": val("engine"), "HOST": val("host"),
            "PORT": str(int(port)) if isinstance(port, (int, float)) and not isinstance(port, bool) else "",
            "USR": val("user"), "PW": val("password"), "DBN": val("database"), "SVC": val("service")}


def oracle_input(f: dict, sql_in: str) -> str:
    pw = f["PW"].replace('"', '""')   # the password is in the SQL (CONNECT on stdin), its quotes doubled
    return ("WHENEVER SQLERROR EXIT FAILURE\n"
            f'CONNECT {f["USR"]}/"{pw}"@//{f["HOST"]}:{f["PORT"]}/{f["SVC"]}\n'
            "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMSPOOL ON\n"
            f"{sql_in}"
            "EXIT\n")


def command(f: dict, argv: list):
    """(program, arguments, extra environment, stdin text or None) for the engine's client."""
    db = (argv[0] if argv and argv[0] else f["DBN"])   # ${1:-$DBN}: an empty argument means the contract's database
    eng = f["ENG"]
    if eng == "sqlserver":
        return ["sqlcmd", "-C", "-S", f"{f['HOST']},{f['PORT']}", "-U", f["USR"], "-b", "-W", "-h", "-1",
                "-d", db or "master"], {"SQLCMDPASSWORD": f["PW"]}
    if eng in ("postgres", "cockroachdb", "yugabytedb"):
        return ["psql", "-h", f["HOST"], "-p", f["PORT"], "-U", f["USR"], "-d", db or "postgres",
                "-v", "ON_ERROR_STOP=1", "-qtAX", "-F", "\t"] + argv[1:], {"PGPASSWORD": f["PW"]}
    if eng in ("mysql", "mariadb"):
        args = [eng, "-h", f["HOST"], "-P", f["PORT"], "-u", f["USR"], "-N", "-B"]
        return (args + [db] if db else args), {"MYSQL_PWD": f["PW"]}
    if eng == "oracle":
        return ["sqlplus", "-s", "/nolog"], {}
    return None, None


def main(argv: list) -> int:
    contract = os.environ.get("SRC_CONTRACT", "")
    if not contract:
        print("sql_client.py: set SRC_CONTRACT to the source contract (source_env.py sets it)", file=sys.stderr)
        return 1
    f = contract_fields(contract)
    prog, extra = command(f, argv)
    if prog is None:
        print(f"sql-client: no default client for {f['ENG']}: give the contract a sql_cmd", file=sys.stderr)
        return 2
    env = dict(os.environ, **extra)
    if f["ENG"] == "oracle":
        sql_in = sys.stdin.read()
        return subprocess.run(prog, input=oracle_input(f, sql_in), text=True, env=env, check=False).returncode
    os.execvpe(prog[0], prog, env)   # the client takes stdin as it comes, and its exit status is ours
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
