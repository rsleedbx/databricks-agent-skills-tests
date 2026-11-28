#!/usr/bin/env python3
"""sqlserver-log-state.py <database> [--no-per-table]: run sqlserver-log-state.sql in <database> and print its JSON
document (one line; pipe it to jq). --no-per-table skips the per-table CHANGETABLE and change-table reads.
How it connects:
  SQL_CMD   a command that reads T-SQL on stdin and takes sqlcmd's -d/-v/-y/-b flags, for example
            SQL_CMD='podman exec -i <container> /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P <pw>'
            (default: sqlcmd -C, with SQLCMDSERVER, SQLCMDUSER and SQLCMDPASSWORD from the environment)
sqlcmd runs on this machine (a laptop, or a host that has it); the batch is the sibling .sql file, unchanged.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SQL_FILE = HERE / "sqlserver-log-state.sql"
USAGE = "usage: sqlserver-log-state.py <database> [--no-per-table]"


def run_batch(db: str, per_table: str, sql_cmd: str, sql_text: str) -> tuple[int, str]:
    """(exit code, output) of the batch. SQL_CMD runs through bash -c, so it can hold quotes; the sqlcmd flags follow
    it as arguments. -y 0: the whole nvarchar(max) value on one line (sqlcmd refuses it with -h); -b: exit nonzero on
    an error (its output isn't used then: its error is on stderr already)."""
    p = subprocess.run(["bash", "-c", f'{sql_cmd} "$@"', "sql-cmd", "-d", db, "-v", f"PER_TABLE={per_table}", "-y", "0", "-b"],
                       input=sql_text, stdout=subprocess.PIPE, text=True, check=False)
    return p.returncode, p.stdout


def main(argv: list[str]) -> int:
    if not argv:
        print(USAGE, file=sys.stderr)
        return 1
    db = argv[0]
    per = "0" if argv[1:2] == ["--no-per-table"] else "1"
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", db):
        print(f"bad database name: {db}", file=sys.stderr)
        return 2
    rc, out = run_batch(db, per, os.environ.get("SQL_CMD") or "sqlcmd -C", SQL_FILE.read_text())
    if rc != 0:
        return rc
    json_line = next((line for line in out.splitlines() if line.startswith("{")), None)
    if json_line is None:
        print("no JSON from the batch:", file=sys.stderr)
        print(out, file=sys.stderr)
        return 1
    print(json.dumps(json.loads(json_line), separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
