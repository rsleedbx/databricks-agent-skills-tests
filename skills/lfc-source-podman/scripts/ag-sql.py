#!/usr/bin/env python3
"""ag-sql.py p|s [db] [--xe [since] [errors|writes|all]] | p|s --sqlcmd <flags>: T-SQL on stdin as sa in the ag-up.py
node <AG_NAME>-<p|s>; --sqlcmd passes the flags to sqlcmd as given (for secondary.py PRIMARY_SQL_CMD / SECONDARY_SQL_CMD);
--xe prints the gateway's statements and errors recorded by the lfc_watch Extended Events session instead
(since: 'yyyy-mm-ddThh:mm:ss' UTC, default all; default kind errors). Environment: AG_NAME, AG_DIR, PODMAN as in ag-up.py.
The container CLI (podman or docker) runs sqlcmd inside the node; stdin passes through to sqlcmd.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ag import Ag
from podman_host import Fail

SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"
USAGE = "usage: ag-sql.py p|s [db] [--xe [since] [errors|writes|all]]"


def xe_query(since: str, kind: str) -> str:
    """The lfc_watch session's events after `since`, of kind errors (error_reported), writes (errors and the gateway's
    DML and CDC calls) or all, one row per event, oldest first."""
    return f"""SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON;
WITH x AS (SELECT CAST(event_data AS XML) e, object_name ev
           FROM sys.fn_xe_file_target_read_file('/var/opt/mssql/log/lfc_watch*.xel', NULL, NULL, NULL)),
y AS (SELECT ev, e.value('(event/@timestamp)[1]', 'datetime2') ts,
        e.value('(event/data[@name="error_number"]/value)[1]', 'int') err,
        LEFT(COALESCE(e.value('(event/data[@name="message"]/value)[1]', 'nvarchar(max)'), ''), 200) msg,
        LEFT(COALESCE(e.value('(event/data[@name="batch_text"]/value)[1]', 'nvarchar(max)'),
                      e.value('(event/data[@name="statement"]/value)[1]', 'nvarchar(max)'),
                      e.value('(event/action[@name="sql_text"]/value)[1]', 'nvarchar(max)'), ''), 220) txt
      FROM x)
SELECT CONVERT(varchar(19), ts, 126) ts, ev, err, REPLACE(msg, CHAR(10), ' ') msg, REPLACE(txt, CHAR(10), ' ') txt FROM y
WHERE ts >= '{since}' AND (('{kind}' = 'errors' AND ev = 'error_reported')
  OR ('{kind}' = 'writes' AND (ev = 'error_reported' OR txt LIKE '%UPDATE %' OR txt LIKE '%MERGE %' OR txt LIKE '%INSERT %'
      OR txt LIKE '%DELETE %' OR txt LIKE '%sp_cdc_%able%' OR txt LIKE '%lakeflow%CaptureInstance%'))
  OR '{kind}' = 'all')
ORDER BY ts;
"""


def main(argv: list[str]) -> int:
    if not argv:
        print(USAGE, file=sys.stderr)
        return 1
    node, rest = argv[0], argv[1:]
    ag = Ag()
    if node not in ("p", "s"):
        print("node: p or s", file=sys.stderr)
        return 2
    container = f"{ag.name}-{node}"
    if rest[:1] == ["--sqlcmd"]:
        sa_pw = ag.env().get("SA_PW", "")
        cmd = [ag.pm, "exec", "-i", "-e", f"SQLCMDPASSWORD={sa_pw}", container, SQLCMD, "-C", "-S", "localhost", "-U", "sa",
               *rest[1:]]
        return subprocess.run(cmd, check=False).returncode
    db = "master"
    if rest and rest[0] != "--xe":
        db, rest = rest[0], rest[1:]
    sa_pw = ag.env().get("SA_PW", "")
    base = [ag.pm, "exec", "-i", "-e", f"SQLCMDPASSWORD={sa_pw}", container, SQLCMD, "-C", "-S", "localhost", "-U", "sa",
            "-d", db, "-b", "-W", "-s", "|"]
    if rest[:1] != ["--xe"]:   # the rest are sqlcmd's own arguments (-Q, -i, ...)
        return subprocess.run([*base, *rest], check=False).returncode
    since = rest[1] if len(rest) > 1 else "2000-01-01"
    kind = rest[2] if len(rest) > 2 else "errors"
    if re.search(r"[^0-9A-Za-z:T.\-]", since + kind):
        raise Fail("bad since/kind", 2)
    return subprocess.run(base, input=xe_query(since, kind), text=True, check=False).returncode


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
