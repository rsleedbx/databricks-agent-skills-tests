"""sqlserver: a SQL Server test source made ready for Lakeflow Connect, through a source SQL command (srcsql.Runner, or
any run(sql, database) -> output):

    db = Database(run, "me_addcol_1790000000", user="lfc_user")
    db.create("CREATE TABLE lfcddemo.t (id INT PRIMARY KEY, v VARCHAR(20));")   # schema lfcddemo, the login's user
    db.utility_objects()                                                         # Microsoft's script; its version
    db.track("ct", ["t"])                                                        # change tracking (or "cdc"), retried
    db.fill("t", 10)
    db.drop()                                                                    # 0 when it's gone
"""
from __future__ import annotations

import re
import time
import urllib.request

UTILITY_URL = "https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql"
SCHEMA = "lfcddemo"
NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _name(n: str) -> str:
    if not NAME.match(n or ""):
        raise ValueError(f"not a plain SQL Server name: {n!r}")
    return n


def _rows(out: str) -> list:
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


class Database:
    def __init__(self, run, name: str, user: str, log=print, sleep=time.sleep):
        self.run, self.name, self.user, self.log, self.sleep = run, _name(name), _name(user), log, sleep
        self.util = ""

    def sql(self, text: str) -> str:
        return self.run(text, self.name)

    def create(self, setup: str = "") -> None:
        """The database, schema lfcddemo, the setup SQL (tables, rows) and a user for the connection's login."""
        self.run(f"CREATE DATABASE {self.name};\nGO\nUSE {self.name};\nGO\nCREATE SCHEMA {SCHEMA};\nGO\n{setup}\nGO\n"
                 f"CREATE USER {self.user} FOR LOGIN {self.user};\n", "master")

    def utility_objects(self, script: str | None = None) -> str:
        """Microsoft's utility objects (lakeflowSetupChangeTracking, ...) in the database; their version."""
        if script is None:
            with urllib.request.urlopen(UTILITY_URL, timeout=60) as r:
                script = r.read().decode("utf-8-sig")
        self.sql(script)
        self.util = (_rows(self.sql("SET NOCOUNT ON; SELECT dbo.lakeflowUtilityVersion();\n")) or [""])[0]
        return self.util

    def untracked(self, kind: str, tables: list) -> list:
        tv = ("(SELECT object_id FROM sys.change_tracking_tables)" if kind == "ct"
              else "(SELECT object_id FROM sys.tables WHERE is_tracked_by_cdc = 1)")
        vals = ",".join(f"('{_name(t)}')" for t in tables)
        out = self.sql(f"SET NOCOUNT ON; SELECT STRING_AGG(n, ' ') FROM (VALUES {vals}) v(n) WHERE "
                       f"OBJECT_ID('{SCHEMA}.' + n) IS NULL OR OBJECT_ID('{SCHEMA}.' + n) NOT IN {tv};\n")
        left = " ".join(_rows(out)).split()
        return [] if left in ([], ["NULL"]) else left

    def track(self, kind: str, tables: list, tries: int = 3) -> None:
        """Change tracking ("ct") or CDC ("cdc") on lfcddemo.<table>, with the login's grants. All tables in one call,
        then the ones not tracked set up again, up to tries times: enabling CDC in several databases at once (parallel
        runs) can fail, and a table left untracked fails later with INGESTION_GATEWAY_CDC_NOT_ENABLED, which looks like
        a product result. Change tracking asks for the DDL objects (utility objects 1.7 make them only on request, and
        the pipelines need them)."""
        if kind not in ("ct", "cdc"):
            raise ValueError("track: ct or cdc")
        proc = "lakeflowSetupChangeTracking" if kind == "ct" else "lakeflowSetupChangeDataCapture"
        ddl = ", @CreateDdlSupportingObjects = 1" if kind == "ct" else ""
        left = list(tables)
        for n in range(1, tries + 1):
            try:
                out = self.sql(f"EXEC dbo.{proc} @Tables = '{','.join(f'{SCHEMA}.{_name(t)}' for t in left)}', "
                               f"@User = '{self.user}'{ddl};\n")
            except Exception as e:  # noqa: BLE001  the check below decides; the error is shown
                out = str(e)
            left = self.untracked(kind, tables)
            if not left:
                break
            first = next((ln for ln in out.splitlines() if re.search(r"error|msg [0-9]", ln, re.IGNORECASE)), "")
            self.log(f"track {kind}: not tracked after try {n}: {' '.join(left)} ({first[:160]})")
            self.sleep(10)
        if left:
            raise RuntimeError(f"could not set up {kind} on {SCHEMA}: {' '.join(left)}")
        self.sql(f"EXEC dbo.lakeflowFixPermissions @User = '{self.user}', @Tables = 'SCHEMAS:{SCHEMA}';\n")

    def fill(self, table: str, n: int, first: int = 1) -> None:
        """n rows (id, v='before') from id first (a cross join: sys.all_objects alone has about 2,000 rows)."""
        self.sql(f"INSERT INTO {SCHEMA}.{_name(table)} (id, v) SELECT TOP {int(n)} {int(first)} + ROW_NUMBER() OVER "
                 "(ORDER BY (SELECT 1)) - 1, 'before' FROM sys.all_objects a CROSS JOIN sys.all_objects b;\n")

    def count(self) -> int | None:
        """How many databases of this name the server has (None: no answer)."""
        out = _rows(self.run(f"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '{self.name}';\n",
                             "master"))
        return int(out[0]) if out and out[0].isdigit() else None

    def drop(self, tries: int = 3, wait: float = 30) -> int | None:
        """CDC off (its own call: an error there would skip the drop), then the database; tried again while a
        pipeline just deleted still holds a session. 0 when it's gone."""
        left = None
        for _ in range(tries):
            for text, db in (("EXEC sys.sp_cdc_disable_db;\n", self.name),
                             (f"IF DB_ID(N'{self.name}') IS NOT NULL BEGIN ALTER DATABASE [{self.name}] SET SINGLE_USER "
                              f"WITH ROLLBACK IMMEDIATE; DROP DATABASE [{self.name}]; END\n", "master")):
                try:
                    self.run(text, db)
                except Exception:  # noqa: BLE001  checked by the count below
                    pass
            try:
                left = self.count()
            except Exception:  # noqa: BLE001  no answer: not known to be gone
                left = None
            if left == 0:
                return 0
            self.sleep(wait)
        return left


def version(run) -> str:
    """'SQL Server <version> <level> <update>, <edition>' of the server."""
    out = run("SET NOCOUNT ON; SELECT CONCAT('SQL Server ', CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(64)), ' ', "
              "CAST(SERVERPROPERTY('ProductUpdateLevel') AS nvarchar(64)), ' ', CAST(SERVERPROPERTY('ProductUpdateReference') "
              "AS nvarchar(64)), ', ', CAST(SERVERPROPERTY('Edition') AS nvarchar(128)));\n", "master")
    return (_rows(out) or [""])[0]


def product_version(run) -> str:
    out = run("SET NOCOUNT ON; SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(32));\n", "master")
    return (_rows(out) or [""])[0]
