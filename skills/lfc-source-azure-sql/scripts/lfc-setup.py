#!/usr/bin/env python3
"""lfc-setup.py: the test login, seed tables, and change tracking + CDC on an Azure SQL Database or Managed Instance.
  Needs HOST (SQL Database: <server>.database.windows.net; MI: mi.py endpoint's host), DB, ADMIN_PW_FILE (sqladmin's
  password), LOGIN_PW_FILE (a private file, or keyring:<name>, with the test login's password). Optional: PORT (1433; MI
  3342), LOGIN (lfc_user), SCHEMA (dbo). Needs sqlcmd (go-sqlcmd); passwords go through SQLCMDPASSWORD, never the command line.
  go-sqlcmd doesn't read a batch from standard input: each batch is a private file passed with -i.
  Steps: the login and a user for it in master (the utility's server-scoped grants need one); the database (MI only:
  SQL Database already has it); the user in DB, db_owner while the setup runs; intpk (primary key, cursor dt) and dtix
  (no key) with 10 rows each; the Lakeflow utility objects from the docs; change tracking on intpk, CDC on dtix,
  lakeflowFixPermissions; then db_owner is dropped again. Idempotent: rerun after a fix.
  The utility script is downloaded from the docs (urllib, where the shell script used curl).
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from script_common import Fail, check_password, exits, need, read_password, tail, tool

UTILITY = "https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql"


def batch_master(login: str, lpw: str, db: str) -> str:
    return (f"IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'{login}') CREATE LOGIN [{login}] WITH PASSWORD = N'{lpw}';\n"
            f"ELSE ALTER LOGIN [{login}] WITH PASSWORD = N'{lpw}';\n"
            f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{login}') CREATE USER [{login}] FOR LOGIN [{login}];\n"
            f"IF DB_ID(N'{db}') IS NULL AND SERVERPROPERTY('EngineEdition') = 8 EXEC (N'CREATE DATABASE [{db}]');\n")


def batch_db(login: str, sc: str) -> str:
    return f"""IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{login}') CREATE USER [{login}] FOR LOGIN [{login}] WITH DEFAULT_SCHEMA = [{sc}];
ALTER ROLE db_owner ADD MEMBER [{login}];
IF SCHEMA_ID(N'{sc}') IS NULL EXEC (N'CREATE SCHEMA [{sc}]');
IF OBJECT_ID(N'{sc}.intpk') IS NULL BEGIN
  CREATE TABLE [{sc}].intpk (pk INT IDENTITY NOT NULL PRIMARY KEY, dt DATETIME NOT NULL DEFAULT GETDATE(), ops VARCHAR(255) DEFAULT 'insert');
  CREATE INDEX idx_intpk_dt ON [{sc}].intpk (dt);
  INSERT INTO [{sc}].intpk (ops) SELECT TOP 10 'insert' FROM sys.all_objects;
END
IF OBJECT_ID(N'{sc}.dtix') IS NULL BEGIN
  CREATE TABLE [{sc}].dtix (pk BIGINT, dt DATETIME NOT NULL DEFAULT GETDATE(), ops VARCHAR(255) DEFAULT 'insert');
  CREATE INDEX idx_dtix_dt ON [{sc}].dtix (dt);
  INSERT INTO [{sc}].dtix (pk) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects;
END
GO
CREATE OR ALTER TRIGGER [{sc}].intpk_set_dt ON [{sc}].intpk AFTER UPDATE AS
  BEGIN SET NOCOUNT ON; UPDATE t SET dt = GETDATE() FROM [{sc}].intpk t JOIN inserted i ON i.pk = t.pk; END
GO
CREATE OR ALTER TRIGGER [{sc}].dtix_set_dt ON [{sc}].dtix AFTER UPDATE AS
  BEGIN SET NOCOUNT ON; UPDATE t SET dt = GETDATE() FROM [{sc}].dtix t JOIN inserted i ON i.pk = t.pk; END
GO
"""


def batch_setup(login: str, sc: str) -> str:
    return (f"EXEC dbo.lakeflowSetupChangeTracking @Tables = '{sc}.intpk', @User = '{login}', @Retention = '2 DAYS', @CreateDdlSupportingObjects = 1;\n"
            f"EXEC dbo.lakeflowSetupChangeDataCapture @Tables = '{sc}.dtix', @User = '{login}';\n"
            f"EXEC dbo.lakeflowFixPermissions @User = '{login}', @Tables = '{sc}.intpk,{sc}.dtix';\n"
            f"ALTER ROLE db_owner DROP MEMBER [{login}];\n")


@exits
def main(argv: list[str]) -> int:
    host, db = need("HOST"), need("DB")
    admin_ref, login_ref = need("ADMIN_PW_FILE"), need("LOGIN_PW_FILE")
    port = os.environ.get("PORT") or "1433"
    login = os.environ.get("LOGIN") or "lfc_user"
    sc = os.environ.get("SCHEMA") or "dbo"
    for ref in (admin_ref, login_ref):
        check_password(ref)
    admin_pw = read_password(admin_ref)
    login_pw = read_password(login_ref)
    lpw = login_pw.replace("'", "''")   # the login's password goes into a private batch file, not onto the command line

    def sq(database: str, *args: str):
        """sqlcmd as the admin on database; its stdout (text). A failure is the caller's to report."""
        return tool(["sqlcmd", "-S", f"{host},{port}", "-U", "sqladmin", "-d", database, "-C", "-l", "60", "-b",
                     "-h", "-1", "-W", *args], env={"SQLCMDPASSWORD": admin_pw})

    with tempfile.TemporaryDirectory() as T:
        def batch(name: str, text: str) -> str:
            path = os.path.join(T, name)
            with open(path, "w") as f:
                f.write(text)
            return path

        p = sq("master", "-i", batch("master.sql", batch_master(login, lpw, db)))
        if p.returncode:
            print(tail(p.stdout, 5))
            return 2
        print(f"login {login} (and its user in master)")
        p = sq(db, "-i", batch("db.sql", batch_db(login, sc)))
        if p.returncode:
            print(tail(p.stdout, 5))
            return 2
        print(f"seed tables {sc}.intpk, {sc}.dtix")
        try:
            with urllib.request.urlopen(UTILITY, timeout=120) as r:
                utility = r.read().decode("utf-8", "replace")
        except OSError as e:
            raise Fail(1, f"can't download {UTILITY}: {e}") from None
        p = sq(db, "-i", batch("utility_script.sql", utility))
        if p.returncode:
            print(tail(p.stdout, 5))
            raise Fail(2, "utility objects install failed")
        version = sq(db, "-Q", "SET NOCOUNT ON; SELECT dbo.lakeflowUtilityVersion()").stdout.splitlines()[:1]
        print(f"utility {version[0] if version else ''}")
        p = sq(db, "-i", batch("setup.sql", batch_setup(login, sc)))
        if p.returncode:
            print(tail(p.stdout, 8))
            return 2
        hits = [x for x in p.stdout.splitlines() if re.search(r"⚠|Error|Msg [0-9]", x)]
        for x in hits[:5]:
            print(x)
        show = sq(db, "-Q", "SET NOCOUNT ON; SELECT 'ct ' + STRING_AGG(OBJECT_NAME(object_id), ',') FROM sys.change_tracking_tables; "
                            "SELECT 'cdc ' + STRING_AGG(name, ',') FROM sys.tables WHERE is_tracked_by_cdc = 1")
        print(show.stdout, end="")
        count = (f"SET NOCOUNT ON; SELECT '{login} reads ' + CAST((SELECT COUNT(*) FROM [{sc}].intpk) AS varchar) + "
                 f"' + ' + CAST((SELECT COUNT(*) FROM [{sc}].dtix) AS varchar) + ' rows'")
        read = tool(["sqlcmd", "-S", f"{host},{port}", "-U", login, "-d", db, "-C", "-l", "60", "-b", "-h", "-1", "-W",
                     "-Q", count], env={"SQLCMDPASSWORD": login_pw})
        print(read.stdout, end="")
        return read.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
