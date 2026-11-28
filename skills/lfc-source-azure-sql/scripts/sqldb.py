#!/usr/bin/env python3
"""sqldb.py plan|create|status|wake|delete: an Azure SQL Database (serverless, auto-pause) on its own logical server.
  Needs RG, LOCATION, SERVER (globally unique), DB. Identity: SUBSCRIPTION, required for every command but plan/create.
  plan/create take az's current subscription, pin it, and print its IDENTITY line: keep it with SUBSCRIPTION in your
  env file. Optional: TENANT_ID (checked against the login), VCORES (1),
  MIN_CAPACITY (0.5), AUTO_PAUSE (60 minutes idle, then paused; -1 never), FREE (1: the free offer, up to 10 databases
  per subscription; when its monthly vCore seconds run out the database pauses until next month), OWNER ($USER).
  create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the sqladmin password).
  create: the server with public access and no firewall rules (closed until lfc-source-firewall's server-fw.py adds
  the same list as every other source), then the database: GeneralPurpose serverless Gen5, local backup redundancy.
  A paused database resumes on the next login; that first login fails with error 40613 ("Database ... is not
  currently available") until it's online, about a minute. wake: logs in until it answers (needs sqlcmd, ADMIN_PW_FILE).
  status: the database's state (Online, Paused, Resuming) and its pause settings. delete: the server and its databases,
  only when its CreatedBy tag is lfc-source-azure-sql (or, made before that tag, purpose=lfc-test with this owner);
  not found exits 3, a failed lookup 1, another's 2. Tags: owner, purpose, created, CreatedBy, RemoveAfter
  (lfc-test-settings REMOVE_AFTER_DAYS, default 7).
  Calls az (the Azure CLI) and sqlcmd as commands; Databricks is not involved. Passwords: a file or keyring:<name>
  (secret-handling), passed to sqlcmd in SQLCMDPASSWORD, and to az as the one flag the server create takes.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lfc_azure as azure
from script_common import (
    Fail,
    check_password,
    exits,
    need,
    print_fields,
    read_password,
    tool,
)

MINE = "lfc-source-azure-sql"
CMDS = ("plan", "create", "status", "wake", "delete")


def srv(A: list, server: str):
    return azure.show_json("sql", "server", "show", *A, "-n", server)


def db_show(A: list, server: str, db: str):
    return azure.show_json("sql", "db", "show", *A, "-s", server, "-n", db)


def admin_file() -> str:
    ref = need("ADMIN_PW_FILE")
    check_password(ref)
    return ref


@exits
def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    cmd = argv[0]
    rg, server, db = need("RG"), need("SERVER"), need("DB")
    if cmd in ("plan", "create"):
        sub = os.environ.get("SUBSCRIPTION") or azure.default_subscription()
    else:
        sub = need("SUBSCRIPTION", "the subscription_id from the create IDENTITY line (the az default is not used)")
    azure.require(sub, os.environ.get("TENANT_ID", ""))
    A = azure.ident(rg, sub)
    if cmd not in CMDS:
        print(f"unknown {cmd}")
        return 2

    if cmd in ("plan", "create"):
        loc = need("LOCATION")
        print(f"server   {server} ({loc}) public access, no firewall rules {'(exists)' if srv(A, server) else '(new)'}")
        free = os.environ.get("FREE", "") == "1"
        print(f"database {db} serverless GP Gen5 {os.environ.get('VCORES', '1')} vCore, "
              f"min {os.environ.get('MIN_CAPACITY', '0.5')}, auto-pause {os.environ.get('AUTO_PAUSE', '60')} min"
              f"{', free offer' if free else ''} {'(exists)' if db_show(A, server, db) else '(new)'}")
        azure.record(sub)
        if cmd == "plan":
            return 0
        pwf = admin_file()
        tg = azure.tags(MINE)
        # az takes the password only as a flag: it's in this machine's process list while the command runs
        if not srv(A, server):
            azure.az("sql", "server", "create", *A, "-n", server, "-l", loc, "--admin-user", "sqladmin",
                     "--admin-password", read_password(pwf), "--enable-public-network", "true",
                     "--minimal-tls-version", "1.2", "-o", "none")
        azure.az("resource", "tag", *A, "--resource-type", "Microsoft.Sql/servers", "-n", server, "--tags", *tg, "-o", "none")
        if not db_show(A, server, db):
            F = ["--use-free-limit", "--free-limit-exhaustion-behavior", "AutoPause"] if free else []
            azure.az("sql", "db", "create", *A, "-s", server, "-n", db, "--edition", "GeneralPurpose",
                     "--compute-model", "Serverless", "--family", "Gen5",
                     "--capacity", os.environ.get("VCORES", "1"), "--min-capacity", os.environ.get("MIN_CAPACITY", "0.5"),
                     "--auto-pause-delay", os.environ.get("AUTO_PAUSE", "60"),
                     "--backup-storage-redundancy", "Local", *F, "--tags", *tg, "-o", "none")
        azure.record(sub)
        print(f"ready: {server}.database.windows.net,1433 database {db}. "
              f"Next: lfc-source-firewall scripts/server-fw.py sql {server}")
        return 0

    if cmd == "status":
        d = db_show(A, server, db)
        if d is None:
            raise Fail(1, f"no database {db} on {server}")
        print_fields({k: d.get(k) for k in ("status", "autoPauseDelay", "minCapacity", "useFreeLimit",
                                              "freeLimitExhaustionBehavior")})
        return 0

    if cmd == "wake":
        pwf = admin_file()
        pw = read_password(pwf)
        for _ in range(20):
            ok = tool(["sqlcmd", "-S", f"{server}.database.windows.net,1433", "-U", "sqladmin", "-d", db, "-C", "-l", "30",
                       "-Q", "SELECT 1", "-h", "-1"], env={"SQLCMDPASSWORD": pw}, stdout=subprocess.DEVNULL).returncode == 0
            if ok:
                print(f"online: {db}")
                return 0
            time.sleep(15)
        d = db_show(A, server, db) or {}
        print(f"still not online after 5 minutes: {d.get('status')}")
        return 1

    # delete
    rc = azure.may_delete(["sql", "server", "show", *A], server, MINE)
    if rc:
        return rc
    azure.az("sql", "server", "delete", *A, "-n", server, "--yes")
    print(f"deleted {server} and its databases")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
