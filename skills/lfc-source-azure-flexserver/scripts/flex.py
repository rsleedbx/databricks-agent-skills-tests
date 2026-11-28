#!/usr/bin/env python3
"""flex.py pg|mysql plan|create|status|stop|start|delete: an Azure Database for PostgreSQL or MySQL Flexible Server set
up for Lakeflow Connect query-based and CDC.
  Needs RG, LOCATION, SERVER (globally unique), DB. Identity: SUBSCRIPTION, required for every command but plan/create
  (those take az's current one, pin it and print its IDENTITY line). Optional: TENANT_ID (checked), SKU (Standard_B1ms),
  TIER (Burstable), STORAGE_GB (32), VERSION (pg 16, mysql 8.0.21), OWNER ($USER).
  create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the lfcadmin password).
  create: the server with public access and no firewall rules (closed until lfc-source-firewall's server-fw.py adds
  the same list as every other source), the database, and the parameters CDC needs, then a restart if one changed:
    pg     wal_level=logical, max_replication_slots>=10, max_wal_senders>=10
    mysql  binlog_row_image=FULL, binlog_expire_logs_seconds>=604800 (7 days), sql_generate_invisible_primary_key=OFF
           (Azure's default ON gives keyless tables a hidden key)
  TLS stays required (require_secure_transport); the UC connection uses TLS.
  stop/start: a stopped server costs storage only; Azure starts it again by itself after 7 days.
  Tags: owner, purpose=lfc-test, created, CreatedBy=lfc-source-azure-flexserver, RemoveAfter (lfc-test-settings
  REMOVE_AFTER_DAYS, default 7). delete removes the server only when its CreatedBy tag is this skill's (or, for one
  made before that tag, purpose=lfc-test with this owner); not found exits 3, a failed lookup 1, another's 2.
  Calls az (the Azure CLI) only. create passes the password to az as its one flag, as the shell script did.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-source-azure-sql", "scripts"))
import lfc_azure as azure
from script_common import Fail, check_password, exits, need, print_fields, read_password

CMDS = ("plan", "create", "status", "stop", "start", "delete")
ENGINES = {   # the az subcommand, the version default, the database flag, public access (postgres: Enabled; mysql: None)
    "pg": ("postgres", "16", "-n", "Enabled"),
    "mysql": ("mysql", "8.0.21", "-d", "None"),
}
CDC_PARAMS = {   # the parameters CDC needs, in order: (name, value or minimum)
    "pg": (("wal_level", "logical"), ("max_replication_slots", 10), ("max_wal_senders", 10)),
    "mysql": (("binlog_row_image", "FULL"), ("binlog_expire_logs_seconds", 604800),
              ("sql_generate_invisible_primary_key", "OFF")),
}


class Params:
    """Sets server parameters: setp when they differ, atleast when they're below a minimum. Each names what it changed."""

    def __init__(self, S: str, A: list, server: str):
        self.S, self.A, self.server = S, A, server

    def param(self, name: str) -> str:
        return azure.az(self.S, "flexible-server", "parameter", "show", *self.A, "--server-name", self.server,
                        "-n", name, "--query", "value", "-o", "tsv").strip()

    def setp(self, name: str, value) -> str:
        """The name when it was set (it needs a restart), "" when it already had the value."""
        cur = self.param(name)
        if cur.lower() == str(value).lower():
            return ""
        azure.az(self.S, "flexible-server", "parameter", "set", *self.A, "--server-name", self.server, "-n", name,
                 "--value", str(value), "-o", "none")
        return name

    def atleast(self, name: str, minimum: int) -> str:
        cur = self.param(name)
        try:
            if int(cur or 0) >= minimum:
                return ""
        except ValueError:
            pass
        return self.setp(name, minimum)


def srv(S: str, A: list, server: str):
    return azure.show_json(S, "flexible-server", "show", *A, "-n", server)


@exits
def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    engine, cmd = argv[0], argv[1]
    rg, server, db = need("RG"), need("SERVER"), need("DB")
    if engine not in ENGINES:
        print("pg or mysql")
        return 2
    if cmd in ("plan", "create"):
        sub = os.environ.get("SUBSCRIPTION") or azure.default_subscription()
    else:
        sub = need("SUBSCRIPTION", "the subscription_id from the create IDENTITY line (the az default is not used)")
    azure.require(sub, os.environ.get("TENANT_ID", ""))
    S, default_version, dbflag, public = ENGINES[engine]
    version = os.environ.get("VERSION") or default_version
    A = azure.ident(rg, sub)
    if cmd not in CMDS:
        print(f"unknown {cmd}")
        return 2

    if cmd in ("plan", "create"):
        loc = need("LOCATION")
        tier = os.environ.get("TIER", "Burstable")
        sku = os.environ.get("SKU", "Standard_B1ms")
        storage = os.environ.get("STORAGE_GB", "32")
        print(f"server   {server} {S} {version} {tier} {sku} {storage} GB ({loc}), public access, no firewall rules "
              f"{'(exists)' if srv(S, A, server) else '(new)'}")
        print(f"database {db}")
        if cmd == "plan":
            return 0
        pwf = need("ADMIN_PW_FILE")
        check_password(pwf)
        tg = azure.tags("lfc-source-azure-flexserver")
        if not srv(S, A, server):
            # az takes the password only as a flag: it's in this machine's process list while the command runs
            azure.az(S, "flexible-server", "create", *A, "-n", server, "-l", loc, "--admin-user", "lfcadmin",
                     "--admin-password", read_password(pwf), "--tier", tier, "--sku-name", sku,
                     "--storage-size", storage, "--version", version, "--public-access", public,
                     "--tags", *tg, "--yes", "-o", "none")
        if not azure.exists(S, "flexible-server", "db", "show", *A, "-s", server, dbflag, db, "-o", "none"):
            azure.az(S, "flexible-server", "db", "create", *A, "-s", server, dbflag, db, "-o", "none")
        params = Params(S, A, server)
        changed = []
        for name, want in CDC_PARAMS[engine]:
            changed.append(params.setp(name, want) if isinstance(want, str) else params.atleast(name, want))
        changed = [c for c in changed if c]
        if changed:
            print(f"set: {' '.join(changed)}")
            azure.az(S, "flexible-server", "restart", *A, "-n", server, "-o", "none")
            print("restarted")
        azure.record(sub)
        fqdn = (srv(S, A, server) or {}).get("fullyQualifiedDomainName", "")
        print(f"ready: {fqdn} database {db}. Next: lfc-source-firewall scripts/server-fw.py {engine} {server}")
        return 0

    if cmd == "status":
        d = srv(S, A, server)
        if d is None:
            raise Fail(1, f"no server {server}")
        print_fields({"state": d.get("state"), "fqdn": d.get("fullyQualifiedDomainName"), "version": d.get("version"),
                      "sku": (d.get("sku") or {}).get("name")})
        return 0
    if cmd in ("stop", "start"):
        azure.az(S, "flexible-server", cmd, *A, "-n", server, "-o", "none")
        print(f"{cmd}: {server}")
        return 0
    rc = azure.may_delete([S, "flexible-server", "show", *A], server, "lfc-source-azure-flexserver")
    if rc:
        return rc
    azure.az(S, "flexible-server", "delete", *A, "-n", server, "--yes", "-o", "none")
    print(f"deleted {server}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
