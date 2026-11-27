#!/usr/bin/env python3
"""grant.py <profile> <principal> --level login|qbc|cdc [--catalog C] [--connection N] [--foreign-catalogs]
[--create-connections] [--apply]: give <principal> (a user's email, a group's name, or "account users") exactly what
Lakeflow Connect needs in this workspace, and no more. Prints the plan; changes nothing without --apply. Then run
check.py to confirm.

  login: workspace assignment (USER) + workspace-access, databricks-sql-access on the principal itself
  qbc:   login + USE CONNECTION on the connection + USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, SELECT, MODIFY
         on the catalog
  cdc:   qbc + CREATE VOLUME on the catalog (staging volumes)
  --foreign-catalogs:   + CREATE FOREIGN CATALOG on the connection, CREATE CATALOG on the metastore
  --create-connections: + CREATE CONNECTION on the metastore

Run as a workspace admin who owns (or can manage) the catalog and the connection. REST through lfc-rest's Rest (no SQL
warehouse, no CLI), so it runs from a laptop and in a notebook. A user, rather than a group, gets the entitlements on
the user itself.
"""
from __future__ import annotations

import argparse
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check import principal

UC = "/api/2.1/unity-catalog"
ENTITLEMENTS = ("workspace-access", "databricks-sql-access")


def plan(level: str, catalog: str = "", connection: str = "", foreign: bool = False, create_conn: bool = False) -> list:
    """[(securable type, name or None for the metastore, [privileges])] for the level (module doc)."""
    out = []
    if level != "login":
        if connection:
            out.append(("connection", connection, ["USE_CONNECTION"]))
        privs = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
        out.append(("catalog", catalog, privs + (["CREATE_VOLUME"] if level == "cdc" else [])))
    if foreign:
        if connection:
            out.append(("connection", connection, ["CREATE_FOREIGN_CATALOG"]))
        out.append(("metastore", None, ["CREATE_CATALOG"]))
    if create_conn:
        out.append(("metastore", None, ["CREATE_CONNECTION"]))
    return out


def grant(api, who: str, level: str, catalog: str = "", connection: str = "", foreign: bool = False,
          create_conn: bool = False, apply: bool = False, out=None) -> int:
    """Print the plan (and carry it out with apply); the number of steps that failed."""
    api, out, bad = Rest.of(api), out or sys.stdout, 0

    def step(what: str, fn) -> None:
        nonlocal bad
        print(f"  {what}", file=out)
        if not apply:
            return
        try:
            fn()
            print("    done", file=out)
        except Exception as e:  # noqa: BLE001  the API's own error, shown; the next step goes on
            bad += 1
            print(f"    FAILED: {str(e)[:200]}", file=out)

    kind, pid, _ = principal(api, who)
    kind = "Users" if kind == "user" else "Groups"
    if not pid:
        print(f"{who} not found in this workspace's account", file=out)
        return 1
    print(f"== {'Applying' if apply else 'Plan (add --apply to make these changes)'} for {who} in {api.name}: level {level}"
          f"{', catalog ' + catalog if catalog else ''}{', connection ' + connection if connection else ''}", file=out)
    try:
        pa = api.get("/api/2.0/preview/permissionassignments").get("permission_assignments") or []
    except Exception as e:  # noqa: BLE001  a non-admin may not read them: said, and the plan goes on
        pa = None
        print(f"  workspace assignment: can't read it ({str(e).split('. Config:')[0][:160]})", file=out)
    cur = [p for a in pa or [] if str((a.get("principal") or {}).get("principal_id")) == str(pid)
           for p in a.get("permissions") or []]
    if pa is None:
        pass
    elif cur:
        print(f"  workspace assignment: already {','.join(cur)}", file=out)
    else:
        step(f"assign {who} to the workspace as USER",
             lambda: api.put(f"/api/2.0/preview/permissionassignments/principals/{pid}", {"permissions": ["USER"]}))
    step(f"entitlements {', '.join(ENTITLEMENTS)} on {who}", lambda: api.patch(
        f"/api/2.0/preview/scim/v2/{kind}/{pid}",
        {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
         "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": e} for e in ENTITLEMENTS]}]}))
    ms = None
    for ty, name, privs in plan(level, catalog, connection, foreign, create_conn):
        if ty == "metastore":
            ms = ms or api.get(f"{UC}/current-metastore-assignment").get("metastore_id")
            name = ms
        body = {"changes": [{"principal": who, "add": privs}]}
        step(f"GRANT {', '.join(p.replace('_', ' ') for p in privs)} ON {ty.upper()} {name} TO `{who}`",
             lambda ty=ty, name=name, body=body: api.patch(f"{UC}/permissions/{ty}/{quote(name, safe='')}", body))
    if apply:
        here = os.path.dirname(os.path.abspath(__file__))
        print(f"now check: python3 -I {os.path.join(here, 'check.py')} {api.name} \"{who}\""
              f"{' --catalog ' + catalog if catalog else ''}{' --connection ' + connection if connection else ''}", file=out)
    return bad


def main(argv=None, api=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("profile")
    ap.add_argument("principal")
    ap.add_argument("--level", required=True, choices=("login", "qbc", "cdc"))
    ap.add_argument("--catalog", default="")
    ap.add_argument("--connection", default="")
    ap.add_argument("--foreign-catalogs", action="store_true")
    ap.add_argument("--create-connections", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    if a.level != "login" and not a.catalog:
        print(f"--catalog is needed for {a.level}")
        return 2
    return 1 if grant(api or Rest.connect(a.profile), a.principal, a.level, a.catalog, a.connection,
                      a.foreign_catalogs, a.create_connections, a.apply) else 0


if __name__ == "__main__":
    sys.exit(main())
