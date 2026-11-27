#!/usr/bin/env python3
"""check.py <profile> <principal> [--catalog C] [--connection N] [--modes qbc,fcqbc,icdc,scdc]: can <principal> (a user's
email, a group's name, or "account users") run Lakeflow Connect in this workspace?

Read-only. One line per requirement: PASS / FAIL / WARN / INFO, and for each FAIL the command or SQL that fixes it (run
those as a workspace admin or the securable's owner; or use grant.py). Exit 1 if anything FAILs. Modes: qbc
(query-based), fcqbc (query-based from a foreign catalog), icdc (integrated CDC), scdc (standard CDC, a gateway).
REST through lfc-rest's Rest, so it runs from a laptop and in a notebook (no profile: `from check import check`).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest

UC = "/api/2.1/unity-catalog"


class Report:
    """Rows printed as they come (status, what, detail; the next command under a FAIL / WARN), FAIL and ASK counted."""
    width = 44

    def __init__(self, out=None, nxt: str = "fix"):
        self.out, self.nxt = out or sys.stdout, nxt
        self.rows: list = []
        self.fails = 0

    def row(self, status: str, what: str, detail: str = "", nxt: str = "") -> None:
        print(f"{status:<5} {what:<{self.width}} {detail}".rstrip(), file=self.out)
        self.rows.append((status, what, detail))
        if status in ("FAIL", "ASK"):
            self.fails += 1
        if nxt:
            print(f"      {self.nxt}: {nxt}", file=self.out)

    def has_row(self, status: str, prefix: str, text: str = "") -> bool:
        return any(s == status and w.startswith(prefix) and text in d for s, w, d in self.rows)


def _get(api: Rest, path: str, **q) -> dict:
    """GET's answer, or {} when it's refused or missing (a check reads what it may)."""
    try:
        return api.get(path, **q)
    except Exception:  # noqa: BLE001  not visible to this caller: the row says so
        return {}


def setting(api: Rest, name: str):
    v = _get(api, f"/api/2.1/settings/{name}")
    return ((v.get("boolean_val") if "boolean_val" in v else v.get("effective_boolean_val")) or {}).get("value")


def privileges(api: Rest, kind: str, name: str, who: str) -> set:
    v = _get(api, f"{UC}/effective-permissions/{kind}/{quote(name, safe='')}", principal=who)
    return {p.get("privilege") for a in v.get("privilege_assignments") or [] for p in a.get("privileges") or []}


def has(privs: set, p: str) -> bool:
    return p in privs or "ALL_PRIVILEGES" in privs


def entitlements(doc: dict) -> set:
    return {e.get("value") for e in doc.get("entitlements") or []}


def principal(api, who: str) -> tuple:
    """(kind "user" | "group", its account id or None, the groups it's in): a user's from the account (and "account
    users"); a group's or "account users"'s: none."""
    api = Rest.of(api)
    if "@" in who:
        u = (_get(api, "/api/2.0/account/scim/v2/Users", filter=f'userName eq "{who}"', attributes="id,groups")
             .get("Resources") or [{}])[0]
        return "user", u.get("id"), {g.get("display") for g in u.get("groups") or []} | {"account users"}
    g = (_get(api, "/api/2.0/account/scim/v2/Groups", filter=f'displayName eq "{who}"', attributes="id")
         .get("Resources") or [{}])[0]
    return "group", g.get("id"), set()


def check(api, who: str, catalog: str = "", connection: str = "", modes: str = "qbc,icdc", out=None,
          r: Report | None = None) -> int:
    """The rows for `who` (module doc), printed to `out`; the number of FAILs."""
    api, r = Rest.of(api), r or Report(out)
    mode = set(modes.split(","))
    name = api.name
    print(f"== {who} in {name} (modes: {modes}{', catalog ' + catalog if catalog else ''}"
          f"{', connection ' + connection if connection else ''})", file=r.out)
    kind, pid, groups = principal(api, who)
    if not pid:
        r.row("FAIL", "principal exists in the account", f"{who} not found")
        return r.fails
    r.row("PASS", f"principal exists in the account ({kind})", f"id {pid}")

    # workspace: Unity Catalog, connector previews
    ms = _get(api, f"{UC}/current-metastore-assignment")
    msid, wsid = ms.get("metastore_id") or "", str(ms.get("workspace_id") or "")
    if msid:
        r.row("PASS", "Unity Catalog metastore assigned")
    else:
        r.row("FAIL", "Unity Catalog metastore assigned", "none", "assign the workspace to a metastore (account admin)")
    if "icdc" in mode:   # PostgreSQL has no setting where its rollout hasn't reached: INFO, not WARN
        for e in ("sql_server", "mysql", "oracle", "postgres"):
            v = setting(api, f"integrated_cdc_{e}_connector")
            if e == "postgres" and v is None:
                r.row("INFO", f"integrated CDC preview: {e}", "no setting on this workspace (rolled out in phases)")
            elif v is True:
                r.row("PASS", f"integrated CDC preview: {e}", "on")
            else:
                r.row("WARN", f"integrated CDC preview: {e}", "not found" if v is None else str(v).lower(),
                      "enable it (lfc-workspace-setup step 1), or ask the account team")
    if "scdc" in mode:
        v = setting(api, "direct_cdc_connector")
        if v is True:
            r.row("PASS", "gateway route: direct_cdc_connector", "on")
        else:
            r.row("WARN", "gateway route: direct_cdc_connector", "not found" if v is None else str(v).lower(),
                  "enable it (lfc-workspace-setup step 1), or ask the account team")

    me = _get(api, "/api/2.0/preview/scim/v2/Me")
    self_ = me.get("userName") == who
    if self_:
        wsadmin = any(g.get("display") == "admins" for g in me.get("groups") or [])
    elif kind == "user":
        wu = (_get(api, "/api/2.0/preview/scim/v2/Users", filter=f'userName eq "{who}"', attributes="groups")
              .get("Resources") or [{}])[0]
        wsadmin = any(g.get("display") == "admins" for g in wu.get("groups") or [])
    else:
        wsadmin = False

    def mine(owner: str) -> bool:   # the principal, a group it's in, or the workspace admins group when it's an admin
        return bool(owner) and (owner == who or owner in groups
                                or (wsadmin and owner.startswith("_workspace_admins_") and owner.endswith("_" + wsid)))

    # login: assigned (directly or through a group), and entitlements
    pa = _get(api, "/api/2.0/preview/permissionassignments")
    readable = "permission_assignments" in pa   # non-admins get nothing back
    assigned = [f"{p.get('display_name') or p.get('user_name') or ''}:{','.join(a.get('permissions') or [])}"
                for a in pa.get("permission_assignments") or [] for p in [a.get("principal") or {}]
                if mine(p.get("display_name") or "") or p.get("user_name") == who]
    if assigned:
        r.row("PASS", "assigned to the workspace", " ".join(assigned))
    elif self_:
        r.row("PASS", "assigned to the workspace", "you're logged in, so through a group ("
              + ", ".join(g.get("display") for g in (me.get("groups") or [])[:4]) + ")")
    elif not readable:
        r.row("WARN", "assigned to the workspace", "can't read assignments without workspace admin")
    else:
        r.row("FAIL", "assigned to the workspace", "not directly or through a group",
              f"databricks api put /api/2.0/preview/permissionassignments/principals/{pid} --json "
              f"'{json.dumps({'permissions': ['USER']})}' --profile {name}")
    admin = any("ADMIN" in a for a in assigned)
    ents: set = set()
    if kind == "user":
        ents |= entitlements((_get(api, "/api/2.0/preview/scim/v2/Users", filter=f'userName eq "{who}"',
                                   attributes="entitlements").get("Resources") or [{}])[0])
    else:
        ents |= entitlements(_get(api, f"/api/2.0/preview/scim/v2/Groups/{pid}", attributes="entitlements"))
    for a in pa.get("permission_assignments") or []:
        p = a.get("principal") or {}
        if p.get("group_name") is not None and mine(p.get("display_name") or ""):
            ents |= entitlements(_get(api, f"/api/2.0/preview/scim/v2/Groups/{p.get('principal_id')}", attributes="entitlements"))
    if self_:
        ents |= entitlements(me)
        for g in me.get("groups") or []:
            ents |= entitlements(_get(api, f"/api/2.0/preview/scim/v2/Groups/{g.get('value')}", attributes="entitlements"))
    for e in ("workspace-access", "databricks-sql-access"):
        if admin or e in ents:
            r.row("PASS", f"entitlement {e}", "as workspace admin" if admin else "granted")
        elif self_ and e == "workspace-access":
            r.row("PASS", f"entitlement {e}", "you're logged in")
        elif not readable or self_:
            r.row("WARN", f"entitlement {e}", "not visible to a non-admin; try it (open a SQL editor) or ask an admin")
        else:
            r.row("FAIL", f"entitlement {e}", "missing",
                  f"PATCH it onto {who}'s group (lfc-workspace-setup step 3; built-in users/admins may be locked)")

    # Unity Catalog: metastore, connection, catalog
    m = privileges(api, "metastore", msid, who) if msid else set()
    msown = _get(api, f"{UC}/metastores/{msid}").get("owner") or "" if msid else ""
    if mine(msown):
        m.add("ALL_PRIVILEGES")
    if has(m, "CREATE_CONNECTION"):
        r.row("PASS", "CREATE CONNECTION on the metastore", f"metastore owner: {msown}" if mine(msown) else "")
    else:
        r.row("WARN", "CREATE CONNECTION on the metastore", "no: can only use existing connections",
              f"GRANT CREATE CONNECTION ON METASTORE TO `{who}`")
    if "fcqbc" in mode:
        if has(m, "CREATE_CATALOG"):
            r.row("PASS", "CREATE CATALOG on the metastore (foreign catalog)")
        else:
            r.row("WARN", "CREATE CATALOG on the metastore (foreign catalog)", "no: needs an existing foreign catalog to use",
                  f"GRANT CREATE CATALOG ON METASTORE TO `{who}`, or GRANT USE CATALOG on an existing foreign catalog")
    if connection:
        c = privileges(api, "connection", connection, who)
        own = _get(api, f"{UC}/connections/{quote(connection, safe='')}").get("owner") or ""
        if mine(own) or has(c, "USE_CONNECTION"):
            r.row("PASS", f"USE CONNECTION on {connection}", f"owner {own}" if own else "")
        else:
            r.row("FAIL", f"USE CONNECTION on {connection}", "no", f"GRANT USE CONNECTION ON CONNECTION `{connection}` TO `{who}`")
        if "fcqbc" in mode:
            if has(c, "CREATE_FOREIGN_CATALOG") or mine(own):
                r.row("PASS", f"CREATE FOREIGN CATALOG on {connection}")
            else:
                r.row("FAIL", f"CREATE FOREIGN CATALOG on {connection}", "no",
                      f"GRANT CREATE FOREIGN CATALOG ON CONNECTION `{connection}` TO `{who}`")
    if catalog:
        cat = _get(api, f"{UC}/catalogs/{quote(catalog, safe='')}")
        k = privileges(api, "catalog", catalog, who) | ({"ALL_PRIVILEGES"} if mine(cat.get("owner") or "") else set())
        # a schema of its own (CREATE SCHEMA) is owned, so it carries USE SCHEMA, CREATE TABLE and CREATE VOLUME in it
        own_schema = has(k, "CREATE_SCHEMA")
        for pv in ("USE_CATALOG", "CREATE_SCHEMA", "USE_SCHEMA", "CREATE_TABLE"):
            if has(k, pv):
                r.row("PASS", f"{pv} on {catalog}")
            elif own_schema and pv in ("USE_SCHEMA", "CREATE_TABLE"):
                r.row("PASS", f"{pv} on {catalog}", "in a schema of its own (CREATE SCHEMA: the owner has it)")
            else:
                r.row("FAIL", f"{pv} on {catalog}", "no", f"GRANT {pv.replace('_', ' ')} ON CATALOG `{catalog}` TO `{who}`")
        if mode & {"icdc", "scdc"}:
            if has(k, "CREATE_VOLUME") or own_schema:
                r.row("PASS", f"CREATE VOLUME on {catalog} (CDC staging)",
                      "" if has(k, "CREATE_VOLUME") else "in a schema of its own (CREATE SCHEMA)")
            else:
                r.row("FAIL", f"CREATE VOLUME on {catalog} (CDC staging)", "no", f"GRANT CREATE VOLUME ON CATALOG `{catalog}` TO `{who}`")
            root = cat.get("storage_root") or ""
            ds = _get(api, f"{UC}/external-locations/__databricks_managed_storage_location").get("url") or ""
            if ds and root.startswith(ds):
                r.row("FAIL", f"{catalog} not on Default Storage (CDC)", "it is", "use a catalog with its own managed location")
            else:
                r.row("PASS", f"{catalog} not on Default Storage (CDC)")
        iso = cat.get("isolation_mode") or ""
        if iso == "ISOLATED":
            b = _get(api, f"{UC}/workspace-bindings/catalogs/{quote(catalog, safe='')}")
            if wsid in {str(x) for x in b.get("workspaces") or []}:
                r.row("PASS", f"{catalog} visible here (ISOLATED, bound to this workspace)")
            else:
                r.row("FAIL", f"{catalog} visible here (ISOLATED)", f"not bound to workspace {wsid}",
                      f"databricks workspace-bindings update {catalog} --json '{{\"assign_workspaces\": [{wsid}]}}' --profile {name}")
        else:
            r.row("PASS", f"{catalog} visible here", iso or "OPEN")
    return r.fails


def main(argv=None, api=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("profile")
    ap.add_argument("principal")
    ap.add_argument("--catalog", default="")
    ap.add_argument("--connection", default="")
    ap.add_argument("--modes", default="qbc,icdc")
    a = ap.parse_args(argv)
    n = check(api or Rest.connect(a.profile, retry_s=20), a.principal, a.catalog, a.connection, a.modes)
    print("\nready: nothing failed" if not n else f"\n{n} requirement(s) failed")
    return 1 if n else 0


if __name__ == "__main__":
    sys.exit(main())
