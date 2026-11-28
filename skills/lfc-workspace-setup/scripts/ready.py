#!/usr/bin/env python3
"""ready.py [<profile>] [--catalog C] [--principal P] [--egress-file FILE] [--no-enable]: is this workspace ready for a
Lakeflow Connect demo?

Checks, in the order a demo needs them: the profile logs in; the workspace's cloud and region (where the sources should
be); serverless and classic compute; the connector previews (integrated CDC per engine, the gateway route, column
selection); a target catalog that works for CDC; access for you (and --principal, e.g. "account users") through check.py;
the serverless egress ranges the source firewall must allow (with --egress-file, saved, and on a later run, what changed);
the Lakeflow Connect connections already here. One line each, PASS / FAIL / WARN / INFO / ASK, with the next command for
anything to fix; ASK means stop and ask the user (the target catalog isn't writable, or there is none). Exit 1 if anything
FAILs or ASKs, 2 on a usage error.

The one change it makes: a connector preview that's off is turned on (as a workspace admin), with a line saying so first.
--no-enable makes it read-only. Turn one off again with the same patch and "value": false.
Without <profile> or --catalog, the saved test settings give them (lfc-test-settings: DATABRICKS_PROFILE, LFC_CATALOG).
Needs databricks-sdk (REST through lfc-rest's Rest, no CLI), so it runs on a laptop (a profile) and in a notebook (no profile: the
notebook's own login): `from ready import main; main(["--catalog", "c"])`, or %run it. The egress list is a shell script: in a
notebook that step says it was skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest, default_catalog

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check import Report, check

HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS = os.path.join(HERE, "..", "..", "lfc-test-settings", "scripts", "settings.py")
EGRESS = os.path.join(HERE, "..", "..", "lfc-source-firewall", "scripts", "serverless-egress.sh")

# the connector previews: integrated CDC per engine, direct_cdc_connector (the gateway route for MySQL, PostgreSQL and
# Oracle: off, a gateway is refused with "Ingestion source '<ENGINE>' is not supported"), column selection
PREVIEWS = ("integrated_cdc_sql_server_connector", "integrated_cdc_mysql_connector", "integrated_cdc_oracle_connector",
            "integrated_cdc_postgres_connector", "direct_cdc_connector", "conn_cdc_col_select")
PHASED = ("integrated_cdc_postgres_connector",)   # rolled out in phases: no setting on some workspaces


UC = "/api/2.1/unity-catalog"


class Ready(Report):
    """api: an lfc_rest.Rest (the workspace's REST API; a fake in tests)."""
    width = 40

    def __init__(self, api, name: str, enable: bool = True, out=None):
        super().__init__(out, nxt="next")
        self.api, self.p, self.enable = api, name, enable

    def call(self, method: str, path: str, body=None):
        """(ok, answer, error text)."""
        try:
            return True, self.api.call(method, path, body), ""
        except Exception as e:  # noqa: BLE001  any API refusal is a check's answer
            return False, None, str(e)

    def j(self, path: str, key: str = ""):
        """GET path: the answer (its `key` list when given), or None when the call fails."""
        ok, v, _ = self.call("GET", path)
        return (v.get(key) or [] if key else v) if ok else None

    def setting(self, name: str):
        """A boolean setting's value: boolean_val (set here) or effective_boolean_val (inherited); None if unreadable."""
        v = self.j(f"/api/2.1/settings/{name}")
        if not isinstance(v, dict):
            return None
        b = v.get("boolean_val") if "boolean_val" in v else v.get("effective_boolean_val")
        return (b or {}).get("value")



def _str(v) -> str:
    return "" if v is None else str(v).lower() if isinstance(v, bool) else str(v)


def compute(r: Ready, me2: dict) -> str:
    """Step 3: serverless warehouses, serverless compute, classic compute and whether you can create clusters; the
    classic verdict (yes, no, noperm, unknown)."""
    wh = r.j("/api/2.0/sql/warehouses", "warehouses") or []
    nsl = sum(1 for w in wh if w.get("enable_serverless_compute") is True)
    if nsl:
        r.row("PASS", "3 serverless SQL warehouses", str(nsl))
    else:
        r.row("WARN", "3 serverless SQL warehouses", "none",
              "create one (databricks-execution-compute), or check serverless is enabled for the workspace")
    sv = r.setting("serverless_compute")
    if sv is True:
        r.row("PASS", "3 serverless compute", "on (serverless_compute setting)")
    else:
        r.row("FAIL", "3 serverless compute", _str(sv) or "not readable",
              "a workspace admin turns serverless compute on (workspace settings), or ask the account team")
    # clusters list-zones: a serverless-only workspace answers "No such workerEnvironment 'serverless-<id>'" (AWS) or
    # "does not have any associated worker environment" (Azure); an Azure workspace with classic compute "Azure worker
    # environment does not support zones". Any other answer (seen on GCP) doesn't tell. (list-node-types answers in all.)
    ok, z, err = r.call("GET", "/api/2.0/clusters/list-zones")
    if ok:
        classic = "yes"
        r.row("INFO", "3 classic compute", f"available (zones {' '.join((z or {}).get('zones') or [])}): classic gateways "
              "possible; their egress IP must be in the source firewall")
    elif "does not support zones" in err:
        classic = "yes"
        r.row("INFO", "3 classic compute", "available (an Azure worker environment): classic gateways possible; their "
              "egress IP must be in the source firewall")
    elif re.search(r"workerEnvironment 'serverless-|does not have any associated worker environment", err):
        classic = "no"
        r.row("INFO", "3 classic compute", "none: a serverless-only workspace (gateways and pipelines run serverless)")
    else:
        classic = "unknown"
        r.row("INFO", "3 classic compute", f"can't tell here ({err[:90].replace(chr(10), ' ')}...)")
    if classic == "yes":   # creating a cluster takes allow-cluster-create (admins have all) or a usable cluster policy
        ents = {e.get("value") for e in me2.get("entitlements") or []}
        for g in me2.get("groups") or []:
            gd = r.j(f"/api/2.0/preview/scim/v2/Groups/{g.get('value')}?attributes=entitlements") or {}
            ents |= {e.get("value") for e in gd.get("entitlements") or []}
        npol = len(r.j("/api/2.0/policies/clusters/list", "policies") or [])
        if any(g.get("display") == "admins" for g in me2.get("groups") or []):
            r.row("PASS", "3 classic: you can create clusters", "as a workspace admin")
        elif "allow-cluster-create" in ents:
            r.row("PASS", "3 classic: you can create clusters", "allow-cluster-create entitlement")
        elif npol:
            r.row("WARN", "3 classic: you can create clusters", f"no allow-cluster-create; {npol} cluster policies "
                  "visible, so only within a policy", "set the classic gateway's cluster policy_id to one you can use, "
                  "or use serverless: true")
        else:
            classic = "noperm"
            r.row("WARN", "3 classic: you can create clusters", "no allow-cluster-create and no cluster policy you can "
                  "use", "use serverless: true, or ask a workspace admin for allow-cluster-create or a policy")
    # a standard CDC gateway runs classic by default (some can run serverless); integrated CDC and query-based run
    # serverless by default. Leave serverless unset, read the choice back from the update, try the opposite on an error
    modes = {"no": "leave serverless unset; a gateway left unset is refused here (\"You must use serverless compute\"): "
                   "set serverless: true for it",
             "yes": "leave serverless unset: query-based and integrated CDC ran serverless, a gateway classic (needs its "
                    "NAT IP in the source firewall); read back with pipelines get-update",
             "noperm": "leave serverless unset; a gateway defaulting to classic needs cluster creation you don't have: if "
                       "it fails, set serverless: true"}
    r.row("INFO", "3 modes on this workspace", modes.get(classic, "leave serverless unset and read back with pipelines "
          "get-update; if the create or update fails on compute, try the opposite"))
    return classic


def previews(r: Ready, admin: bool) -> None:
    """Step 4: each connector preview on; one that's off turned on when allowed (module doc)."""
    for s in PREVIEWS:
        v = r.setting(s)
        body = {"name": s, "boolean_val": {"value": True}}
        fix = f"databricks workspace-settings-v2 patch-public-workspace-setting {s} --json '{json.dumps(body)}'"
        was = _str(v) or "not readable"
        if v is True:
            r.row("PASS", f"4 preview {s}", "on")
        elif s in PHASED and v is None:
            r.row("INFO", f"4 preview {s}", "no setting on this workspace (rolled out in phases; ask the account team)")
        elif not r.enable:
            r.row("WARN", f"4 preview {s}", was, f"{fix}   (workspace admin; or rerun without --no-enable)")
        elif not admin:
            r.row("WARN", f"4 preview {s}", f"{was}; not a workspace admin, so not enabled", f"{fix}   (a workspace admin)")
        else:
            print(f"      >>> turning on preview {s} in {r.p} (it was {was}; --no-enable to skip)", file=r.out)
            ok, _, err = r.call("PATCH", f"/api/2.1/settings/{s}", body)
            if ok and r.setting(s) is True:
                r.row("PASS", f"4 preview {s}", f"turned on now (was {was})")
            else:
                r.row("WARN", f"4 preview {s}", f"couldn't turn it on: {err[:120].replace(chr(10), ' ')}",
                      "the workspace's Previews page, or the account team")
    r.row("INFO", "4 a gateway refused anyway", "\"Ingestion source ... is not supported\" with direct_cdc_connector on: "
          "the account team")


def target(r: Ready, cat: str, me: str, my_groups: set, admin: bool) -> str:
    """Step 5: the target catalog (--catalog, else the workspace default), schema default; the catalog for step 6, empty
    when the user must be asked."""
    ds = (r.j(f"{UC}/external-locations/__databricks_managed_storage_location") or {}).get("url") or ""
    dflt = default_catalog(r.api)
    keep = saved("LFC_CATALOG")   # the saved one when this workspace has it (saved settings can come from another)
    if keep and not r.j(f"/api/2.1/unity-catalog/catalogs/{quote(keep)}"):
        keep = ""
    cat, sch = cat or keep or dflt, "default"   # then this workspace's own default: never an assumed main
    wsid = str((r.j(f"{UC}/current-metastore-assignment") or {}).get("workspace_id") or "")

    def mine(owner: str) -> bool:   # owning gives every privilege, which the effective-privileges API doesn't report
        return bool(owner) and (owner == me or owner in my_groups or owner == "account users"
                                or (admin and owner.startswith("_workspace_admins_") and owner.endswith("_" + wsid)))

    def eff(kind: str, name: str) -> set:
        v = r.j(f"{UC}/effective-permissions/{kind}/{quote(name)}?principal={quote(me)}") or {}
        return {p.get("privilege") for a in v.get("privilege_assignments") or [] for p in a.get("privileges") or []}

    def has(privs: set, p: str) -> bool:
        return p in privs or "ALL_PRIVILEGES" in privs

    def on_default_storage(root: str) -> bool:
        return bool(ds) and bool(root) and root.startswith(ds)

    def choices() -> None:
        print("      writable targets here (pick one, then rerun with --catalog <name>):", file=r.out)
        k = 0
        for c in r.j(f"{UC}/catalogs", "catalogs") or []:
            n = c.get("name")
            if c.get("catalog_type") != "MANAGED_CATALOG" or n in ("system", "hive_metastore"):
                continue
            p = eff("catalog", n) | ({"ALL_PRIVILEGES"} if mine(c.get("owner") or "") else set())
            if has(p, "USE_CATALOG") and has(p, "CREATE_SCHEMA"):
                k += 1
                root = c.get("storage_root") or ""
                note = ("metastore root storage" if not root else
                        "Default Storage: query-based only" if on_default_storage(root) else "CDC ok")
                print(f"        {k:2d}. {n:<40} {note}", file=r.out)
        if not k:
            print("        none: ask for a catalog and grants (SKILL.md step 2)", file=r.out)

    ask = "ask the user which catalog and schema the target tables go to"
    c = r.j(f"{UC}/catalogs/{quote(cat)}") if cat and cat not in ("system", "hive_metastore") else None
    if not cat:
        r.row("ASK", "5 target catalog", "no default catalog set", ask)
    elif cat in ("system", "hive_metastore"):
        r.row("ASK", f"5 target catalog {cat}", f"{'the default; ' if dflt == cat else ''}pipelines can't write to {cat}", ask)
    elif c is None:
        r.row("ASK", f"5 target catalog {cat}", "not found or not visible here",
              "ask the user which catalog to use (or bind this one to the workspace)")
    else:
        root = c.get("storage_root") or ""
        ck = eff("catalog", cat) | ({"ALL_PRIVILEGES"} if mine(c.get("owner") or "") else set())
        s = r.j(f"{UC}/schemas/{quote(cat + '.' + sch)}")
        sk = (eff("schema", f"{cat}.{sch}") | ({"ALL_PRIVILEGES"} if mine(s.get("owner") or "") else set())) if s else set()
        where = f"{cat}.{sch}" + (" (the workspace default)" if cat == dflt else "")
        if has(ck, "USE_CATALOG") and ((has(sk, "USE_SCHEMA") and has(sk, "CREATE_TABLE")) or has(ck, "CREATE_SCHEMA")):
            if on_default_storage(root):
                r.row("WARN", f"5 target {where}", "writable, but on Default Storage: query-based only, not CDC",
                      "if the demo uses CDC, ask the user to pick one marked CDC ok")
                choices()
            else:
                r.row("PASS", f"5 target {where}", "writable" + ("; on the metastore's root storage" if not root else ""))
            return cat
        r.row("ASK", f"5 target {where}", f"not writable by {me} (needs USE CATALOG and CREATE SCHEMA, or CREATE TABLE "
              f"in {sch})", "ask the user to pick a target below, or for the grants")
    choices()
    return ""


def access(r: Ready, who: list, cat: str) -> None:
    """Step 6: check.py's check for each principal (the catalog's grants only for a settled target)."""
    import io
    modes = "qbc,fcqbc" if r.has_row("WARN", "5 ", "Default Storage") else "qbc,fcqbc,icdc"
    for w in who:
        sub = Report(io.StringIO())
        bad = check(r.api, w, cat, modes=modes, r=sub)
        warn = [f"{s_:<5} {x:<44} {d}" for s_, x, d in sub.rows if s_ == "WARN"]
        fail = [f"{s_:<5} {x:<44} {d}" for s_, x, d in sub.rows if s_ == "FAIL"]
        on = f", on {cat}" if cat else ""
        if not bad:
            r.row("PASS", f"6 access: {w}", f"check.py: 0 failed, {len(warn)} warnings{on}")
        else:
            r.row("FAIL", f"6 access: {w}", f"check.py: {bad} failed{on}",
                  f"python3 -I {os.path.join(HERE, 'check.py')} {r.p} \"{w}\"{' --catalog ' + cat if cat else ''} (for the fixes), then grant.py")
        for x in (warn if not bad else fail):
            print("        " + x, file=r.out)


def egress(r: Ready, cloud: str, region: str, state: str) -> None:
    """Step 7: the serverless egress ranges (lfc-source-firewall's serverless-egress.sh)."""
    if not shutil.which("bash") or not shutil.which("curl"):
        r.row("INFO", "7 serverless egress ranges", "serverless-egress.sh needs bash and curl: skipped here")
        return
    p = subprocess.run(["bash", EGRESS, cloud, region] + (["--state", state] if state else []),
                       capture_output=True, text=True, check=False)
    lines = p.stdout.splitlines()
    n = sum(1 for x in lines if x and x[0] not in "#+-")
    what = f"7 serverless egress ranges ({cloud} {region})"
    if p.returncode == 0:
        same = ", unchanged since the saved list" if state and any(x.startswith("# unchanged") for x in lines) else ""
        r.row("PASS", what, f"{n} IPv4 CIDRs in ip-ranges.json{same}: lfc-source-firewall")
    elif p.returncode == 3:
        add, rem = sum(x.startswith("+") for x in lines), sum(x.startswith("-") for x in lines)
        r.row("WARN", what, f"CHANGED since {state}: {add} added, {rem} removed", "sync the source firewall (lfc-source-firewall)")
    else:
        r.row("WARN", what, "region not in ip-ranges.json (or not reachable)", "don't guess the IPs: check lfc-source-firewall")


def connections(r: Ready) -> None:
    by: dict = {}
    for c in r.j(f"{UC}/connections", "connections") or []:
        t = c.get("connection_type")
        if t in ("MYSQL", "POSTGRESQL", "SQLSERVER", "ORACLE"):
            by[t] = by.get(t, 0) + 1
    got = ", ".join(f"{t} {n}" for t, n in sorted(by.items())) or "none"
    r.row("INFO", "8 database connections", f"{got} (lfc-connection-sync creates them; test each with remote_query)")


def in_workspace() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def saved(key: str) -> str:
    if not os.path.exists(SETTINGS):
        return ""
    p = subprocess.run([sys.executable, SETTINGS, "get", key], capture_output=True, text=True, check=False)
    return p.stdout.strip() if p.returncode == 0 else ""


def main(argv=None, api=None) -> int:
    """argv as on the command line; api: an lfc_rest.Rest to use instead of signing in (tests)."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("profile", nargs="?")
    ap.add_argument("--catalog", default="")
    ap.add_argument("--principal", default="")
    ap.add_argument("--egress-file", default="")
    ap.add_argument("--no-enable", action="store_true")
    a = ap.parse_args(argv)
    prof = a.profile or saved("DATABRICKS_PROFILE")
    if not prof and not in_workspace() and api is None:
        print(f"ready.py: no <profile>, and no DATABRICKS_PROFILE saved: python3 {SETTINGS} setup")
        return 2
    cat = a.catalog   # else this workspace's default catalog (target()), then the saved LFC_CATALOG
    api = api or Rest.connect(prof or None, retry_s=20)   # read-only probes: a refusal is an answer, not worth 5 min
    name = prof or api.name
    r = Ready(api, prof, enable=not a.no_enable)
    print(f"== {name}")
    ok, me_doc, err = r.call("GET", "/api/2.0/preview/scim/v2/Me")
    if not ok:
        r.row("FAIL", "1 profile logs in", err[:160].replace("\n", " "), f"databricks auth login --profile {prof}")
        print("\nstopped: nothing else can be checked")
        return 1
    me = (me_doc or {}).get("userName") or ""
    r.row("PASS", "1 profile logs in", f"as {me}")
    ms = r.j(f"{UC}/metastore_summary") or {}
    cloud = str(ms.get("global_metastore_id") or "").split(":")[0]
    region = ms.get("region") or ""
    if cloud:
        r.row("PASS", "2 Unity Catalog metastore; cloud, region", f"{cloud} {region}")
    else:
        r.row("FAIL", "2 Unity Catalog metastore", "none assigned", "assign a metastore (account admin)")
    me2 = me_doc or {}
    groups = {g.get("display") for g in me2.get("groups") or []}
    admin = "admins" in groups
    compute(r, me2)
    previews(r, admin)
    settled = target(r, cat, me, groups, admin)
    access(r, [me] + ([a.principal] if a.principal else []), settled)
    if cloud:
        egress(r, cloud, region, a.egress_file)
    connections(r)
    print()
    if r.fails:
        print(f"{name}: NOT READY: {r.fails} check(s) failed or need an answer (ASK)")
    elif r.has_row("WARN", "5 ", "Default Storage"):
        print(f"{name}: ready for query-based; NOT ready for CDC: the target is on Default Storage (pick a catalog "
              "marked CDC ok, rerun with --catalog)")
    else:
        print(f"{name}: ready for query-based and CDC{' into ' + settled if settled else ''} (read any WARN lines)")
    return 1 if r.fails else 0


if __name__ == "__main__":
    sys.exit(main())
