"""lfc_azure: the Azure calls the source skills share (lfc-source-azure-sql, -flexserver, -eventhubs and -firewall): az with
its identity named on every call, the IDENTITY record and the login check (cli-setup's cloud-identity.sh, in Python),
the tags a created resource carries, and the ownership check a delete makes before it removes anything. az is the Azure
CLI, a cloud CLI: a subprocess, as the scripts called it before.

    import lfc_azure as azure
    A = azure.ident(rg, subscription)                  # ["--resource-group", rg, "--subscription", sub]
    azure.az("sql", "server", "show", *A, "-n", name)  # az's stdout; Fail (az's exit code) when az fails
    rc = azure.may_delete(["sql", "server", "show", *A], name, "lfc-source-azure-sql")   # 0: this skill's to delete
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from script_common import Fail

HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS = os.path.join(HERE, "..", "..", "lfc-test-settings", "scripts", "settings.py")


def az(*args: str, quiet: bool = False) -> str:
    """az's stdout. Its stderr is shown, unless quiet: then it is kept in the Fail's text (for a lookup that may fail)."""
    try:
        p = subprocess.run(["az", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE if quiet else None, text=True, check=False)
    except FileNotFoundError:
        raise Fail(127, "az: not on PATH (cli-setup's check-tools.sh azure names the install command)") from None
    if p.returncode:
        raise Fail(p.returncode, (p.stderr or "").strip() if quiet else "")
    return p.stdout


def show_json(*args: str):
    """az's JSON answer, or None when the lookup fails (its error isn't shown)."""
    try:
        return json.loads(az(*args, "-o", "json", quiet=True) or "null")
    except Fail:
        return None


def exists(*args: str) -> bool:
    """Whether az succeeds (its output and error are not shown)."""
    try:
        az(*args, quiet=True)
        return True
    except Fail:
        return False


def ident(rg: str, subscription: str) -> list[str]:
    """The identity every call for a resource names: its resource group and subscription."""
    return ["--resource-group", rg, "--subscription", subscription]


def default_subscription() -> str:
    """az's current subscription (the default plan and create take, then pin)."""
    return az("account", "show", "--query", "id", "-o", "tsv").strip()


def record(subscription: str) -> None:
    """Prints the IDENTITY line (cloud-identity.sh's az_record): keep its values with the resource's other settings."""
    j = json.loads(az("account", "show", "--subscription", subscription, "-o", "json") or "null")
    print(f"IDENTITY azure tenant_id={j['tenantId']} subscription_id={j['id']} subscription_name={j['name']}")


def require(subscription: str, tenant: str = "") -> None:
    """cloud-identity.sh's az_require: this az login reaches the subscription (in the tenant, when given). Else Fail."""
    try:
        got = az("account", "show", "--subscription", subscription, "--query", "tenantId", "-o", "tsv", quiet=True).strip()
    except Fail:
        raise Fail(1, f"subscription {subscription} isn't in this az login: az login --tenant {tenant or '<tenant_id>'}") from None
    if tenant and got != tenant:
        raise Fail(1, f"subscription {subscription} is in tenant {got}, not {tenant}: az login --tenant {tenant}")


def owner() -> str:
    """Who a resource is for: OWNER, else the login's USER."""
    return os.environ.get("OWNER") or os.environ.get("USER", "")


def remove_after() -> str:
    """The RemoveAfter date for a new resource: today + REMOVE_AFTER_DAYS (lfc-test-settings' remove-after)."""
    p = subprocess.run([sys.executable, SETTINGS, "remove-after"], stdout=subprocess.PIPE, text=True, check=False)
    if p.returncode:
        raise Fail(p.returncode, "settings.py remove-after failed")
    return p.stdout.strip()


def tags(skill: str) -> list[str]:
    """What a created resource is tagged with, as `--tags` words: owner, purpose=lfc-test, created (UTC date),
    CreatedBy=<skill>, RemoveAfter."""
    return [f"owner={owner()}", "purpose=lfc-test", f"created={datetime.now(timezone.utc):%Y-%m-%d}",
            f"CreatedBy={skill}", f"RemoveAfter={remove_after()}"]


def lookup(show: list[str], name: str):
    """A resource's tags: ("ok", tags: a dict, or None), ("missing", ""), or ("error", az's error text). show: the az
    words up to the name, e.g. ["sql", "server", "show", *ident(rg, sub)]."""
    try:
        return "ok", json.loads(az(*show, "-n", name, "--query", "tags", "-o", "json", quiet=True) or "null")
    except Fail as e:
        return ("missing", "") if "ResourceNotFound" in e.text else ("error", e.text)


def is_ours(t, skill: str, owner_name: str | None = None, also: tuple = ()) -> bool:
    """Whether a resource with tags t is this skill's: CreatedBy is the skill (or one of also). With no CreatedBy (made
    before the tag), purpose=lfc-test and this owner (owner_name None: no such fallback)."""
    t = t or {}
    made = t.get("CreatedBy") or ""
    if made:
        return made == skill or made in also
    return owner_name is not None and (t.get("purpose") or "") == "lfc-test" and (t.get("owner") or "") == owner_name


def may_delete(show: list[str], name: str, skill: str) -> int:
    """Checks what a delete removes. Prints why and returns the exit code when it's not this skill's: 3 not found,
    1 its tags can't be read (nothing deleted), 2 another's. 0 when it's this skill's to delete."""
    status, t = lookup(show, name)
    if status == "missing":
        print(f"{name} not found: nothing to delete")
        return 3
    if status == "error":
        print(f"couldn't read {name}'s tags; nothing deleted:")
        print(t)
        return 1
    me = owner()
    if is_ours(t, skill, me):
        return 0
    print(f"{name} wasn't made by this skill for {me} (tags: {json.dumps(t, separators=(',', ':'))}): not deleted")
    return 2
