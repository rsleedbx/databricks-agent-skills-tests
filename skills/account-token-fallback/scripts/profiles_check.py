#!/usr/bin/env python3
"""profiles_check.py [--json FILE] [--verify]: for every workspace profile in ~/.databrickscfg, does it log in, and if not,
can an account login cover it (ws_token.py)? Read-only: one `databricks auth profiles -o json`, which validates each
profile (a few seconds for dozens). --json FILE reads a saved copy of that output instead.
Prints TSV, one line per workspace profile:
  profile  cloud  account (first 8)  login (valid | expired)  status  next step
status: ok (its own login works) | try (expired, but a fallback may work: a valid account profile for its account,
or on Azure the Azure CLI: ws_token.py <profile>) | account-expired (an account profile exists, its login has expired) |
no-account-profile (none for that account: add_account.py <account-id> <cloud>) | no-account-id (the profile doesn't say
its account).
Without --verify the status is a guess from the profiles alone; exit 1 if any workspace profile isn't ok.
--verify runs ws_token.py for every profile that isn't ok and replaces its status with covered (it got a working token)
or needs-login (it didn't; the reason follows). It reads and writes nothing in ~/.databrickscfg, and deletes each token
file. Exit 1 if any profile needs a login. Without --verify, the accounts that workspace profiles need but no valid
account profile covers follow on stderr.
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run
from ws_token import real_env


def load(json_file: str | None):
    """The profiles: from a saved copy, else `databricks auth profiles -o json` (validated). None on failure."""
    if json_file:
        with open(json_file) as fh:
            return json.load(fh).get("profiles") or []
    rc, out, err = run(["databricks", "auth", "profiles", "-o", "json"], 120)
    if rc != 0:
        print(err, file=sys.stderr, end="")
        return None
    return json.loads(out).get("profiles") or []


def is_account(p: dict) -> bool:
    return (p.get("host") or "").startswith("https://accounts.")


def classify(profiles: list[dict]):
    """(rows, need): one row per workspace profile, and for each account that needs a login or an add, its note."""
    ok_for: dict[str, str] = {}    # account_id -> a valid account profile
    any_for: dict[str, str] = {}   # account_id -> any account profile (all expired)
    for p in profiles:
        if is_account(p) and p.get("account_id"):
            if p.get("valid") and p["account_id"] not in ok_for:
                ok_for[p["account_id"]] = p["name"]
            any_for.setdefault(p["account_id"], p["name"])
    rows, need = [], {}
    for p in profiles:
        if is_account(p):
            continue
        name, cloud, acct = p["name"], p.get("cloud") or "", p.get("account_id") or ""
        valid = bool(p.get("valid"))
        if valid:
            status, nxt = "ok", ""
        elif not acct:
            status, nxt = "no-account-id", f"databricks auth login --profile {name}  (a new login writes account_id)"
        elif acct in ok_for or cloud == "azure":
            via = f"  (via {ok_for[acct]})" if acct in ok_for else "  (Azure CLI)"
            status, nxt = "try", f"python3 {HERE}/ws_token.py {name}{via}"
        elif acct in any_for:
            status, nxt = "account-expired", f"databricks auth login --profile {any_for[acct]}"
            need[acct] = (cloud, f"log in again: databricks auth login --profile {any_for[acct]}")
        else:
            status, nxt = "no-account-profile", f"python3 {HERE}/add_account.py {acct} {cloud}"
            need[acct] = (cloud, f"add one: python3 {HERE}/add_account.py {acct} {cloud}")
        rows.append({"profile": name, "cloud": cloud, "account": acct, "valid": valid, "status": status, "next": nxt})
    return rows, need


def _after_prefix(line: str) -> str:
    """'ws-token: <profile>: rest' -> 'rest' (the part of a ws_token line after the profile name)."""
    text = line.removeprefix("ws-token: ")
    _, sep, rest = text.partition(": ")
    return rest if sep else text


def verify(row: dict) -> dict:
    """Run ws_token.py for a profile: covered (its token works) or needs-login (the reason, from its stderr)."""
    rc, out, err = run([sys.executable, os.path.join(HERE, "ws_token.py"), row["profile"]], 300, real_env(), stdin="")
    lines = [x for x in err.splitlines() if x.strip()]
    cfg = out.strip()
    if rc == 0 and cfg:
        try:
            os.remove(cfg)
        except OSError:
            pass
        return dict(row, status="covered", next=_after_prefix(lines[-1]) if lines else "")
    kept = [x.removeprefix("ws-token: ") for x in lines
            if ": using a token" not in x and "its login has expired and no account profile" not in x]
    why = "; ".join(kept) if kept else (_after_prefix(lines[-1]) if lines else "")
    return dict(row, status="needs-login", next=why)


def main(argv: list[str]) -> int:
    json_file, want_verify = None, False
    rest = list(argv)
    while rest:
        a = rest.pop(0)
        if a == "--json" and rest:
            json_file = rest.pop(0)
        elif a == "--verify":
            want_verify = True
        else:
            print(f"unknown {a}", file=sys.stderr)
            return 2
    profiles = load(json_file)
    if profiles is None:
        return 1
    rows, need = classify(profiles)
    print("profile\tcloud\taccount\tlogin\tstatus\tnext")
    if want_verify:
        final = [r if r["status"] == "ok" else verify(r) for r in rows]
        for r in final:
            print_row(r)
        return 1 if any(r["status"] == "needs-login" for r in final) else 0
    for r in rows:
        print_row(r)
    for acct, (cloud, text) in need.items():
        print(f"# account {acct} ({cloud}): no valid account profile; {text}", file=sys.stderr)
    return 1 if any(r["status"] != "ok" for r in rows) else 0


def print_row(r: dict) -> None:
    print("\t".join([r["profile"], r["cloud"], (r["account"] or "-")[:8], "valid" if r["valid"] else "expired",
                     r["status"], r["next"]]))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
