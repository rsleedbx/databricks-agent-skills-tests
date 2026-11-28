#!/usr/bin/env python3
"""ws_token.py <profile> [--host URL [--account-id ID]] [--cfg FILE] [--force]: keep a workspace profile usable when its
OAuth login has expired, with a token from another login you already have: the Azure CLI (Azure), or the account login
for the same account.
  --host: a workspace you have no profile for yet (one just created in an account you're logged in to). <profile> is
    then only the name the config file gives it. With --account-id the account profiles for that account are tried;
    without it, every account profile on the workspace's cloud.
  - The profile's own login works: prints nothing, exits 0 (keep using ~/.databrickscfg).
  - It doesn't: on Azure, tries an Entra ID token from the Azure CLI (any workspace in its tenant); then an account
    profile (from `databricks auth profiles`) with the same account_id, minting a short-lived token from it
    (`databricks auth token --host <accounts host> --account-id <id>`). Checks the token against the workspace, writes a
    one-profile config FILE (mode 600; default $TMPDIR/ws-token-<profile>.cfg) and prints FILE. Use it as
        CFG=$(python3 ws_token.py <profile>) && [ -n "$CFG" ] && export DATABRICKS_CONFIG_FILE=$CFG
    so `--profile <profile>` keeps working. The token lasts about an hour (some workspaces: 30 minutes): rerun when
    calls start failing, or every 20 minutes in a long run. The default FILE is shared by every shell on the machine:
    give a long run a FILE of its own with --cfg.
  - Exit 1 with the reason when neither works: the account login first when it has expired too (one login covers every
    workspace in the account), else databricks auth login --profile <profile>.
  - Exit 3 when every route failed and the workspace answered "Did not find workspace" (deleted or expired, such as an
    FEVM sandbox): no login brings it back; use another workspace or recreate it.
  - Exit 4 when every route failed and its host didn't resolve: a deleted workspace, or the network (VPN off, a
    private-link workspace, DNS): check the network before recreating anything.
  --force mints even when the profile's login works (to test the fallback). The token is never printed.

Profile fields come from `databricks auth profiles` (the CLI reads ~/.databrickscfg; this file never does). Laptop only.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cli-setup", "scripts"))
from laptop import run

AZURE_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"   # the Azure Databricks resource id
ACCOUNT_HOSTS = {"azure": "https://accounts.azuredatabricks.net", "gcp": "https://accounts.gcp.databricks.com",
                 "aws": "https://accounts.cloud.databricks.com"}


def say(msg: str) -> None:
    print(f"ws-token: {msg}", file=sys.stderr)


def real_env() -> dict:
    """The environment for the real profiles: DATABRICKS_CONFIG_FILE_SOURCE if set, else the CLI's default config (never
    a token file this script wrote earlier)."""
    env = dict(os.environ)
    src = os.environ.get("DATABRICKS_CONFIG_FILE_SOURCE")
    if src:
        env["DATABRICKS_CONFIG_FILE"] = src
    else:
        env.pop("DATABRICKS_CONFIG_FILE", None)
    return env


def profiles(validate: bool = False, env: dict | None = None, timeout: float = 60) -> list[dict]:
    """Every profile `databricks auth profiles` lists (name, host, account_id, valid, ...). Not validated unless asked
    (validating every profile takes seconds per dozen); env defaults to real_env()."""
    argv = ["databricks", "auth", "profiles", "-o", "json"] + ([] if validate else ["--skip-validate"])
    rc, out, _ = run(argv, timeout, real_env() if env is None else env)
    if rc != 0 or not out.strip():
        return []
    return json.loads(out).get("profiles") or []


def cloud_accounts_host(host: str) -> str:
    if "azuredatabricks.net" in host:
        return ACCOUNT_HOSTS["azure"]
    if "gcp.databricks.com" in host:
        return ACCOUNT_HOSTS["gcp"]
    return ACCOUNT_HOSTS["aws"]


class Fallback:
    """One ws-token run: the profile's fallbacks in order, until one token is accepted."""

    def __init__(self, profile: str, host: str, cfg: str, prof: list[dict]):
        self.profile, self.host, self.cfg, self.prof = profile, host, cfg, prof
        self.err = ""           # the last try's error text (up to 400 bytes)
        self.gone = ""          # "Did not find workspace": the workspace no longer exists
        self.unreachable = ""   # its host doesn't resolve

    def field(self, name: str, key: str) -> str:
        for p in self.prof:
            if p.get("name") == name:
                return str(p.get(key) or "")
        return ""

    def try_token(self, token: str, label: str) -> str | None:
        """Write the one-profile config for token (mode 600) and check it against the workspace. When the workspace
        accepts it, the file takes FILE's place and FILE is returned; a failed try leaves FILE as it was."""
        tmp = f"{self.cfg}.{os.getpid()}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"[{self.profile}]\nhost  = {self.host}\ntoken = {token}\n")
        env = dict(os.environ, DATABRICKS_CONFIG_FILE=tmp)
        rc, _, err = run(["databricks", "current-user", "me", "--profile", self.profile, "-o", "json"], 60, env)
        if rc == 0:
            os.replace(tmp, self.cfg)
            say(f"{self.profile}: using a token from {label} (about an hour)")
            return self.cfg
        self.err = err[:400]
        os.remove(tmp)
        self.seen()
        return None

    def seen(self) -> None:
        first = self.err.splitlines()[0][:120] if self.err.strip() else ""
        if "Did not find workspace" in self.err:
            self.gone = first
        elif "no such host" in self.err or "Could not resolve host" in self.err:
            self.unreachable = first

    def azure_cli(self) -> str | None:
        """Azure: an Entra ID token from the Azure CLI works for every workspace in its tenant. A workspace in another
        tenant names it in the error; retry there if az knows that tenant."""
        if ".azuredatabricks.net" not in self.host:
            return None
        _, tok, _ = run(["az", "account", "get-access-token", "--resource", AZURE_RESOURCE,
                         "--query", "accessToken", "-o", "tsv"], 60)
        tok = tok.strip()
        if not tok:
            return None
        found = self.try_token(tok, "the Azure CLI login")
        if found:
            return found
        m = re.search(r"Expected iss claim to be: https://sts\.windows\.net/([0-9a-f-]+)", self.err)
        ten = re.search(r"[0-9a-f-]{36}", m.group(1)) if m else None
        if not ten:
            say(f"{self.profile}: the Azure CLI token isn't accepted")
            return None
        tenant = ten.group(0)
        _, tok, _ = run(["az", "account", "get-access-token", "--resource", AZURE_RESOURCE, "--tenant", tenant,
                          "--query", "accessToken", "-o", "tsv"], 60)
        if tok.strip():
            found = self.try_token(tok.strip(), f"the Azure CLI login, tenant {tenant[:8]}...")
            if found:
                return found
        say(f"{self.profile}: its workspace is in Entra tenant {tenant[:8]}..., which the Azure CLI isn't logged in to: "
            f"az login --tenant {tenant}")
        return None

    def account_profiles(self, account_id: str, host_arg: str) -> list[str]:
        """Account profiles to try: an accounts.* host with the same account_id; with --host and no account id, every
        account profile with an account id on the workspace's cloud."""
        if account_id:
            return [p["name"] for p in self.prof if p.get("account_id") == account_id
                    and (p.get("host") or "").startswith("https://accounts.")]
        if host_arg:
            want = cloud_accounts_host(self.host)
            return [p["name"] for p in self.prof if (p.get("host") or "") == want and p.get("account_id")]
        return []


def main(argv: list[str]) -> int:
    if not argv or argv[0].startswith("-"):
        print("usage: ws_token.py <profile> [--host URL [--account-id ID]] [--cfg FILE] [--force]", file=sys.stderr)
        return 2
    profile, rest = argv[0], argv[1:]
    cfg, force, host_arg, aid_arg = "", False, "", ""
    while rest:
        a = rest.pop(0)
        if a == "--cfg" and rest:
            cfg = rest.pop(0)
        elif a == "--force":
            force = True
        elif a == "--host" and rest:
            host_arg = rest.pop(0).rstrip("/")
        elif a == "--account-id" and rest:
            aid_arg = rest.pop(0)
        else:
            print(f"unknown {a}", file=sys.stderr)
            return 2
    cfg = cfg or os.path.join(tempfile.gettempdir(), f"ws-token-{profile}.cfg")
    prof = profiles()
    fb = Fallback(profile, "", cfg, prof)
    host, aid = fb.field(profile, "host"), fb.field(profile, "account_id")
    if host_arg and host.rstrip("/") != host_arg:   # no profile of its own
        host, aid = host_arg, aid_arg
    elif not force:
        rc, _, _ = run(["databricks", "current-user", "me", "--profile", profile, "-o", "json"], 60, real_env())
        if rc == 0:
            say(f"{profile}: its own login works")
            return 0
    fb.host = host
    if not host:
        say(f"{profile}: no such profile (databricks auth profiles); for a workspace without one, pass --host")
        return 1

    found = fb.azure_cli()
    if found:
        print(found)
        return 0

    if aid:
        names = fb.account_profiles(aid, host_arg)
    elif host_arg:
        names = fb.account_profiles("", host_arg)
    else:
        say(f"{profile}: no account_id in its profile, so no account login to fall back on: "
            f"databricks auth login --profile {profile}")
        return 1
    expired = ""
    for a in names:
        ah, aa = fb.field(a, "host"), fb.field(a, "account_id")
        rc, out, _ = run(["databricks", "auth", "token", "--host", ah, "--account-id", aa, "--profile", a, "-o", "json"],
                         60, real_env())
        try:
            tok = json.loads(out).get("access_token") or "" if rc == 0 else ""
        except json.JSONDecodeError:
            tok = ""
        if not tok:
            say(f"account profile {a}: no token (login expired?)")
            expired = expired or a
            continue
        found = fb.try_token(tok, f"account profile {a}")
        del tok
        if found:
            print(found)
            return 0
        say(f"account profile {a}: its token isn't accepted by {host} (some workspaces, such as field sandboxes, take "
            "only their own login)")

    if fb.gone:
        say(f"{profile}: the workspace {host} no longer exists (deleted or expired; FEVM sandboxes expire): no login "
            f"brings it back; use another workspace or recreate it ({fb.gone})")
        return 3
    if fb.unreachable:
        say(f"{profile}: {host} can't be reached: its name doesn't resolve. Check the network first (VPN, private link, "
            f"DNS); if the workspace was deleted, use another one ({fb.unreachable})")
        return 4
    if host_arg:
        say(f"{profile}: no account profile{f' for account {aid[:8]}...' if aid else ''} could mint a token {host} "
            "accepts: log in to its account (python3 add_account.py <account-id> <cloud>), or to the workspace: "
            f"databricks auth login --host {host} --profile {profile}")
    elif expired:
        say(f"{profile}: its login has expired, and so has the account login that covers it: log in to the account "
            f"(every workspace in it then works): databricks auth login --profile {expired}; or to this workspace "
            f"only: databricks auth login --profile {profile}")
    elif aid:
        say(f"{profile}: its login has expired and no account profile for account {aid[:8]}... could mint a token: "
            f"log in to the account (python3 add_account.py {aid} <cloud>), or to the workspace: "
            f"databricks auth login --profile {profile}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
