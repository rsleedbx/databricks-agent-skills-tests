#!/usr/bin/env python3
"""add_account.py <account-id> <aws|azure|gcp> [--dry-run] [--free-port]: the one standard way to add an account profile
to ~/.databrickscfg, so ws_token.py can mint tokens for every workspace in that account.
Standard name: account-<cloud>-<account-id>. If a valid account profile for this account already exists (any name), it's
reused and nothing is added; if only expired ones exist, one of them is logged in to again (no duplicate). Otherwise adds
the standard one. Either way runs `databricks auth login` for the cloud's account host, which opens a browser: run it in
your own terminal (in Claude Code: ! python3 add_account.py ...).
--dry-run prints what it would do. The browser login returns to 127.0.0.1:8020; when another process holds that port,
it uses the first free port from 8021 (DATABRICKS_OAUTH_CALLBACK_PORT) and leaves the process alone. --free-port
stops that process instead (ask its owner first) and keeps 8020.
"""
from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run
from ws_token import ACCOUNT_HOSTS, profiles

UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def listeners(port: int) -> list[str]:
    """pids listening on a TCP port (lsof)."""
    _, out, _ = run(["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"], 15)
    return sorted(set(out.split()))


def holder_of(port: int) -> str:
    """'<command> (pid N)' of the first listener on the port, or ''."""
    _, out, _ = run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"], 15)
    for line in out.splitlines()[1:]:
        f = line.split()
        if len(f) > 1:
            return f"{f[0]} (pid {f[1]})"
    return ""


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: add_account.py <account-id> <aws|azure|gcp> [--dry-run] [--free-port]", file=sys.stderr)
        return 2
    aid, cloud, flags = argv[0], argv[1], argv[2:]
    dry = "--dry-run" in flags
    free = "--free-port" in flags
    unknown = [f for f in flags if f not in ("--dry-run", "--free-port")]
    if unknown:
        print(f"unknown {unknown[0]}", file=sys.stderr)
        return 2
    if cloud not in ("aws", "azure", "gcp"):
        print("cloud must be aws, azure or gcp", file=sys.stderr)
        return 2
    ah = ACCOUNT_HOSTS[cloud]
    if not UUID.match(aid):
        print(f"not an account id: {aid}", file=sys.stderr)
        return 2
    name = f"account-{cloud}-{aid}"

    # an existing valid account profile for this account wins, whatever its name
    have = [(p["name"], bool(p.get("valid"))) for p in profiles(validate=True, env=dict(os.environ), timeout=120)
            if p.get("account_id") == aid and p.get("host") == ah]
    for pname, valid in have:
        if valid:
            print(f"account {aid} already has a valid profile: {pname} (nothing added)")
            return 0
    # only expired profiles: log in to one of them again (the standard name if there is one), rather than add a duplicate
    if have:
        names = [n for n, _ in have]
        if name not in names:
            name = names[0]
        print(f"account {aid} has profile(s) whose login expired: {' '.join(names)}; "
              f"logging in to {name} again (no new profile)")

    cmd = f"databricks auth login --host {ah} --account-id {aid} --profile {name}"
    env = dict(os.environ)
    # the browser login comes back to 127.0.0.1:8020. If another program holds it (even one that a launch agent restarts
    # when stopped), the login would hang: use the first free port from 8021, and leave that program alone. --free-port
    # stops it instead and keeps 8020.
    holder = holder_of(8020)
    if holder and free and not dry:
        print(f"stopping {holder}, which holds port 8020, and starting the login right away")
        for pid in listeners(8020):
            try:
                os.kill(int(pid), signal.SIGTERM)
            except (OSError, ValueError):
                pass
        time.sleep(0.3)
    elif holder:
        port = 8021
        while port < 8030 and listeners(port):
            port += 1
        if listeners(port):
            print(f"ports 8020-8030 are all in use: free one, or rerun with --free-port to stop {holder} (ask first)",
                  file=sys.stderr)
            return 3
        print(f"port 8020 is held by {holder}: the login will come back to port {port} instead")
        env["DATABRICKS_OAUTH_CALLBACK_PORT"] = str(port)
        cmd = f"DATABRICKS_OAUTH_CALLBACK_PORT={port} {cmd}"
    if dry:
        print(f"would run: {cmd}")
        return 0
    # the login is interactive (a browser): its output goes to this terminal, not captured
    if subprocess.call(["databricks", "auth", "login", "--host", ah, "--account-id", aid, "--profile", name],
                       env=env) != 0:
        return 1
    rc, _, _ = run(["databricks", "auth", "describe", "--profile", name], 60)
    if rc == 0:
        print(f"ready: {name} logs in")
        return 0
    print(f"logged in, but {name} doesn't authenticate", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
