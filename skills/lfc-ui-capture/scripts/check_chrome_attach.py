#!/usr/bin/env python3
"""check_chrome_attach.py: before a UI capture, check that the Chrome DevTools tools will attach to your own Chrome.
Checks: the chrome-devtools MCP entry in ~/.claude.json has --autoConnect; a running server was started with it (a server
started before the flag was added still opens its own blank browser); your Chrome is running. Changes nothing. Exit 0
when all pass, 1 with the fix for the first that fails.
"""
from __future__ import annotations

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run

CFG = os.path.join(os.path.expanduser("~"), ".claude.json")


def main(argv: list[str]) -> int:
    try:
        with open(CFG) as fh:
            args = json.load(fh)["mcpServers"]["chrome-devtools"]["args"]
    except (OSError, ValueError, KeyError, TypeError):
        print(f"no chrome-devtools MCP server in {CFG}: add it (npx chrome-devtools-mcp --autoConnect)")
        return 1
    print("config: " + " ".join(args))
    if "--autoConnect" not in args:
        print(f"FIX: --autoConnect is missing (something rewrote the entry). Add it to mcpServers.chrome-devtools.args "
              f"in {CFG}, then /mcp -> chrome-devtools -> Reconnect")
        return 1
    # one line per server process: an old one (a session that ended, or a reconnect) can outlive its session
    _, ps_out, _ = run(["ps", "-axo", "pid=,lstart=,command="], 30)
    servers = [ln for ln in ps_out.splitlines() if re.search(r"node .*bin/chrome-devtools-mcp", ln) and "grep" not in ln]
    if not servers:
        print("server: not running yet (Claude Code starts it with the session; /mcp shows its state)")
    else:
        for ln in servers:
            parts = ln.split()
            pid, started, cmd = parts[0], " ".join(parts[2:4]), " ".join(parts[6:])
            flag = "--autoConnect" if " --autoConnect " in f" {cmd} " else "no --autoConnect: opens its own browser"
            print(f"server: pid {pid}, started {started}, {flag}")
        if not any("--autoConnect" in ln for ln in servers):
            print("NOTE: no server runs with --autoConnect: /mcp -> chrome-devtools -> Reconnect")
    if run(["pgrep", "-x", "Google Chrome"], 15)[0] != 0:
        print("FIX: your Chrome isn't running: start it and sign in to the workspace")
        return 1
    print("chrome: running. In it, chrome://inspect/#remote-debugging must be on; accept its prompt when the tools connect")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
