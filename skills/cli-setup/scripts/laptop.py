#!/usr/bin/env python3
"""laptop.py: the laptop's own command-line tools (the cloud CLIs, yq, jq), run with a timeout, no shell, and an answer
back as data. Imported by the laptop scripts (cli-setup, lfc-test-settings, account-token-fallback, secret-handling);
it is not a Databricks call, so a workspace never runs it: Databricks calls go through lfc_rest.Rest.

    from laptop import have, run, json_out
    rc, out, err = run(["az", "account", "show"], timeout=30)   # rc 127 (not installed) or 124 (timed out) included
    data = json_out(["az", "group", "list", "-o", "json"])      # None unless the command exits 0 with JSON

Stdlib only.
"""
from __future__ import annotations

import json
import shutil
import subprocess


def have(cmd: str) -> bool:
    """The command is on PATH (command -v)."""
    return shutil.which(cmd) is not None


def run(argv: list[str], timeout: float = 60, env: dict | None = None, stdin: str | None = None):
    """(exit code, stdout, stderr). A missing command is 127 and a timed-out one 124, with the reason on stderr."""
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env, input=stdin, check=False)
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError as e:
        return 127, "", f"{argv[0]}: {e.strerror}"
    except subprocess.TimeoutExpired:
        return 124, "", f"{argv[0]}: timed out after {timeout:g} s"


def out_of(argv: list[str], timeout: float = 60, env: dict | None = None) -> str | None:
    """stdout of a command that exits 0 (its trailing newline removed), else None."""
    rc, out, _ = run(argv, timeout, env)
    return out.rstrip("\n") if rc == 0 else None


def json_out(argv: list[str], timeout: float = 60, env: dict | None = None):
    """The JSON a command prints when it exits 0, else None."""
    rc, out, _ = run(argv, timeout, env)
    if rc != 0 or not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None
