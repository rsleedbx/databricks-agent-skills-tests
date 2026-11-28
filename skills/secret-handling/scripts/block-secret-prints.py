#!/usr/bin/env python3
"""block-secret-prints.py [--self-test]: Claude Code PreToolUse hook that refuses a Bash command printing a secret file.

Reads the hook's JSON on stdin; for a Bash command, splits it into segments (; && || | newline) and blocks (exit 2, the
reason on stderr, which the agent sees) a segment where a printing tool (cat, head, tail, less, yq, jq, python, sed,
awk, grep without -l/-L/-c/-q, base64, xxd, strings, ...) reads a file whose name matches a secret pattern. Allowed:
capturing into a variable (NAME=$(yq -r .password f)), writing to the file (> f), the safe tools (ls, chmod, stat,
test, rm, the secret-*.sh scripts), and any other command (a client reading the file itself doesn't print it).
Patterns: SECRET_PATHS (colon-separated globs) if set, else ~/.config/secret-handling/paths (one glob per line) if it
exists, else the defaults below. A glob with a / matches the whole path (~ expanded), one without matches the name.
Install: in ~/.claude/settings.json, hooks.PreToolUse: [{"matcher": "Bash", "hooks": [{"type": "command",
"command": "python3 <path>/block-secret-prints.py"}]}]. A heuristic: it stops the usual accidents, not a determined
reader; `X=$(cat f); echo "$X"` still prints.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import sys
from pathlib import Path

DEFAULTS = [
    "*.pw",
    "*credentials*.json*",
    "*credentials*.y*ml*",
    "*.pem",
    "*.key",
    "*_rsa",
    "*_ed25519",
    ".databrickscfg",
    ".pgpass",
    ".my.cnf",
    "*.env",
    ".netrc",
]
PRINTERS = {
    "cat",
    "head",
    "tail",
    "less",
    "more",
    "bat",
    "yq",
    "jq",
    "python",
    "python3",
    "sed",
    "awk",
    "gawk",
    "grep",
    "rg",
    "ugrep",
    "strings",
    "xxd",
    "od",
    "hexdump",
    "base64",
    "cut",
    "sort",
    "uniq",
    "tr",
    "diff",
    "nl",
    "tac",
    "column",
    "fold",
    "pr",
    "openssl",
}
WRAPPERS = {
    "sudo",
    "env",
    "command",
    "exec",
    "time",
    "nice",
    "nohup",
    "timeout",
    "xargs",
}
SAFE = {
    "ls",
    "chmod",
    "chown",
    "stat",
    "test",
    "[",
    "rm",
    "mkdir",
    "touch",
    "mv",
    "secret_run.py",
    "secret_find.py",
    "secret_perms.py",
    "block-secret-prints.py",
    "git",
}
GREP_QUIET = {
    "-l",
    "-L",
    "-c",
    "-q",
    "--files-with-matches",
    "--files-without-match",
    "--count",
    "--quiet",
}
CAPTURE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*=\$\((?:[^()]|\([^()]*\))*\)")


def patterns() -> list[str]:
    if os.environ.get("SECRET_PATHS"):
        return [p for p in os.environ["SECRET_PATHS"].split(":") if p]
    f = Path.home() / ".config" / "secret-handling" / "paths"
    if f.exists():
        return [
            ln.strip()
            for ln in f.read_text().splitlines()
            if ln.strip() and not ln.startswith("#")
        ]
    return DEFAULTS


def is_secret(tok: str, pats: list[str]) -> bool:
    tok = tok.strip("'\"")
    if not tok or tok.startswith("-"):
        return False
    full = os.path.expanduser(tok)
    for p in pats:
        if "/" in p:
            if fnmatch.fnmatch(full, os.path.expanduser(p)):
                return True
        elif fnmatch.fnmatch(os.path.basename(full), p):
            return True
    return False


def segments(cmd: str) -> list[list[str]]:
    cmd = CAPTURE.sub(" ", cmd)
    out = []
    for part in re.split(r"\|\||&&|[;|\n]|\$\(|`", cmd):
        try:
            toks = shlex.split(part, comments=True)
        except ValueError:
            toks = part.split()
        if toks:
            out.append(toks)
    return out


def verdict(cmd: str) -> str | None:
    pats = patterns()
    for toks in segments(cmd):
        for (
            t
        ) in toks:  # a quoted command inside the command: ssh host '...', bash -c '...'
            if re.search(r"\s", t) and (why := verdict(t)):
                return why
        i = 0
        while i < len(toks) and (
            re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", toks[i])
            or os.path.basename(toks[i]) in WRAPPERS
        ):
            i += 1
            while (
                i < len(toks)
                and toks[i].startswith("-")
                and os.path.basename(toks[i - 1]) in WRAPPERS
            ):
                i += 1
        if i >= len(toks):
            continue
        word = os.path.basename(toks[i])
        if word in {"python", "python3"} and i + 1 < len(toks) and os.path.basename(toks[i + 1]) in SAFE:
            continue  # python3 scripts/secret_run.py ...: judged by the script, as its bare name is
        if word in SAFE or word not in PRINTERS:
            continue
        args, j = toks[i + 1 :], 0
        if word in {"grep", "rg", "ugrep"} and any(
            a in GREP_QUIET or re.match(r"^-[a-zA-Z]*[lLcq]", a) for a in args
        ):
            continue
        while j < len(args):
            a = args[j]
            if a in {">", ">>", "2>", "&>"}:  # a write target, not a read
                j += 2
                continue
            if re.match(r"^\d?>>?", a):
                j += 1
                continue
            path = args[j + 1] if a == "<" and j + 1 < len(args) else a
            if is_secret(path, pats):
                return (
                    f"refused: `{word}` would print the secret file {path.strip(chr(39) + chr(34))}. "
                    "Read only the fields you need into a variable for the one command that uses them "
                    "(secret_run.py <file> <field> <ENV> -- <command>), or check a value without printing it "
                    "(secret_find.py, a length, a hash). Skill: secret-handling."
                )
            j += 2 if a == "<" else 1
    return None


def self_test() -> int:
    os.environ["SECRET_PATHS"] = ":".join(DEFAULTS + ["~/.lfc-secrets/*"])
    block = [
        "cat ~/.lfc-secrets/pg.pw",
        "yq '.' creds_credentials.json",
        "head -3 a.env | tail -1",
        "jq . x_credentials.json | grep host",
        "sudo cat /etc/app.pem",
        "python3 -c 'print(1)' < db.pw",
        "ssh vm 'cat ~/.mssql.env'",
        "grep password app_credentials.yaml",
    ]
    allow = [
        "PW=$(yq -r .password app_credentials.json) sqlcmd -Q 'SELECT 1'",
        "ls -la ~/.lfc-secrets/",
        "chmod 600 db.pw",
        "printf 'x' > new.pw",
        "grep -l password app_credentials.yaml",
        "secret_run.py c_credentials.json .dba.password SQLCMDPASSWORD -- sqlcmd -Q 'SELECT 1'",
        "python3 scripts/secret_run.py c_credentials.json .dba.password SQLCMDPASSWORD -- sqlcmd -Q 'SELECT 1'",
        "sqlcmd -i script.sql",
        "cat README.md",
        "PGPASSWORD=$(cat ~/.lfc-secrets/pg.pw) psql -c 'SELECT 1'",
    ]
    bad = [c for c in block if not verdict(c)] + [c for c in allow if verdict(c)]
    for c in bad:
        print("self-test FAIL:", c, "->", verdict(c))
    print(
        f"self-test: {len(block) + len(allow) - len(bad)}/{len(block) + len(allow)} ok"
    )
    return 1 if bad else 0


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    try:
        ev = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if ev.get("tool_name") != "Bash":
        return 0
    why = verdict((ev.get("tool_input") or {}).get("command", ""))
    if why:
        print(why, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
