#!/usr/bin/env python3
"""secret_find.py <file> <field|-> [dir...]: where a secret's value appears, without printing it. Lists every file under
the dirs that contains the value (default: ~/.claude, $TMPDIR, the current directory) and, for each git checkout in or up
to 3 levels under a dir, the commits on any branch that add or remove it (git log -S). <file> and <field> as in
secret_run.py. Exit 0 when the value is found nowhere but <file>, 1 when it is found elsewhere (a leak to clean up), 2 on
usage. After a leak: rotate the secret first (the value is out), then update every copy that holds it.
git log -S takes the value as an argument, so it shows in the process list while the search runs.
"""
from __future__ import annotations

import os
import stat
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run
from secret_run import read_secret

SKIP_DIRS = {"node_modules", ".venv"}


def git_checkouts(d: str) -> list[str]:
    """The checkout d is in, and every checkout up to 3 levels below d (a worktree has a .git file)."""
    found = set()
    rc, top, _ = run(["git", "-C", d, "rev-parse", "--show-toplevel"], 30)
    if rc == 0 and top.strip():
        found.add(top.strip())
    for root, dirs, files in os.walk(d):
        depth = root[len(d):].count(os.sep)
        if (".git" in dirs or ".git" in files) and depth + 1 <= 4:
            found.add(root)
        dirs[:] = [x for x in dirs if x != ".git" and depth < 3]
    return sorted(found)


def holds(f: str, value: bytes) -> bool:
    """Whether the regular file holds the value; read in chunks that overlap by len(value) - 1 bytes."""
    overlap = max(len(value) - 1, 0)
    tail = b""
    with open(f, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                return False
            if value in tail + chunk:
                return True
            tail = (tail + chunk)[-overlap:] if overlap else b""


def files_with(value: bytes, d: str, selfpath: str):
    """Regular files under d (not symlinks, FIFOs or sockets, not in node_modules or .venv) whose bytes hold the value."""
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
        for name in files:
            f = os.path.join(root, name)
            try:
                if not stat.S_ISREG(os.lstat(f).st_mode) or os.path.realpath(f) == selfpath:
                    continue
                if holds(f, value):
                    yield f
            except OSError:
                continue


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.split("\n\n")[0] + "\n\nusage: secret_find.py <file> <field|-> [dir...]", file=sys.stderr)
        return 2
    path, field, dirs = argv[0], argv[1], argv[2:]
    if not os.access(path, os.R_OK):
        print(f"secret-find: can't read {path}", file=sys.stderr)
        return 2
    value = read_secret(path, field)
    if len(value) < 6:
        print(f"secret-find: value at {field} is empty or under 6 characters: too short to search for", file=sys.stderr)
        return 2
    if not dirs:
        dirs = [os.path.join(os.path.expanduser("~"), ".claude"), os.environ.get("TMPDIR") or tempfile.gettempdir(),
                os.getcwd()]
    selfpath = os.path.realpath(path)
    raw = value.encode()
    del value
    hits = 0
    for d in dirs:
        if not os.path.exists(d):
            continue
        for f in files_with(raw, d, selfpath):
            print(f"file: {f}")
            hits += 1
        for g in git_checkouts(d):
            _, log, _ = run(["git", "-C", g, "log", "--all", f"-S{raw.decode()}", "--format=%h %ad %s",
                             "--date=short"], 120)
            for line in log.splitlines():
                print(f"commit: {g} {line}")
                hits += 1
    if hits == 0:
        print(f"secret-find: found only in {path}")
        return 0
    print(f"secret-find: {hits} place(s) besides {path}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
