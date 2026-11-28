#!/usr/bin/env python3
"""secret_perms.py [--fix] <path...>: secret files that another local user can read. Each path is a file or a directory
(its files, recursively). Prints the mode and path of each file not limited to its owner; --fix sets it to 600 (and the
directories to 700). Exit 0 when all are owner-only (or fixed), 1 when some aren't, 2 on usage.
"""
from __future__ import annotations

import os
import sys


def walk(paths: list[str]):
    """Each path and, for a directory, everything under it (regular files and directories; symlinks skipped)."""
    for p in paths:
        if os.path.isdir(p) and not os.path.islink(p):
            yield p
            for root, dirs, files in os.walk(p):
                for name in dirs + files:
                    full = os.path.join(root, name)
                    if not os.path.islink(full):
                        yield full
        elif os.path.isfile(p):
            yield p


def mode_of(path: str) -> int:
    return os.stat(path).st_mode & 0o777


def main(argv: list[str]) -> int:
    fix = bool(argv) and argv[0] == "--fix"
    paths = argv[1:] if fix else argv
    if not paths:
        print(__doc__.split("\n\n")[0] + "\n\nusage: secret_perms.py [--fix] <path...>", file=sys.stderr)
        return 2
    bad = 0
    for f in walk(paths):
        try:
            mode = mode_of(f)
        except OSError:
            continue
        if mode & 0o077 == 0:
            continue
        if fix:
            os.chmod(f, 0o700 if os.path.isdir(f) else 0o600)
            print(f"fixed {mode:o} -> {mode_of(f):o} {f}")
        else:
            print(f"{mode:o} {f}")
            bad = 1
    return bad


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
