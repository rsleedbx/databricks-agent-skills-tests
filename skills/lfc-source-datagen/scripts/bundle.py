#!/usr/bin/env python3
"""bundle.py <out.tgz>: the generator as one portable file: copies.py, python/fill.py with its planner, drivers and engines,
and the SQL planner files of each engine (postgres/, mysql/, sqlserver/, oracle/) that fill.py's client mode runs. Unpack it
anywhere with Python 3 and the engine's SQL client (or a driver for fill.py's driver mode), then run copies.py. Nothing to
install beyond that; no Databricks call.
    python3 bundle.py /tmp/lfc-datagen.tgz
    scp /tmp/lfc-datagen.tgz model.sql vm: && ssh vm 'mkdir -p g && tar xzf lfc-datagen.tgz -C g && ...'
"""
from __future__ import annotations

import os
import sys
import tarfile

HERE = os.path.dirname(os.path.abspath(__file__))
FILES = ["copies.py"]
TREES = ["python", "sqlserver", "postgres", "mysql", "oracle"]
USAGE = "usage: bundle.py <out.tgz>"


def members():
    """(path in the bundle, file on disk) for every file the generator needs, without Python's caches."""
    out = [(f, os.path.join(HERE, f)) for f in FILES]
    for tree in TREES:
        for root, dirs, files in os.walk(os.path.join(HERE, tree)):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for name in sorted(files):
                if name.endswith(".pyc"):
                    continue
                full = os.path.join(root, name)
                out.append((os.path.relpath(full, HERE), full))
    return out


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    if len(a) != 1:
        print(USAGE, file=sys.stderr)
        return 2
    with tarfile.open(a[0], "w:gz") as tar:
        for name, full in members():
            tar.add(full, arcname=name)
    print(a[0])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
