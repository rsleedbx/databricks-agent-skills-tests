#!/usr/bin/env python3
"""check_defaults.py [--file defaults.yaml]: the stated test defaults (defaults.yaml at the repo root) still
match the code. For each default, every "in" file must exist and contain its phrase (where the default is set or
stated); every "forbid" pattern must appear in no file under its folders (the defaults file itself aside). One line per
problem; "defaults: N defaults, M places, K forbidden patterns: ok" when there is none; exit 1 on any problem.
Run by bin/test_scripts.sh. Python 3 standard library, and yq to read the YAML.
"""
import argparse
import json
import os
import re
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT = os.path.join(REPO, "defaults.yaml")


def load(path):
    """The defaults file as data, through yq (a tool every test needs) to JSON."""
    out = subprocess.run(["yq", "-o", "json", ".", path], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def files_under(folders):
    for top in folders:
        for root, dirs, names in os.walk(os.path.join(REPO, top)):
            dirs[:] = [d for d in dirs if d not in (".git", "__pycache__", "node_modules")]
            for n in names:
                yield os.path.join(root, n)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--file", default=DEFAULT)
    a = p.parse_args()
    data = load(a.file)
    me = os.path.realpath(a.file)
    problems, places, forbids = [], 0, 0
    for d in data.get("defaults") or []:
        did = d.get("id", "?")
        for k in ("id", "area", "value", "why"):
            if not d.get(k):
                problems.append(f"{did}: no {k}")
        for w in d.get("in") or []:
            places += 1
            f = os.path.join(REPO, w["file"])
            if not os.path.isfile(f):
                problems.append(f"{did}: {w['file']} doesn't exist")
            elif w["has"] not in open(f, encoding="utf-8", errors="replace").read():
                problems.append(f"{did}: {w['file']} no longer says {w['has']!r}: update the file or the default")
        for fb in d.get("forbid") or []:
            forbids += 1
            pat = re.compile(re.escape(fb["pattern"]))
            for f in files_under(fb["in"]):
                if os.path.realpath(f) == me:
                    continue
                try:
                    text = open(f, encoding="utf-8").read()
                except (UnicodeDecodeError, OSError):
                    continue
                for i, line in enumerate(text.splitlines(), 1):
                    if pat.search(line):
                        problems.append(f"{did}: {os.path.relpath(f, REPO)}:{i} has {fb['pattern']!r}, which the default rules out")
    for x in problems:
        print(x)
    if problems:
        sys.exit(1)
    print(f"defaults: {len(data.get('defaults') or [])} defaults, {places} places, {forbids} forbidden patterns: ok")


if __name__ == "__main__":
    main()
