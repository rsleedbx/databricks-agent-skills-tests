#!/usr/bin/env python3
"""bin/test_timings.py [timings.tsv] [--top N]: what bin/test_scripts.py's checks cost, from the timings it appends
(TEST_TIMINGS, default ~/.cache/test_scripts/timings.tsv: run time, seconds, result, name). Prints the latest run (checks
run and skipped, total seconds, the slowest, the many quick ones together) and each check's median over every run.
"""
import argparse
import os
import statistics
import sys
from collections import defaultdict


def load(path: str) -> list:
    rows = []
    with open(path) as f:
        for line in f:
            p = line.rstrip("\n").split("\t", 3)
            if len(p) == 4 and p[1].isdigit():
                rows.append((p[0], int(p[1]), p[2], p[3]))
    return rows


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="What test_scripts.py's checks cost")
    ap.add_argument("path", nargs="?", default=os.environ.get("TEST_TIMINGS", os.path.expanduser(
        "~/.cache/test_scripts/timings.tsv")))
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args(argv)
    if not os.path.exists(a.path):
        sys.exit(f"no timings at {a.path}: run bin/test_scripts.py first")
    rows = load(a.path)
    last = max(r[0] for r in rows)
    run = [r for r in rows if r[0] == last]
    ran = [r for r in run if r[2] != "skip"]
    quick = [r for r in ran if r[1] < 1]
    print(f"run {last}: {len(ran)} checks ran ({sum(r[1] for r in ran)} s), {len(run) - len(ran)} skipped, "
          f"{sum(r[2] == 'fail' for r in ran)} failed; {len(quick)} under 1 s each")
    print(f"slowest {a.top}:")
    for r in sorted(ran, key=lambda r: -r[1])[:a.top]:
        print(f"  {r[1]:5d} s  {r[2]:4}  {r[3][:110]}")
    by = defaultdict(list)
    for r in rows:
        if r[2] != "skip":
            by[r[3]].append(r[1])
    print(f"checks seen: {len(by)}; runs recorded: {len({r[0] for r in rows})}")
    slow = sorted(((statistics.median(v), len(v), n) for n, v in by.items() if statistics.median(v) >= 5), reverse=True)
    if slow:
        print("median 5 s or more over every run:")
        for m, k, n in slow[:a.top]:
            print(f"  {m:5.0f} s  x{k:<3} {n[:110]}")


if __name__ == "__main__":
    main()
