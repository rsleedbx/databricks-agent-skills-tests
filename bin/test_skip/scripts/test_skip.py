#!/usr/bin/env python3
# Vendored copy of shared unit 'test_skip': edit the master, then run `reuse-first vendor sync` (any other edit shows as drift).
"""test_skip.py: run a test only when what it depends on changed since it last passed.

  test_skip.py skip   <runner> <key> [--value V ...] <path>...     exit 0: unchanged since a pass (skip it); 1: run it
  test_skip.py record <runner> <key> pass|fail [--value V ...] <path>...
  test_skip.py forget <runner> [<key>]

A test's fingerprint is the SHA-256 of every file under its paths (directories recurse; caches and build output left
out) plus any --value (a version, a connection's updated_at). A pass is kept per runner and key in
$TEST_SKIP_DIR/<runner>.json (default ~/.cache/test-skip; local, never committed), so a fresh machine or CI runs
everything. TEST_SKIP_AGAIN=1 (or skip --again) runs every test anyway.

Library:  fp = fingerprint(paths, values); skip, last = should_skip(runner, key, fp); record(runner, key, fp, ok)
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path

IGNORE = {"__pycache__", ".git", ".databricks", ".pytest_cache", ".ruff_cache", "node_modules", ".venv"}


def ledger_path(runner: str) -> Path:
    d = Path(os.environ.get("TEST_SKIP_DIR") or Path.home() / ".cache/test-skip").expanduser()
    return d / f"{runner}.json"


def _files(p: Path):
    if p.is_file():
        yield p
    elif p.is_dir():
        for f in sorted(p.rglob("*")):
            if f.is_file() and not (IGNORE & set(f.parts)) and f.suffix not in (".pyc",):
                yield f


def fingerprint(paths: list[str | Path], values: list[str] | None = None) -> str:
    """One hash of the files under paths (by path and content) and the values; a missing path counts as missing."""
    h = hashlib.sha256()
    for p in sorted({str(Path(x).expanduser()) for x in paths}):
        path = Path(p)
        if not path.exists():
            h.update(f"missing:{p}\n".encode())
        for f in _files(path):
            h.update(f"{f}\n".encode())
            h.update(hashlib.sha256(f.read_bytes()).digest())
    for v in values or []:
        h.update(f"value:{v}\n".encode())
    return h.hexdigest()[:16]


def _load(runner: str) -> dict:
    p = ledger_path(runner)
    return json.loads(p.read_text()) if p.exists() else {}


def should_skip(runner: str, key: str, fp: str, again: bool = False) -> tuple[bool, dict | None]:
    """(True, last pass) when key last passed with this fingerprint; again or TEST_SKIP_AGAIN=1 never skips."""
    last = _load(runner).get(key)
    again = again or os.environ.get("TEST_SKIP_AGAIN", "") not in ("", "0")
    return (not again and bool(last) and last.get("ok") is True and last.get("fingerprint") == fp), last


def record(runner: str, key: str, fp: str, ok: bool) -> None:
    """Keep this run's result; only a pass lets a later run skip."""
    p = ledger_path(runner)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = _load(runner)
    data[key] = {"fingerprint": fp, "ok": ok,
                 "when": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    tmp.replace(p)


def forget(runner: str, key: str | None = None) -> None:
    data = _load(runner)
    if key is None:
        data = {}
    else:
        data.pop(key, None)
    p = ledger_path(runner)
    if p.exists() or data:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=1, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("skip")
    s.add_argument("--again", action="store_true")
    r = sub.add_parser("record")
    for p in (s, r):
        p.add_argument("runner")
        p.add_argument("key")
    r.add_argument("result", choices=["pass", "fail"])
    for p in (s, r):
        p.add_argument("--value", action="append", default=[])
        p.add_argument("paths", nargs="*")
    f = sub.add_parser("forget")
    f.add_argument("runner")
    f.add_argument("key", nargs="?")
    a = ap.parse_args(argv)
    if a.cmd == "forget":
        forget(a.runner, a.key)
        return 0
    fp = fingerprint(a.paths, a.value)
    if a.cmd == "skip":
        skip, last = should_skip(a.runner, a.key, fp, a.again)
        if skip:
            print(f"passed {last['when']}, unchanged since")
        return 0 if skip else 1
    record(a.runner, a.key, fp, a.result == "pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
