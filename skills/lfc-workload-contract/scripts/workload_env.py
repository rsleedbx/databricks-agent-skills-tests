#!/usr/bin/env python3
"""workload_env.py <workload.json>: validate a workload file (contract 1, lfc-workload-contract) and print the exports the
consumers use: eval "$(workload_env.py workload.json)". Exit 2 with one line when the file breaks the contract (a missing
field, a bad name or value, a path that isn't there, a secret in the file, a contract version this script doesn't read).
    Exports WL_MODEL, WL_SCALE, WL_DIALECT, WL_TABLES (space-separated), WL_NTABLES, WL_DDL (the DDL file), WL_FILL (fill_cmd),
    WL_FILL_IN_BUNDLE (fill_cmd as it runs from an unpacked bundle: each absolute word becomes its base name), and WL_BUNDLE
    (bundle_cmd; unset when the file has none), and WL_VERIFY (verify: the model's rule checks; unset when the file has
    none). Nothing else is set, and no password is ever in the file.
In Python: check(path) -> the exports as {name: value or None (unset)}, or raises Refused with the one line; main(argv)
prints them. No database, no Databricks call.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import sys


class Refused(Exception):
    """The file breaks the contract: the message is the one line the command prints (after its name)."""


PLAIN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
LOWER = re.compile(r"[a-z][a-z0-9_]*")
SAFE_PATH = re.compile(r"/[A-Za-z0-9_./-]*")
SAFE_WORD_CHARS = re.compile(r"[A-Za-z0-9_./ -]*")
SECRET = re.compile(r"password|passwd|secret|token|credential|pwd", re.IGNORECASE)


def _is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_string(v):
    return isinstance(v, str)


def _safe_path(v):
    return _is_string(v) and SAFE_PATH.fullmatch(v) is not None


def _safe_words(v):
    return (_is_string(v) and len(v) > 0 and SAFE_WORD_CHARS.fullmatch(v) is not None
            and all(w != "" for w in v.split(" ")))


def _problems(obj):
    """Every problem in the JSON (the order the checks run in), one message each; none means the fields hold."""
    out = []
    if "model" not in obj:
        out.append("missing model")
    elif not (_is_string(obj["model"]) and LOWER.fullmatch(obj["model"])):
        out.append("model must be a lowercase name (letters, digits, _): the copies are named <model>_NNNN")
    if "scale" not in obj:
        out.append("missing scale")
    elif not _is_number(obj["scale"]) or obj["scale"] <= 0:
        out.append("scale must be a number above 0")
    if "dialect" not in obj:
        out.append("missing dialect")
    elif not (_is_string(obj["dialect"]) and LOWER.fullmatch(obj["dialect"])):
        out.append("dialect must be a lowercase name (tsql, postgres, ...)")
    if "tables" not in obj:
        out.append("missing tables")
    elif not isinstance(obj["tables"], list) or len(obj["tables"]) == 0:
        out.append("tables must be a non-empty list of names")
    elif not all(_is_string(t) and PLAIN.fullmatch(t) for t in obj["tables"]):
        out.append("a table name is not a plain identifier (letters, digits, _)")
    elif len(set(obj["tables"])) != len(obj["tables"]):
        out.append("a table name is listed twice")
    if "ddl" not in obj:
        out.append("missing ddl")
    elif not _safe_path(obj["ddl"]):
        out.append("ddl must be an absolute path of letters, digits, _ . / - (no spaces)")
    if "fill_cmd" not in obj:
        out.append("missing fill_cmd")
    elif not _safe_words(obj["fill_cmd"]):
        out.append("fill_cmd must be words separated by one space: letters, digits, _ . / - (no quotes or shell operators)")
    if "verify" in obj and obj["verify"] is not None and not _safe_path(obj["verify"]):
        out.append("verify must be an absolute path of letters, digits, _ . / - (no spaces)")
    if "bundle_cmd" in obj and obj["bundle_cmd"] is not None and not _safe_words(obj["bundle_cmd"]):
        out.append("bundle_cmd must be words separated by one space: letters, digits, _ . / - (no quotes or shell operators)")
    for k in sorted(obj):
        if SECRET.search(k):
            out.append(f"{k} is a secret: a workload file holds no secrets")
    return out


def _number_text(v):
    """A JSON number as jq's tostring prints it: 1 for 1.0, 0.01 for 0.01."""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return json.dumps(v)


def _words(s):
    """The command's words: a command is split on spaces only (the consumers never glob or quote)."""
    return s.split(" ")


def check(path):
    """The exports for a valid workload file: {name: value}, a value of None meaning unset. Raises Refused."""
    def bad(msg):
        raise Refused(f"{path}: {msg}")
    if not os.path.isfile(path) or not os.access(path, os.R_OK):
        bad("can't read it")
    try:
        with open(path) as f:
            obj = json.load(f)
    except (ValueError, OSError):
        obj = None
    if not isinstance(obj, dict):
        bad("not a JSON object")
    cv = obj.get("contract")
    if cv is None or cv is False:
        bad('missing contract (the version: "contract": 1)')
    if not (type(cv) in (int, float) and cv == 1):
        raise Refused(f"{path} is contract {json.dumps(cv)}; this reads contract 1 only: use a workload_env.py that reads "
                      f"{json.dumps(cv)}, or have the producer write contract 1")
    probs = _problems(obj)
    if probs:
        bad("; ".join(probs))
    files = []
    ddl = obj["ddl"]
    if not os.path.isfile(ddl):
        files.append(f"ddl {ddl} is not a file")
    vf = obj.get("verify") or ""
    if vf and not os.path.isfile(vf):
        files.append(f"verify {vf} is not a file")
    for w in _words(f"{obj['fill_cmd']} {obj.get('bundle_cmd') or ''}"):
        if w.startswith("/") and not os.path.isfile(w):
            files.append(f"{w} is not a file")
    if files:
        bad(", ".join(files))
    ex = {
        "WL_MODEL": obj["model"], "WL_SCALE": _number_text(obj["scale"]), "WL_DIALECT": obj["dialect"],
        "WL_TABLES": " ".join(obj["tables"]), "WL_NTABLES": str(len(obj["tables"])), "WL_DDL": ddl,
        "WL_FILL": obj["fill_cmd"],
        "WL_FILL_IN_BUNDLE": " ".join(os.path.basename(w) if w.startswith("/") else w for w in _words(obj["fill_cmd"])),
        "WL_BUNDLE": obj.get("bundle_cmd"), "WL_VERIFY": obj.get("verify"),
    }
    return ex


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    if len(a) < 1:
        print("usage: workload_env.py <workload.json>", file=sys.stderr)
        print("   prints the exports: eval \"$(workload_env.py workload.json)\"", file=sys.stderr)
        return 2
    try:
        ex = check(a[0])
    except Refused as x:
        print(f"workload-env: {x}", file=sys.stderr)
        return 2
    for name, value in ex.items():
        if value is None:
            print(f"unset {name}")
        else:
            print(f"export {name}={shlex.quote(str(value))}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
