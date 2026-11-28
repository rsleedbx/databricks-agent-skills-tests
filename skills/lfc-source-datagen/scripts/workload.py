#!/usr/bin/env python3
"""workload.py --model <model> --sf <scale> --dialect <tsql|postgres> --out <dir> [--spec]: write a workload for a statschema
model (schemas/<model>.yaml, the TPC models) shaped at a scale: <dir>/model.yaml (shape.py, checked against model format 1),
<dir>/model.sql (ddl.py for the dialect), and <dir>/workload.json (workload contract 1, lfc-workload-contract), whose fill_cmd
and bundle_cmd are this folder's copies.py and bundle.py. Prints the workload file's path. Exit 2 with one line when a step
fails; the workload file is checked by the contract's workload_env.py before the path is printed.
  --spec: TPC-H or TPC-B as tpc/tpc-schema.py writes them from the spec (keys, prices, and dates across tables: ship = order
  + 1..121), read back by statschema's ddl-to-schema.py, in place of the statschema model; TPC-H also gets <dir>/verify.sql
  (the spec's checks, tpc/tpch-verify.sql, for the dialect: postgres so far), named in the workload file's verify.
  python3 workload.py --model tpcc --sf 0.01 --dialect tsql --out /abs/work/tpcc
  python3 workload.py --model tpch --sf 0.1 --dialect postgres --spec --out /abs/work/tpch
The shape and ddl steps run statschema's scripts with this Python (they need pyyaml and sqlglot, as their headers say); no uv.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SK = os.path.normpath(os.path.join(HERE, "..", ".."))
SCH = os.path.join(SK, "lfc-source-statschema")
CON = os.path.join(SK, "lfc-workload-contract", "scripts")
sys.path.insert(0, CON)
import workload_env  # the contract: lfc-workload-contract's own module


class Die(Exception):
    pass


def die(msg):
    raise Die(f"workload.py: {msg}")


def _run(argv, stdin=None):
    return subprocess.run(argv, input=stdin, capture_output=True, text=True, check=False)


def _tail(text, n=2):
    return " ".join(text.strip().splitlines()[-n:])


def build(model, sf, dialect, out, spec):
    """Writes the workload folder; returns the workload file's path (raises Die)."""
    if not re.fullmatch(r"[a-z0-9_]+", model):
        die(f"--model must be a lowercase name: {model}")
    if spec:
        if model not in ("tpch", "tpcb"):
            die(f"--spec: tpch or tpcb (the models tpc-schema.py writes from the spec): {model}")
    elif not os.access(os.path.join(SCH, "schemas", f"{model}.yaml"), os.R_OK):
        die(f"no model {model} (no schemas/{model}.yaml in lfc-source-statschema)")
    if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", sf):
        die(f"--sf must be a number such as 0.01 or 10: {sf}")
    if not float(sf) > 0:
        die(f"--sf must be above 0: {sf}")
    if dialect == "tsql":
        engine = "ms"
    elif dialect == "postgres":
        engine = "pg"
    else:
        die(f"--dialect {dialect}: only tsql and postgres have a fill command so far (copies.py fills SQL Server and PostgreSQL)")
    os.makedirs(out, exist_ok=True)
    out = os.path.abspath(out)
    if " " in out or " " in HERE:
        die("the paths of the output folder and of this skill can't have a space (the workload file's paths are words)")
    with tempfile.TemporaryDirectory(prefix="workload-") as tmp:
        ss = os.path.join(SCH, "scripts")
        if spec:
            from importlib.util import module_from_spec, spec_from_file_location
            ts = spec_from_file_location("tpc_schema_cli", os.path.join(HERE, "tpc", "tpc-schema.py"))
            tsm = module_from_spec(ts)
            ts.loader.exec_module(tsm)
            try:
                text = tsm.schema_text(model, sf)
            except ValueError as e:
                die(f"tpc-schema.py failed: {e}")
            p = _run([sys.executable, os.path.join(ss, "ddl-to-schema.py"), "--dialect", "mysql"], stdin=text)
            if p.returncode != 0:
                die(f"tpc-schema.py | ddl-to-schema.py failed: {_tail(p.stderr)}")
            model_yaml = p.stdout
        else:
            p = _run([sys.executable, os.path.join(ss, "shape.py"), os.path.join(SCH, "schemas", f"{model}.yaml"), "--sf", sf,
                      "--expected", os.path.join(tmp, "expected.json")])
            if p.returncode != 0:
                die(f"shape.py failed: {_tail(p.stderr)}")
            model_yaml = p.stdout
        with open(os.path.join(out, "model.yaml"), "w") as f:
            f.write(model_yaml)
        p = _run([sys.executable, os.path.join(CON, "validate-model.py"), os.path.join(out, "model.yaml")])
        if p.returncode != 0:
            die(f"model.yaml breaks model format 1: {_tail(p.stdout + p.stderr, 3)}")
        p = _run([sys.executable, os.path.join(ss, "ddl.py"), os.path.join(out, "model.yaml"), "--dialect", dialect])
        if p.returncode != 0:
            die(f"ddl.py failed: {_tail(p.stderr)}")
        with open(os.path.join(out, "model.sql"), "w") as f:
            f.write(p.stdout)
    ddl = p.stdout
    if dialect == "tsql":
        names = re.findall(r"^CREATE TABLE \[([A-Za-z_0-9]+)\]", ddl, flags=re.MULTILINE)
    else:
        names = re.findall(r'^CREATE TABLE "?([A-Za-z_0-9]+)"?', ddl, flags=re.MULTILINE)
    if not names:
        die(f"no CREATE TABLE in {out}/model.sql")
    verify = ""
    if spec and model == "tpch" and dialect == "postgres":        # the spec's checks, unqualified, DATEDIFF as Postgres subtraction
        with open(os.path.join(HERE, "tpc", "tpch-verify.sql")) as f:
            checks = re.sub(r"DATEDIFF\(([^,]+), ([^)]+)\)", r"(\1 - \2)", f.read())
        verify = os.path.join(out, "verify.sql")
        with open(verify, "w") as f:
            f.write(checks)
    num = int(sf) if re.fullmatch(r"[0-9]+", sf) else float(sf)
    doc = {"contract": 1, "model": model, "scale": num, "dialect": dialect, "tables": names,
           "ddl": os.path.join(out, "model.sql"), "fill_cmd": f"python3 {os.path.join(HERE, 'copies.py')} {engine}",
           "bundle_cmd": f"python3 {os.path.join(HERE, 'bundle.py')}"}
    if verify:
        doc["verify"] = verify
    path = os.path.join(out, "workload.json")
    with open(path, "w") as f:
        json.dump(doc, f, separators=(", ", ": "))
        f.write("\n")
    try:
        workload_env.check(path)
    except workload_env.Refused as x:
        die(f"the workload file fails its contract ({x})")
    print(f"workload.py: {model}{' (spec)' if spec else ''} at SF {sf}, {len(names)} tables, {dialect}", file=sys.stderr)
    return path


USAGE = ("usage: workload.py --model <model> --sf <scale> --dialect <tsql|postgres> --out <dir> [--spec]\n"
         "  e.g. workload.py --model tpcc --sf 0.01 --dialect tsql --out /abs/work/tpcc")


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    opts = {"--model": None, "--sf": None, "--dialect": None, "--out": None}
    spec = False
    i = 0
    while i < len(a):
        if a[i] == "--spec":
            spec = True
            i += 1
            continue
        if i + 1 >= len(a) or a[i] not in opts:
            print(USAGE, file=sys.stderr)
            return 2
        opts[a[i]] = a[i + 1]
        i += 2
    if not all(opts.values()):
        print(USAGE, file=sys.stderr)
        return 2
    try:
        print(build(opts["--model"], opts["--sf"], opts["--dialect"], opts["--out"], spec))
    except Die as x:
        print(x, file=sys.stderr)
        return 2
    except OSError as x:
        print(f"workload.py: {x}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
