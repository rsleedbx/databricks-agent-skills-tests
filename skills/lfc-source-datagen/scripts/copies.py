#!/usr/bin/env python3
# copies.py <ms|pg> <model.sql> <model> <first> <last> [parallel]: build copies <first>..<last> of a model, each in its own
# schema <model>_NNNN (4 digits, so up to 9,999 sort in order), and fill them with python/fill.py. Runs anywhere Python
# and the engine's SQL client do: the client is a subprocess (it reads SQL on stdin, connected to the target database), and
# no Databricks call is made.
#
#   model.sql  the model's DDL for the engine, from lfc-source-statschema ddl.py (tables unqualified; tags keyed by
#              SCHEMA_NAME()). Each copy gets it with its tables qualified by the copy's schema (pg: run with the
#              copy's schema as the search_path, which the COMMENT ON tags follow).
#   MSSQL      ms: the client, a command that reads SQL on stdin, connected to the target database, output with -y 0
#              (as fill.py needs). Default: ./sq in this folder.
#   PSQL       pg: the client, a command that reads SQL on stdin, connected to the target database, with psql's own
#              output (-At, | between fields, as fill.py needs). Default: psql. SRC (fill.py's row source) defaults
#              to series (generate_series), so a database without lfc_seed.sed fills.
#   DIR        the planner files (default: sqlserver/ or postgres/ next to this script)
#   FILL       the fill command, run as FILL <ms|pg> <schema> (default: python3 python/fill.py next to this script; the
#              tests stub it)
#
# Prints one line per copy, "done <schema> <seconds>s" or "failed <schema> <reason>", then "all done" or
# "finished with failures"; exits 1 if any copy failed. A copy already filled is filled again only to its target
# (fill.py resumes), and a schema that exists is reused, so a rerun after an interruption continues.
# SQL Server (ms) and PostgreSQL (pg) so far; the other engines' fill paths are the same and can be added here.

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
SLEEP = time.sleep                      # a retry's wait (the tests stub it)
USAGE = "usage: copies.py <ms|pg> <model.sql> <model> <first> <last> [parallel]"


def _read(path):
    with open(path) as f:
        return f.read()


def _names(model_sql, engine):
    """The table names in the model's CREATE TABLE lines, joined for a regex alternation ('' when none)."""
    text = _read(model_sql)
    if engine == "ms":
        found = re.findall(r"^CREATE TABLE \[([A-Za-z_0-9]+)\]", text, flags=re.MULTILINE)
    else:
        found = re.findall(r'^CREATE TABLE "?([A-Za-z_0-9]+)"?', text, flags=re.MULTILINE)
    return "|".join(found)


def _run(argv, text, env):
    """A client (or the fill command) with text on stdin: (exit code, output)."""
    p = subprocess.run(argv, input=text, capture_output=True, text=True, env=env, check=False)
    return p.returncode, p.stdout + p.stderr


def build(engine, model_sql, model, first, last, par=4, client=None, fill=None):
    """The copies' build: (the output lines, all done). client: the engine's SQL client argv (default: PSQL or MSSQL from
    the environment); fill: the fill command's argv (default: python/fill.py under this interpreter, or FILL)."""
    re_names = _names(model_sql, engine)
    if not re_names:
        raise ValueError(f"no CREATE TABLE in {model_sql}")
    if client is None:
        client = shlex.split(os.environ.get("PSQL" if engine == "pg" else "MSSQL") or ("psql" if engine == "pg" else os.path.join(HERE, "sq")))
    if fill is None:
        fill = shlex.split(os.environ["FILL"]) if os.environ.get("FILL") else [sys.executable, os.path.join(HERE, "python", "fill.py")]
    env = dict(os.environ, RE=re_names)                  # fill.py gets the client (PSQL or MSSQL) and the planner files
    if engine == "pg":
        env.setdefault("SRC", "series")                  # generate_series: a database without lfc_seed.seed fills
        env["PSQL"] = shlex.join(client)
        env.setdefault("DIR", os.path.join(HERE, "postgres"))
    else:
        env["MSSQL"] = shlex.join(client)
        env.setdefault("DIR", os.path.join(HERE, "sqlserver"))
    with tempfile.TemporaryDirectory(prefix="copies-") as logs, ThreadPoolExecutor(max_workers=par) as ex:
        futs = [ex.submit(_one, engine, model_sql, model, i, client, fill, env, logs) for i in range(first, last + 1)]
        lines, ok = [], True
        for f in as_completed(futs):
            line, good = f.result()
            lines.append(line); ok = ok and good
    return lines, ok


def _one(engine, model_sql, model, i, client, fill, env, logs):
    """One copy: its schema and tables unless they are there, then the fill (retried)."""
    s = f"{model}_{i:04d}"
    t0 = time.time()
    if engine == "pg":
        cnt = _run(client + ["-X", "-At"], f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{s}';\n", env)[1]
        if cnt.strip().replace(" ", "") == "0":
            text = f"CREATE SCHEMA IF NOT EXISTS {s};\nSET search_path = {s};\n" + _read(model_sql)
            rc, out = _run(client + ["-X", "-q", "-v", "ON_ERROR_STOP=1"], text, env)
            if rc != 0:
                return f"failed {s} DDL: {next((l for l in out.splitlines() if 'ERROR' in l), '')}", False
    else:
        cnt = _run(client, f"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE SCHEMA_NAME(schema_id) = '{s}';\n", env)[1]
        if next((x.strip() for x in cnt.splitlines() if re.fullmatch(r" *[0-9]+ *", x)), "x") == "0":
            _run(client, f"IF SCHEMA_ID('{s}') IS NULL EXEC('CREATE SCHEMA {s}');\n", env)
            text = re.sub(r"(CREATE TABLE|ALTER TABLE|REFERENCES) \[(" + env["RE"] + r")\]", rf"\1 [{s}].[\2]", _read(model_sql))
            rc, out = _run(client, text.replace("SCHEMA_NAME()", f"N'{s}'"), env)
            if rc != 0 or "Msg " in out:
                return f"failed {s} DDL: {next((l for l in out.splitlines() if l.startswith('Msg')), '')}", False
    out = ""
    for n in (1, 2, 3):
        rc, out = _run(fill + [engine, s], "", env)
        if rc == 0:
            return f"done {s} {int(time.time() - t0)}s", True
        SLEEP(n * 20)
    reason = next((l for l in out.splitlines() if re.search(r"Msg [0-9]|ERROR|no rows added", l)), "")
    return f"failed {s} fill: {reason}", False


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    if not a:
        print(USAGE, file=sys.stderr); print("copies.py: ms or pg so far", file=sys.stderr); return 2
    engine, model_sql, model = a[0], a[1] if len(a) > 1 else "", a[2] if len(a) > 2 else ""
    if engine not in ("ms", "pg"):
        print("copies.py: ms or pg so far", file=sys.stderr); return 2
    if len(a) < 5:
        print(USAGE, file=sys.stderr); return 2
    try:
        lines, ok = build(engine, model_sql, model, int(a[3]), int(a[4]), int(a[5]) if len(a) > 5 else 4)
    except (ValueError, OSError) as x:
        print(f"copies.py: {x}", file=sys.stderr); return 2
    for line in lines:
        print(line)
    print("all done" if ok else "finished with failures")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
