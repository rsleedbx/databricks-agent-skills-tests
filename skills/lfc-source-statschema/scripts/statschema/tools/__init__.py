"""statschema.tools: the DDL tools as functions, for a notebook, a job or a pipeline as well as the command line.

    from statschema.tools import ddl, ddl_to_schema, model, shape
    yaml_text = ddl_to_schema(mysql_ddl, dialect="mysql")      # DDL -> schema.yaml (text)
    sql_text = ddl(yaml_text, dialect="postgres")              # schema.yaml (a path, YAML text or a dict) -> DDL (text)
    shaped, expected = shape(model("tpcc"), sf=0.01)           # a model at a scale: schema.yaml with the fill's tags

Each runs the script beside it (ddl.py, ddl_to_schema.py, shape.py) unchanged, with its arguments, input and output in memory,
so the text is the same as the command line's: scripts/ddl.py and scripts/ddl-to-schema.py are wrappers over the same
files. The scripts' messages on stderr stay on stderr. ddl.py changes how sqlglot's MySQL reader binds %; that change
is undone after each call, so the caller's own sqlglot and the next call start as a fresh process does.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import runpy
import sys
import tempfile

import yaml
from sqlglot.dialects.mysql import MySQL

HERE = os.path.dirname(os.path.abspath(__file__))
# the models (schemas/<model>.yaml): in a checkout two folders up from statschema/, in an installed package beside it
SCHEMAS = next((d for d in (os.path.join(HERE, "..", "_schemas"), os.path.join(HERE, "..", "..", "..", "schemas"))
                if os.path.isdir(d)), None)


def _run(script: str, argv: list[str], stdin: str = "") -> str:
    out = io.StringIO()
    old_argv, old_stdin, old_path = sys.argv, sys.stdin, list(sys.path)
    old_term, old_factor = MySQL.Parser.TERM, MySQL.Parser.FACTOR
    try:
        sys.argv, sys.stdin = [script, *argv], io.StringIO(stdin)
        with contextlib.redirect_stdout(out):
            runpy.run_path(os.path.join(HERE, script), run_name="__main__")
    finally:
        sys.argv, sys.stdin, sys.path[:] = old_argv, old_stdin, old_path
        MySQL.Parser.TERM, MySQL.Parser.FACTOR = old_term, old_factor
    return out.getvalue()


def ddl(schema, dialect: str, source_dialect: str | None = None) -> str:
    """schema.yaml -> DDL for the dialect (postgres, mysql, tsql, oracle, db2, informix, databricks, cockroachdb, ...).
    schema: a path to a schema.yaml, its YAML text, or the loaded dict."""
    extra = ["--source-dialect", source_dialect] if source_dialect else []
    if isinstance(schema, str) and "\n" not in schema and os.path.exists(schema):
        return _run("ddl.py", [schema, "--dialect", dialect, *extra])
    text = schema if isinstance(schema, str) else yaml.safe_dump(schema, sort_keys=False)
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(text)
    try:
        return _run("ddl.py", [f.name, "--dialect", dialect, *extra])
    finally:
        os.unlink(f.name)


def ddl_to_schema(ddl_text: str, dialect: str) -> str:
    """DDL in a sqlglot dialect -> statschema's schema.yaml (text), with the lfc tags kept."""
    return _run("ddl_to_schema.py", ["--dialect", dialect], ddl_text)


def model(name: str) -> str:
    """The path of a statschema model (tpcb, tpcc, tpch, tpcds, tpce, tpcdi): schemas/<name>.yaml."""
    path = os.path.join(SCHEMAS or "", f"{name}.yaml")
    if not SCHEMAS or not os.path.exists(path):
        have = sorted(f[:-5] for f in os.listdir(SCHEMAS) if f.endswith(".yaml")) if SCHEMAS else []
        raise ValueError(f"model {name}: one of {', '.join(have) or '(no schemas folder found)'}")
    return os.path.abspath(path)


def shape(schema, sf: float = 1.0, stats=None, scale: float = 1.0, keep_small: int = 1000) -> tuple[str, dict]:
    """A schema.yaml (a path, YAML text or a dict; a model from model()) at a scale factor -> (the shaped schema.yaml
    as text, the expected statistics shape.py writes to expected.json)."""
    with tempfile.TemporaryDirectory() as d:
        def path(x, n):
            if isinstance(x, str) and "\n" not in x and os.path.exists(x):
                return x
            f = os.path.join(d, n)
            with open(f, "w") as out:
                out.write(x if isinstance(x, str) else yaml.safe_dump(x, sort_keys=False))
            return f
        argv = [path(schema, "schema.yaml"), "--sf", repr(float(sf)), "--scale", repr(float(scale)),
                "--keep-small", str(int(keep_small)), "--expected", os.path.join(d, "expected.json")]
        if stats is not None:
            argv += ["--stats", path(stats, "stats.yaml")]
        text = _run("shape.py", argv)
        with open(os.path.join(d, "expected.json")) as f:
            return text, json.load(f)
