"""lfc_source_datagen.signals: a model's signals as its schema's signals: block says (statschema schemas/<model>.yaml),
on lfc_source_datagen.sources.build. Each signal is a SQL step (tpc/<sql>) with its parameters as psql variables, its
parameters added to <table>'s comment as lfc:signal=<JSON> after the fill's tags (lfc:rows, lfc:n, lfc:by, which a
later fill of the table still reads), and a summary query that reads them back. No model's data is in Python: a new
signal is a SQL file and a YAML block; one signal per table.

    from lfc_source_contract.jdbc import dbapi
    from lfc_source_datagen.signals import model_sources
    connect = lambda: dbapi(spark, "orders_jdbc", read_connection="orders_pg", database="orders")
    rows = model_sources(connect, "tpcc", {"local": (1, "3,0,2,5,2,1,2,12,3,2")}, login="lfc_cdc")

A source's numbers are its signals' parameters in the block's order, one signal's comma-separated, signals joined by
'/' and either one empty for none (TPC-DS: <returns>/<ship>). Per signal, the block gives:
  table            the table whose comment keeps the signal's name and parameters
  sql              its step, a file in tpc/ (psql variables: the parameters; a query used twice is written once as
                   "-- fragment <name>" ... "-- end" and used as {{<name>}}). Rerun on the same rows, it changes none.
  parameters       the numbers' names, in order; at_least_1: those that must be 1 or more
  summary          one row about a source: {s} is its schema, :<parameter> a parameter's value
  summary_columns  the row's names

  lfc-sources --model <model> [--lakebase P | --host H --password-file F ...] <database> <name>:<sf>[:<numbers>] ...
"""
from __future__ import annotations

import decimal
import functools
import json
import re

from lfc_source_contract.jdbc import ident, literal

from .sources import _fill, build, check_name, parse, psql_vars, read_sql, statements


@functools.lru_cache(maxsize=None)
def spec(model: str) -> tuple[dict, ...]:
    """The model's signals that have a SQL step (TPC-H's lead times are built by tpch_sources)."""
    import yaml
    from statschema.tools import model as model_path
    with open(model_path(model)) as f:
        return tuple(s for s in (yaml.safe_load(f) or {}).get("signals") or [] if s.get("sql"))


def split(model: str, extra: str) -> dict[str, str]:
    """A source's numbers -> {signal: its numbers}, for the signals it has."""
    sigs = spec(model)
    if not extra:
        return {}
    parts = extra.split("/")
    if len(parts) != len(sigs):
        raise ValueError(f"{model} numbers are " + "/".join(f"<{s['name']}>" for s in sigs)
                         + (" (either part empty for none)" if len(sigs) > 1 else ""))
    return {s["name"]: p for s, p in zip(sigs, parts) if p}


def check(model: str, srcs) -> None:
    """Each signal's form: as many whole numbers as it has parameters, at_least_1 ones 1 or more."""
    sigs = {s["name"]: s for s in spec(model)}
    for name, _sf, extra in srcs:
        for sig, nums in split(model, extra).items():
            names = sigs[sig]["parameters"]
            if not re.fullmatch(r"[0-9]+(,[0-9]+)*", nums) or nums.count(",") != len(names) - 1:
                raise ValueError(f"{name}: {sig} is {len(names)} whole numbers, comma-separated ({','.join(names)})")
            v = dict(zip(names, map(int, nums.split(","))))
            low = [k for k in sigs[sig].get("at_least_1") or [] if v[k] < 1]
            if low:
                raise ValueError(f"{name}: {sig}'s {', '.join(low)} at least 1")


def comment(model: str, sig: str, nums: str) -> str:
    """The signal's table comment: its name and parameters, as JSON."""
    names = next(s for s in spec(model) if s["name"] == sig)["parameters"]
    return json.dumps({"signal": sig, "parameters": dict(zip(names, map(int, nums.split(","))))}, separators=(",", ":"))


TAG = "lfc:signal="


def tag(table: str, text: str) -> str:
    """The statement that puts lfc:signal=<text> last in the table's comment, keeping the fill's tags before it (and
    replacing an earlier lfc:signal, so a rerun leaves one). Run with search_path at the source's copy."""
    t, q = literal(table), literal(ident(table))   # to_regclass reads a quoted name as is (TPC-DI's DimAccount)
    return ("DO $sig$ BEGIN EXECUTE format('COMMENT ON TABLE %I IS %L', " + t + ", concat_ws(' ', NULLIF(regexp_replace("
            "COALESCE(obj_description(to_regclass(" + q + "), 'pg_class'), ''), ' *lfc:signal=.*$', ''), ''), "
            + literal(TAG + text) + ")); END $sig$")


FRAGMENT = re.compile(r"^-- fragment (\w+)\n(.*?)^-- end\n", re.M | re.S)


def expand(text: str) -> str:
    """A SQL file's fragments ("-- fragment <name>" ... "-- end") put in for each {{<name>}}, and taken out."""
    parts = dict(FRAGMENT.findall(text))
    text = FRAGMENT.sub("", text)
    for _ in range(10):
        new = re.sub(r"\{\{(\w+)\}\}", lambda m: parts[m.group(1)].rstrip("\n") if m.group(1) in parts else m.group(0), text)
        if new == text:
            break
        text = new
    if re.search(r"\{\{\w+\}\}", text):
        raise ValueError("a {{fragment}} that the SQL file doesn't define")
    return text


def steps(model: str, name: str, extra: str) -> list[str]:
    """build()'s steps: each signal the source has, in the block's order (deterministic), then its comment."""
    sigs = {s["name"]: s for s in spec(model)}
    out = []
    for sig, nums in split(model, extra).items():
        out += statements(psql_vars(expand(read_sql(sigs[sig]["sql"])), dict(zip(sigs[sig]["parameters"], nums.split(",")))))
        out.append(tag(sigs[sig]["table"], comment(model, sig, nums)))
    return out


def parameters(text: str | None, sig: str) -> dict | None:
    """The signal's parameters from a table's comment: the JSON after lfc:signal=, or a comment that is only the JSON
    (sources built before the tag); None when the comment names another signal or none."""
    if not text:
        return None
    i = text.find(TAG)
    body = text[i + len(TAG):] if i >= 0 else text if text.lstrip().startswith("{") else None
    try:
        got = json.loads(body) if body else None
        return got["parameters"] if got and got.get("signal") == sig else None
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _number(x):
    if isinstance(x, decimal.Decimal):
        return float(x)
    return x


def summary(model: str, cur, name: str) -> dict:
    """Each signal's summary_columns: its summary row when the source has the signal (its table's comment), else none."""
    s = ident(name)
    out = {}
    for sig in spec(model):
        cols = sig.get("summary_columns") or []
        cur.execute(f"SELECT obj_description({literal(s + '.' + ident(sig['table']))}::regclass, 'pg_class')")
        params = parameters((cur.fetchall() or [[None]])[0][0], sig["name"])
        if params is None or not sig.get("summary"):
            out |= dict.fromkeys(cols)
            continue
        cur.execute(psql_vars(sig["summary"].replace("{s}", s), params))
        row = cur.fetchall()[0]
        if len(row) != len(cols):
            raise ValueError(f"{model} {sig['name']}: summary gave {len(row)} values for {len(cols)} summary_columns")
        out |= {c: _number(v) for c, v in zip(cols, row)}
    return out


def model_sources(connect, model: str, sources, login: str | None = None, publication: str | None = None,
                  sessions: int = 4, log=print) -> list[dict]:
    """The model's sources with their signals (the module's docstring); see sources.build."""
    srcs = parse(sources)
    check(model, srcs)
    return build(connect, model, srcs, login, publication, sessions, *bound(model)[1:3], log=log)


def bound(model: str):
    """(check, steps, summary) for sources.run and build, with the model fixed; the model's own steps when it has
    signals, else (None, None, None)."""
    if not spec(model):
        return None, None, None
    return (functools.partial(check, model), functools.partial(steps, model), functools.partial(summary, model))


def reapply(cur, model: str, name: str, log=print) -> dict[str, list[int]]:
    """Each signal the source has (its table's comment), run again with its own parameters on the rows there now:
    {signal: rows each statement changed}. The steps change only rows whose values differ, so on unchanged rows every
    count is 0 (the last statement, the comment, counts none)."""
    s = ident(name)
    cur.execute(f"SET search_path = {s}")
    out = {}
    for sig in spec(model):
        cur.execute(f"SELECT obj_description({literal(s + '.' + ident(sig['table']))}::regclass, 'pg_class')")
        params = parameters((cur.fetchall() or [[None]])[0][0], sig["name"])
        if params is None:
            continue
        nums = ",".join(str(params[k]) for k in sig["parameters"])
        counts = []
        for st in steps(model, name, "/".join(nums if x["name"] == sig["name"] else "" for x in spec(model))):
            cur.execute(st)
            counts.append(max(cur.rowcount, 0) if not st.startswith("DO ") else 0)
        out[sig["name"]] = counts
        log(f"signals: {name}: {sig['name']} re-applied, rows changed per statement {counts}")
    return out


def append(connect, model: str, names, rows: int, sessions: int = 4, log=print) -> list[dict]:
    """Add `rows` rows to every table of each existing source (python/fill.py --mode append; a table whose rows use
    every key value is left out), then re-apply its signals (reapply), so the new rows follow them too. rows 0: only
    re-apply. One summary row per source."""
    out = []
    for name in names:
        check_name(name, "source")
        con = connect()
        cur = con.cursor()
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (name,))
        if not cur.fetchall():
            raise ValueError(f"source {name} doesn't exist: build it first")
        if rows > 0:
            log(f"signals: {name}: appending {rows} rows to every table")
            _fill(connect, name, sessions, ("--mode", "append", "--rows", str(rows)))
        changed = reapply(cur, model, name, log)
        out.append({"source": name, "changed": sum(sum(v) for v in changed.values())} | summary(model, cur, name))
        con.close()
    return out
