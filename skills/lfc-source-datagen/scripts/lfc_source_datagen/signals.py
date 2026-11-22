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
  rebuilds         tables the step rebuilds from others (an append leaves them to it)

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
    """The model's signals that have a step: a rule (rule_sql) or a SQL file (TPC-H's lead times are built by
    tpch_sources)."""
    import yaml
    from statschema.tools import model as model_path
    with open(model_path(model)) as f:
        return tuple(s for s in (yaml.safe_load(f) or {}).get("signals") or [] if s.get("sql") or s.get("rule"))


def _formulas(text: str, f: dict) -> str:
    """{name} -> (the rule's formulas: entry), formulas using each other."""
    for _ in range(20):
        new = re.sub(r"\{(\w+)\}", lambda m: f"({f[m.group(1)]})" if m.group(1) in f else m.group(0), text)
        if new == text:
            return text
        text = new
    raise ValueError("rule formulas refer to each other in a loop")


def rule_sql(sig: dict) -> str:
    """A signal's rule (the schema's signals: block) as its SQL step, psql variables (:<parameter>) left in. Nothing
    here names a model, table or column: the rule says them all.

      table      the table whose rows the rule marks (signals: table)
      rule:
        formulas   names for expressions over the table's columns, used as {name}
        row        the row's scramble, a whole number that is the same on every rerun: row % 1000 decides the mark
        per_mille  the base rate, and causes: [{when: <predicate>, add: <per mille>}], each added when it holds
        marked     {column: expression} for a marked row; unmarked: {column: expression} for the others
        totals     [{table, column, key, sum, by}]: a parent's column set to the sum of the marked table's `sum` over
                   its rows (by = key), so balances stay the sum of their history
    Each statement updates only the rows whose value changes, so a rerun changes nothing."""
    r, t = sig["rule"], sig["table"]
    f = r.get("formulas") or {}
    x = lambda e: _formulas(str(e), f)   # noqa: E731
    row = f"({x(r['row'])})"
    rate = " + ".join([f"({x(r['per_mille'])})"] +
                      [f"CASE WHEN ({x(c['when'])}) THEN ({x(c['add'])}) ELSE 0 END" for c in r.get("causes") or []])
    marked, unmarked = r["marked"], r.get("unmarked") or {}
    cols = list(dict.fromkeys(list(marked) + list(unmarked)))
    v = {c: ident(f"v_{c}") for c in cols}
    val = [f"CASE WHEN {row} % 1000 < {rate} THEN ({x(marked.get(c, c))}) ELSE ({x(unmarked.get(c, c))}) END AS {v[c]}"
           for c in cols]
    out = [f"WITH v AS (SELECT ctid AS rid, {', '.join(val)} FROM {ident(t)})\n"
           f"UPDATE {ident(t)} x SET {', '.join(f'{ident(c)} = v.{v[c]}' for c in cols)} FROM v WHERE x.ctid = v.rid "
           f"AND (" + " OR ".join(f"x.{ident(c)} IS DISTINCT FROM v.{v[c]}" for c in cols) + ");"]
    for g in r.get("totals") or []:
        p, k, c = ident(g["table"]), ident(g["key"]), ident(g["column"])
        out.append(f"WITH s AS (SELECT p.{k} AS id, COALESCE(SUM(h.{ident(g['sum'])}), 0) AS v FROM {p} p "
                   f"LEFT JOIN {ident(t)} h ON h.{ident(g['by'])} = p.{k} GROUP BY 1)\n"
                   f"UPDATE {p} p SET {c} = s.v FROM s WHERE p.{k} = s.id AND p.{c} IS DISTINCT FROM s.v;")
    return "\n".join(out) + "\n"


def expected_rate(sig: dict, columns: dict[str, str] | None = None, formulas: dict[str, str] | None = None) -> str:
    """The rate a row of the marked table is marked at (0 .. 1), as SQL over its columns, :<parameter> left in:
    per_mille plus each cause that holds, / 1000. columns renames them ({h_t_id: teller_id}: a gold table's names),
    so a readout of the signal comes from the same rule as the data; formulas replaces some of the rule's ({hr: hour}:
    a column the gold table already has)."""
    r = sig["rule"]
    f = (r.get("formulas") or {}) | (formulas or {})
    e = _formulas(" + ".join([f"({r['per_mille']})"] + [f"CASE WHEN ({c['when']}) THEN ({c['add']}) ELSE 0 END"
                                                        for c in r.get("causes") or []]), f)
    for a, b in (columns or {}).items():
        e = re.sub(rf"\b{re.escape(a)}\b", b, e)
    return f"(({e}) / 1000.0)"


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
        text = rule_sql(sigs[sig]) if sigs[sig].get("rule") else expand(read_sql(sigs[sig]["sql"]))
        out += statements(psql_vars(text, dict(zip(sigs[sig]["parameters"], nums.split(",")))))
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
                  sessions: int = 4, log=print, fill: bool = True) -> list[dict]:
    """The model's sources with their signals (the module's docstring); see sources.build (fill=False: empty
    tables, no signals and no summary)."""
    srcs = parse(sources)
    if not fill:
        return build(connect, model, srcs, login, publication, sessions, log=log, fill=False)
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
    count is 0 (the last statement, the comment, counts none). A count is None where the connection doesn't report one
    (a UC JDBC connection's writes run as Spark's sessionInitStatement, which returns no count)."""
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
            rc = getattr(cur, "rowcount", -1)
            counts.append(0 if st.startswith("DO ") else None if rc is None or rc < 0 else rc)   # UC JDBC: -1
        out[sig["name"]] = counts
        log(f"signals: {name}: {sig['name']} re-applied, rows changed per statement {counts}")
    return out


def append(connect, model: str, names, grow: float, sessions: int = 4, log=print) -> list[dict]:
    """Grow each existing source by `grow` scale factors (python/fill.py --mode append --grow: each table its rows per
    scale factor times grow, so the tables keep their ratios; a fixed-size table gets none), then re-apply its signals
    (reapply), so the new rows follow them too. grow 0: only re-apply. One summary row per source."""
    out = []
    for name in names:
        check_name(name, "source")
        con = connect()
        cur = con.cursor()
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (name,))
        if not cur.fetchall():
            raise ValueError(f"source {name} doesn't exist: build it first")
        if grow > 0:
            log(f"signals: {name}: growing by {grow:g} scale factors")
            rebuilt = sorted({tb for s in spec(model) for tb in s.get("rebuilds") or []})   # the signals rebuild these
            _fill(connect, name, sessions, ("--mode", "append", "--grow", repr(float(grow)), "--skip", ",".join(rebuilt)))
        changed = reapply(cur, model, name, log)
        n = [c for v in changed.values() for c in v]
        out.append({"source": name, "changed": None if None in n else sum(n)} | summary(model, cur, name))
        con.close()
    return out


def batch(connect, model: str, names, rows: int, quiet: list[str] = (), log=print) -> list[dict]:
    """One batch of changes per source: `rows` new rows in every table of the model but the `quiet` ones (its
    dimensions: the caller decides them, by the one rule lfc_medallion.split_tables applies to the schema YAML's
    roles), by python/fill.py --mode append, continuing each table's row numbers by its generation rules, then its
    signals re-applied (their totals: the parents' sums). A rerun, or a retried task, appends another batch: the data
    stays consistent (the signal covers every row, the totals are recomputed), the rows are more than asked. Works on
    any DB-API connection connect() returns: psycopg, or a UC JDBC connection (lfc_source_contract.jdbc.dbapi)."""
    out = []
    for name in names:
        check_name(name, "source")
        _fill(connect, name, 1, ("--mode", "append", "--rows", str(int(rows))) + (("--skip", ",".join(quiet))
                                                                                   if quiet else ()))
        con = connect()
        cur = con.cursor()
        changed = reapply(cur, model, name, log)
        n = [c for v in changed.values() for c in v]
        out.append({"source": name, "rows": int(rows), "changed": None if None in n else sum(n)})
        con.close()
    return out
