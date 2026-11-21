"""check: a pack's marts against its model, offline (no workspace): what an n:1 silver and gold must get right.

    from lfc_medallion_pack.check import check
    problems = check(pack.load("tpcb"))         # [] when the marts are right

  - each all_<table> read is a table of the model (n:1 defines one per table);
  - each column read through an all_<table> alias is a column of that table, or the source column;
  - each JOIN's condition equates the source column of both sides: keys repeat across LOBs (each starts at 1), so a
    join without it matches one LOB's row to another's;
  - a pack's tables list is the model's tables.
The marts are SQL files of CREATE OR REFRESH MATERIALIZED VIEW statements (Lakeflow Spark Declarative Pipelines);
${name} pipeline settings are read as placeholders.
"""
from __future__ import annotations

import re

STMT = re.compile(r"CREATE\s+OR\s+REFRESH\s+(?:MATERIALIZED\s+VIEW|STREAMING\s+TABLE)\s+([\w.${}]+)", re.I)
BODY = re.compile(r"\bAS\s+(SELECT|WITH)\b", re.I)


def columns(model: str) -> dict[str, list[str]]:
    """The model's tables and their columns, as its sources have them (lfc_source_datagen builds them)."""
    import yaml
    from lfc_source_datagen.sources import model_schema
    return {t["name"]: [c["name"] for c in t["columns"]] for t in yaml.safe_load(model_schema(model, 1))["tables"]}


def statements(sql: str) -> list[tuple[str, str]]:
    """(view name, its SELECT) per statement, -- comments dropped and ${x} settings as placeholders."""
    sql = "\n".join(ln.split("--")[0] if not ln.lstrip().startswith("--") else "" for ln in sql.splitlines())
    sql = re.sub(r"DATE\s*'\$\{[\w]+\}'", "DATE'2000-01-01'", sql)
    sql = re.sub(r"\$\{[\w]+\}\.\$\{[\w]+\}\.", "", sql)
    sql = re.sub(r"\$\{([\w]+)\}", r"\1", sql)
    out = []
    for part in [p for p in sql.split(";") if p.strip()]:
        m, b = STMT.search(part), BODY.search(part)
        if m and b:
            out.append((m.group(1).split(".")[-1], part[b.start() + 3:].strip()))
    return out


def check(p: dict, cols: dict[str, list[str]] | None = None) -> list[str]:
    import sqlglot
    from sqlglot import exp

    from .pack import mart_sql
    cols = cols if cols is not None else columns(p["model"])
    col = p["source_column"]
    problems = []
    if sorted(p["tables"]) != sorted(cols):
        problems.append(f"pack tables {sorted(p['tables'])} aren't the model's {sorted(cols)}")
    for f, sql in mart_sql(p).items():
        for view, body in statements(sql):
            where = f"{f}: {view}"
            try:
                tree = sqlglot.parse_one(body, read="databricks")
            except sqlglot.errors.ParseError as e:
                problems.append(f"{where}: doesn't parse ({str(e).splitlines()[0]})")
                continue
            alias = {}
            for t in tree.find_all(exp.Table):
                name = t.name
                alias[t.alias_or_name] = name
                if name.startswith("all_") and name[4:] not in cols:
                    problems.append(f"{where}: {name}: no table {name[4:]} in the model")
            for c in tree.find_all(exp.Column):
                src = alias.get(c.table)
                if src and src.startswith("all_") and src[4:] in cols and c.name not in cols[src[4:]] + [col]:
                    problems.append(f"{where}: {c.table}.{c.name}: no column {c.name} in {src[4:]}")
            for j in tree.find_all(exp.Join):
                right = j.this.alias_or_name   # a table's alias, or a subquery's
                on = j.args.get("on")
                ok = on is not None and any(
                    isinstance(e, exp.EQ) and {getattr(e.left, "name", None), getattr(e.right, "name", None)} == {col}
                    and right in (getattr(e.left, "table", None), getattr(e.right, "table", None))
                    for e in on.find_all(exp.EQ))
                if not ok:
                    problems.append(f"{where}: the join of {right} doesn't equate {col} on both sides")
    return problems
