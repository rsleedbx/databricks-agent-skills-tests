"""verify: what a deployed module holds, against its plan (plan.py), from a notebook or job after the transform:

    from lfc_medallion_pack.verify import run
    rows = run(spark, plan, "bank", catalog="main", prefix="me_febar")     # one row per check: check, want, got, ok

Every module (the no-data test too):
  bronze       one bronze schema per LOB, each with every table of the pack
  n1           all_<table> per table, with the source column, in the silver schema
  marts        every view the pack's marts create, in silver or gold
With data (data: true):
  rows         each table's rows in n:1 equal the sum of its bronze tables' (no LOB lost or doubled)
  readout      the pack's signal read back from gold: per LOB, the label's share against the rate its tier's numbers
               give each row (the model's signal rule, its columns as gold names them; or readout.rate), within
               tolerance;
               and per tier, the shares in the tiers' order of their expected rates
"""
from __future__ import annotations

import math
import os
import re

from .check import STMT


def mart_names(sql: str) -> list[str]:
    """The views a marts file creates, by their last name part (${catalog}.${gold_schema}.x -> x)."""
    return [n.split(".")[-1] for n in STMT.findall(sql)]


def _one(spark, q: str):
    return spark.sql(q).collect()


def _row(check: str, want, got, ok: bool, detail: str = "") -> dict:
    return {"check": check, "want": str(want), "got": str(got), "ok": bool(ok), "detail": detail}


def params(model: str, signals: str) -> dict:
    """A LOB's signal numbers by their names, from the model's signals: block (lfc_source_datagen.signals)."""
    from lfc_source_datagen.signals import spec
    out = {}
    for sig, nums in zip(spec(model), signals.split("/")):
        if nums:
            out |= dict(zip(sig["parameters"], (int(x) for x in nums.split(","))))
    return out


def built_signals(spark, plan: dict, module: str, connection: str, database: str) -> dict[str, dict]:
    """Each LOB's signal parameters as it was built, from its signal tables' comments (lfc:signal=<JSON>, read with
    lfc_source_datagen.signals.parameters), in one remote_query for every LOB. A LOB keeps the numbers it was built
    with when the module grows and re-ranks it into another tier; a LOB without the comment is left out (its plan's
    numbers are used)."""
    from lfc_source_datagen.signals import parameters, spec
    m = next(x for x in plan["modules"] if x["module"] == module)
    sigs = spec(m["model"])
    schemas = {x["source_schema"]: x["name"] for x in m["lobs"]}
    names = ", ".join("'" + s.replace("'", "''") + "'" for s in schemas)
    tables = ", ".join("'" + g["table"] + "'" for g in sigs)
    q = ("SELECT n.nspname AS s, c.relname AS t, obj_description(c.oid, 'pg_class') AS d FROM pg_class c "
         f"JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ({names}) AND c.relname IN ({tables})")
    out: dict[str, dict] = {}
    for r in spark.sql("SELECT * FROM remote_query(:c, database => :d, query => :q)",
                       args={"c": connection, "d": database, "q": q}).collect():
        for g in sigs:
            got = parameters(r.d, g["name"]) if r.t == g["table"] else None
            if got:
                out.setdefault(schemas[r.s], {}).update(got)
    return out


def readout_rate(model: str, ro: dict) -> str:
    """The rate a gold row should be labelled at: the model's signal rule (lfc_source_datagen.signals.expected_rate)
    with the rule's columns and formulas as the gold table has them (readout: columns, formulas)."""
    from lfc_source_datagen.signals import expected_rate, spec
    sig = next(s for s in spec(model) if s["name"] == ro["signal"])
    return expected_rate(sig, ro.get("columns"), ro.get("formulas"))


def bind(expr: str, values: dict) -> str:
    return re.sub(r":([a-z_]+)\b", lambda m: str(values[m.group(1)]), expr)


def read_marts(folder: str) -> dict[str, str]:
    """{file name: SQL} of a marts folder (the repo's marts/<pack>/)."""
    out = {}
    for f in sorted(os.listdir(folder)):
        if f.endswith(".sql"):
            with open(os.path.join(folder, f)) as fh:
                out[f] = fh.read()
    return out


def run(spark, plan: dict, module: str, catalog: str, prefix: str, marts: dict[str, str] | None = None,
        tolerance: float = 4.0, marts_dir: str | None = None, built: dict | None = None) -> list[dict]:
    """The checks (the module's docstring); marts: the SQL files, or marts_dir/<pack>/ read for them; built: each
    LOB's signal parameters as it was built (built_signals), else its plan's."""
    m = next(x for x in plan["modules"] if x["module"] == module)
    if marts is None and marts_dir:
        marts = read_marts(os.path.join(marts_dir, m["pack"]))
    n, col = m["module"], m["source_column"]
    bronze = f"{prefix}_{n}_bronze_"
    silver, gold = f"`{catalog}`.`{prefix}_{n}_silver`", f"`{catalog}`.`{prefix}_{n}_gold`"
    out = []
    have = {r.databaseName[len(bronze):] for r in _one(spark, f"SHOW SCHEMAS IN `{catalog}` LIKE '{bronze}*'")}
    want = [x["name"] for x in m["lobs"]]
    missing = sorted(set(want) - have)
    out.append(_row("bronze schemas", len(want), len(have & set(want)), not missing, f"missing {missing[:5]}"))
    q = (f"SELECT table_schema, table_name FROM `{catalog}`.information_schema.tables "
         f"WHERE table_schema LIKE '{bronze}%'")
    tabs: dict[str, set] = {}
    for r in _one(spark, q):
        tabs.setdefault(r.table_schema[len(bronze):], set()).add(r.table_name)
    short = {lob: sorted(set(m["tables"]) - tabs.get(lob, set())) for lob in want}
    short = {k: v for k, v in short.items() if v}
    out.append(_row("bronze tables", len(want) * len(m["tables"]),
                    sum(len(set(m["tables"]) & tabs.get(lob, set())) for lob in want), not short,
                    f"{len(short)} LOBs short of tables, e.g. {dict(list(short.items())[:3])}" + (
                        ": LOBs added to a deployed pipeline need its gateway restarted (it reads the source's tables "
                        "when it starts), then the refresh again" if short else "")))
    for t in m["tables"]:
        cols = [r.col_name for r in _one(spark, f"DESCRIBE TABLE {silver}.`all_{t}`")]
        out.append(_row(f"n1 all_{t}", f"{col} first", cols[:1], cols[:1] == [col]))
    views = {}
    for f, sql in (marts or {}).items():
        for v in mart_names(sql):
            views[v] = f
    for v in sorted(views):
        found = [s for s in (silver, gold) if _one(spark, f"SHOW TABLES IN {s} LIKE '{v}'")]
        out.append(_row(f"mart {v}", "exists", found[:1] or "missing", bool(found), views[v]))
    if not m["data"]:
        return out
    for t in m["tables"]:
        parts = [f"SELECT COUNT(*) AS c FROM `{catalog}`.`{bronze}{lob}`.`{t}`" for lob in want]
        b = sum(int(r.c) for k in range(0, len(parts), 100)
                for r in _one(spark, " UNION ALL ".join(parts[k:k + 100])))
        s = int(_one(spark, f"SELECT COUNT(*) AS c FROM {silver}.`all_{t}`")[0].c)
        out.append(_row(f"rows all_{t}", b, s, b == s and b > 0))
    ro = m.get("readout")
    if not ro:
        return out
    rate = ro.get("rate") or readout_rate(m["model"], ro)
    tiers: dict[str, list] = {}
    for x in m["lobs"]:
        p = (built or {}).get(x["name"]) or params(m["model"], x["signals"])   # as built: a re-tiered LOB's own
        r = _one(spark, f"SELECT COUNT(*) AS n, AVG(CASE WHEN {ro['label']} THEN 1.0 ELSE 0.0 END) AS got, "
                        f"AVG({bind(rate, p)}) AS want FROM {gold}.`{ro['table']}` "
                        f"WHERE `{col}` = '{x['name']}'")[0]
        cnt, got, exp = int(r.n), float(r.got or 0), float(r.want or 0)
        se = math.sqrt(max(exp * (1 - exp), 1e-9) / max(cnt, 1))
        ok = cnt > 0 and abs(got - exp) <= tolerance * se + 0.001
        out.append(_row(f"readout {x['name']} ({x['tier']})", f"{exp:.4f}", f"{got:.4f}", ok, f"{cnt} rows"))
        tiers.setdefault(x["tier"], []).append((exp, got))
    means = {t: (sum(e for e, _ in v) / len(v), sum(g for _, g in v) / len(v)) for t, v in tiers.items()}
    by_want = sorted(means, key=lambda t: means[t][0])
    by_got = sorted(means, key=lambda t: means[t][1])
    out.append(_row("readout tiers in order", by_want, by_got, by_want == by_got,
                    ", ".join(f"{t} {means[t][1]:.4f} (want {means[t][0]:.4f})" for t in by_want)))
    return out


def job(spark, w: dict, plan_path: str, marts_dir: str | None = None, show=None) -> list[dict]:
    """The verify job's whole notebook, so a project's notebook is one call and every fix arrives with the package:
    the checks (run, each LOB's signal numbers as built), the checksums (source, bronze, n:1), the source and gateway
    positions, the gateways' telemetry and change files (cdc.capture: every gateway found), the dashboard's views.
    w: the notebook's widgets (module, catalog, prefix, connection, database, control); show: display. Raises
    AssertionError naming the failed checks."""
    from . import cdc, checksum, observe
    from .plan import read_plan
    plan = read_plan(plan_path)
    built = built_signals(spark, plan, w["module"], w["connection"], w["database"])
    rows = run(spark, plan, w["module"], catalog=w["catalog"], prefix=w["prefix"], marts_dir=marts_dir, built=built)
    sums = checksum.run(spark, plan, w["module"], w["catalog"], w["prefix"], w["connection"], w["database"])
    checksum.save(spark, sums, w["catalog"], w["control"])   # the dashboard reads <control>.checksums
    cdc.save(spark, cdc.run(spark, plan, w["prefix"], w["connection"], w["database"]), w["catalog"], w["control"])
    observe.ensure_tables(spark, plan, w["catalog"], w["control"])
    got = cdc.capture(spark, plan, w["prefix"], w["catalog"], w["control"])
    print("capture:", got)
    lost = [g for g in got if not g["found"]]
    rows.append(_row("gateway capture (telemetry and change files, every gateway)", f"{len(got)} found",
                     f"{len(got) - len(lost)} found", not lost and bool(got),
                     "; ".join(f"{g['gateway']}: {g.get('why')}" for g in lost[:5])))
    observe.create_views(spark, plan, w["catalog"], w["prefix"], w["control"])   # v_changes, v_latency, ...
    off = [x for x in sums if x["status"] != "match"]
    rows.append(_row("checksums (rows and contents: source, bronze, n:1)", f"{len(sums)} match",
                     f"{len(sums) - len(off)} match", not off,
                     "; ".join(f"{x['lob']}.{x['table']}: {x['status']}" for x in off[:5])))
    if show:
        show(spark.createDataFrame(rows))
    bad = [r for r in rows if not r["ok"]]
    if bad:
        raise AssertionError(f"{len(bad)} of {len(rows)} checks failed: " + "; ".join(
            f"{r['check']}: want {r['want']}, got {r['got']} {r['detail']}" for r in bad[:10]))
    print(f"all {len(rows)} checks passed")
    return rows
