"""live_cases.py: edge cases written as each producer writes them (lfc_change_source.synth), through every silver cell
on a warehouse, each against a Python reference model of the case. Each case is a snapshot and events at positions;
the engines' encodings differ (formats.py), the silver must not. Two rounds (half the events each), so MERGEs and
history run incrementally. Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> [SCHEMA=<scratch>] [ENGINES=POSTGRESQL,ORACLE] python3 -I live_cases.py
"""
import datetime as dt
import decimal
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-change-source", "lfc-log-silver", "lfc-routing"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source import formats, synth  # noqa: E402
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_log_silver import history, silver  # noqa: E402

D = decimal.Decimal
COLS = [{"name": "id", "type": "integer", "primary_key": True}, {"name": "v", "type": "string"},
        {"name": "d", "type": "date"}, {"name": "amt", "type": "decimal", "precision": 10, "scale": 2},
        {"name": "ts", "type": "timestamp", "precision": 6}]
ROW = lambda i, v="x", d=dt.date(2026, 1, 1), a=D("1.50"), ts=dt.datetime(2026, 1, 1, 12, 0, 0, 123456): (  # noqa: E731
    {"id": i, "v": v, "d": d, "amt": a, "ts": ts})
P = 1000   # every case's snapshot position; its events come after (one case puts one at or before it)

# name: (snapshot rows, [(position, operation, row)])
CASES = {
    "basic": ([ROW(1), ROW(2)], [(1010, "create", ROW(3)), (1020, "update", ROW(1, "y")), (1030, "delete", ROW(2))]),
    "delete_reinsert_one_position": ([ROW(1)], [(1010, "delete", ROW(1)), (1010, "create", ROW(1, "again"))]),
    "replay": ([ROW(1)], [(1010, "update", ROW(1, "y")), (1010, "update", ROW(1, "y"))]),
    "twice_in_a_round": ([ROW(1)], [(1010, "update", ROW(1, "a")), (1011, "update", ROW(1, "b"))]),
    "nulls": ([ROW(1)], [(1010, "update", {"id": 1, "v": None, "d": None, "amt": None, "ts": None})]),
    "at_or_before_snapshot": ([ROW(1, "snap")], [(1000, "update", ROW(1, "old")), (990, "update", ROW(1, "older"))]),
    "dates": ([], [(1010, "create", ROW(1, d=dt.date(1969, 12, 31))), (1011, "create", ROW(2, d=dt.date(1900, 1, 1))),
                   (1012, "create", ROW(3, d=dt.date(9999, 12, 31)))]),
    "microseconds": ([], [(1010, "create", ROW(1, ts=dt.datetime(1969, 12, 31, 23, 59, 59, 999999))),
                          (1011, "create", ROW(2, ts=dt.datetime(2026, 6, 30, 0, 0, 0, 1)))]),
    "text": ([], [(1010, "create", ROW(1, "O'Brien")), (1011, "create", ROW(2, "back\\slash")),
                  (1012, "create", ROW(3, "日本語 ü"))]),
    "decimals": ([], [(1010, "create", ROW(1, a=D("99999999.99"))), (1011, "create", ROW(2, a=D("-0.01"))),
                      (1012, "create", ROW(3, a=D("0.00")))]),
    "delete_a_snapshot_key": ([ROW(1), ROW(2)], [(1010, "delete", ROW(2))]),
    "insert_a_snapshot_key": ([ROW(1)], [(1010, "create", ROW(1))]),
    "select": ([ROW(1)], [(1010, "update", ROW(1, "y"))]),   # reserved words as table names
    "order": ([ROW(1)], [(1010, "create", ROW(2))]),
}
KEYLESS = {"keyless": ([{"v": "s", "n": 1}], [(1010, "create", {"v": "a", "n": 2}), (1010, "create", {"v": "a", "n": 2}),
                                               (1011, "update", {"v": "s", "n": 9}), (1012, "create", {"v": "b", "n": 3})])}
KCOLS = [{"name": "v", "type": "string"}, {"name": "n", "type": "integer"}]


def model(snap: list, events: list, upto: int) -> tuple:
    """(current rows by key, history versions [(row, start, end)]) after the events at positions in (P, upto]."""
    evs = sorted({(n, op == "delete", json.dumps(r, default=str, sort_keys=True)): (n, op, r)
                  for n, op, r in events if P < n <= upto}.values(), key=lambda e: (e[0], e[1] != "delete"))
    cur = {r["id"]: r for r in snap}
    points = {r["id"]: [(P, 1, r)] for r in snap}
    for n, op, r in evs:
        k = r["id"]
        if op == "delete":
            cur.pop(k, None)
        else:
            cur[k] = r
        points.setdefault(k, []).append((n, 0 if op == "delete" else 1, r))
    hist = []
    for k, ps in points.items():
        ps = sorted({(n, v): (n, v, r) for n, v, r in ps}.values(), key=lambda x: (x[0], x[1]))
        for i, (n, v, r) in enumerate(ps):
            if v == 1:
                hist.append((r, n, ps[i + 1][0] if i + 1 < len(ps) else None))
    return cur, hist


def model_keyless(snap: list, events: list, upto: int) -> list:
    seen = {(n, json.dumps(r, sort_keys=True)) for n, op, r in events if op == "create" and P < n <= upto}
    return list(snap) + [json.loads(r) for _, r in sorted(seen)]


def lit(c: dict, v) -> str:
    if v is None:
        return "NULL"
    t = c["type"]
    if t == "date":
        return f"DATE'{v.isoformat()}'" if not isinstance(v, dt.datetime) else f"TIMESTAMP_NTZ'{v.isoformat(' ')}'"
    if t == "timestamp":   # a date in a timestamp column (Oracle's DATE): its midnight
        v = v if isinstance(v, dt.datetime) else dt.datetime(v.year, v.month, v.day)
        return f"TIMESTAMP_NTZ'{v.isoformat(' ')}'"
    if t == "decimal":
        return f"CAST('{v}' AS DECIMAL(10,2))"
    if t == "string":
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"
    return str(v)


def run(wh, cat: str, sch: str, engine: str) -> list:
    q = lambda s: wh.sql(s).collect()  # noqa: E731
    S = f"`{cat}`.`{sch}_{engine.lower()}`"
    q(f"CREATE SCHEMA IF NOT EXISTS {S}")
    for t in ("_log_silver", "_shared_0", "schema_drift"):
        q(f"DROP TABLE IF EXISTS {S}.{t}")
    log = f"{S}.cdc_log"
    q(f"CREATE OR REPLACE TABLE {log} (destination STRING, operation STRING, key VARIANT, value VARIANT)")
    kind = formats.DEBEZIUM[engine]["position"]
    cols = [dict(c, type="timestamp", precision=0) if engine == "ORACLE" and c["type"] == "date" else c for c in COLS]
    dtype = {"integer": "INT", "string": "STRING", "date": "DATE", "decimal": "DECIMAL(10,2)",
             "timestamp": "TIMESTAMP_NTZ"}
    sql_t = lambda c: dtype[c["type"]]  # noqa: E731
    server = {"SQLSERVER": "mssql1.db.dbo", "ORACLE": "oracle1.TPCH1"}.get(engine, "src1.s")
    tables, rows, cut = {}, [], 1015
    every = {**CASES, **KEYLESS}
    for name, (snap, events) in every.items():
        cc = KCOLS if name in KEYLESS else cols
        dest = f"{server}.{name}"
        ddl = ", ".join("`" + c["name"] + "` " + sql_t(c) for c in cc)
        q(f"CREATE OR REPLACE TABLE {S}.snap_{name} ({ddl})")
        if snap:
            q(f"INSERT INTO {S}.snap_{name} VALUES " + ", ".join(
                "(" + ", ".join(lit(c, r.get(c["name"])) for c in cc) + ")" for r in snap))
        for n, op, r in events:
            key, val = synth.debezium(engine, dest, op, r, cc, n)
            key = None if name in KEYLESS else key   # a keyless table's events have no key
            k = f"parse_json('{json.dumps(key)}')" if key is not None else "NULL"
            v = json.dumps(val).replace("\\", "\\\\").replace("'", "\\'")
            rows.append(f"('{dest}', '{op}', {k}, parse_json('{v}'))")
        src = CdcLog("src1", log, name, dest, [c["name"] for c in cc if c.get("primary_key")],
                     {c["name"]: sql_t(c) for c in cc}, seq=kind,
                     kinds={c["name"]: c["type"] for c in cc if c["type"] in ("date", "timestamp")},
                     precision={c["name"]: c.get("precision") for c in cc if c["type"] in ("date", "timestamp")})
        method = "append" if name in KEYLESS else {"basic": "merge", "nulls": "view", "text": "shared", "order": "view", "select": "shared"}.get(name, "merge")
        tables[name] = silver.Table(src, method, snapshot_table=f"{S}.snap_{name}", position=Position(kind, str(P)),
                                    group="0" if method == "shared" else "", history=name not in KEYLESS)
    q(f"INSERT INTO {log} VALUES " + ", ".join(rows))
    silver.run(wh, tables, S, Position(kind, str(cut)), threads=8, first=True)
    silver.run(wh, tables, S, Position(kind, str(2000)), threads=8)
    bad = []
    for name, (snap, events) in every.items():
        cc = KCOLS if name in KEYLESS else cols
        names = ", ".join(f"`{c['name']}`" for c in cc)
        if name in KEYLESS:
            want = model_keyless(snap, events, 2000)
            exp = " UNION ALL ".join("SELECT " + ", ".join(f"{lit(c, r.get(c['name']))} AS `" + c["name"] + "`" for c in cc)
                                     for r in want)
            checks = [(name, f"SELECT {names} FROM {S}.`{name}`", exp)]
        else:
            cur, hist = model(snap, events, 2000)
            conv = lambda r: {c["name"]: (dt.datetime(r[c["name"]].year, r[c["name"]].month, r[c["name"]].day)  # noqa: E731
                              if engine == "ORACLE" and c["type"] == "timestamp" and c["name"] == "d" and r.get(c["name"])
                              else r.get(c["name"])) for c in cc}
            exp = " UNION ALL ".join("SELECT " + ", ".join(f"{lit(c, conv(r)[c['name']])} AS `" + c["name"] + "`" for c in cc)
                                     for r in cur.values()) or f"SELECT {names} FROM {S}.snap_{name} WHERE false"
            hexp = " UNION ALL ".join("SELECT " + ", ".join(f"{lit(c, conv(r)[c['name']])} AS `" + c["name"] + "`" for c in cc)
                                      + f", CAST({s} AS BIGINT) AS __START_AT, CAST({e if e is not None else 'NULL'} AS BIGINT) AS __END_AT"
                                      for r, s, e in hist) or f"SELECT {names}, 0L AS __START_AT, 0L AS __END_AT WHERE false"
            checks = [(name, f"SELECT {names} FROM {S}.`{name}`", exp),
                      (name + history.SUFFIX, f"SELECT {names}, __START_AT, __END_AT FROM {S}.`{name}{history.SUFFIX}`", hexp)]
        for label, got, exp in checks:
            extra = q(f"SELECT count(*) FROM (({got}) EXCEPT ALL ({exp}))")[0][0]
            missing = q(f"SELECT count(*) FROM (({exp}) EXCEPT ALL ({got}))")[0][0]
            if int(extra) or int(missing):
                bad.append(f"{engine} {label} ({tables[name].method}): {extra} extra, {missing} missing")
    return bad


if __name__ == "__main__":
    w = WorkspaceClient(profile=os.environ["PROFILE"])
    wh = Warehouse(os.environ["WAREHOUSE"], w)
    engines = os.environ.get("ENGINES", ",".join(e for e in formats.DEBEZIUM)).split(",")
    allbad = []
    for e in engines:
        bad = run(wh, os.environ["CATALOG"], os.environ.get("SCHEMA", "lfc_log_silver_cases"), e)
        print(e, "ok" if not bad else "\n  ".join(["MISMATCH"] + bad), flush=True)
        allbad += bad
    sys.exit(1 if allbad else 0)
