#!/usr/bin/env python3
"""The test's SQL for any model, from its canonical schema (statschema's schema.yaml, lfc-source-statschema): nothing
here names a table, a column or a type, so a new model needs a schema.yaml and nothing else.

usage: model-sql.py <schema.yaml> <command> [options]
  state  --prefix P                     one SELECT on the source: table, column, MAX for every integer key column that
                                        isn't a foreign key; save its output (with the header row) for --state
  round N --state F --prefix P [--part before|after] [--manifest out.json]
                                        the DML of round N (1, 2, 3) on the source, and what it should produce
  alter N --prefix P [--part ddl|after] [--gold gold.sql] [--manifest out.json] [--state F]
                                        round 4 (a column added, with a default) or 5 (a column dropped, one retyped)
  select T --side source|databricks --prefix P [--from NAME] [--where COND] [--order COLS]
                                        table T's columns, every value rendered as the same text on both sides (NULL as
                                        <null>), each under its own name, ordered by the key (every column for a table
                                        without one), then COLS; --from reads another table with T's columns (T_history)
  digest --side source|databricks --prefix P --tables t1[:silver1],t2 [--where COND]
                                        one query: per table, its rows and the sum of CRC32 of each row rendered as in
                                        select (the same on both sides), "table<TAB>rows<TAB>sum"; silverN reads table
                                        tN's columns from another table (tN_history); --where on the Databricks side
  --dialect crdb|pg|mysql|mssql|oracle|db2|informix (default pg, which is also YugabyteDB; mysql: select and digest
  only so far); --only t1,t2 limits the workload to those tables. mssql, oracle, db2 and informix take each table's
  first row (its key, and a parent's key for the inserts) from the state's "first:" rows instead of a subquery
  (they lack LIMIT, a tuple IN, or, Informix, a subquery on the table an UPDATE changes), and fold names as the
  database does: Oracle and Db2 keep an unquoted name in upper case, so do their change events (fold).

The workload, generic over keys and foreign keys:
  - a table with a primary key gets inserts of new keys (each integer key column that isn't a foreign key at its MAX
    plus n, so the rows are new), updates of one column that isn't a key, a foreign key or unique, and deletes only of
    rows the workload inserted itself, so no foreign key ever blocks one. A foreign key column takes an existing parent
    row's value (the parent's first row by its key), so every insert is valid.
    round 1: insert keys 1, 2, 3, update the table's first row, delete key 3
    round 2: two updates of the first row, delete and reinsert key 1, reinsert key 3 (deleted in round 1)
    round 3: insert key 4, two updates of key 2; --part after (once the producer is restarted): update key 4
  - a table whose key is all foreign keys gets the updates only; a table without a key gets inserts only (append-only).
  Round 4 adds lfc_r4 (VARCHAR(20) NOT NULL DEFAULT 'none') to the first table with a key, then sets it on one row.
  Round 5 retypes the first non-key decimal column (two more digits; else an integer becomes BIGINT, but not on
  CockroachDB, whose INT is already 8 bytes), drops another column of that table (not a key, a foreign key, nor named in
  --gold), then writes a value only the new type holds.
The manifest (JSON): "expect", each table's events through this round {table: {create, update, delete}}; "events", how
many this part adds; "probe", a predicate on the normalized change log that holds once the part's last change arrived;
for alter, also "table", "column", "value", and for round 5 "retype" and "drop".
"""
import argparse
import datetime
import json
import os
import re
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # canonical_sql.py sits next to this file (python3 -I)
from canonical_sql import ctype  # noqa: E402

INTS = ("integer", "long")


DIALECT = "pg"   # set from --dialect: MySQL quotes identifiers with backticks
# the first rows come from the state, as literals (MySQL too: it refuses a subquery on the table an UPDATE changes)
LITERAL_FIRST = ("mssql", "oracle", "db2", "informix", "mysql")


def fold(n):   # a name as the database keeps it, and as its change events carry it (ddl.py writes them unquoted)
    return n.upper() if DIALECT in ("oracle", "db2") else n.lower() if DIALECT == "informix" else n


def qi(n):   # a source identifier, quoted: names keep their case (TPC-DI's are mixed case)
    if DIALECT == "mysql":
        return "`" + n.replace("`", "``") + "`"
    if DIALECT == "mssql":
        return "[" + n.replace("]", "]]") + "]"
    if DIALECT == "informix":   # quoting needs DELIMIDENT; unquoted names are folded to lower case
        return n.lower()
    return '"' + fold(n).replace('"', '""') + '"'


def bq(n):   # a Databricks identifier
    return "`" + n.replace("`", "``") + "`"


def vpath(base, n):   # a VARIANT field: the plain form for simple names, brackets otherwise
    # a bracket right after a column is a map's; on a VARIANT column it follows a colon (key:['SK_TimeID'])
    return f"{base}:{n}" if re.fullmatch(r"[a-z_][a-z0-9_]*", n) else f"{base}{'' if ':' in base else ':'}['{n}']"


class Model:
    def __init__(self, path, only=None):
        tables = yaml.safe_load(open(path))["tables"]
        self.t = {}
        for t in tables:
            cols = t["columns"]
            fks = [(list(f["columns"]), f["parent_table"], list(f["parent_columns"])) for f in t.get("fk_constraints") or []]
            fks += [([c["name"]], c["references"][0], [c["references"][1]]) for c in cols if c.get("references")]
            self.t[t["name"]] = {"cols": cols, "keys": [c["name"] for c in cols if c.get("primary_key")], "fks": fks}
        names = [n for n in self.t if not only or n in only]
        self.order = []   # parents before children
        seen = set()

        def visit(n, stack=()):
            if n in seen or n in stack or n not in self.t:
                return
            for _, p, _ in self.t[n]["fks"]:
                visit(p, stack + (n,))
            seen.add(n)
            self.order.append(n)
        for n in self.t:
            visit(n)
        self.order = [n for n in self.order if n in names]
        for n, t in self.t.items():
            fkc = {c for cs, _, _ in t["fks"] for c in cs}
            t["fkc"] = fkc
            t["free"] = [k for k in t["keys"] if k not in fkc]
            t["upd"] = next((c for c in t["cols"] if c["name"] not in t["keys"] and c["name"] not in fkc
                             and not c.get("unique") and not (c.get("generation") or {}).get("unique")
                             and ctype(c) != "binary"), None)

    def col(self, t, n):
        return next(c for c in self.t[t]["cols"] if c["name"] == n)


def value(c, seed):
    """A literal of the column's canonical type, from seed (PostgreSQL syntax: PostgreSQL, YugabyteDB, CockroachDB)."""
    ty = ctype(c)
    if ty == "integer":
        return str(seed % 30000 + 1)   # fits SMALLINT too
    if ty == "long":
        return str(seed)
    if ty == "decimal":
        p, s = int(c.get("precision") or 18), int(c.get("scale") or 0)
        ip = seed % 10 ** min(max(p - s, 0), 6)
        return f"{ip}.{seed % 10 ** s:0{s}d}" if s else str(ip)
    if ty in ("float", "double"):
        return f"{seed}.5"
    if ty == "boolean":
        return {"mssql": "1" if seed % 2 else "0", "informix": "'t'" if seed % 2 else "'f'"}.get(
            DIALECT, "TRUE" if seed % 2 else "FALSE")
    epoch = datetime.datetime(2030, 1, 1)
    if ty == "date":
        return literal(c, f"{(epoch + datetime.timedelta(days=seed % 3000)).date()}")
    if ty in ("timestamp", "timestamptz"):
        return literal(c, f"{epoch + datetime.timedelta(seconds=seed):%Y-%m-%d %H:%M:%S}")
    if ty in ("time", "timetz"):
        return literal(c, f"{datetime.timedelta(seconds=seed % 86400)}".rjust(8, "0"))
    if ty == "binary":
        return literal(c, f"{seed:08x}")
    # base 36, so a short column still gets a new value from each seed (the last digit of r2000 and r2100 is the same,
    # and an UPDATE to the value a row has is no change: SQL Server doesn't log it, so its CDC has no event)
    b, x = "", seed
    while True:
        b, x = "0123456789abcdefghijklmnopqrstuvwxyz"[x % 36] + b, x // 36
        if not x:
            break
    n = int(c.get("length") or 0)
    s = "r" + b if not n or n > len(b) else b[-n:]
    return "'" + s + "'"


def literal(c, v):
    """A value given as text (an integer, a decimal, a string, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS[.ffffff], HH:MM:SS,
    hex for binary) as a literal of the column's type, in the dialect."""
    ty, d = ctype(c), DIALECT
    if ty in INTS or ty in ("decimal", "float", "double"):
        return v
    if ty == "boolean":
        t = v.lower() in ("true", "t", "1")
        return {"mssql": "1" if t else "0", "informix": "'t'" if t else "'f'"}.get(d, "TRUE" if t else "FALSE")
    if ty == "date":
        if d == "mssql":
            return f"CAST('{v}' AS DATE)"
        if d == "informix":   # MDY: no dependence on DBDATE
            y, m, dd = v.split("-")
            return f"MDY({int(m)}, {int(dd)}, {int(y)})"
        return f"DATE('{v}')" if d == "db2" else f"DATE '{v}'"
    if ty in ("timestamp", "timestamptz"):
        if d == "mssql":
            return f"CAST('{v}' AS {'DATETIME2' if ty == 'timestamp' else 'DATETIMEOFFSET'})"
        if d == "informix":
            return f"DATETIME({v}) YEAR TO FRACTION(5)" if "." in v else f"DATETIME({v}) YEAR TO SECOND"
        if d == "db2":
            return f"TIMESTAMP('{v}')"
        return f"TIMESTAMP '{v}'" + ("::TIMESTAMPTZ" if ty == "timestamptz" and d in ("pg", "crdb") else "")
    if ty in ("time", "timetz"):
        return {"mssql": f"CAST('{v}' AS TIME)", "informix": f"DATETIME({v}) HOUR TO SECOND",
                # Oracle has no TIME: ddl.py makes it TIMESTAMP(0), on the data generator's 1970-01-01
                "oracle": f"TIMESTAMP '1970-01-01 {v}'", "db2": f"TIME('{v}')"}.get(d, f"TIME '{v}'")
    if ty == "binary":
        return {"mssql": f"0x{v}", "oracle": f"HEXTORAW('{v}')", "db2": f"BX'{v}'", "mysql": f"X'{v}'"}.get(d, f"decode('{v}', 'hex')")
    return "'" + v.replace("'", "''") + "'"


class Rounds:
    def __init__(self, m, prefix, state):
        self.m, self.p = m, prefix
        self.base, self.firsts = {}, {}
        for line in (open(state).read().splitlines() if state else []):
            f = [x.strip() for x in line.split("\t")]
            if len(f) < 3 or f[0] in ("tbl", "TBL"):   # a header (one per result set where a database sends several)
                continue
            if f[1].startswith("first:"):   # a table's first row by its key: column -> its value as text
                self.firsts.setdefault(f[0], {})[f[1][6:]] = f[2]
            else:
                self.base[(f[0], f[1])] = int(f[2] or 0) if f[2] not in ("", "NULL") else 0

    def tbl(self, t):
        return qi(t) if DIALECT == "informix" else f"{self.p}.{qi(t)}"   # Informix: a database per source

    def firstlit(self, t, k):   # the first row's value of column k, as a literal (the state's first: rows)
        v = self.firsts.get(t, {}).get(k)
        if v is None:
            raise SystemExit(f"no first row of {t} in the state: run state first (the table must have rows)")
        return literal(self.m.col(t, k), v)

    def keylit(self, t, k, n):
        c = self.m.col(t, k)
        if ctype(c) in INTS:
            return str(self.base.get((t, k), 0) + n)
        if ctype(c) in ("string", "char"):   # 'zz..n': after the generated keys (lfc-source-datagen numbers from 0)
            L = int(c.get("length") or 0)
            return "'" + ("zyxw"[n - 1] if L == 1 else ("z" * L + str(n))[-L:] if L else f"zz{n}") + "'"
        return value(c, 900000 + n)

    def first(self, t):   # the table's first row by its key: never one the workload inserts (those keys come after MAX)
        if DIALECT in LITERAL_FIRST:
            return " AND ".join(f"{qi(k)} = {self.firstlit(t, k)}" for k in self.m.t[t]["keys"])
        ks = ", ".join(qi(k) for k in self.m.t[t]["keys"])
        return f"({ks}) IN (SELECT {ks} FROM {self.tbl(t)} ORDER BY {ks} LIMIT 1)"

    def at(self, t, n):
        return " AND ".join(f"{qi(k)} = {self.keylit(t, k, n)}" for k in self.m.t[t]["free"])

    def insert(self, t, n, rnd):
        d = self.m.t[t]
        names, vals, froms = [], [], []
        for i, (cs, parent, pcs) in enumerate(d["fks"]):
            if parent not in self.m.t:
                continue
            if DIALECT in LITERAL_FIRST:   # the parent's first row, from the state
                for c, pc in zip(cs, pcs):
                    if c not in names:
                        names.append(c)
                        vals.append(self.firstlit(parent, pc))
                continue
            pk = ", ".join(qi(x) for x in pcs)
            froms.append(f"(SELECT {pk} FROM {self.tbl(parent)} ORDER BY {pk} LIMIT 1) f{i}")
            for c, pc in zip(cs, pcs):
                if c not in names:
                    names.append(c)
                    vals.append(f"f{i}.{qi(pc)}")
        for c in d["cols"]:
            if c["name"] in names:
                continue
            names.append(c["name"])
            vals.append(self.keylit(t, c["name"], n) if c["name"] in d["free"] else value(c, n * 10 + rnd))
        cols = ", ".join(qi(x) for x in names)
        if froms:
            return f"INSERT INTO {self.tbl(t)} ({cols}) SELECT {', '.join(vals)} FROM {', '.join(froms)};"
        return f"INSERT INTO {self.tbl(t)} ({cols}) VALUES ({', '.join(vals)});"

    def update(self, t, where, seed):
        u = self.m.t[t]["upd"]
        return f"UPDATE {self.tbl(t)} SET {qi(u['name'])} = {value(u, seed)} WHERE {where};"

    def delete(self, t, n):
        return f"DELETE FROM {self.tbl(t)} WHERE {self.at(t, n)};"

    def plan(self, rnd, part):
        """[(table, sql, op)] for one round and part, parents first (deletes, of rows nothing refers to, in any order)."""
        out = []
        for t in self.m.order:
            d = self.m.t[t]
            ins = bool(d["keys"]) and bool(d["free"])
            upd = bool(d["keys"]) and d["upd"] is not None
            s = []
            if not d["keys"]:   # append-only
                s = {1: [1, 2], 2: [3], 3: [4] if part == "before" else []}[rnd]
                out += [(t, self.insert(t, n, rnd), "create") for n in s]
                continue
            if rnd == 1:
                s = [("create", n) for n in (1, 2, 3)] if ins else []
                s += [("update", "first")] if upd else []
                s += [("delete", 3)] if ins else []
            elif rnd == 2:
                s = [("update", "first"), ("update", "first")] if upd else []
                s += [("delete", 1), ("create", 1), ("create", 3)] if ins else []
            elif part == "before":
                s = [("create", 4)] if ins else []
                s += [("update", 2), ("update", 2)] if ins and upd else []
            else:
                s = [("update", 4)] if ins and upd else []
            for i, (op, n) in enumerate(s):
                if op == "create":
                    out.append((t, self.insert(t, n, rnd), op))
                elif op == "delete":
                    out.append((t, self.delete(t, n), op))
                else:
                    where = self.first(t) if n == "first" else self.at(t, n)
                    out.append((t, self.update(t, where, 1000 * rnd + 100 * i + (7 if part == "after" else 0)), op))
        return out

    def probe(self, rnd, part):
        """A predicate on the normalized change log: the part's last change for the first table with an insertable key."""
        m = self.m
        for t in m.order:
            d = m.t[t]
            if d["keys"] and d["free"] and d["upd"] and all(ctype(m.col(t, k)) in INTS for k in d["free"]):
                n = {1: 2, 2: 3, 3: 2 if part == "before" else 4}[rnd]
                op = "update" if (rnd, part) in ((3, "before"), (3, "after")) else "create"
                cond = " AND ".join(f"{vpath('key', fold(k))}::STRING = '{self.keylit(t, k, n)}'" for k in d["free"])
                return f"destination LIKE '%.{fold(t)}' AND operation = '{op}' AND {cond}"
        return "true"


def count(steps):   # by each table's name as the change log carries it (fold: Oracle's and Db2's in upper case)
    e = {}
    for t, _, op in steps:
        e.setdefault(fold(t), {"create": 0, "update": 0, "delete": 0})[op] += 1
    return e


def add(a, b):
    for t, ops in b.items():
        for op, n in ops.items():
            a.setdefault(t, {"create": 0, "update": 0, "delete": 0})[op] += n
    return a


def cmd_round(m, a):
    r = Rounds(m, a.prefix, a.state)
    exp = {}
    for n in range(1, a.n + 1):
        add(exp, count(r.plan(n, "before")))
        if n == 3 and a.part == "after":
            add(exp, count(r.plan(n, "after")))
    steps = r.plan(a.n, a.part)
    print("\n".join(s for _, s, _ in steps))
    write(a, {"expect": exp, "events": len(steps), "probe": r.probe(a.n, a.part)})


def first_keyed(m):
    return next(t for t in m.order if m.t[t]["keys"])


def cmd_alter(m, a):
    r = Rounds(m, a.prefix, a.state)   # the state's first rows, for the dialects that name a row by literals
    gold = set(re.findall(r"\w+", open(a.gold).read())) if a.gold else set()
    if a.n == 4:
        t = first_keyed(m)
        man = {"table": t, "column": "lfc_r4", "value": "r4_set", "default": "none",
               "probe": f"destination LIKE '%.{fold(t)}' AND {vpath('value:after', fold('lfc_r4'))}::STRING = 'r4_set'"}
        sql = (f"ALTER TABLE {r.tbl(t)} ADD COLUMN lfc_r4 VARCHAR(20) NOT NULL DEFAULT 'none';" if a.part == "ddl" else
               f"UPDATE {r.tbl(t)} SET lfc_r4 = 'r4_set' WHERE {r.first(t)};")
    else:
        pick = None
        for want in ("decimal", "integer"):
            for t in m.order:
                d = m.t[t]
                c = next((c for c in d["cols"] if d["keys"] and c["name"] not in d["keys"] and c["name"] not in d["fkc"]
                          and ctype(c) == want), None)
                if c and pick is None and not (want == "integer" and a.dialect == "crdb"):
                    pick = (t, c)
            if pick:
                break
        t, c = pick or (first_keyed(m), None)
        d = m.t[t]
        drop = next((x["name"] for x in reversed(d["cols"]) if x["name"] not in d["keys"] and x["name"] not in d["fkc"]
                     and (c is None or x["name"] != c["name"]) and x["name"] not in gold), None)
        man = {"table": t, "drop": drop, "retype": None, "probe": "true"}
        ddl = ["SET sql_safe_updates = false;", "SET enable_experimental_alter_column_type_general = true;"] if a.dialect == "crdb" else []
        if drop:
            ddl.append(f"ALTER TABLE {r.tbl(t)} DROP COLUMN {qi(drop)};")
        if c is not None:
            if ctype(c) == "decimal":
                p, s = int(c.get("precision") or 18), int(c.get("scale") or 0)
                new, v = f"DECIMAL({p + 2},{s})", "9" * (p - s + 1) + ("." + "1" * s if s else "")
            else:
                new, v = "BIGINT", "3000000000"
            ddl.append(f"ALTER TABLE {r.tbl(t)} MODIFY COLUMN {qi(c['name'])} {new}{'' if c.get('nullable', True) and not c.get('not_null') else ' NOT NULL'};"
                       if a.dialect == "mysql" else f"ALTER TABLE {r.tbl(t)} ALTER COLUMN {qi(c['name'])} TYPE {new};")
            man.update(retype={"column": c["name"], "to": new}, column=c["name"], value=v,
                       probe=f"destination LIKE '%.{fold(t)}' AND {vpath('value:after', fold(c['name']))}::STRING = '{v}'")
        sql = "\n".join(ddl) if a.part == "ddl" else (f"UPDATE {r.tbl(t)} SET {qi(c['name'])} = {man['value']} WHERE {r.first(t)};" if c else "")
    print(sql)
    write(a, man)


def render(c, side, dialect):
    """One column as text, the same on both sides: NULL as <null>, times to the microsecond, chars without padding."""
    return f"{expr(c, side, dialect)} AS {qi(c['name']) if side == 'source' else bq(c['name'])}"


def expr(c, side, dialect):
    n, ty = c["name"], ctype(c)
    if side == "source" and dialect == "mysql":   # MySQL: DATE_FORMAT, CAST AS CHAR, HEX for binary
        x = qi(n)
        f = "'%Y-%m-%d %H:%i:%s.%f'"
        e = {"boolean": f"CASE WHEN {x} THEN 'true' ELSE 'false' END", "float": f"CAST(CAST({x} AS DECIMAL(38,6)) AS CHAR)",
             "double": f"CAST(CAST({x} AS DECIMAL(38,6)) AS CHAR)", "timestamp": f"DATE_FORMAT({x}, {f})",
             "timestamptz": f"DATE_FORMAT(CONVERT_TZ({x}, @@session.time_zone, '+00:00'), {f})",
             "time": f"DATE_FORMAT(TIMESTAMP('2000-01-01', {x}), '%H:%i:%s.%f')", "char": f"RTRIM({x})",
             "binary": f"LOWER(HEX({x}))"}.get(ty, f"CAST({x} AS CHAR)")
        return f"COALESCE({e}, '<null>')"
    if side == "source" and dialect in LITERAL_FIRST:
        return f"COALESCE({source_text(c, qi(n), dialect)}, '<null>')"
    if side == "source":
        x = qi(n)
        fn, ymd, hms = ("experimental_strftime", "%Y-%m-%d ", "%H:%M:%S.%f") if dialect == "crdb" else \
            ("to_char", "YYYY-MM-DD ", "HH24:MI:SS.US")

        def fmt(e, f=ymd + hms):
            return f"{fn}({e}, '{f}')"
        e = {"boolean": f"CASE WHEN {x} THEN 'true' ELSE 'false' END", "float": f"CAST({x} AS DECIMAL(38,6))::TEXT",
             "double": f"CAST({x} AS DECIMAL(38,6))::TEXT", "timestamp": fmt(x), "timestamptz": fmt(f"{x} AT TIME ZONE 'UTC'"),
             "time": fmt(f"DATE '2000-01-01' + {x}", hms), "char": f"RTRIM({x})",
             "binary": f"encode({x}, 'hex')"}.get(ty, f"{x}::TEXT")
        return f"COALESCE({e}, '<null>')"
    else:   # silver: a time is already text (silver-from-schema.py renders it as HH:mm:ss.SSSSSS)
        x = bq(n)
        e = {"boolean": f"CASE WHEN {x} THEN 'true' ELSE 'false' END", "float": f"CAST(CAST({x} AS DECIMAL(38,6)) AS STRING)",
             "double": f"CAST(CAST({x} AS DECIMAL(38,6)) AS STRING)",
             "timestamp": f"date_format({x}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
             "timestamptz": f"date_format({x}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')", "char": f"RTRIM({x})",
             "binary": f"lower(hex({x}))"}.get(ty, f"CAST({x} AS STRING)")
        if dialect == "informix" and ty in ("string", "varchar", "text"):   # Informix ignores a string's trailing blanks
            e = f"RTRIM({e})"   # (its text cast and LENGTH drop them); silver keeps them, as the source stored them
    return f"COALESCE({e}, '<null>')"


def ifx_len(c):   # an Informix cast's length: a row of casts, sorted, must fit its 32 KB row (-361)
    if ctype(c) in ("string", "char", "varchar", "text"):
        return max(int(c.get("length") or 255), 1)
    return 40   # numbers, dates, times as text


def source_text(c, x, d):
    """SQL Server, Oracle, Db2, Informix: column x as the text Databricks renders (expr's databricks side)."""
    ty = ctype(c)
    # Informix: each cast at the column's length (255 without one), as a row of LVARCHAR(4000)s passes its row limit (-361)
    vc = ("VARCHAR2(4000)" if d == "oracle" else "NVARCHAR(4000)" if d == "mssql"
          else f"LVARCHAR({ifx_len(c)})" if d == "informix" else "VARCHAR(4000)")
    if ty == "decimal":   # at the column's scale, a 0 before the point, as Databricks' CAST(DECIMAL AS STRING)
        p, sc = int(c.get("precision") or 18), int(c.get("scale") or 0)
        fmt = f"'FM{'9' * max(p - sc - 1, 0)}0{'.' + '0' * sc if sc else ''}'"
        if d == "oracle":
            return f"TO_CHAR({x}, {fmt})"
        if d == "db2":   # CAST AS VARCHAR writes 0.50 as .50 (and VARCHAR_FORMAT takes no FM mask for a decimal)
            v = f"CAST({x} AS VARCHAR(64))"
            return (f"CASE WHEN {x} > -1 AND {x} < 0 THEN '-0' || SUBSTR({v}, 2) WHEN {x} >= 0 AND {x} < 1 AND {sc} > 0 "
                    f"THEN '0' || {v} ELSE {v} END")
        return f"CAST({x} AS {vc})"
    if ty in ("float", "double"):
        if d == "oracle":
            return f"TO_CHAR(CAST({x} AS NUMBER(38,6)), 'FM{'9' * 31}0.000000')"
        return f"CAST(CAST({x} AS DECIMAL(31,6)) AS {vc})"
    if ty == "boolean":
        return {"mssql": f"CASE WHEN {x} = 1 THEN 'true' WHEN {x} = 0 THEN 'false' END",
                "informix": f"CASE WHEN {x} = 't' THEN 'true' WHEN {x} = 'f' THEN 'false' END"}.get(
            d, f"CASE WHEN {x} THEN 'true' WHEN NOT {x} THEN 'false' END")
    if ty == "date":
        return render_text(c, x)
    if ty in ("timestamp", "timestamptz"):
        if d == "informix":   # FRACTION(5) at most: the sixth digit is 0
            return f"TO_CHAR({x}, '%Y-%m-%d %H:%M:%S.%F5') || '0'"
        return render_text(c, x)
    if ty in ("time", "timetz"):
        return {"mssql": f"CONVERT(VARCHAR(15), CAST({x} AS TIME(6)), 114)",
                "oracle": f"TO_CHAR({x}, 'HH24:MI:SS') || '.000000'",   # a TIMESTAMP(0): ddl.py's time
                "informix": f"{x}::LVARCHAR || '.000000'",   # DATETIME HOUR TO SECOND: text without a fraction
                "db2": f"VARCHAR_FORMAT(TIMESTAMP(DATE('2000-01-01'), {x}), 'HH24:MI:SS') || '.000000'"}.get(d, f"CAST({x} AS {vc})")
    if ty == "char":
        return f"RTRIM({x})"
    if ty == "binary":
        return render_text(c, x)
    return f"CAST({x} AS {vc})"


def cmd_select(m, a):
    d = m.t[a.table]
    cols = ", ".join(render(c, a.side, a.dialect) for c in d["cols"])
    q = qi if a.side == "source" else bq
    order = [q(k) for k in d["keys"]] if d["keys"] else [str(i + 1) for i in range(len(d["cols"]))]
    where = f" WHERE {a.where}" if a.where else ""
    src = q(a.__dict__['from'] or a.table)
    src = src if a.side == "source" and DIALECT == "informix" else f"{a.prefix}.{src}"   # Informix: a database per source
    print(f"SELECT {cols} FROM {src}{where} ORDER BY {', '.join(order + ([a.order] if a.order else []))}"
          + (";" if a.side == "source" else ""))


def cmd_digest(m, a):
    """Per table: rows, and the sum of CRC32 over each row's columns rendered as select renders them, joined by |:
    equal on both sides when the rows are (in any order). CRC32 is the same function in MySQL and Databricks."""
    parts = []
    for spec in a.tables.split(","):
        t, _, frm = spec.partition(":")
        if t not in m.t:
            continue
        cols = m.t[t]["cols"]
        if a.side == "source":
            row = f"CONCAT_WS('|', {', '.join(expr(c, 'source', a.dialect) for c in cols)})"
            parts.append(f"SELECT '{t}' AS tbl, COUNT(*) AS n, CAST(COALESCE(SUM(CRC32({row})), 0) AS CHAR) AS h FROM "
                         f"{qi(t) if DIALECT == 'informix' else a.prefix + '.' + qi(t)}")
        else:
            row = f"concat_ws('|', {', '.join(expr(c, 'databricks', a.dialect) for c in cols)})"
            where = f" WHERE {a.where}" if a.where else ""
            parts.append(f"SELECT '{frm or t}' AS tbl, COUNT(*) AS n, CAST(COALESCE(SUM(crc32({row})), 0) AS STRING) AS h "
                         f"FROM {a.prefix}.{bq(frm or t)}{where}")
    print(" UNION ALL ".join(parts) + (";" if a.side == "source" else ""))


def cmd_state(m, a):
    r = Rounds(m, a.prefix, None)
    if DIALECT not in LITERAL_FIRST:
        parts = [f"SELECT '{t}' AS tbl, '{k}' AS col, COALESCE(MAX({qi(k)}), 0)::TEXT AS mx FROM {r.tbl(t)}"
                 for t in m.order for k in m.t[t]["free"] if ctype(m.col(t, k)) in INTS]
        print(" UNION ALL ".join(parts) + ";" if parts else "SELECT '' AS tbl, '' AS col, '0' AS mx;")
        return
    # one statement per table (a single row needs TOP, FETCH FIRST or FIRST, which a UNION member can't always have):
    # its free integer keys' maxima, then its first row's key and every column a child's foreign key reads
    out = []
    txt = {"oracle": "VARCHAR2(400)", "mysql": "CHAR", "informix": "LVARCHAR(400)"}.get(DIALECT, "VARCHAR(400)")   # Informix: VARCHAR stops at 255
    referenced = {(p, pc) for t in m.t.values() for _, p, pcs in t["fks"] for pc in pcs}
    for t in m.order:
        d = m.t[t]
        for k in d["free"]:
            if ctype(m.col(t, k)) in INTS:
                out.append(f"SELECT '{t}' AS tbl, '{k}' AS col, CAST(COALESCE(MAX({qi(k)}), 0) AS {txt}) AS mx FROM {r.tbl(t)}")
        if not d["keys"]:
            continue
        cols = list(d["keys"]) + [c for (p, c) in sorted(referenced) if p == t and c not in d["keys"]]
        order = ", ".join(qi(k) for k in d["keys"])
        for c in cols:
            v = render_text(m.col(t, c), qi(c))
            sel = f"'{t}' AS tbl, 'first:{c}' AS col, {v} AS mx FROM {r.tbl(t)} ORDER BY {order}"
            out.append({"mssql": f"SELECT TOP 1 {sel}", "informix": f"SELECT FIRST 1 {sel}", "mysql": f"SELECT {sel} LIMIT 1"}.get(
                DIALECT, f"SELECT {sel} FETCH FIRST 1 ROWS ONLY"))
    print(";\n".join(out) + ";")


def render_text(c, x):
    """A column as the text literal() takes back: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS.ffffff, hex, plain numbers."""
    ty, d = ctype(c), DIALECT
    if ty == "date":
        return {"mssql": f"CONVERT(VARCHAR(10), {x}, 23)", "oracle": f"TO_CHAR({x}, 'YYYY-MM-DD')", "mysql": f"DATE_FORMAT({x}, '%Y-%m-%d')",
                "informix": f"TO_CHAR({x}, '%Y-%m-%d')"}.get(d, f"VARCHAR_FORMAT({x}, 'YYYY-MM-DD')")
    if ty in ("timestamp", "timestamptz"):   # Oracle: a DATE column too (it holds a time; Debezium sends it as one)
        return {"mssql": f"CONVERT(VARCHAR(26), CAST({x} AS DATETIME2(6)), 121)",
                "oracle": f"TO_CHAR(CAST({x} AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS.FF6')", "mysql": f"DATE_FORMAT({x}, '%Y-%m-%d %H:%i:%s.%f')",
                "informix": f"TO_CHAR({x}, '%Y-%m-%d %H:%M:%S.%F5')"}.get(d, f"VARCHAR_FORMAT({x}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
    if ty == "binary":
        return {"mssql": f"LOWER(CONVERT(VARCHAR(400), {x}, 2))", "oracle": f"LOWER(RAWTOHEX({x}))"}.get(d, f"LOWER(HEX({x}))")
    if d == "mysql":
        return f"TRIM(CAST({x} AS CHAR))"
    if ty == "boolean" and d == "mssql":
        return f"CASE WHEN {x} = 1 THEN 'true' ELSE 'false' END"
    return f"TRIM(CAST({x} AS {'VARCHAR2(400)' if d == 'oracle' else 'LVARCHAR(400)' if d == 'informix' else 'VARCHAR(400)'}))"


def write(a, man):
    if a.manifest:
        json.dump(man, open(a.manifest, "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("schema")
    ap.add_argument("cmd", choices=["state", "round", "alter", "select", "digest"])
    ap.add_argument("arg", nargs="?")
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--state")
    ap.add_argument("--part", default="before")
    ap.add_argument("--manifest")
    ap.add_argument("--gold")
    ap.add_argument("--side", default="source", choices=["source", "databricks"])
    ap.add_argument("--dialect", default="pg", choices=["pg", "crdb", "mysql", "mssql", "oracle", "db2", "informix"])
    ap.add_argument("--tables")
    ap.add_argument("--only")
    ap.add_argument("--from")
    ap.add_argument("--where")
    ap.add_argument("--order")
    a = ap.parse_args()
    global DIALECT
    DIALECT = a.dialect
    m = Model(a.schema, set(a.only.split(",")) if a.only else None)
    if a.cmd in ("round", "alter"):
        a.n = int(a.arg)
        if a.cmd == "alter" and a.part == "before":
            a.part = "ddl"
    if a.cmd == "select":
        a.table = a.arg
    {"state": cmd_state, "round": cmd_round, "alter": cmd_alter, "select": cmd_select, "digest": cmd_digest}[a.cmd](m, a)


if __name__ == "__main__":
    main()
