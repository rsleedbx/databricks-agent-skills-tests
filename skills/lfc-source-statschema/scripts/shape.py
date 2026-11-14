#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml"]
# ///
# uv run shape.py schema.yaml [--stats stats.yaml] [--sf 1] [--scale 1] [--keep-small 1000]
#   > shaped.yaml   (and expected.json, the statistics the copy should have, for compare.sql)
# statschema's canonical schema (and, if given, its statistics) -> the same schema with each table's target rows and each
# column's lfc-source-datagen tags in its comment, still in statschema's format; ddl.py turns it into any engine's DDL.
#   Rows: row_count_per_sf x --sf, or row_count (a collected schema: the source's rows) x --scale, where tables of
#   --keep-small rows or fewer keep their size. Formulas are written in MySQL syntax; ddl.py transpiles them.
#   A column's value, first match wins:
#     1. its generation rule (schema.yaml): sequential, cyclic, block, block_cyclic, constant, uniform, zipf, normal, a value
#        list with weights, or a format_pattern (names, phones, addresses, ...); unique columns keep plain n
#     2. its statistics (stats.yaml): null fraction, most common values and frequencies, distinct count, range, histogram
#     3. nothing: the planner's own scramble, or for a foreign key the planner's probe of the parent's range
#   Foreign keys without a rule: the parent's key formula at a parent row drawn uniformly, or zipf if fk_distribution says so.
#   Keys: a key from rules gets lfc:n from the rules (sequential, cyclic, block, block_cyclic form a mixed radix); a
#   composite key without rules is numbered from its first two columns; a table without a key numbers its first integer column.
import argparse, datetime, decimal, json, math, re, sys, zlib
import yaml

ap = argparse.ArgumentParser()
ap.add_argument("schema"); ap.add_argument("--stats"); ap.add_argument("--sf", type=float, default=1.0)
ap.add_argument("--scale", type=float, default=1.0); ap.add_argument("--keep-small", type=int, default=1000)
ap.add_argument("--expected", default="expected.json")
a = ap.parse_args()
S = yaml.safe_load(open(a.schema))
ST = yaml.safe_load(open(a.stats)) if a.stats else {"tables": []}
T = {t["name"]: t for t in S["tables"]}
TS = {t["name"]: t for t in ST.get("tables", [])}
log = lambda *m: print("--", *m, file=sys.stderr)

def rows_of(t):
    if t.get("row_count_per_sf"): return max(1, round(t["row_count_per_sf"] * a.sf))
    r = int(t.get("row_count") or 0)
    return r if r <= a.keep_small else max(1, round(r * a.scale))
R = {n: rows_of(t) for n, t in T.items()}

def primes(k, start=2147483647):            # large primes: (n - 1) * p mod rows is a permutation of the rows; times m DIV rows spreads it evenly over m values
    out, n = [], start
    while len(out) < k:
        if all(n % d for d in range(3, int(n ** 0.5) + 1, 2)): out.append(n)
        n -= 2
    return out
P = primes(64)
def K(*key): return P[zlib.crc32(":".join(map(str, key)).encode()) % len(P)]

def kind(c):
    t = str(c.get("type", "string")).lower()
    if t in ("integer", "int", "long", "bigint", "smallint", "tinyint", "mediumint"): return "int"
    if t in ("decimal", "numeric", "number", "money"): return "dec"
    if t in ("float", "double", "real"): return "float"
    if t in ("date", "time", "boolean"): return t
    if t in ("timestamp", "timestamptz", "datetime"): return "ts"
    if t in ("binary", "varbinary", "blob", "bytea"): return "bin"
    return "str"

def to_u(c, v):                             # a value -> the integer the column's cast turns back into it
    if v is None: return None
    k = kind(c)
    try:
        if k in ("int", "float"): return round(float(v))
        if k == "dec": return int(decimal.Decimal(str(v)).scaleb(int(c.get("scale") or 0)).to_integral_value())
        if k == "date": return (datetime.date.fromisoformat(str(v)[:10]) - datetime.date(1970, 1, 1)).days
        if k == "ts": return int((datetime.datetime.fromisoformat(str(v)[:19]) - datetime.datetime(1970, 1, 1)).total_seconds())
        if k == "time": h, m, s = str(v)[:8].split(":"); return int(h) * 3600 + int(m) * 60 + int(float(s))
        if k == "boolean": return 1 if str(v).lower() in ("t", "true", "1") else 0
    except (ValueError, decimal.InvalidOperation):
        return None
    return None

A36 = "'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'"
def b36(L):                                 # n - 1 as L base-36 characters: short text keys (CHAR(2) holds 1,296 values)
    return "CONCAT(" + ", ".join(f"SUBSTRING({A36}, (x.n - 1) DIV {36 ** (L - 1 - i)} % 36 + 1, 1)" for i in range(L)) + ")"
def b36n(col, L):                           # and back: 1 + the base-36 number
    return "1 + " + " + ".join(f"(POSITION(SUBSTRING({col}, {i + 1}, 1) IN {A36}) - 1) * {36 ** (L - 1 - i)}" for i in range(L))
STRUCT = ("sequential", "cyclic", "block", "block_cyclic")
def codec(c, rows):                         # a key column: (kind, n back from the column, encode(index expr), how many values)
    k, g, name = kind(c), c.get("generation") or {}, c["name"]
    if k == "str":
        vals, L = g.get("values"), int(c.get("length") or 0)
        if vals:                             # a code list: the i-th code (1-based, wrapping), back by its position; sorted, so n
            vals = sorted(map(str, vals))    # rises with the key's text order, which the resume probe needs
            back = "CASE " + name + " " + " ".join(f"WHEN {lit(x)} THEN {i + 1}" for i, x in enumerate(vals)) + " END"
            return "e", back, (lambda i, vals=vals: pick([str(x) for x in vals], f"(({i}) - 1) % {len(vals)}")), len(vals)
        if 0 < L < 19 and rows >= 10 ** L:   # digits can't hold them: base 36
            return "e", b36n(name, L), (lambda i, L=L: b36(L).replace("x.n", f"({i})")), 36 ** L
        if 0 < L < 19: return "v", f"CAST({name} AS SIGNED)", (lambda i: i), 10 ** L - 1
        return "v", f"CAST(LEFT({name}, 19) AS SIGNED)", (lambda i: i), None
    if k in ("date", "ts") and g.get("min_value") and g.get("max_value"):
        # days (seconds) from the rule's range, so the key has as many values as the range holds: without it, a composite
        # key's date took one day per row and ran past 9999-12-31 (TPC-E daily_market: duplicate keys)
        e0 = datetime.datetime(1970, 1, 1); sec = lambda x: int((datetime.datetime.fromisoformat(str(x)) - e0).total_seconds())
        lo, hi = (sec(g[x]) // (1 if k == "ts" else 86400) for x in ("min_value", "max_value"))
        f = "LFC_EPOCH" if k == "ts" else "LFC_DAYS"
        return "v", f"{f}({name}) - {lo - 1}", (lambda i, lo=lo: f"({lo - 1} + {i})"), hi - lo + 1
    if k == "date": return "v", f"LFC_DAYS({name})", (lambda i: i), None
    if k == "ts": return "v", f"LFC_EPOCH({name})", (lambda i: i), None
    return "v", name, (lambda i: i), None
def lit(s): return "'" + str(s).replace("'", "''") + "'"
def perm(tn, col, salt, rows): return f"(x.n - 1) * {K(tn, col, salt)} % {rows}"
def draw(tn, col, salt, m): return f"(x.n - 1) * {K(tn, col, salt)} % {m}"         # 0 .. m-1, each equally often
def unit(tn, col, salt, rows): return f"(({perm(tn, col, salt, rows)}) + 0.5) / {rows}"   # 0 < u < 1, one per row

# format patterns: string expressions from n (portable SQL: CONCAT, LPAD, CASE; LPAD gets text, which Postgres needs)
WORDS = {"first": ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
                   "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"],
         "last": ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                  "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"],
         "city": ["Springfield", "Riverside", "Franklin", "Greenville", "Bristol", "Clinton", "Fairview", "Salem", "Madison",
                  "Georgetown", "Arlington", "Ashland", "Burlington", "Manchester", "Oxford", "Milton", "Newport", "Dover",
                  "Hudson", "Kingston"],
         "street": ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill", "Park"],
         "syl": ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"],   # TPC-C 4.3.2.3
         "word": ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod",
                  "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua", "enim"],
         "alpha": [chr(65 + i) for i in range(26)]}
def pick(words, idx):
    if len(words) == 1: return lit(words[0])
    return "CASE " + idx + " " + " ".join(f"WHEN {i} THEN {lit(w)}" for i, w in enumerate(words[:-1])) + f" ELSE {lit(words[-1])} END"
def pattern(p, tn, col, rows, length=None):
    q = lambda s, m: f"({draw(tn, col, s, m)})"
    p = str(p).strip('"')
    if p in ("first_name", "name_first"): return pick(WORDS["first"], q("f", 20))
    if p in ("last_name", "name_last"): return pick(WORDS["last"], q("l", 20))
    if p == "full_name": return f"CONCAT({pick(WORDS['first'], q('f', 20))}, ' ', {pick(WORDS['last'], q('l', 20))})"
    if p == "city": return pick(WORDS["city"], q("c", 20))
    if p == "address": return f"CONCAT(1 + {q('a', 9999)}, ' ', {pick(WORDS['street'], q('s', 10))}, ' St')"
    if p == "phone_us": return f"CONCAT(200 + {q('a', 800)}, '-', LPAD(CONCAT('', {q('b', 1000)}), 3, '0'), '-', LPAD(CONCAT('', {q('c', 10000)}), 4, '0'))"
    if p == "phone_intl": return f"CONCAT('+', 1 + {q('a', 98)}, ' ', LPAD(CONCAT('', {q('b', 1000000000)}), 9, '0'))"
    if p == "postal_us": return f"LPAD(CONCAT('', {q('z', 100000)}), 5, '0')"
    if p == "email": return f"CONCAT('user', x.n, '@example.com')"
    if p == "zip_code": return f"LPAD(CONCAT('', {q('z', 100000)}), 5, '0')"
    if p == "zero_padded_int": return f"LPAD(CONCAT('', x.n), {length or 10}, '0')"
    if p == "upper_id": return f"CONCAT('ID', LPAD(CONCAT('', x.n), 14, '0'))"           # unique, like a business key
    if p == "upper_alpha": return pick(WORDS["alpha"], q("a", 26))
    if p == "url": return f"CONCAT('http://www.example.com/page', x.n)"
    if p == "word": return pick(WORDS["word"], q("w", 20))
    if p == "sentence": return "CONCAT(" + ", ' ', ".join(pick(WORDS["word"], q(f"s{i}", 20)) for i in range(6)) + ", '.')"
    if p == "tpcc_last_name":             # three syllables from the digits of 0 .. 999
        d = q("n", 1000)
        return f"CONCAT({pick(WORDS['syl'], f'({d}) DIV 100')}, {pick(WORDS['syl'], f'(({d}) DIV 10) % 10')}, {pick(WORDS['syl'], f'({d}) % 10')})"
    return None

def zipf_rank(u, n, s):                    # 1 .. n with P(rank <= k) ~ k^(1-s) (s = 1: ~ log k): inverse CDF of a power law
    if abs(s - 1) < 1e-9: return f"CAST(LEAST({n}, FLOOR(POW({n} + 1, {u}))) AS SIGNED)"   # an integer: Postgres has no % on doubles
    return f"CAST(LEAST({n}, GREATEST(1, FLOOR(POW(1 + ({u}) * (POW({n} + 1, {1 - s}) - 1), {1 / (1 - s)})))) AS SIGNED)"

out_tables, expected = [], []
keyf = {}                                    # table -> {key column: formula in x.n}, for children's foreign keys
order, seen = [], set()
def visit(n, stack=()):
    if n in seen or n in stack or n not in T: return
    for f in T[n].get("fk_constraints") or []: visit(f["parent_table"], stack + (n,))
    seen.add(n); order.append(n)
for n in [t["name"] for t in S["tables"]]: visit(n)

for tn in order:
    t, rows = T[tn], R[tn]
    st = TS.get(tn, {}); rs = int(st.get("row_count") or t.get("row_count") or 0)
    cs = {c["name"]: c for c in st.get("columns", [])}
    cols = {c["name"]: c for c in t["columns"]}
    pk = [c["name"] for c in t["columns"] if c.get("primary_key")]
    fks = t.get("fk_constraints") or []
    fkcol = {cc: (f, pc) for f in fks for cc, pc in zip(f["columns"], f["parent_columns"])}
    v, e, ln, tags = {}, {}, {}, [f"lfc:rows={rows}"]
    def pcodec(col):                           # a key column that is a foreign key: the parent column's codec
        if col not in fkcol: return None
        f, pc = fkcol[col]; pt = T[f["parent_table"]]
        return codec(dict(next(x for x in pt["columns"] if x["name"] == pc), name=col), R[f["parent_table"]])   # read back from this column
    exp_cols = []

    def rule(c):                             # a generation rule -> (formula kind, text), or None
        g = c.get("generation") or {}
        name, dist, pr = c["name"], g.get("distribution", "auto"), g.get("distribution_params") or {}
        lo, hi = to_u(c, g.get("min_value")), to_u(c, g.get("max_value"))
        if name in fkcol and dist in ("block", "sequential"):   # wrap at the parent column's range, so every value is a parent key
            f, pc = fkcol[name]; pg = next((x.get("generation") or {} for x in T[f["parent_table"]]["columns"] if x["name"] == pc), {})
            if pg.get("distribution") == "cyclic" and pg.get("max_value") is not None:
                span = int(pg["max_value"]) - int(pg.get("min_value") or 0) + 1      # (a cyclic parent's min defaults to 0, as below)
                b0 = lo if lo is not None else int(pg.get("min_value") or 0)          # (and the child starts where the parent does)
                if dist == "block" and "block_size" in pr:
                    return "v", f"{b0} + ((x.n - 1) DIV {pr['block_size']}) % {span}", ("block", b0, int(pr["block_size"]), span)
        if dist == "sequential": return "v", f"{lo if lo is not None else 1} + x.n - 1", ("seq", lo if lo is not None else 1, 1, None)
        if dist == "cyclic" and lo is not None and hi is not None: return "v", f"{lo} + (x.n - 1) % {hi - lo + 1}", ("cyc", lo, 1, hi - lo + 1)
        if dist == "block" and "block_size" in pr:
            return "v", f"{lo or 0} + (x.n - 1) DIV {pr['block_size']}", ("block", lo or 0, int(pr["block_size"]), None)
        if dist == "block_cyclic" and "block_size" in pr:
            cyc = int(pr.get("cycle") or ((hi - lo + 1) if hi is not None else rows))
            return "v", f"{lo or 0} + ((x.n - 1) DIV {pr['block_size']}) % {cyc}", ("block", lo or 0, int(pr["block_size"]), cyc)
        vals, w = g.get("values"), g.get("weights")
        if dist == "constant":
            x = (vals or [g.get("min_value")])[0]
            return ("e", lit(x), None) if kind(c) == "str" else ("v", str(to_u(c, x)), None)
        if g.get("format_pattern"):
            p = pattern(g["format_pattern"], tn, name, rows, c.get("length"))
            if p: return "e", (f"LEFT({p}, {c['length']})" if c.get("length") else p), None
            log(f"{tn}.{name}: format_pattern {g['format_pattern']} not known; left to the planner")
        if vals:                             # a value list: weights by share of a permutation, or zipf, or uniform
            n = len(vals); u = unit(tn, name, "w", rows)
            if dist == "zipf": idx = f"{zipf_rank(u, n, float(pr.get('a', pr.get('exponent', 1.2))))} - 1"
            else:
                w = w or [1 / n] * n; cum, whens = 0.0, []
                for i, wi in enumerate(w[:-1]):
                    cum += wi / sum(w); whens.append(f"WHEN {perm(tn, name, 'w', rows)} < {round(cum * rows)} THEN {i}")
                idx = f"CASE {' '.join(whens)} ELSE {n - 1} END" if whens else "0"
            if kind(c) == "str": return "e", pick([str(x) for x in vals], f"({idx})"), None
            us = [to_u(c, x) for x in vals]
            if n == 1: return "v", str(us[0]), None
            return "v", "CASE (" + idx + ") " + " ".join(f"WHEN {i} THEN {x}" for i, x in enumerate(us[:-1])) + f" ELSE {us[-1]} END", None
        if lo is not None and hi is not None and hi >= lo:
            span = hi - lo + 1
            if dist == "zipf": return "v", f"{lo} + {zipf_rank(unit(tn, name, 'z', rows), span, float(pr.get('a', pr.get('exponent', 1.2))))} - 1", None
            if dist == "normal":
                mu, sd = float(pr.get("mean", (lo + hi) / 2)), float(pr.get("std", pr.get("stddev", span / 6)))
                z = f"SQRT(-2 * LN({unit(tn, name, 'n1', rows)})) * COS(2 * PI() * {unit(tn, name, 'n2', rows)})"
                return "v", f"CAST(LEAST({hi}, GREATEST({lo}, ROUND({mu} + {sd} * {z}))) AS SIGNED)", None
            if dist in ("uniform", "auto") and g.get("min_value") is not None:
                return "v", f"{lo} + {draw(tn, name, 'u', span)}", None
        return None

    # 1. rules for every column; then the keys, each through a codec (an integer index <-> the column's values) that its
    #    children's foreign keys share, so a child's value is always a parent key
    radix = {}
    for c in t["columns"]:
        r = rule(c)
        if r:
            (v if r[0] == "v" else e)[c["name"]] = r[1]
            if r[2] and c["name"] in pk and kind(c) == "int": radix[c["name"]] = r[2]
    def put(col, kf):                            # set a column from a (kind, formula) pair
        v.pop(col, None); e.pop(col, None)
        (v if kf[0] == "v" else e)[col] = kf[1]
    if pk and all(k in radix for k in pk):      # n - 1 = sum((col - min) * stride): a mixed radix, the largest stride leads
        if len(pk) > 1 or radix[pk[0]][1] != 1:
            tags.append("lfc:n={1 + " + " + ".join(f"({k} - {radix[k][1]}) * {radix[k][2]}" for k in pk) + "}")
            if len(pk) > 1: tags.append(f"lfc:by={{{max(pk, key=lambda k: radix[k][2])}}}")
        keyf[tn] = {k: ("v", v[k]) for k in pk}
    elif len(pk) == 1:
        k = pk[0]; cd = pcodec(k) or codec(cols[k], rows)
        if k in fkcol and rows > R[fkcol[k][0]["parent_table"]]:   # one child per parent: no more rows than parents
            pr = R[fkcol[k][0]["parent_table"]]; log(f"{tn}: {rows} rows but its key {k} is unique and points at {pr} parents; {pr} rows")
            rows = R[tn] = pr; tags[0] = f"lfc:rows={rows}"
        if cd[1] != k: tags.append(f"lfc:n={{{cd[1]}}}")
        put(k, (cd[0], cd[2]("x.n"))); keyf[tn] = {k: (cd[0], cd[2]("x.n"))}
    elif len(pk) > 1:                           # number by the first two columns: k1 = (n - 1) DIV f + 1, k2 = (n - 1) % f + 1
        k1, k2 = pk[0], pk[1]
        c1 = pcodec(k1) or codec(cols[k1], rows); c2 = pcodec(k2) or codec(cols[k2], rows)
        nd = (cs.get(k1) or {}).get("n_distinct") or 0
        cap = lambda k, cd: min(x for x in (cd[3], R.get(fkcol[k][0]["parent_table"]) if k in fkcol else None, 10 ** 18) if x)
        d1 = min((math.ceil(-nd * rows) if nd < 0 else math.ceil(nd / rs * rows) if nd and rs else None) or rows, cap(k1, c1))
        f = max(1, math.ceil(rows / max(d1, 1)))         # rows per k1 value; a foreign key k1 stays within its parent
        c2n = cap(k2, c2)
        if c2n < 10 ** 18 and f > c2n:          # (a small scale: TPC-E's 15 securities per watch list, 7 securities at SF 0.001)
            log(f"{tn}: {f} {k2} per {k1} but only {c2n} values of {k2}; {c2n} per {k1}"); f = c2n
            c1n = cap(k1, c1)
            if c1n < 10 ** 18 and math.ceil(rows / f) > c1n:   # more rows than the two keys' pairs: the key can't be unique
                log(f"{tn}: {rows} rows but only {c1n} x {f} key pairs; {c1n * f} rows")
                rows = R[tn] = c1n * f; tags[0] = f"lfc:rows={rows}"
        step = max(1, c2n // f) if c2n < 10 ** 18 else 1
        put(k1, (c1[0], c1[2](f"((x.n - 1) DIV {f} + 1)"))); put(k2, (c2[0], c2[2](f"(((x.n - 1) % {f}) * {step} + 1)")))
        for k in pk[2:]:
            ck = pcodec(k) or codec(cols[k], rows); put(k, (ck[0], ck[2]("1")))
        tags.append(f"lfc:n={{({c1[1]} - 1) * {f} + ({c2[1]} - 1) DIV {step} + 1}}")
        keyf[tn] = {k: ("v" if k in v else "e", v.get(k, e.get(k))) for k in pk}
    else:                                       # no key: a timestamp (one second per row) or an integer column numbers the rows
        free = [c for c in t["columns"] if c["name"] not in fkcol]
        ts = next((c["name"] for c in free if kind(c) == "ts"), None)
        n_col = ts or next((c["name"] for c in free if kind(c) == "int"), None)
        if not n_col: log(f"{tn}: no primary key and no timestamp or integer column to number the rows: skipped"); continue
        if ts: put(ts, ("v", "1700000000 + x.n")); tags += [f"lfc:n={{LFC_EPOCH({ts}) - 1700000000}}", f"lfc:by={{{ts}}}"]
        else: put(n_col, ("v", "x.n")); tags += [f"lfc:n={{{n_col}}}", f"lfc:by={{{n_col}}}"]
        log(f"{tn}: no primary key; {n_col} numbers the rows" + (" (one second each, from 2023-11-14)" if ts else ""))

    # foreign keys: the parent's key formula at a parent row m, unless the column has a structural rule of its own (TPC-C's
    # block and cyclic keys, which follow the parent's) or is part of this table's key (set above)
    for f in fks:
        p = f["parent_table"]
        if p not in keyf: log(f"{tn}: foreign key to {p} isn't generated here: left to the planner"); continue
        own = [cc for cc in f["columns"] if cc in pk or (cc in v and (cols[cc].get("generation") or {}).get("distribution") in STRUCT)]
        if len(own) == len(f["columns"]): continue
        s0 = cs.get(f["columns"][0]) or {}
        if len(f["columns"]) == 1 and keyf[p][f["parent_columns"][0]][0] == "v" and (s0.get("n_distinct") or s0.get("most_common_values")):
            continue                              # statistics below spread it over the parent's keys
        u = unit(tn, "+".join(f["columns"]), "fk", rows)
        m = (f"({zipf_rank(u, R[p], float((f.get('fk_distribution_params') or {}).get('a', 1.2)))})" if f.get("fk_distribution") == "zipf"
             else f"(1 + {perm(tn, '+'.join(f['columns']), 'fk', rows)} % {R[p]})")
        for cc, pc in zip(f["columns"], f["parent_columns"]):
            if cc in own: continue
            if cc in v or cc in e: log(f"{tn}.{cc}: its own rule replaced by the parent key {p}.{pc}, so every value is a key")
            kf = keyf[p][pc]; put(cc, (kf[0], kf[1].replace("x.n", m)))

    # 2. statistics for the rest
    for c in t["columns"]:
        name, k = c["name"], kind(c)
        s = cs.get(name)
        if not s: continue
        ex = {"name": name}
        if k == "str" and s.get("avg_width_bytes"):            # Postgres widths include a 1-byte header for short values
            L = max(1, int(s["avg_width_bytes"]) - 1)
            if str(c.get("type")).lower() != "char" and (not c.get("length") or int(c["length"]) > L): ln[name] = L
        if name in v or name in e or (len(pk) == 1 and name == pk[0]) or c.get("unique"):
            exp_cols.append(ex); continue
        p = (s.get("null_fraction") or 0) if not c.get("not_null") else 0
        d = s.get("n_distinct") or None
        if d is not None:
            d = math.ceil(-d * rows) if d < 0 else (math.ceil(d / rs * rows) if rs and d >= 0.1 * rs else int(d))
            d = max(1, min(d, rows))
        par = R.get(fkcol[name][0]["parent_table"]) if name in fkcol and len(fkcol[name][0]["columns"]) == 1 else None
        if par:                                               # the share of parents that have children, as in the source
            ps, nd = int((TS.get(fkcol[name][0]["parent_table"]) or {}).get("row_count") or 0), s.get("n_distinct")
            share = (-nd * rs if nd and nd < 0 else (nd or 0)) / ps if nd and ps else 1
            d = max(1, min(par, math.ceil(min(share, 1) * par)))
        mcv = [m for m in (s.get("most_common_values") or []) if m.get("frequency")]
        lo, hi = to_u(c, s.get("min_value")), to_u(c, s.get("max_value"))
        if d and lo is not None and hi is not None and hi >= lo and not par: d = min(d, hi - lo + 1)
        ex.update(null_fraction=round(p, 4), n_distinct=d)
        if mcv: ex["top_frequency"] = round(mcv[0]["frequency"] * (1 - p), 4)
        exp_cols.append(ex)
        if p >= 1: v[name] = "NULL"; continue
        if d is None and not p and not mcv: continue
        if d is None: d = par or rows
        hist = [to_u(c, b) for b in (s.get("histogram_bounds") or [])]
        hist = hist if len(hist) >= 3 and None not in hist and not par else []
        if hist:                               # the median of common values and histogram together, to check the shape
            hb = s["histogram_bounds"]; mv = [(to_u(c, m.get("value")), m["frequency"]) for m in (s.get("most_common_values") or [])]
            if all(u is not None for u, _ in mv):
                hw = max(1 - sum(f for _, f in mv), 0) / len(hb)
                vals = [m.get("value") for m in (s.get("most_common_values") or [])]
                pts = sorted([(u, f, x) for (u, f), x in zip(mv, vals)] + [(u, hw, x) for u, x in zip(hist, hb)])
                tot, cum = sum(p[1] for p in pts), 0.0
                med = next(x for u, w, x in pts if (cum := cum + w) >= tot / 2)
                ex.update(median=med, low=hb[0], high=hb[-1])        # the source's values, as the copy's statistics show them
        pkey = keyf.get(fkcol[name][0]["parent_table"], {}).get(fkcol[name][1]) if par else None
        if pkey and pkey[0] == "e": continue     # a text-coded parent key: set above from the parent's codec
        def place(i, nvals):                                   # value index 0 .. nvals-1 -> the integer for the cast
            if par: return (pkey[1] if pkey else "x.n").replace("x.n", f"(1 + ({i}) * {par} DIV {d})")
            if hist:                                           # equi-height: index -> quantile -> between two bounds
                B = len(hist) - 1
                arr = lambda j: "CASE " + j + " " + " ".join(f"WHEN {q} THEN {hb}" for q, hb in enumerate(hist[:-1])) + f" ELSE {hist[-1]} END"
                b = f"(({i}) * {B} DIV {nvals})"
                within = f"(({i}) * {B} - {b} * {nvals})"
                return f"{arr(b)} + {within} * ({arr(f'{b} + 1')} - {arr(b)}) DIV {nvals}"
            if lo is not None and hi is not None and hi > lo: return f"{lo} + ({i}) * {hi - lo} DIV {max(nvals - 1, 1)}"
            return f"({i})"
        cap = 100                                             # most common values kept per column
        extra = [to_u(c, m.get("value")) for m in mcv[cap:]] if len(mcv) > cap and len(mcv) >= d and not par else []
        if None in extra or len(",".join(map(str, extra))) > 300: extra = []
        if len(mcv) > cap: log(f"{tn}.{name}: {len(mcv)} most common values, {cap} kept" + (f", {len(extra)} more drawn evenly" if extra else ""))
        mcv = mcv[:min(len(mcv), d, cap)]
        if mcv and len(mcv) >= d:                             # they are every value: their frequencies are the whole column
            tot = sum(m["frequency"] for m in mcv); mcv = [dict(m, frequency=m["frequency"] / tot) for m in mcv]
        if mcv:
            r, cum, whens = perm(tn, name, "m", rows), 0.0, []
            for i, m in enumerate(mcv):
                cum += m["frequency"]                         # stats.yaml: fractions of the non-null rows
                u = to_u(c, m.get("value"))
                whens.append(f"WHEN {r} < {min(round(cum * rows), rows)} THEN {u if (u is not None and not par) else place(i, d)}")
            rest_n = d - len(mcv)
            rest = (f"ELT(1 + ({perm(tn, name, 'j', rows)}) * {len(extra)} DIV {rows}, {', '.join(map(str, extra))})" if extra else
                    place(f"{len(mcv)} + ({perm(tn, name, 'j', rows)}) * {rest_n} DIV {rows}", d) if rest_n > 0 else place(len(mcv) - 1, d))
            if hist and not extra and rest_n > 0: rest = place(f"({perm(tn, name, 'j', rows)}) * {rest_n} DIV {rows}", rest_n)
            val = f"CASE {' '.join(whens)} ELSE {rest} END"
        else:
            val = place(f"({perm(tn, name, 'j', rows)}) * {d} DIV {rows}", d)
        if p > 0: val = f"CASE WHEN {perm(tn, name, 'z', rows)} < {round(p * rows)} THEN NULL ELSE {val} END"
        v[name] = val

    # 3. order between columns. An offset rule (distribution: offset, distribution_params: {from: <column>, min, max}, in
    #    days for a date, seconds for a timestamp) is the other column's formula plus min .. max, each equally often, so the
    #    order holds by construction. Then each temporal_ordering_constraints entry ("a <= b", "a < b", ">=", ">"): one an
    #    offset already keeps is checked; any other pair gets b = GREATEST(b, a), which piles b's low values onto a's
    def offset_of(c):                           # (from, min, max) of an offset rule, or None
        g = c.get("generation") or {}
        if g.get("distribution") != "offset": return None
        pr = g.get("distribution_params") or {}
        return pr.get("from"), int(pr.get("min", 0)), int(pr.get("max", 0))
    for c in t["columns"]:
        o = offset_of(c)
        if not o: continue
        src, lo_o, hi_o = o
        if src not in v or src not in cols or kind(cols[src]) != kind(c) or hi_o < lo_o:
            log(f"{tn}.{c['name']}: offset from {src} needs a {kind(c)} column of this table with a formula, and min <= max: left as is"); continue
        put(c["name"], ("v", f"({v[src]}) + {lo_o} + {draw(tn, c['name'], 'o', hi_o - lo_o + 1)}"))
    for con in t.get("temporal_ordering_constraints") or []:
        m = re.fullmatch(r"\s*(\w+)\s*(<=|<|>=|>)\s*(\w+)\s*", con)
        if not m or m[1] not in cols or m[3] not in cols:
            log(f"{tn}: ordering {con!r}: not two columns of this table; not enforced"); continue
        a_, b_, strict = (m[1], m[3], m[2] == "<") if m[2] in ("<=", "<") else (m[3], m[1], m[2] == ">")
        o = offset_of(cols[b_])
        if o and o[0] == a_ and o[1] >= (1 if strict else 0) and b_ in v:
            log(f"{tn}: {con} holds: {b_} is {a_} + {o[1]} .. {o[2]}"); continue
        if a_ not in v or b_ not in v or b_ in pk:
            log(f"{tn}: ordering {con!r}: {a_} and {b_} need formulas, and {b_} can't be a key; not enforced"); continue
        put(b_, ("v", f"GREATEST({v[b_]}, ({v[a_]}) + {1 if strict else 0})"))
        log(f"{tn}: {con} by GREATEST: {b_} below {a_}{' + 1' if strict else ''} is raised to it, so many rows sit there (an offset rule keeps {b_}'s spread)")

    # the shaped table: statschema's format, tags in the column comments and in lfc_tags (ddl.py writes the table comment)
    nt = {k: x for k, x in t.items() if k not in ("row_count_per_sf",)}
    nt["row_count"] = rows; nt["lfc_tags"] = " ".join(tags)
    nt["columns"] = []
    for c in t["columns"]:
        c2 = dict(c); com = []
        if c["name"] in e: com.append(f"lfc:e={{{e[c['name']]}}}")
        elif c["name"] in v: com.append(f"lfc:v={{{v[c['name']]}}}")
        if c["name"] in ln: com.append(f"lfc:len={{{ln[c['name']]}}}")
        if com: c2["comment"] = " ".join(com)
        nt["columns"].append(c2)
    if st.get("indexes"): nt["lfc_indexes"] = [{"name": i["name"], "columns": i["columns"], "unique": i.get("unique", False)} for i in st["indexes"]]
    out_tables.append(nt)
    expected.append({"name": tn, "row_count": rows, "columns": exp_cols})

yaml.safe_dump({"version": S.get("version", "1.0"), "tables": out_tables}, sys.stdout, sort_keys=False, allow_unicode=True, width=10**9)
json.dump({"sf": a.sf, "scale": a.scale, "keep_small": a.keep_small, "tables": expected}, open(a.expected, "w"), indent=1)
log(f"{len(out_tables)} tables, {sum(R[t['name']] for t in out_tables):,} rows")
