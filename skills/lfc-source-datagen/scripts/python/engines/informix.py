# Informix (14.10): the catalog (systables, syscolumns, sysconstraints, sysindexes, sysreferences, and every tag from
# lfc_tags in the database, which statschema's ddl.py --dialect informix fills), and a cast per type in the same shape as
# the other engines, except that strings have no MD5 filler (Informix has no hash function): a 19-digit zero-padded n
# first (and last from 38 characters), n repeated between. Col.ty is the type's name (DATETIME with its qualifier).
# The schema is a database (Informix has no schemas inside one); names are unquoted (Informix stores them lower case).
# Rows come from CONNECT BY LEVEL on sysmaster:sysdual. Informix has no ORDER BY in a subquery (the probes use MAX and
# MIN), no %, no GREATEST or LEAST, and no random function: keys go in ascending order and data=random isn't available.
# n is cast to INT8: a key formula (lfc:n) is DECIMAL there, and MOD of a DECIMAL is NULL. Each INSERT is one transaction,
# and every session's share the logical logs (10 x 10 MB in the developer image, a long transaction from 70%): two
# sessions of 100,000 TPC-H partsupp rows each aborted with -458, so --rows defaults to 50,000.
import re
from planner import Col, Table

BASE = {0: "CHAR", 1: "SMALLINT", 2: "INTEGER", 3: "FLOAT", 4: "SMALLFLOAT", 5: "DECIMAL", 6: "SERIAL", 7: "DATE", 8: "MONEY",
        10: "DATETIME", 11: "BYTE", 12: "TEXT", 13: "VARCHAR", 14: "INTERVAL", 15: "NCHAR", 16: "NVARCHAR", 17: "INT8", 18: "SERIAL8",
        43: "LVARCHAR", 45: "BOOLEAN", 52: "BIGINT", 53: "BIGSERIAL"}
QUAL = {0: "YEAR", 2: "MONTH", 4: "DAY", 6: "HOUR", 8: "MINUTE", 10: "SECOND"}
P19 = "LPAD(CAST(x.n AS VARCHAR(20)), 19, '0')"
# days by DATE arithmetic (NUMTODSINTERVAL(n, 'DAY') beyond a few hundred days gives NULL), then seconds
TS = "EXTEND(MDY(1, 1, 1970) + MOD(TRUNC(x.n / 86400), 2932897), YEAR TO SECOND) + NUMTODSINTERVAL(MOD(x.n, 86400), 'SECOND')"
INTS = ("SMALLINT", "INTEGER", "SERIAL", "INT8", "SERIAL8", "BIGINT", "BIGSERIAL")


def qual(n):                                      # a DATETIME qualifier from its code
    return QUAL.get(n, f"FRACTION({n - 10})") if n <= 10 else f"FRACTION({n - 10})"


class Engine:
    name = "informix"
    rows = 50000

    def default_db(self, s): return s

    def catalog(self, cur, sch):
        cur.execute("SELECT COUNT(*) FROM systables WHERE tabname = 'lfc_tags'")
        side = {}
        if cur.fetchone()[0]:
            cur.execute("SELECT tbl, col, tag FROM lfc_tags")
            side = {(t.strip(), c.strip()): (g or "") for t, c, g in cur.fetchall()}
        cur.execute("SELECT tabid, tabname FROM systables WHERE tabtype = 'T' AND tabid >= 100 AND tabname <> 'lfc_tags' ORDER BY tabname")
        ids = {i: n.strip() for i, n in cur.fetchall()}
        tabs = {n: Table(n, [], [], side.get((n, "-"), "")) for n in ids.values()}
        cur.execute("SELECT extended_id, name FROM sysxtdtypes")
        xt = {i: (n or "").strip().upper() for i, n in cur.fetchall()}
        cur.execute("SELECT tabid, colname, colno, coltype, collength, extended_id FROM syscolumns WHERE tabid >= 100 ORDER BY tabid, colno")
        cols = {}
        for tid, cn, no, ct, ln, xid in cur.fetchall():
            if tid not in ids: continue
            b = ct & 0xFF
            ty, length, p, s = BASE.get(b) or xt.get(xid, f"TYPE{b}"), ln, None, None
            if ty in ("VARCHAR", "NVARCHAR"): length = ln % 256
            if ty in ("DECIMAL", "MONEY"): p, s = ln // 256, ln % 256
            if ty in ("DATETIME", "INTERVAL"): ty = f"{ty} {qual((ln % 256) // 16)} TO {qual(ln % 16)}"
            cols[(tid, no)] = Col(cn.strip(), no, ty, length=length, prec=p, scale=s)
        # constraints and their index columns: P (key), U (unique), R (foreign key, with its parent)
        parts = ", ".join(f"i.part{k}" for k in range(1, 17))
        cur.execute(f"""SELECT c.tabid, c.constrtype, c.constrid, {parts} FROM sysconstraints c JOIN sysindexes i ON i.idxname = c.idxname AND i.tabid = c.tabid
                        WHERE c.constrtype IN ('P', 'U', 'R') AND c.tabid >= 100""")
        cons = cur.fetchall()
        cur.execute("SELECT r.constrid, r.ptabid FROM sysreferences r")
        ref = dict(cur.fetchall())
        cur.execute(f"SELECT i.tabid, {parts} FROM sysindexes i WHERE i.idxtype = 'U' AND i.tabid >= 100")
        uq = {(r[0], abs(p)) for r in cur.fetchall() for p in r[1:] if p}
        pk, fk = {}, {}
        for tid, ty, cid, *ps in cons:
            ps = [abs(p) for p in ps if p]
            if tid not in ids: continue
            if ty == "P": pk[tid] = ps
            if ty in ("P", "U"): uq |= {(tid, p) for p in ps}
            if ty == "R" and ref.get(cid) in ids:
                tabs[ids[tid]].parents.add(ids[ref[cid]])
                if len(ps) == 1: fk[(tid, ps[0])] = ids[ref[cid]]
        for (tid, no), c in sorted(cols.items()):
            t = tabs[ids[tid]]
            c.pk, c.unique = no in pk.get(tid, []), (tid, no) in uq
            if c.ty in ("SERIAL", "SERIAL8", "BIGSERIAL") and not c.pk: continue      # the database numbers it
            c.fk = None if c.pk else fk.get((tid, no))
            c.tags = side.get((t.name, c.name), "")
            t.cols.append(c)
        for tid, ps in pk.items():
            tabs[ids[tid]].pk = [cols[(tid, p)].name for p in ps]
        return list(tabs.values())

    def q(self, s): return s                      # unquoted: Informix folds names to lower case
    def qby(self, s): return s
    def t(self, sch, tbl): return tbl             # in the connection's database

    def length(self, c, o):
        m = re.search(r"lfc:len=\{([0-9]+)\}", c.tags)
        return int(m.group(1)) if m else c.length

    def cast(self, c, L, o):
        ty = c.ty
        def st(l):                                # n first (and last from 38), n repeated between
            if l < 19: return f"RIGHT({P19}, {l})"
            if l < 38: return f"RPAD({P19}, {l}, {P19})"
            return f"RPAD({P19}, {l - 19}, {P19}) || {P19}"
        if ty in ("DECIMAL", "MONEY"):
            if c.scale == 255: return f"CAST(x.n AS {ty}({c.prec}))"           # floating-point DECIMAL(p)
            return f"CAST(MOD(x.n, 1{'0' * min(c.prec, 18)}){' * 0.' + '0' * (c.scale - 1) + '1' if c.scale > 0 else ''} AS {ty}({c.prec},{c.scale}))"
        if ty in ("CHAR", "VARCHAR", "LVARCHAR", "NCHAR", "NVARCHAR"): return st(L)
        m = re.fullmatch(r"DATETIME (\w+(?:\(\d\))?) TO (\w+(?:\(\d\))?)", ty)
        if m and m.group(1) == "YEAR": return f"EXTEND({TS}, YEAR TO {m.group(2)})"
        if m and m.group(1) == "HOUR": return f"EXTEND({TS}, HOUR TO {m.group(2)})"   # (an HOUR TO SECOND plus an interval: -1266)
        return CAST.get(ty)                       # BYTE, TEXT, BLOB, CLOB, INTERVAL, other DATETIMEs: not handled

    def to_int(self, c, col):
        if c is None: return None
        if c.ty in INTS: return col
        if c.ty == "DECIMAL" and c.scale == 0: return f"CAST({col} AS INT8)"
        if c.ty in ("CHAR", "VARCHAR", "LVARCHAR", "NCHAR", "NVARCHAR") and (c.length or 0) >= 19: return f"CAST(LEFT({col}, 19) AS INT8)"
        return None

    def probe(self, sch, t, hi):                  # MAX or MIN of the key column (an index probe), then decoded to n
        f = "MAX" if hi else "MIN"
        if t.has_n: return f"(SELECT {f}({t.pk_int}) FROM {t.name} WHERE {t.byc} = (SELECT {f}({t.byc}) FROM {t.name}))"
        k = next(c for c in t.cols if c.name == t.pk[0])
        return self.to_int(k, f"(SELECT {f}({k.name}) FROM {t.name})")

    def scr(self, v, a, b):
        m = f"MOD({v}, 2147483647)"
        return f"MOD(MOD({m} * MOD({m} + {a}, 2147483647), 2147483647) + {b}, 2147483647)"
    def scr1(self, v, a, b): return f"({self.scr(v, a, b)} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + MOD({s}, {hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN MOD(TRUNC((x.n - 1) / {block}), {k}) = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        if o.data == "random": raise SystemExit("Informix has no random function: use --data scramble or seq")
        have = "NVL(hi - lo + 1, 0)"
        need = (f"CASE WHEN {t.target} - {have} <= 0 THEN 0 WHEN {t.target} - {have} > {o.rows} THEN {o.rows} ELSE {t.target} - {have} END"
                if o.mode == "fill" else str(o.rows))
        nr = min(o.rows, t.target) if o.mode == "fill" else o.rows
        return (f"/* lfc level {t.level} */ INSERT INTO {t.name} ({', '.join(c.name for c in cols)}) "
                f"SELECT {', '.join(exprs)} FROM (SELECT CAST(b.base + g.i AS INT8) AS n FROM (SELECT NVL(hi, 0) AS base, {need} AS need "
                f"FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo FROM sysmaster:sysdual) m) b, "
                f"(SELECT CAST(LEVEL AS INT8) AS i FROM sysmaster:sysdual CONNECT BY LEVEL <= {nr}) g WHERE g.i <= b.need) x")

    def status(self, sch, tabs):
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)} FROM sysmaster:sysdual" for t in tabs[i:i + 200])
                for i in range(0, len(tabs), 200)]

    def prepare(self, cur, o): pass


CAST = {"SMALLINT": "MOD(x.n, 32767)", "INTEGER": "MOD(x.n, 2147483647)", "SERIAL": "MOD(x.n, 2147483647)",
        "INT8": "x.n", "SERIAL8": "x.n", "BIGINT": "x.n", "BIGSERIAL": "x.n",
        "FLOAT": "CAST(x.n AS FLOAT)", "SMALLFLOAT": "CAST(x.n AS SMALLFLOAT)",
        "DATE": "MDY(1, 1, 1970) + MOD(x.n, 2932897)",
        "BOOLEAN": "CASE WHEN MOD(x.n, 2) = 1 THEN 't'::BOOLEAN ELSE 'f'::BOOLEAN END"}
