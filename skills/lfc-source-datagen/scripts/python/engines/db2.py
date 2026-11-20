# Db2 for LUW (11.5): the catalog (SYSCAT views, and every tag from LFC_TAGS in the schema, which statschema's ddl.py
# --dialect db2 fills), and a cast per type in the same shape as the other engines: a 19-digit zero-padded n first (and
# last from 38 characters) with MD5 filler (HASH(..., 0)) between, n's 8 bytes first (and last from 16) in binaries, days
# or seconds from 1970 for dates and times. Col.ty is SYSCAT.COLUMNS.TYPENAME; Col.length its LENGTH (bytes; the
# precision for DECIMAL). The schema is upper case, as Db2 stores unquoted names.
# Rows come from a recursive common table expression (1M rows in 0.5 s), so no seed table is needed; random key order
# sorts by a hash of n (RAND() isn't allowed there). Each INSERT is one transaction: the default log (LOGFILSIZ 1024 x
# (16 + 22) files of 4 KB pages, 150 MB) holds about 100,000 rows of a wide table, the default --rows (512,000 BIGINT
# rows with their index filled it); raise LOGFILSIZ / LOGSECOND for more per statement.
import math, re
from planner import Col, Table, P

P19 = "LPAD(CAST(x.n AS VARCHAR(20)), 19, '0')"
MD5 = "LOWER(HEX(HASH(CAST(x.n AS VARCHAR(20)), 0)))"      # 32 hex digits
H16 = "LPAD(TO_HEX(x.n), 16, '0')"                          # n's 8 bytes, big-endian (HEX of a BIGINT is little-endian)
MD5H = "HEX(HASH(CAST(x.n AS VARCHAR(20)), 0))"


class Engine:
    name = "db2"
    rows = 100000

    def schema(self, s): return s.upper()

    def catalog(self, cur, sch):
        cur.execute("SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = 'LFC_TAGS'", (sch,))
        side = {}
        if cur.fetchone()[0]:
            cur.execute(f'SELECT TBL, COL, TAG FROM "{sch}".LFC_TAGS')
            side = {(t, c): g or "" for t, c, g in cur.fetchall()}
        cur.execute("SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TYPE = 'T' AND TABNAME <> 'LFC_TAGS' ORDER BY TABNAME", (sch,))
        tabs = {r[0]: Table(r[0], [], [], side.get((r[0], "-"), "")) for r in cur.fetchall()}
        cur.execute("""SELECT DISTINCT i.TABNAME, u.COLNAME FROM SYSCAT.INDEXES i JOIN SYSCAT.INDEXCOLUSE u ON u.INDSCHEMA = i.INDSCHEMA AND u.INDNAME = i.INDNAME
                       WHERE i.TABSCHEMA = ? AND i.UNIQUERULE IN ('U', 'P')""", (sch,))
        uq = set(cur.fetchall())
        cur.execute("""SELECT r.TABNAME, k.COLNAME, r.REFTABNAME, r.COLCOUNT FROM SYSCAT.REFERENCES r
                       JOIN SYSCAT.KEYCOLUSE k ON k.TABSCHEMA = r.TABSCHEMA AND k.TABNAME = r.TABNAME AND k.CONSTNAME = r.CONSTNAME
                       WHERE r.TABSCHEMA = ? AND r.REFTABSCHEMA = ?""", (sch, sch))
        fk = {}
        for t, c, p, n in cur.fetchall():
            if t in tabs: tabs[t].parents.add(p)
            if n == 1: fk[(t, c)] = p
        # insertable columns: not GENERATED ALWAYS (identity or expression), and not a non-key identity
        cur.execute("""SELECT TABNAME, COLNAME, COLNO, TYPENAME, LENGTH, SCALE, IDENTITY, GENERATED, KEYSEQ FROM SYSCAT.COLUMNS
                       WHERE TABSCHEMA = ? ORDER BY TABNAME, COLNO""", (sch,))
        rows = cur.fetchall()
        for t, c, pos, ty, ln, s, ident, gen, kseq in sorted(rows, key=lambda r: (r[0], r[8] or 0)):
            if t in tabs and kseq: tabs[t].pk.append(c)
        for t, c, pos, ty, ln, s, ident, gen, kseq in rows:
            if t not in tabs or gen == "A" or (ident == "Y" and not kseq): continue
            tabs[t].cols.append(Col(c, pos, ty.strip(), length=ln, prec=ln if ty.strip() == "DECIMAL" else None, scale=s, pk=bool(kseq),
                                    unique=(t, c) in uq, fk=None if kseq else fk.get((t, c)), tags=side.get((t, c), "")))
        return list(tabs.values())

    def q(self, s): return f'"{s}"'
    def qby(self, s): return s                    # lfc:by holds the name as Db2 reads it (unquoted: upper case)
    def t(self, sch, tbl): return f'"{sch}"."{tbl}"'

    def length(self, c, o):
        m = re.search(r"lfc:len=\{([0-9]+)\}", c.tags)
        if m: return int(m.group(1))
        if c.ty in ("CLOB", "BLOB", "DBCLOB"): return max(o.lob_len, 38)
        return c.length

    def cast(self, c, L, o):
        ty = c.ty
        def st(l):
            if l < 19: return f"RIGHT({P19}, {l})"
            if l < 38: return f"LEFT({P19} || {MD5}, {l})"
            return f"{P19} || LEFT(REPEAT({MD5}, {max(math.ceil((l - 38) / 32), 1)}), {l - 38}) || {P19}"
        def bn(l):
            if l < 16: return f"HEXTORAW(LEFT({H16} || {MD5H}, {2 * l}))"
            return f"HEXTORAW({H16} || LEFT(REPEAT({MD5H}, {max(math.ceil((l - 16) / 16), 1)}), {2 * l - 32}) || {H16})"
        if ty == "DECIMAL":
            return f"CAST(MOD(x.n, 1{'0' * min(c.prec, 18)}){' * 0.' + '0' * (c.scale - 1) + '1' if c.scale > 0 else ''} AS DECIMAL({c.prec},{c.scale}))"
        if ty in ("CHARACTER", "VARCHAR"): return st(L)
        if ty == "CLOB": return f"CAST({st(L)} AS CLOB({max(L, 1)}))"
        if ty in ("BINARY", "VARBINARY"): return bn(L)
        if ty == "BLOB": return f"CAST({bn(L)} AS BLOB({max(L, 1)}))"
        return CAST.get(ty)                       # GRAPHIC, VARGRAPHIC, DBCLOB, XML, user types: not handled

    def to_int(self, c, col):
        if c is None: return None
        if c.ty in ("SMALLINT", "INTEGER", "BIGINT"): return col
        if c.ty == "DECIMAL" and c.scale == 0: return f"CAST({col} AS BIGINT)"
        if c.ty in ("CHARACTER", "VARCHAR") and (c.length or 0) >= 19: return f"CAST(LEFT({col}, 19) AS BIGINT)"
        return None

    def probe(self, sch, t, hi):
        d = " DESC" if hi else ""
        if not t.has_n: return f"(SELECT {t.pk_int} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} FETCH FIRST 1 ROW ONLY)"
        return (f"(SELECT {'MAX' if hi else 'MIN'}({t.pk_int}) FROM {self.t(sch, t.name)} WHERE {t.byc} = "
                f"(SELECT {t.byc} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} FETCH FIRST 1 ROW ONLY))")

    def scr(self, v, a, b):
        m = f"MOD({v}, 2147483647)"
        return f"MOD(MOD({m} * MOD({m} + {a}, 2147483647), 2147483647) + {b}, 2147483647)"
    def scr1(self, v, a, b): return f"({self.scr(v, a, b)} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + MOD({s}, {hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN MOD((x.n - 1) / {block}, {k}) = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        need = f"LEAST({o.rows}, GREATEST({t.target} - COALESCE(hi - lo + 1, 0), 0))" if o.mode == "fill" else str(o.rows)
        nr = min(o.rows, t.target) if o.mode == "fill" else o.rows                  # rows the generator makes
        order = {"asc": " ORDER BY x.n", "desc": " ORDER BY x.n DESC"}.get(o.order, " ORDER BY MOD(x.n * 1103515245, 2147483647)")
        r = ", BIGINT(RAND() * 2147483647) AS r" if o.data == "random" else ""
        return (f"/* lfc level {t.level} */ INSERT INTO {self.t(sch, t.name)} ({', '.join(self.q(c.name) for c in cols)}) "
                f"WITH g(i) AS (SELECT CAST(1 AS BIGINT) FROM SYSIBM.SYSDUMMY1 UNION ALL SELECT i + 1 FROM g WHERE i < {nr}) "
                f"SELECT {', '.join(exprs)} FROM (SELECT b.base + g.i AS n{r} FROM (SELECT COALESCE(hi, 0) AS base, {need} AS need "
                f"FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo FROM SYSIBM.SYSDUMMY1) m) b, "
                f"g WHERE g.i <= b.need) x{order}")

    def status(self, sch, tabs):
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)} FROM SYSIBM.SYSDUMMY1" for t in tabs[i:i + 500])
                for i in range(0, len(tabs), 500)]

    def prepare(self, cur, o): pass

CAST = {                                          # wrapped to the type's positive range
    "SMALLINT": "MOD(x.n, 32767)", "INTEGER": "MOD(x.n, 2147483647)", "BIGINT": "x.n",
    "REAL": "CAST(x.n AS REAL)", "DOUBLE": "CAST(x.n AS DOUBLE)", "DECFLOAT": "CAST(x.n AS DECFLOAT)",
    "DATE": "DATE('1970-01-01') + MOD(x.n, 2932897) DAYS",
    "TIMESTAMP": "TIMESTAMP('1970-01-01-00.00.00') + MOD(x.n / 86400, 2932897) DAYS + MOD(x.n, 86400) SECONDS",
    "TIME": "TIME('00:00:00') + MOD(x.n, 86400) SECONDS",
    "BOOLEAN": "CASE WHEN MOD(x.n, 2) = 1 THEN TRUE ELSE FALSE END",
}
