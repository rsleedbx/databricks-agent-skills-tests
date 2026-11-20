# Oracle: the catalog (all_* views, and every tag from LFC_TAGS in the schema, which statschema's ddl.py fills), and
# the casts of ../../oracle/cast.sql. Col.ty is all_tab_cols.data_type (TIMESTAMP(6), INTERVAL DAY(2) TO SECOND(6), ...);
# Col.length is char_length. The schema is a user (upper case); connect to its PDB as a user that can insert into it.
# Rows come from CONNECT BY LEVEL on DUAL; /*+ APPEND */ makes a direct-path insert, committed by autocommit before the
# next statement reads the table.
import re
from planner import Col, Table, P

P19 = "LPAD(TO_CHAR(x.n), 19, '0')"
MD5 = "LOWER(RAWTOHEX(STANDARD_HASH(TO_CHAR(x.n), 'MD5')))"
H16 = "LPAD(TRIM(TO_CHAR(x.n, 'XXXXXXXXXXXXXXXX')), 16, '0')"
MD5U = "RAWTOHEX(STANDARD_HASH(TO_CHAR(x.n), 'MD5'))"
TS = "TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(MOD(FLOOR(x.n / 86400), 2932897), 'DAY') + NUMTODSINTERVAL(MOD(x.n, 86400), 'SECOND')"


def text(v): return v.read() if hasattr(v, "read") else (v or "")


class Engine:
    name = "oracle"
    rows = 1000000

    def schema(self, s): return s.upper()

    def catalog(self, cur, sch):
        cur.execute("SELECT COUNT(*) FROM all_tables WHERE owner = :o AND table_name = 'LFC_TAGS'", {'o': sch})
        side = {}
        if cur.fetchone()[0]:
            cur.execute(f'SELECT tbl, col, tag FROM "{sch}".LFC_TAGS')
            side = {(t, c): text(g) for t, c, g in cur.fetchall()}
        cur.execute("""SELECT table_name FROM all_tables WHERE owner = :o AND table_name <> 'LFC_TAGS' AND temporary = 'N' AND nested = 'NO'
                       AND secondary = 'N' AND dropped = 'NO' ORDER BY table_name""", {"o": sch})
        tabs = {r[0]: Table(r[0], [], [], side.get((r[0], "-"), "")) for r in cur.fetchall()}
        cur.execute("""SELECT cc.table_name, cc.column_name FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
                       WHERE k.owner = :o AND k.constraint_type = 'P' ORDER BY cc.table_name, cc.position""", {"o": sch})
        for t, c in cur.fetchall():
            if t in tabs: tabs[t].pk.append(c)
        cur.execute("""SELECT DISTINCT ic.table_name, ic.column_name FROM all_indexes i JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name
                       WHERE i.table_owner = :o AND i.uniqueness = 'UNIQUE'""", {"o": sch})
        uq = set(cur.fetchall())
        cur.execute("""SELECT k.table_name, MAX(cc.column_name), MAX(p.table_name), COUNT(*)
                       FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
                       JOIN all_constraints p ON p.owner = k.r_owner AND p.constraint_name = k.r_constraint_name AND p.constraint_type = 'P'
                       WHERE k.owner = :o AND k.constraint_type = 'R' AND p.owner = :o GROUP BY k.table_name, k.constraint_name""", {"o": sch})
        fk = {}
        for t, c, p, n in cur.fetchall():
            if t in tabs: tabs[t].parents.add(p)
            if n == 1: fk[(t, c)] = p
        # insertable columns: not hidden, not virtual, and not a non-key identity (the database numbers it)
        cur.execute("""SELECT table_name, column_name, column_id, data_type, data_precision, data_scale, char_length, identity_column FROM all_tab_cols
                       WHERE owner = :o AND hidden_column = 'NO' AND virtual_column = 'NO' ORDER BY table_name, column_id""", {"o": sch})
        for t, c, pos, ty, p, s, ln, ident in cur.fetchall():
            if t not in tabs: continue
            pk = c in tabs[t].pk
            if ident == "YES" and not pk: continue
            tabs[t].cols.append(Col(c, pos, ty, length=ln, prec=p, scale=s, pk=pk, unique=(t, c) in uq, fk=None if pk else fk.get((t, c)),
                                    tags=side.get((t, c), "")))
        return list(tabs.values())

    def q(self, s): return f'"{s}"'
    def qby(self, s): return s                    # lfc:by holds the name as Oracle reads it (unquoted: upper case)
    def t(self, sch, tbl): return f'"{sch}"."{tbl}"'

    def length(self, c, o):
        m = re.search(r"lfc:len=\{([0-9]+)\}", c.tags)
        return int(m.group(1)) if m else None

    def cast(self, c, L, o):
        ty, p, s = c.ty, c.prec, c.scale
        def st(l):                                # 19-digit n first (and last from 38), MD5 between
            if l < 19: return f"SUBSTR({P19}, -{l})"
            if l < 38: return f"SUBSTR({P19} || {MD5}, 1, {l})"
            return f"{P19} || RPAD({MD5}, {l - 38}, {MD5}) || {P19}"
        def bn(l):                                # n's 8 bytes first (and last from 16), MD5 between
            if l < 16: return f"HEXTORAW(SUBSTR({H16} || {MD5U}, 1, {2 * l}))"
            return f"HEXTORAW({H16} || RPAD({MD5U}, {2 * l - 32}, {MD5U}) || {H16})"
        if ty == "NUMBER":
            if p is None: return "x.n"            # NUMBER, INTEGER
            if (s or 0) <= 0: return f"MOD(x.n, 1{'0' * min(p, 18)})"
            return f"MOD(x.n, 1{'0' * min(p, 18)}) / 1{'0' * s}"   # exact: NUMBER division
        if ty in ("VARCHAR2", "CHAR"): return st(L or c.length)
        if ty in ("NVARCHAR2", "NCHAR"): return f"TO_NCHAR({st(L or c.length)})"
        if ty == "CLOB": return f"TO_CLOB({st(L or max(o.lob_len, 38))})"
        if ty == "NCLOB": return f"TO_NCLOB({st(L or max(o.lob_len, 38))})"
        if ty == "RAW": return bn(L or c.length)
        if ty == "BLOB": return f"TO_BLOB({bn(L or max(o.lob_len, 16))})"
        if re.fullmatch(r"TIMESTAMP\(\d\) WITH LOCAL TIME ZONE", ty): return f"CAST(FROM_TZ({TS}, '+00:00') AS TIMESTAMP WITH LOCAL TIME ZONE)"
        if re.fullmatch(r"TIMESTAMP\(\d\) WITH TIME ZONE", ty): return f"FROM_TZ({TS}, '+00:00')"
        if re.fullmatch(r"TIMESTAMP\(\d\)", ty): return TS
        if re.fullmatch(r"INTERVAL YEAR\(\d\) TO MONTH", ty): return f"NUMTOYMINTERVAL(MOD(x.n, {12 * (10 ** (p if p is not None else 2) - 1)}), 'MONTH')"
        if re.fullmatch(r"INTERVAL DAY\(\d\) TO SECOND\(\d\)", ty): return f"NUMTODSINTERVAL(MOD(x.n, {86400 * (10 ** (p if p is not None else 2) - 1)}), 'SECOND')"
        return CAST.get(ty)                       # ROWID, BFILE, LONG, XMLTYPE, SDO_GEOMETRY, VECTOR, ...: not handled

    def to_int(self, c, col):
        if c is None: return None
        if c.ty == "NUMBER" and (c.scale or 0) == 0: return col
        if c.ty in ("VARCHAR2", "CHAR") and (c.length or 0) >= 19: return f"TO_NUMBER(SUBSTR({col}, 1, 19))"
        return None

    def probe(self, sch, t, hi):
        d = " DESC" if hi else ""
        if not t.has_n: return f"(SELECT {t.pk_int} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} FETCH FIRST 1 ROW ONLY)"
        return (f"(SELECT {'MAX(' if hi else 'MIN('}{t.pk_int}) FROM {self.t(sch, t.name)} WHERE {t.byc} = (SELECT "
                f"{t.byc} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} FETCH FIRST 1 ROW ONLY))")

    def scr(self, v, a, b):
        m = f"MOD({v}, 2147483647)"
        return f"MOD(MOD({m} * MOD({m} + {a}, 2147483647), 2147483647) + {b}, 2147483647)"
    def scr1(self, v, a, b): return f"({self.scr(v, a, b)} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + MOD({s}, {hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN MOD(TRUNC((x.n - 1) / {block}), {k}) = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        need = f"LEAST({o.rows}, GREATEST({t.target} - COALESCE(hi - lo + 1, 0), 0))" if o.mode == "fill" else str(o.rows)
        nr = min(o.rows, t.target) if o.mode == "fill" else o.rows                  # rows the generator makes
        order = {"asc": " ORDER BY x.n", "desc": " ORDER BY x.n DESC"}.get(o.order, " ORDER BY ORA_HASH(x.n)")
        r = ", TRUNC(DBMS_RANDOM.VALUE(0, 2147483647)) AS r" if o.data == "random" else ""
        return (f"/* lfc level {t.level} */ INSERT /*+ APPEND */ INTO {self.t(sch, t.name)} ({', '.join(self.q(c.name) for c in cols)}) "
                f"SELECT {', '.join(exprs)} FROM (SELECT b.base + g.i AS n{r} FROM (SELECT COALESCE(hi, 0) AS base, {need} AS need "
                f"FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo FROM DUAL) m) b, "
                f"(SELECT LEVEL AS i FROM DUAL CONNECT BY LEVEL <= {nr}) g WHERE g.i <= b.need) x{order}")

    def status(self, sch, tabs):
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)} FROM DUAL" for t in tabs[i:i + 500])
                for i in range(0, len(tabs), 500)]

    def prepare(self, cur, o): pass


CAST = {"FLOAT": "x.n", "BINARY_FLOAT": "TO_BINARY_FLOAT(x.n)", "BINARY_DOUBLE": "TO_BINARY_DOUBLE(x.n)",
        "DATE": "DATE '1970-01-01' + MOD(x.n, 2932897)", "BOOLEAN": "CASE MOD(x.n, 2) WHEN 1 THEN TRUE ELSE FALSE END",
        "JSON": "JSON_OBJECT('n' VALUE x.n RETURNING JSON)"}
