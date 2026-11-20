# MySQL: the catalog (information_schema, and lfc_seed.tags for tags longer than a comment holds), and the casts of
# ../../mysql/cast.sql. Col.ty is information_schema.columns.data_type; Col.x holds column_type and srs_id.
# Rows come from the seed table lfc_seed.seed (i = 1 .. 2^20, r a random permutation of i), which prepare() makes
# when it's missing, so a statement adds at most 1,048,576 rows.
import math
from planner import Col, Table, P

SEED = 1 << 20


class Engine:
    name = "mysql"
    rows = 100000

    def catalog(self, cur, sch):
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'lfc_seed' AND table_name = 'tags'")
        side = {}
        if cur.fetchone()[0]:
            cur.execute("SELECT tbl, col, tag FROM lfc_seed.tags WHERE db = %s", (sch,))
            side = {(t, c): g or "" for t, c, g in cur.fetchall()}
        cur.execute("SELECT table_name, table_comment FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name", (sch,))
        tabs = {n: Table(n, [], [], f"{cm} {side.get((n, ''), '')}") for n, cm in cur.fetchall()}
        cur.execute("SELECT table_name, column_name FROM information_schema.statistics WHERE table_schema = %s AND index_name = 'PRIMARY' ORDER BY table_name, seq_in_index", (sch,))
        for t, c in cur.fetchall(): tabs[t].pk.append(c)
        cur.execute("SELECT DISTINCT table_name, column_name FROM information_schema.statistics WHERE table_schema = %s AND non_unique = 0", (sch,))
        uq = set(cur.fetchall())
        cur.execute("""SELECT k.table_name, k.column_name, k.referenced_table_name, (SELECT COUNT(*) FROM information_schema.key_column_usage k2
                         WHERE k2.constraint_schema = k.constraint_schema AND k2.constraint_name = k.constraint_name AND k2.table_name = k.table_name)
                       FROM information_schema.key_column_usage k WHERE k.table_schema = %s AND k.referenced_table_name IS NOT NULL""", (sch,))
        fk = {}
        for t, c, p, n in cur.fetchall():
            if t in tabs: tabs[t].parents.add(p)
            if n == 1: fk[(t, c)] = p
        # insertable columns: not generated, and not a non-key AUTO_INCREMENT column (the database numbers it)
        # srs_id is MySQL 8's column (spatial types); MariaDB has none, so it reads as 0 there
        cur.execute("SELECT VERSION()")
        srs = "0" if "mariadb" in str(cur.fetchone()[0]).lower() else "srs_id"
        cur.execute(f"""SELECT table_name, column_name, ordinal_position, data_type, column_type, character_maximum_length, numeric_precision,
                              numeric_scale, column_key, extra, column_comment, {srs}
                       FROM information_schema.columns WHERE table_schema = %s AND COALESCE(generation_expression, '') = ''
                       ORDER BY table_name, ordinal_position""", (sch,))
        for t, c, pos, dt, ct, ml, p, s, key, extra, cm, srs in cur.fetchall():
            if t not in tabs or ("auto_increment" in extra and key != "PRI"): continue
            pk = c in tabs[t].pk
            tabs[t].cols.append(Col(c, pos, dt, length=ml, prec=p, scale=s, pk=pk, unique=(t, c) in uq, fk=None if pk else fk.get((t, c)),
                                    tags=f"{cm} {side.get((t, c), '')}", x={"ct": ct, "srs": srs or 0}))
        return list(tabs.values())

    def q(self, s): return "`" + s.replace("`", "``") + "`"
    def qby(self, s): return self.q(s)
    def t(self, sch, tbl): return f"{self.q(sch)}.{self.q(tbl)}"

    def length(self, c, o):                       # the declared length, or lob_len (at least 38) for text and blob types
        import re
        m = re.search(r"lfc:len=\{([0-9]+)\}", c.tags)
        if m: return int(m.group(1))
        if c.ty in ("char", "varchar", "binary", "varbinary"): return c.length
        return min(max(o.lob_len, 38), c.length) if c.length is not None else None

    def cast(self, c, L, o):
        ty, ct, uns = c.ty, c.x["ct"], "unsigned" in c.x["ct"]
        n = ct.count(",") + 1                     # values of an enum or set
        g = lambda f: f"ST_GeomFromText(CONCAT({f}), {c.x['srs']})"
        if ty in INTS: return f"x.n % {INTS[ty][uns]}" if INTS[ty][uns] else "x.n"
        if ty == "decimal":
            return (f"CAST((x.n % 1{'0' * min(c.prec, 18)}){' * 0.' + '0' * (c.scale - 1) + '1' if c.scale > 0 else ''}"
                    f" AS DECIMAL({c.prec},{c.scale}))")
        if ty == "bit": return "x.n" if c.prec == 64 else f"x.n % {2 ** c.prec}"
        if ty in ("char", "varchar", "tinytext", "text", "mediumtext", "longtext"):
            if L < 19: return f"RIGHT(LPAD(x.n, 19, '0'), {L})"
            if L < 38: return f"LEFT(CONCAT(LPAD(x.n, 19, '0'), MD5(x.n)), {L})"
            return f"CONCAT(LPAD(x.n, 19, '0'), LEFT(REPEAT(MD5(x.n), {max(math.ceil((L - 38) / 32), 1)}), {L - 38}), LPAD(x.n, 19, '0'))"
        if ty in ("binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"):
            if L < 16: return f"LEFT(CONCAT(UNHEX(LPAD(HEX(x.n), 16, '0')), UNHEX(MD5(x.n))), {L})"
            return (f"CONCAT(UNHEX(LPAD(HEX(x.n), 16, '0')), LEFT(REPEAT(UNHEX(MD5(x.n)), {max(math.ceil((L - 16) / 16), 1)}), {L - 16}), "
                    f"UNHEX(LPAD(HEX(x.n), 16, '0')))")
        if ty == "enum": return f"1 + x.n % {n}"
        if ty == "set": return f"x.n % {2 ** n}"
        if ty in ("point", "geometry"): return g("'POINT(', x.n % 90, ' ', x.n % 89, ')'")
        if ty == "linestring": return g("'LINESTRING(0 0, ', 1 + x.n % 89, ' ', 1 + x.n % 89, ')'")
        if ty == "polygon": return g("'POLYGON((0 0, ', 1 + x.n % 89, ' 0, ', 1 + x.n % 89, ' ', 1 + x.n % 89, ', 0 ', 1 + x.n % 89, ', 0 0))'")
        if ty == "multipoint": return g("'MULTIPOINT(0 0, ', x.n % 90, ' ', x.n % 89, ')'")
        if ty == "multilinestring": return g("'MULTILINESTRING((0 0, ', 1 + x.n % 89, ' ', 1 + x.n % 89, '))'")
        if ty == "multipolygon": return g("'MULTIPOLYGON(((0 0, ', 1 + x.n % 89, ' 0, ', 1 + x.n % 89, ' ', 1 + x.n % 89, ', 0 ', 1 + x.n % 89, ', 0 0)))'")
        if ty == "geomcollection": return g("'GEOMETRYCOLLECTION(POINT(', x.n % 90, ' ', x.n % 89, '))'")
        return CAST.get(ty)

    def to_int(self, c, col):
        if c is None: return None
        if c.ty in INTS: return col
        if c.ty in ("char", "varchar") and (c.length or 0) >= 19: return f"CAST(LEFT({col}, 19) AS UNSIGNED)"
        return None

    def probe(self, sch, t, hi):
        d = " DESC" if hi else ""
        if not t.has_n: return f"(SELECT {t.pk_int} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} LIMIT 1)"
        return (f"(SELECT {'MAX' if hi else 'MIN'}({t.pk_int}) FROM {self.t(sch, t.name)} WHERE {t.byc} = "
                f"(SELECT {t.byc} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} LIMIT 1))")

    def scr(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P})"
    def scr1(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + {s} % ({hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN (x.n - 1) DIV {block} % {k} = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        need = f"LEAST({o.rows}, GREATEST({t.target} - COALESCE(hi - lo + 1, 0), 0))" if o.mode == "fill" else str(o.rows)
        g = "r" if o.order == "random" else "i"
        order = {"asc": " ORDER BY g.i", "desc": " ORDER BY g.i DESC"}.get(o.order, "")
        r = ", FLOOR(RAND() * 2147483647) AS r" if o.data == "random" else ""
        return (f"/* lfc level {t.level} */ INSERT INTO {self.t(sch, t.name)} ({', '.join(self.q(c.name) for c in cols)}) "
                f"SELECT /*+ NO_MERGE(x) */ {', '.join(exprs)} FROM (SELECT b.base + g.{g} AS n{r} FROM "
                f"(SELECT COALESCE(hi, 0) AS base, {need} AS need FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo) m) b, "
                f"lfc_seed.seed g WHERE g.{g} <= b.need{order}) x")

    def status(self, sch, tabs):
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)}" for t in tabs[i:i + 500])
                for i in range(0, len(tabs), 500)]

    def prepare(self, cur, o):                    # the seed table, 1 .. 2^20 by doubling, with a random permutation r
        if o.rows > SEED: raise SystemExit(f"--rows is at most {SEED} on MySQL (the seed table's size)")
        cur.execute("CREATE DATABASE IF NOT EXISTS lfc_seed")
        cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'lfc_seed' AND table_name = 'seed' AND column_name = 'r'")
        if cur.fetchone()[0]:
            cur.execute("SELECT MAX(i) FROM lfc_seed.seed")
            if (cur.fetchone()[0] or 0) >= SEED: return
        for s in ["DROP TABLE IF EXISTS lfc_seed.seed_i", "DROP TABLE IF EXISTS lfc_seed.seed",
                  "CREATE TABLE lfc_seed.seed_i (i bigint PRIMARY KEY)",
                  "INSERT INTO lfc_seed.seed_i (i) VALUES " + ",".join(f"({i})" for i in range(1, 1025))] + \
                 ["INSERT INTO lfc_seed.seed_i (i) SELECT s.i + b.m FROM lfc_seed.seed_i s, (SELECT MAX(i) AS m FROM lfc_seed.seed_i) b"] * 10 + \
                 ["CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY, r bigint NOT NULL, KEY (r))",
                  "INSERT INTO lfc_seed.seed SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed_i", "DROP TABLE lfc_seed.seed_i"]:
            cur.execute(s)


INTS = {"tinyint": (127, 255), "smallint": (32767, 65535), "mediumint": (8388607, 16777215), "int": (2147483647, 4294967295), "bigint": (None, None)}
CAST = {
    "float": "CAST(x.n AS FLOAT)", "double": "CAST(x.n AS DOUBLE)",
    "date": "DATE_ADD('1970-01-01', INTERVAL x.n % 2932897 DAY)", "datetime": "DATE_ADD('1970-01-01 00:00:00', INTERVAL x.n % 253402300800 SECOND)",
    "timestamp": "FROM_UNIXTIME(1 + x.n % 2147483646)", "time": "SEC_TO_TIME(x.n % 86400)", "year": "1901 + x.n % 255",
    "json": "JSON_OBJECT('n', x.n)",
}
