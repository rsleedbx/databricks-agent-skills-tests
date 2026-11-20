# SQL Server 2022 (compatibility level 160, for GENERATE_SERIES): the catalog (sys views, and every tag from
# lfc_seed.tags, which statschema's ddl.py fills), and the casts of ../../sqlserver/cast.sql. Col.ty is the type's name
# (a CLR type's own name: geometry, geography, hierarchyid); Col.length is in characters (-1 for max).
# Connect to the schema's database (--database); identity keys are inserted with SET IDENTITY_INSERT in the same batch.
import math, re
from planner import Col, Table, P

S19 = "RIGHT(REPLICATE('0', 19) + CAST(x.n AS varchar(20)), 19)"              # n, 19 digits zero-padded
MD5 = "LOWER(CONVERT(varchar(32), HASHBYTES('MD5', CAST(x.n AS varchar(20))), 2))"
N8 = "CAST(x.n AS binary(8))"                                                  # n's 8 bytes


class Engine:
    name = "sqlserver"
    rows = 1000000

    def catalog(self, cur, sch):
        cur.execute("SELECT OBJECT_ID('lfc_seed.tags')")
        side = {}
        if cur.fetchone()[0] is not None:
            cur.execute("SELECT tbl, col, tag FROM lfc_seed.tags WHERE sch = ?", (sch,))
            side = {(t, c): g or "" for t, c, g in cur.fetchall()}
        cur.execute("SELECT t.name FROM sys.tables t WHERE t.schema_id = SCHEMA_ID(?) ORDER BY t.name", (sch,))
        tabs = {r[0]: Table(r[0], [], [], side.get((r[0], ""), "")) for r in cur.fetchall()}
        cur.execute("""SELECT OBJECT_NAME(i.object_id), c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                       JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                       WHERE i.is_primary_key = 1 AND OBJECT_SCHEMA_NAME(i.object_id) = ? ORDER BY 1, ic.key_ordinal""", (sch,))
        for t, c in cur.fetchall():
            if t in tabs: tabs[t].pk.append(c)
        cur.execute("""SELECT DISTINCT OBJECT_NAME(i.object_id), c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                       JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                       WHERE i.is_unique = 1 AND ic.key_ordinal > 0 AND OBJECT_SCHEMA_NAME(i.object_id) = ?""", (sch,))
        uq = {(t, c) for t, c in cur.fetchall()}
        cur.execute("""SELECT OBJECT_NAME(f.parent_object_id), COL_NAME(fc.parent_object_id, fc.parent_column_id), OBJECT_NAME(f.referenced_object_id),
                              (SELECT COUNT(*) FROM sys.foreign_key_columns x WHERE x.constraint_object_id = f.object_id)
                       FROM sys.foreign_keys f JOIN sys.foreign_key_columns fc ON fc.constraint_object_id = f.object_id
                       WHERE OBJECT_SCHEMA_NAME(f.parent_object_id) = ?""", (sch,))
        fk = {}
        for t, c, p, n in cur.fetchall():
            if t in tabs: tabs[t].parents.add(p)
            if n == 1: fk[(t, c)] = p
        # insertable columns: not computed, not rowversion, and not a non-key identity (the database numbers it)
        cur.execute("""SELECT OBJECT_NAME(c.object_id), c.name, c.column_id,
                              CASE WHEN c.system_type_id = 240 THEN TYPE_NAME(c.user_type_id) ELSE TYPE_NAME(c.system_type_id) END,
                              CASE WHEN TYPE_NAME(c.system_type_id) IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN c.max_length / 2 ELSE c.max_length END,
                              c.precision, c.scale, c.is_identity
                       FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
                       WHERE t.schema_id = SCHEMA_ID(?) AND c.is_computed = 0 AND TYPE_NAME(c.system_type_id) <> 'timestamp'
                       ORDER BY 1, c.column_id""", (sch,))
        for t, c, pos, ty, ln, p, s, ident in cur.fetchall():
            pk = c in tabs[t].pk
            if ident and not pk: continue
            tabs[t].cols.append(Col(c, pos, ty, length=ln, prec=p, scale=s, pk=pk, unique=(t, c) in uq, ident=bool(ident and pk),
                                    fk=None if pk else fk.get((t, c)), tags=side.get((t, c), "")))
        return list(tabs.values())

    def q(self, s): return "[" + s.replace("]", "]]") + "]"
    def qby(self, s): return s if s.startswith("[") else self.q(s)
    def t(self, sch, tbl): return f"{self.q(sch)}.{self.q(tbl)}"
    def schema(self, s): return s

    def length(self, c, o):                       # the declared length, or lob_len (at least 38) for max and text types
        if c.ty not in ("char", "varchar", "nchar", "nvarchar", "text", "ntext", "binary", "varbinary", "image"): return None
        m = re.search(r"lfc:len=\{([0-9]+)\}", c.tags)
        if m: return int(m.group(1))
        return max(o.lob_len, 38) if c.length == -1 or c.ty in ("text", "ntext") else c.length

    def cast(self, c, L, o):
        ty = c.ty
        if ty in ("decimal", "numeric"):
            return (f"CAST((x.n % 1{'0' * min(c.prec, 18)}){' * 0.' + '0' * (c.scale - 1) + '1' if c.scale > 0 else ''}"
                    f" AS {ty}({c.prec},{c.scale}))")
        if ty in ("char", "varchar", "nchar", "nvarchar", "text", "ntext"):
            if L < 19: return f"RIGHT({S19}, {L})"
            if L < 38: return f"LEFT({S19} + {MD5}, {L})"
            return f"{S19} + LEFT(REPLICATE(CAST({MD5} AS varchar(max)), {max(math.ceil((L - 38) / 32), 1)}), {L - 38}) + {S19}"
        if ty in ("binary", "varbinary", "image"):
            if L < 16: return f"SUBSTRING({N8} + HASHBYTES('MD5', CAST(x.n AS varchar(20))), 1, {L})"
            return f"{N8} + SUBSTRING(CAST(REPLICATE(CAST({MD5} AS varchar(max)), {max(math.ceil((L - 16) / 32), 1)}) AS varbinary(max)), 1, {L - 16}) + {N8}"
        return CAST.get(ty)

    def to_int(self, c, col):
        if c is None: return None
        if c.ty in ("tinyint", "smallint", "int", "bigint") or (c.ty in ("decimal", "numeric") and c.scale == 0): return f"CAST({col} AS bigint)"
        if c.ty in ("char", "varchar", "nchar", "nvarchar") and (c.length or 0) >= 19: return f"CAST(LEFT({col}, 19) AS bigint)"
        return None

    def probe(self, sch, t, hi):
        d = " DESC" if hi else ""
        if not t.has_n: return f"(SELECT TOP (1) {t.pk_int} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d})"
        return (f"(SELECT {'MAX' if hi else 'MIN'}({t.pk_int}) FROM {self.t(sch, t.name)} WHERE {t.byc} = "
                f"(SELECT TOP (1) {t.byc} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d}))")

    def scr(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P})"
    def scr1(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + {s} % ({hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN (x.n - 1) / {block} % {k} = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        tt = self.t(sch, t.name)
        need = f"LEAST({o.rows}, GREATEST({t.target} - COALESCE(hi - lo + 1, 0), 0))" if o.mode == "fill" else str(o.rows)
        on = f"SET IDENTITY_INSERT {tt} ON; " if any(c.ident for c in cols) else ""
        off = f"; SET IDENTITY_INSERT {tt} OFF" if on else ""
        order = {"asc": " ORDER BY x.n", "desc": " ORDER BY x.n DESC"}.get(o.order, "")   # random: SQL Server sorts by the clustered key anyway
        r = ", CAST(ABS(CHECKSUM(NEWID())) AS bigint) AS r" if o.data == "random" else ""
        return (f"/* lfc level {t.level} */ {on}INSERT INTO {tt} WITH (TABLOCK) ({', '.join(self.q(c.name) for c in cols)}) "
                f"SELECT {', '.join(exprs)} FROM (SELECT b.base + g.value AS n{r} FROM (SELECT COALESCE(hi, 0) AS base, {need} AS need "
                f"FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo) m) b "
                f"CROSS APPLY GENERATE_SERIES(CAST(1 AS bigint), CAST(b.need AS bigint), CAST(1 AS bigint)) g) x{order}{off}")

    def status(self, sch, tabs):
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)}" for t in tabs[i:i + 500])
                for i in range(0, len(tabs), 500)]

    def prepare(self, cur, o): pass


CAST = {                                 # wrapped to the type's positive range (tinyint is unsigned)
    "bit": "CAST(x.n % 2 AS bit)", "tinyint": "x.n % 255", "smallint": "x.n % 32767", "int": "x.n % 2147483647", "bigint": "x.n",
    "money": "CAST((x.n % 100000000000) / 100.0 AS money)", "smallmoney": "CAST((x.n % 20000000) / 100.0 AS smallmoney)",
    "float": "CAST(x.n AS float)", "real": "CAST(x.n AS real)",
    # 1970-01-01 plus n days or seconds (DATEADD takes an int, so seconds split into days and seconds)
    "date": "DATEADD(day, CAST(x.n % 2932897 AS int), CAST('1970-01-01' AS date))",
    "datetime2": "DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST('1970-01-01' AS datetime2(0))))",
    "datetime": "DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST('1970-01-01' AS datetime2(0))))",
    "datetimeoffset": "TODATETIMEOFFSET(DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST('1970-01-01' AS datetime2(0)))), '+00:00')",
    "smalldatetime": "DATEADD(minute, CAST(x.n % 94063680 AS int), CAST('1900-01-01' AS smalldatetime))",
    "time": "CAST(DATEADD(second, CAST(x.n % 86400 AS int), CAST('00:00' AS time(0))) AS time)",
    # n in a uuid's last 6 bytes, which SQL Server sorts first: keys stay in n order below 2^48
    "uniqueidentifier": "CAST('00000000-0000-0000-' + LEFT(CONVERT(varchar(16), CAST(x.n AS binary(8)), 2), 4) + '-' + RIGHT(CONVERT(varchar(16), CAST(x.n AS binary(8)), 2), 12) AS uniqueidentifier)",
    "xml": "CAST('<n>' + CAST(x.n AS varchar(20)) + '</n>' AS xml)", "sql_variant": "CAST(x.n AS sql_variant)",
    "geometry": "geometry::Point(x.n % 1000, x.n % 997, 0)", "geography": "geography::Point(x.n % 90, x.n % 180, 4326)",
    "hierarchyid": "hierarchyid::Parse('/' + CAST(x.n % 2147483647 AS varchar(20)) + '/')",
}
