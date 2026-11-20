# PostgreSQL (and Postgres-protocol engines): the catalog, and the casts of ../../postgres/cast.sql (lfc_cast,
# lfc_to_int). Types are pg_type rows, read once; Col.ty is the type's regtype name, Col.x holds its oid and typmod.
# Rows come from generate_series, so no seed table is needed.
import math, re
from planner import Col, Table, P


class Engine:
    name = "postgres"
    rows = 1000000

    def catalog(self, cur, sch):
        cur.execute("SELECT word FROM pg_get_keywords() WHERE catcode <> 'U'")
        self.kw = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT oid, typtype, typbasetype, typtypmod, typcategory, typelem, oid::regtype::text FROM pg_type")
        # CockroachDB's regtype names are the short ones (int8, varchar, timestamp): PostgreSQL's long names, which the rest uses
        self.types = {r[0]: r[1:6] + (LONG.get(r[6], r[6]),) for r in cur.fetchall()}
        # the key's columns in key order, by array_position: no unnest(pk.conkey), whose implicit LATERAL CockroachDB refuses
        cur.execute("""
          SELECT r.relname, coalesce(obj_description(r.oid, 'pg_class'), ''),
                 (SELECT array_agg(a.attname ORDER BY array_position(pk.conkey, a.attnum)) FROM pg_constraint pk
                  JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = ANY (pk.conkey)
                  WHERE pk.conrelid = r.oid AND pk.contype = 'p'),
                 (SELECT array_agg(DISTINCT p.relname) FROM pg_constraint f JOIN pg_class p ON p.oid = f.confrelid
                  WHERE f.conrelid = r.oid AND f.contype = 'f')
          FROM pg_class r JOIN pg_namespace s ON s.oid = r.relnamespace
          WHERE s.nspname = %s AND r.relkind = 'r' ORDER BY r.relname""", (sch,))
        tabs = {n: Table(n, [], pk or [], tags, set(ps or [])) for n, tags, pk, ps in cur.fetchall()}
        # insertable columns: not generated, and not a non-key identity or serial column (the database numbers those)
        cur.execute("""
          SELECT r.relname, a.attname, a.attnum, a.atttypid, a.atttypmod, a.attidentity <> '', coalesce(col_description(r.oid, a.attnum), ''),
                 coalesce(a.attnum = ANY (pk.conkey), false),
                 EXISTS (SELECT 1 FROM pg_index u WHERE u.indrelid = r.oid AND u.indisunique AND a.attnum = ANY (u.indkey)),
                 (SELECT p.relname FROM pg_constraint f JOIN pg_constraint ppk ON ppk.conrelid = f.confrelid AND ppk.contype = 'p' AND ppk.conkey = f.confkey
                  JOIN pg_class p ON p.oid = f.confrelid
                  WHERE f.conrelid = r.oid AND f.contype = 'f' AND cardinality(f.conkey) = 1 AND f.conkey[1] = a.attnum LIMIT 1)
          FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid AND r.relkind = 'r'
          JOIN pg_namespace s ON s.oid = r.relnamespace AND s.nspname = %s
          LEFT JOIN pg_constraint pk ON pk.conrelid = r.oid AND pk.contype = 'p'
          LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
          WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
            AND (coalesce(a.attnum = ANY (pk.conkey), false) OR (a.attidentity = '' AND NOT coalesce(pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%%', false)))
          ORDER BY r.relname, a.attnum""", (sch,))
        for tn, cn, pos, oid, mod, ident, tags, pk, uq, fk in cur.fetchall():
            m = re.search(r"lfc:len=\{([0-9]+)\}", tags)                 # lfc:len={L}: strings L long
            tabs[tn].cols.append(Col(cn, pos, self.types[oid][5], pk=pk, unique=uq, ident=ident and pk, fk=fk, tags=tags,
                                     x={"oid": oid, "mod": int(m.group(1)) + 4 if m else mod}))
        return list(tabs.values())

    # names: quoted only where Postgres would need it (quote_ident)
    def q(self, s):
        return s if re.fullmatch(r"[a-z_][a-z0-9_$]*", s) and s not in self.kw else '"' + s.replace('"', '""') + '"'
    def qby(self, s): return self.q(s)
    def t(self, sch, tbl): return f"{self.q(sch)}.{self.q(tbl)}"

    def length(self, c, o): return None                       # the casts read the typmod
    def cast(self, c, L, o): return self.cst(c.x["oid"], c.x["mod"], max(o.lob_len, 38))

    def cst(self, ty, m, lob):
        typtype, base, basemod, cat, elem, name = self.types[ty]
        if typtype == "d": return self.cst(base, basemod, lob)        # a domain: its base type (CHECKs still apply)
        if typtype == "e": return f"(enum_range(NULL::{name}))[1 + x.n % cardinality(enum_range(NULL::{name}))]"
        if cat == "A" and elem != 0:
            e = self.cst(elem, m, lob)
            return f"ARRAY[{e}]" if e is not None else None
        ln = lambda m: lob if m == -1 else m - 4
        def s(m):                                                   # 19-digit n first (and last from 38), MD5 between
            l = ln(m)
            if l < 19: return f"right(lpad(x.n::text, 19, '0'), {l})"
            if l < 38: return f"left(lpad(x.n::text, 19, '0') || repeat(md5(x.n::text), 1), {l})"
            return f"lpad(x.n::text, 19, '0') || left(repeat(md5(x.n::text), {max(math.ceil((l - 38) / 32), 1)}), {l - 38}) || lpad(x.n::text, 19, '0')"
        def b(l): return f"(int8send(x.n) || substr(decode(repeat(md5(x.n::text), {max(math.ceil((l - 16) / 16), 1)}), 'hex'), 1, {l - 16}) || int8send(x.n))"
        if name == "numeric":
            if m < 0: return "x.n::numeric"
            p, sc = (m - 4) >> 16, (m - 4) & 65535
            return f"((x.n % 1{'0' * min(p, 18)})::numeric / 1{'0' * sc})::numeric({p},{sc})"
        if name == "bit":
            return f"x.n::bit({max(m, 1)})" if m <= 64 else f"(repeat('0', {m - 64}) || x.n::bit(64)::text)::bit({m})"
        if name == "bit varying": return f"x.n::bit({m if 1 <= m <= 64 else 64})::varbit"
        if name in ("character varying", "character", "text"): return s(m)
        if name == "name": return s(67)
        if name == "bytea": return b(ln(m))
        return CAST.get(name)

    def to_int(self, c, col):
        if c is None: return None
        ty, m = c.x["oid"], c.x["mod"]
        while self.types[ty][0] == "d": ty, m = self.types[ty][1], self.types[ty][2]
        name = self.types[ty][5]
        if name in ("smallint", "integer", "bigint"): return col
        if name == "numeric": return f"{col}::bigint" if m < 0 or ((m - 4) & 65535) == 0 else None
        # zero-padded text: CockroachDB reads '0008'::bigint as octal and fails, so the leading zeros go first (0000 is 0)
        if name == "character varying": return f"COALESCE(NULLIF(LTRIM(LEFT({col}, 19), '0'), '')::bigint, 0)" if m == -1 or m - 4 >= 19 else None
        if name == "character": return f"COALESCE(NULLIF(LTRIM(LEFT({col}, 19), '0'), '')::bigint, 0)" if m - 4 >= 19 else None
        if name == "text": return f"COALESCE(NULLIF(LTRIM(LEFT({col}, 19), '0'), '')::bigint, 0)"
        if name == "uuid": return f"('x' || right(replace({col}::text, '-', ''), 16))::bit(64)::bigint"
        return None

    # the max and min of n: two index probes
    def probe(self, sch, t, hi):
        d = " DESC" if hi else ""
        if not t.has_n: return f"(SELECT {t.pk_int} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} LIMIT 1)"
        return (f"(SELECT {'max' if hi else 'min'}({t.pk_int}) FROM {self.t(sch, t.name)} WHERE {t.byc} = "
                f"(SELECT {t.byc} FROM {self.t(sch, t.name)} ORDER BY {t.byc}{d} LIMIT 1))")

    def scr(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P})"
    def scr1(self, v, a, b): return f"(((({v} % {P}) * (({v} % {P} + {a}) % {P})) % {P} + {b}) % {P} + 1)"
    def fk(self, lo, hi, s): return f"({lo} + {s} % ({hi} - {lo} + 1))"
    def plain(self, block, k, x, s): return f"CASE WHEN (x.n - 1) / {block} % {k} = 0 THEN {x} ELSE {s} END"

    def insert(self, sch, t, cols, exprs, o):
        need = f"least({o.rows}, greatest({t.target} - coalesce(hi - lo + 1, 0), 0))" if o.mode == "fill" else str(o.rows)
        ovr = " OVERRIDING SYSTEM VALUE" if any(c.ident for c in cols) else ""
        order = {"asc": " ORDER BY g.i", "desc": " ORDER BY g.i DESC"}.get(o.order, " ORDER BY random()")
        r = ", floor(random() * 2147483647)::bigint AS r" if o.data == "random" else ""
        return (f"/* lfc level {t.level} */ INSERT INTO {self.t(sch, t.name)} ({', '.join(self.q(c.name) for c in cols)}){ovr} "
                f"SELECT {', '.join(exprs)} FROM (SELECT b.base + g.i AS n{r} FROM (SELECT coalesce(hi, 0) AS base, {need} AS need "
                f"FROM (SELECT {self.probe(sch, t, True)} AS hi, {self.probe(sch, t, False)} AS lo) m) b, "
                f"generate_series(1::bigint, b.need::bigint) g(i){order} OFFSET 0) x")

    def status(self, sch, tabs):                                     # one SELECT of (table, lo, hi) per 500 tables
        return [" UNION ALL ".join(f"SELECT '{t.name}', {self.probe(sch, t, False)}, {self.probe(sch, t, True)}" for t in tabs[i:i + 500])
                for i in range(0, len(tabs), 500)]

    def prepare(self, cur, o): pass


LONG = {"int2": "smallint", "int4": "integer", "int8": "bigint", "float4": "real", "float8": "double precision", "bool": "boolean",
        "bpchar": "character", "varchar": "character varying", "timestamp": "timestamp without time zone",
        "timestamptz": "timestamp with time zone", "time": "time without time zone", "timetz": "time with time zone"}

CAST = {                                # the rest of lfc_cast: n into a valid value of the type
    "smallint": "(x.n % 32767)::smallint", "integer": "(x.n % 2147483647)::int", "bigint": "x.n", "oid": "(x.n % 4294967296)::oid",
    "money": "((x.n % 100000000000)::numeric / 100)::money", "real": "x.n::real", "double precision": "x.n::float8",
    "date": "(date '1970-01-01' + (x.n % 2932897)::int)",
    "timestamp without time zone": "(timestamp '1970-01-01' + (x.n % 253402300800) * interval '1 second')",
    "timestamp with time zone": "(timestamptz '1970-01-01 00:00:00+00' + (x.n % 253402300800) * interval '1 second')",
    "time without time zone": "(time '00:00' + (x.n % 86400) * interval '1 second')",
    "time with time zone": "(timetz '00:00+00' + (x.n % 86400) * interval '1 second')",
    "interval": "((x.n % 1000000000000) * interval '1 second')",
    "boolean": "(x.n % 2 = 1)", "uuid": "lpad(to_hex(x.n), 32, '0')::uuid", "json": "json_build_object('n', x.n)",
    "jsonb": "jsonb_build_object('n', x.n)", "xml": "xmlelement(name n, x.n)",
    "pg_lsn": "(to_hex(x.n >> 32) || '/' || to_hex(x.n & 4294967295))::pg_lsn",
    "inet": "('0.0.0.0'::inet + (x.n % 4294967296))", "cidr": "set_masklen('0.0.0.0'::inet + (x.n % 4294967296), 32)::cidr",
    "macaddr": "lpad(to_hex(x.n % 281474976710656), 12, '0')::macaddr", "macaddr8": "lpad(to_hex(x.n), 16, '0')::macaddr8",
    "point": "point(x.n % 1000, x.n % 997)", "line": "line(point(0, 0), point(1 + x.n % 1000, 1))",
    "lseg": "lseg(point(0, 0), point(x.n % 1000, 1))", "box": "box(point(0, 0), point(x.n % 1000, 1))",
    "path": "path(polygon(box(point(0, 0), point(1 + x.n % 1000, 1))))", "polygon": "polygon(box(point(0, 0), point(1 + x.n % 1000, 1)))",
    "circle": "circle(point(x.n % 1000, 0), 1 + x.n % 100)",
    "int4range": "int4range((x.n % 2147483646)::int, (x.n % 2147483646)::int + 1)", "int8range": "int8range(x.n, x.n + 1)",
    "numrange": "numrange(x.n, x.n + 1)",
    "daterange": "daterange(date '1970-01-01' + (x.n % 2932895)::int, date '1970-01-01' + (x.n % 2932895)::int + 1)",
    "tsrange": "tsrange(timestamp '1970-01-01' + (x.n % 253402300799) * interval '1 second', timestamp '1970-01-01' + (x.n % 253402300799 + 1) * interval '1 second')",
    "tstzrange": "tstzrange(timestamptz '1970-01-01 00:00:00+00' + (x.n % 253402300799) * interval '1 second', timestamptz '1970-01-01 00:00:00+00' + (x.n % 253402300799 + 1) * interval '1 second')",
    "int4multirange": "int4multirange(int4range((x.n % 2147483646)::int, (x.n % 2147483646)::int + 1))",
    "int8multirange": "int8multirange(int8range(x.n, x.n + 1))", "nummultirange": "nummultirange(numrange(x.n, x.n + 1))",
    "datemultirange": "datemultirange(daterange(date '1970-01-01' + (x.n % 2932895)::int, date '1970-01-01' + (x.n % 2932895)::int + 1))",
    "tsvector": "to_tsvector('simple', 'w' || x.n)", "tsquery": "('w' || x.n)::tsquery",
}
