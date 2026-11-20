-- pg_temp.lfc_cast(type, typmod): SQL expression that turns x.n (bigint, any n >= 1) into a valid value of the type,
-- or NULL if the type isn't supported. Lives only in the planning session; the SQL it writes doesn't need it.
SET check_function_bodies = off;
-- Strings and binaries of the column's length: n at the start and, from 38 characters (16 bytes), at the end too, so a
-- truncated value shows up in the audit. Unbounded types (text, varchar, bytea) are :lob_len long (default 100).
\if :{?lob_len} \else \set lob_len 100 \endif
SELECT set_config('lfc.lob_len', greatest(:lob_len, 38)::text, false) AS lfc_lob_len \gset
CREATE OR REPLACE FUNCTION pg_temp.lfc_len(m int) RETURNS int LANGUAGE sql STABLE AS $f$
SELECT CASE WHEN m = -1 THEN current_setting('lfc.lob_len')::int ELSE m - 4 END
$f$;
CREATE OR REPLACE FUNCTION pg_temp.lfc_str(m int) RETURNS text LANGUAGE sql STABLE AS $f$
SELECT CASE WHEN l < 19 THEN format('right(lpad(x.n::text, 19, ''0''), %s)', l)
            WHEN l < 38 THEN format('left(lpad(x.n::text, 19, ''0'') || repeat(md5(x.n::text), 1), %s)', l)
            ELSE format('lpad(x.n::text, 19, ''0'') || left(repeat(md5(x.n::text), %s), %s) || lpad(x.n::text, 19, ''0'')',
                        greatest(ceil((l - 38) / 32.0)::int, 1), l - 38) END
FROM (SELECT pg_temp.lfc_len(m) AS l) q
$f$;
CREATE OR REPLACE FUNCTION pg_temp.lfc_bin(l int) RETURNS text LANGUAGE sql IMMUTABLE AS $f$
SELECT format('(int8send(x.n) || substr(decode(repeat(md5(x.n::text), %s), ''hex''), 1, %s) || int8send(x.n))',
              greatest(ceil((l - 16) / 16.0)::int, 1), l - 16)
$f$;
CREATE OR REPLACE FUNCTION pg_temp.lfc_cast(ty oid, m int) RETURNS text LANGUAGE sql STABLE AS $f$
SELECT CASE
  WHEN t.typtype = 'd' THEN pg_temp.lfc_cast(t.typbasetype, t.typtypmod)            -- domain: its base type (CHECKs still apply)
  WHEN t.typtype = 'e' THEN format('(enum_range(NULL::%1$s))[1 + x.n %% cardinality(enum_range(NULL::%1$s))]', ty::regtype)
  WHEN t.typcategory = 'A' AND t.typelem <> 0 THEN 'ARRAY[' || pg_temp.lfc_cast(t.typelem, m) || ']'
  ELSE CASE ty::regtype::text
    -- integers
    WHEN 'smallint' THEN '(x.n % 32767)::smallint'
    WHEN 'integer'  THEN '(x.n % 2147483647)::int'
    WHEN 'bigint'   THEN 'x.n'
    WHEN 'oid'      THEN '(x.n % 4294967296)::oid'
    -- exact numeric: integer part kept below 10^(p-s), at most 18 digits of n
    WHEN 'numeric'  THEN CASE WHEN m < 0 THEN 'x.n::numeric'
                         ELSE format('((x.n %% 1%s)::numeric / 1%s)::numeric(%s,%s)',     -- p digits of n, scaled by 10^s
                                     repeat('0', least((m - 4) >> 16, 18)), repeat('0', (m - 4) & 65535), (m - 4) >> 16, (m - 4) & 65535) END
    WHEN 'money'    THEN '((x.n % 100000000000)::numeric / 100)::money'
    -- floats
    WHEN 'real'     THEN 'x.n::real'
    WHEN 'double precision' THEN 'x.n::float8'
    -- strings: 19-digit zero-padded n first (so a key maps back to n), then MD5 filler, cut to the length
    WHEN 'character varying' THEN pg_temp.lfc_str(m)
    WHEN 'character' THEN pg_temp.lfc_str(m)
    WHEN 'text'      THEN pg_temp.lfc_str(m)                                   -- m = -1 unless lfc:len sets it
    WHEN 'name'      THEN pg_temp.lfc_str(67)
    WHEN 'bytea'     THEN pg_temp.lfc_bin(pg_temp.lfc_len(m))
    -- date/time: epoch plus n days or seconds, wrapped to stay at or before 9999-12-31
    WHEN 'date'      THEN '(date ''1970-01-01'' + (x.n % 2932897)::int)'
    WHEN 'timestamp without time zone' THEN '(timestamp ''1970-01-01'' + (x.n % 253402300800) * interval ''1 second'')'
    WHEN 'timestamp with time zone'    THEN '(timestamptz ''1970-01-01 00:00:00+00'' + (x.n % 253402300800) * interval ''1 second'')'
    WHEN 'time without time zone'      THEN '(time ''00:00'' + (x.n % 86400) * interval ''1 second'')'
    WHEN 'time with time zone'         THEN '(timetz ''00:00+00'' + (x.n % 86400) * interval ''1 second'')'
    WHEN 'interval'  THEN '((x.n % 1000000000000) * interval ''1 second'')'
    -- other scalars
    WHEN 'boolean'   THEN '(x.n % 2 = 1)'
    WHEN 'uuid'      THEN 'lpad(to_hex(x.n), 32, ''0'')::uuid'
    WHEN 'json'      THEN 'json_build_object(''n'', x.n)'
    WHEN 'jsonb'     THEN 'jsonb_build_object(''n'', x.n)'
    WHEN 'xml'       THEN 'xmlelement(name n, x.n)'
    WHEN 'pg_lsn'    THEN '(to_hex(x.n >> 32) || ''/'' || to_hex(x.n & 4294967295))::pg_lsn'
    -- network: n as an IPv4 address or a MAC address
    WHEN 'inet'      THEN '(''0.0.0.0''::inet + (x.n % 4294967296))'
    WHEN 'cidr'      THEN 'set_masklen(''0.0.0.0''::inet + (x.n % 4294967296), 32)::cidr'
    WHEN 'macaddr'   THEN 'lpad(to_hex(x.n % 281474976710656), 12, ''0'')::macaddr'
    WHEN 'macaddr8'  THEN 'lpad(to_hex(x.n), 16, ''0'')::macaddr8'
    -- bit strings: the low bits of n
    WHEN 'bit'       THEN CASE WHEN m <= 64 THEN format('x.n::bit(%s)', greatest(m, 1))
                               ELSE format('(repeat(''0'', %s) || x.n::bit(64)::text)::bit(%s)', m - 64, m) END
    WHEN 'bit varying' THEN format('x.n::bit(%s)::varbit', CASE WHEN m BETWEEN 1 AND 64 THEN m ELSE 64 END)
    -- geometric: coordinates from n, always well-formed
    WHEN 'point'     THEN 'point(x.n % 1000, x.n % 997)'
    WHEN 'line'      THEN 'line(point(0, 0), point(1 + x.n % 1000, 1))'
    WHEN 'lseg'      THEN 'lseg(point(0, 0), point(x.n % 1000, 1))'
    WHEN 'box'       THEN 'box(point(0, 0), point(x.n % 1000, 1))'
    WHEN 'path'      THEN 'path(polygon(box(point(0, 0), point(1 + x.n % 1000, 1))))'
    WHEN 'polygon'   THEN 'polygon(box(point(0, 0), point(1 + x.n % 1000, 1)))'
    WHEN 'circle'    THEN 'circle(point(x.n % 1000, 0), 1 + x.n % 100)'
    -- ranges: [v, v + 1) from n
    WHEN 'int4range' THEN 'int4range((x.n % 2147483646)::int, (x.n % 2147483646)::int + 1)'
    WHEN 'int8range' THEN 'int8range(x.n, x.n + 1)'
    WHEN 'numrange'  THEN 'numrange(x.n, x.n + 1)'
    WHEN 'daterange' THEN 'daterange(date ''1970-01-01'' + (x.n % 2932895)::int, date ''1970-01-01'' + (x.n % 2932895)::int + 1)'
    WHEN 'tsrange'   THEN 'tsrange(timestamp ''1970-01-01'' + (x.n % 253402300799) * interval ''1 second'', timestamp ''1970-01-01'' + (x.n % 253402300799 + 1) * interval ''1 second'')'
    WHEN 'tstzrange' THEN 'tstzrange(timestamptz ''1970-01-01 00:00:00+00'' + (x.n % 253402300799) * interval ''1 second'', timestamptz ''1970-01-01 00:00:00+00'' + (x.n % 253402300799 + 1) * interval ''1 second'')'
    WHEN 'int4multirange' THEN 'int4multirange(int4range((x.n % 2147483646)::int, (x.n % 2147483646)::int + 1))'
    WHEN 'int8multirange' THEN 'int8multirange(int8range(x.n, x.n + 1))'
    WHEN 'nummultirange'  THEN 'nummultirange(numrange(x.n, x.n + 1))'
    WHEN 'datemultirange' THEN 'datemultirange(daterange(date ''1970-01-01'' + (x.n % 2932895)::int, date ''1970-01-01'' + (x.n % 2932895)::int + 1))'
    -- text search
    WHEN 'tsvector'  THEN 'to_tsvector(''simple'', ''w'' || x.n)'
    WHEN 'tsquery'   THEN '(''w'' || x.n)::tsquery'
    END
  END
FROM pg_type t WHERE t.oid = ty
$f$;
-- column type -> n, for a primary key (NULL if the type can't map back to n)
CREATE OR REPLACE FUNCTION pg_temp.lfc_to_int(ty oid, m int, col text) RETURNS text LANGUAGE sql STABLE AS $f$
SELECT CASE
  WHEN t.typtype = 'd' THEN pg_temp.lfc_to_int(t.typbasetype, t.typtypmod, col)
  ELSE CASE ty::regtype::text
    WHEN 'smallint' THEN col WHEN 'integer' THEN col WHEN 'bigint' THEN col
    WHEN 'numeric' THEN CASE WHEN m < 0 OR ((m - 4) & 65535) = 0 THEN col || '::bigint' END
    WHEN 'character varying' THEN CASE WHEN m = -1 OR m - 4 >= 19 THEN format('left(%s, 19)::bigint', col) END
    WHEN 'character' THEN CASE WHEN m - 4 >= 19 THEN format('left(%s, 19)::bigint', col) END
    WHEN 'text' THEN format('left(%s, 19)::bigint', col)
    WHEN 'uuid' THEN format('(''x'' || right(replace(%s::text, ''-'', ''''), 16))::bit(64)::bigint', col)
    END
  END
FROM pg_type t WHERE t.oid = ty
$f$;
-- column -> n, for auditing a data column against its key: the SQL that decodes the column back to n modulo the
-- cast's wrap, as "<decoded> = (<key> % <wrap>)" (NULL if the type can't be decoded exactly).
CREATE OR REPLACE FUNCTION pg_temp.lfc_check(ty oid, m int, col text, k text) RETURNS text LANGUAGE sql STABLE AS $f$
SELECT CASE
  WHEN t.typtype = 'd' THEN pg_temp.lfc_check(t.typbasetype, t.typtypmod, col, k)
  WHEN t.typtype = 'e' THEN format('array_position(enum_range(NULL::%1$s), %2$s) - 1 = %3$s %% cardinality(enum_range(NULL::%1$s))', ty::regtype, col, k)
  ELSE CASE ty::regtype::text
    WHEN 'smallint' THEN format('%s = %s %% 32767', col, k)
    WHEN 'integer'  THEN format('%s = %s %% 2147483647', col, k)
    WHEN 'bigint'   THEN format('%s = %s', col, k)
    WHEN 'oid'      THEN format('%s::bigint = %s %% 4294967296', col, k)
    WHEN 'numeric'  THEN CASE WHEN m < 0 THEN format('%s = %s', col, k)
                         ELSE format('%s * 1%s = %s %% 1%s', col, repeat('0', (m - 4) & 65535), k, repeat('0', least((m - 4) >> 16, 18))) END
    WHEN 'money'    THEN format('%s::numeric * 100 = %s %% 100000000000', col, k)
    WHEN 'double precision' THEN format('%s = %s::float8', col, k)
    -- strings: n at the start (and at the end from 38 characters) and the full length; a value that isn't digits where
    -- n should be counts as a mismatch, not an error
    WHEN 'character varying' THEN pg_temp.lfc_strchk(pg_temp.lfc_len(m), col, k)
    WHEN 'character' THEN pg_temp.lfc_strchk(pg_temp.lfc_len(m), col, k)
    WHEN 'text'      THEN pg_temp.lfc_strchk(pg_temp.lfc_len(m), col, k)
    WHEN 'bytea'     THEN format('(''x'' || encode(substr(%1$s, 1, 8), ''hex''))::bit(64)::bigint = %2$s AND (''x'' || encode(substr(%1$s, length(%1$s) - 7, 8), ''hex''))::bit(64)::bigint = %2$s AND length(%1$s) = %3$s',
                                 col, k, pg_temp.lfc_len(m))
    WHEN 'date'      THEN format('%s - date ''1970-01-01'' = %s %% 2932897', col, k)
    WHEN 'timestamp without time zone' THEN format('extract(epoch from %s)::bigint = %s %% 253402300800', col, k)
    WHEN 'timestamp with time zone'    THEN format('extract(epoch from %s)::bigint = %s %% 253402300800', col, k)
    WHEN 'time without time zone'      THEN format('extract(epoch from %s)::bigint = %s %% 86400', col, k)
    WHEN 'interval'  THEN format('extract(epoch from %s)::bigint = %s %% 1000000000000', col, k)
    WHEN 'boolean'   THEN format('%s = (%s %% 2 = 1)', col, k)
    WHEN 'uuid'      THEN format('(''x'' || right(replace(%s::text, ''-'', ''''), 16))::bit(64)::bigint = %s', col, k)
    WHEN 'json'      THEN format('CASE WHEN %1$s->>''n'' ~ ''^[0-9]+$'' THEN (%1$s->>''n'')::bigint END = %2$s', col, k)
    WHEN 'jsonb'     THEN format('CASE WHEN %1$s->>''n'' ~ ''^[0-9]+$'' THEN (%1$s->>''n'')::bigint END = %2$s', col, k)
    WHEN 'inet'      THEN format('%s - ''0.0.0.0''::inet = %s %% 4294967296', col, k)
    WHEN 'int8range' THEN format('lower(%s) = %s', col, k)
    END
  END
FROM pg_type t WHERE t.oid = ty
$f$;

CREATE OR REPLACE FUNCTION pg_temp.lfc_strchk(l int, col text, k text) RETURNS text LANGUAGE sql IMMUTABLE AS $f$
SELECT CASE WHEN l < 19 THEN format('CASE WHEN %1$s ~ ''^[0-9]+$'' THEN %1$s::bigint END = %2$s %% 1%3$s', col, k, repeat('0', l))
            -- from 38 characters both ends carry n, which is enough to catch truncation and keeps values written before a
            -- column was widened valid; shorter strings must have the exact length
            WHEN l >= 38 THEN format('CASE WHEN left(%1$s, 19) ~ ''^[0-9]{19}$'' THEN left(%1$s, 19)::bigint END = %2$s AND right(%1$s, 19) = lpad((%2$s)::text, 19, ''0'')', col, k)
            ELSE format('CASE WHEN left(%1$s, 19) ~ ''^[0-9]{19}$'' THEN left(%1$s, 19)::bigint END = %2$s AND length(%1$s) = %3$s', col, k, l) END
$f$;
-- Tags in comments, in braces: a column's lfc:v={integer expression in x.n} replaces n in its cast (spec formulas: sparse
-- keys, composite keys, value ranges); a table's lfc:n={expression of its columns} gives n back from a row, for composite
-- keys or tables without a primary key, and lfc:by={column} names the column n rises with (default: the first key column).
-- A column's lfc:e={SQL expression in x.n} is its value as is, not cast (literal strings from a spec's value lists or
-- formats); the audit compares the column with it. A column's lfc:len={L} makes its strings or binaries L long instead of the declared length (or :lob_len), for a source's
-- average width; lfc_mod gives the typmod the casts and checks use.
CREATE OR REPLACE FUNCTION pg_temp.lfc_mod(rel oid, att int2, m int) RETURNS int LANGUAGE sql STABLE AS $f$
SELECT coalesce(substring(col_description(rel, att) from 'lfc:len=\{([0-9]+)\}')::int + 4, m)
$f$;
CREATE OR REPLACE FUNCTION pg_temp.lfc_tag(c text, tag text) RETURNS text LANGUAGE sql IMMUTABLE AS $f$
SELECT substring(c from 'lfc:' || tag || '=\{([^}]*)\}')
$f$;
