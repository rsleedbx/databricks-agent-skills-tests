-- The declarations every Oracle planner starts with (cat cast.sql plan-insert.sql | sqlplus -s ...): the parameters, the
-- schema's lfc tags (from lfc_tags in the schema, which statschema's ddl.py fills), keys, unique columns, foreign keys
-- and levels, read from the dictionary once, and the cast that turns x.n (any NUMBER n >= 1) into a valid value of each
-- type. Parameters are SQL*Plus substitution variables (fill.py's client mode defines them):
--   DEFINE schema = 'TPCH'  DEFINE rows = 1000000  DEFINE mode = fill  DEFINE order = random  DEFINE data = scramble
--   DEFINE plain_every = 4  DEFINE block = 1000  DEFINE lob_len = 100
-- and SET SERVEROUTPUT ON SIZE UNLIMITED FORMAT WRAPPED, FEEDBACK OFF, VERIFY OFF. The planners print with DBMS_OUTPUT,
-- in lines of at most about 2,000 characters broken at spaces outside string literals (SQL*Plus reads lines of at most
-- 2,499), so a statement spans lines; it starts with its '/* lfc level N */' line and ends with ';'.
DECLARE
  sch VARCHAR2(128) := UPPER('&schema');
  rws NUMBER := &rows;
  md  VARCHAR2(10) := LOWER('&mode');
  ord VARCHAR2(10) := LOWER('&order');
  dat VARCHAR2(10) := LOWER('&data');
  pe  NUMBER := &plain_every;
  blk NUMBER := &block;
  lob NUMBER := &lob_len;
  TYPE smap IS TABLE OF VARCHAR2(32767) INDEX BY VARCHAR2(300);
  tags smap;                  -- 'TABLE.COLUMN' (or 'TABLE.-') -> the tags
  pkc  smap;                  -- 'TABLE.COLUMN' -> its position in the primary key
  npk  smap;                  -- 'TABLE' -> primary key columns
  pk1  smap;                  -- 'TABLE' -> the primary key's first column
  unq  smap;                  -- 'TABLE.COLUMN' -> 'Y' for a one-column unique index
  fkp  smap;                  -- 'TABLE.COLUMN' -> parent table, for a one-column foreign key to the parent's one-column key
  lvl  smap;                  -- 'TABLE' -> level: 0 for a table that references none, else 1 + its parents' highest
  FUNCTION q(s VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN '"' || s || '"'; END;
  FUNCTION g(m smap, k VARCHAR2) RETURN VARCHAR2 IS BEGIN IF m.EXISTS(k) THEN RETURN m(k); END IF; RETURN NULL; END;
  -- a tag: lfc:v={...} (a column's formula in x.n), e (its value as is), len (its string length), n (a table's n from a
  -- row), by (the column n rises with), rows (the target), fixed (every key value used: an append skips the table)
  FUNCTION tg(t VARCHAR2, c VARCHAR2, k VARCHAR2) RETURN VARCHAR2 IS s VARCHAR2(32767) := g(tags, t || '.' || NVL(c, '-'));
  BEGIN
    IF s IS NULL THEN RETURN NULL; END IF;
    IF k IN ('rows', 'added', 'fixed') THEN RETURN REGEXP_SUBSTR(s, 'lfc:' || k || '(=([0-9]+))?', 1, 1, NULL, CASE k WHEN 'rows' THEN 2 ELSE 0 END); END IF;
    RETURN REGEXP_SUBSTR(s, 'lfc:' || k || '=\{([^}]*)\}', 1, 1, NULL, 1);
  END;
  PROCEDURE init IS
    TYPE vt IS TABLE OF VARCHAR2(128);
    TYPE ct IS TABLE OF CLOB;
    a vt; b vt; c ct;
    no_table EXCEPTION; PRAGMA EXCEPTION_INIT(no_table, -942);
  BEGIN
    BEGIN                     -- (no lfc_tags: no tags) the CLOB is read here: SQL returns at most 4,000 bytes of it
      EXECUTE IMMEDIATE 'SELECT tbl, col, tag FROM ' || q(sch) || '.LFC_TAGS' BULK COLLECT INTO a, b, c;
      FOR i IN 1 .. a.COUNT LOOP tags(a(i) || '.' || b(i)) := c(i); END LOOP;
    EXCEPTION WHEN no_table THEN NULL; END;
    FOR r IN (SELECT cc.table_name t, cc.column_name c, cc.position p, COUNT(*) OVER (PARTITION BY cc.table_name) n
              FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
              WHERE k.owner = sch AND k.constraint_type = 'P') LOOP
      pkc(r.t || '.' || r.c) := r.p; npk(r.t) := r.n; IF r.p = 1 THEN pk1(r.t) := r.c; END IF;
    END LOOP;
    FOR r IN (SELECT ic.table_name t, MAX(ic.column_name) c FROM all_indexes i JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name
              WHERE i.table_owner = sch AND i.uniqueness = 'UNIQUE' GROUP BY ic.table_name, ic.index_name HAVING COUNT(*) = 1) LOOP
      unq(r.t || '.' || r.c) := 'Y';
    END LOOP;
    FOR r IN (SELECT k.table_name t, MAX(cc.column_name) c, MAX(p.table_name) pt
              FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
              JOIN all_constraints p ON p.owner = k.r_owner AND p.constraint_name = k.r_constraint_name AND p.constraint_type = 'P'
              WHERE k.owner = sch AND k.constraint_type = 'R' AND p.owner = sch
                AND (SELECT COUNT(*) FROM all_cons_columns x WHERE x.owner = p.owner AND x.constraint_name = p.constraint_name) = 1
              GROUP BY k.table_name, k.constraint_name HAVING COUNT(*) = 1) LOOP
      fkp(r.t || '.' || r.c) := r.pt;
    END LOOP;
    FOR r IN (WITH e (child, parent) AS (SELECT DISTINCT k.table_name, p.table_name FROM all_constraints k
                JOIN all_constraints p ON p.owner = k.r_owner AND p.constraint_name = k.r_constraint_name
                WHERE k.owner = sch AND k.constraint_type = 'R' AND p.owner = sch AND p.table_name <> k.table_name),
              l (tbl, lv) AS (SELECT table_name, 0 FROM all_tables WHERE owner = sch
                              UNION ALL SELECT e.child, l.lv + 1 FROM e JOIN l ON l.tbl = e.parent WHERE l.lv < 50)
              SELECT tbl, MAX(lv) lv FROM l GROUP BY tbl) LOOP
      lvl(r.tbl) := r.lv;
    END LOOP;
  END;
  -- the value of a column from x.n: a 19-digit zero-padded n first (so a key maps back to n) and, from 38 characters,
  -- last, MD5 filler between, to the length; n's 8 bytes first (and from 16 bytes last) in binaries; days or seconds
  -- from 1970 for dates and times
  FUNCTION str(l NUMBER) RETURN VARCHAR2 IS
    p19 VARCHAR2(100) := 'LPAD(TO_CHAR(x.n), 19, ''0'')';
    md5 VARCHAR2(100) := 'LOWER(RAWTOHEX(STANDARD_HASH(TO_CHAR(x.n), ''MD5'')))';
  BEGIN
    IF l < 19 THEN RETURN 'SUBSTR(' || p19 || ', -' || l || ')'; END IF;
    IF l < 38 THEN RETURN 'SUBSTR(' || p19 || ' || ' || md5 || ', 1, ' || l || ')'; END IF;
    RETURN p19 || ' || RPAD(' || md5 || ', ' || (l - 38) || ', ' || md5 || ') || ' || p19;
  END;
  FUNCTION bin(l NUMBER) RETURN VARCHAR2 IS
    h16 VARCHAR2(100) := 'LPAD(TRIM(TO_CHAR(x.n, ''XXXXXXXXXXXXXXXX'')), 16, ''0'')';
    md5 VARCHAR2(100) := 'RAWTOHEX(STANDARD_HASH(TO_CHAR(x.n), ''MD5''))';
  BEGIN
    IF l < 16 THEN RETURN 'HEXTORAW(SUBSTR(' || h16 || ' || ' || md5 || ', 1, ' || (2 * l) || '))'; END IF;
    RETURN 'HEXTORAW(' || h16 || ' || RPAD(' || md5 || ', ' || (2 * l - 32) || ', ' || md5 || ') || ' || h16 || ')';
  END;
  FUNCTION cst(ty VARCHAR2, p NUMBER, s NUMBER, clen NUMBER, lenv NUMBER) RETURN VARCHAR2 IS
    ts VARCHAR2(400) := 'TIMESTAMP ''1970-01-01 00:00:00'' + NUMTODSINTERVAL(MOD(FLOOR(x.n / 86400), 2932897), ''DAY'') + NUMTODSINTERVAL(MOD(x.n, 86400), ''SECOND'')';
  BEGIN
    IF ty = 'NUMBER' THEN
      IF p IS NULL THEN RETURN 'x.n'; END IF;                                         -- NUMBER, INTEGER
      IF NVL(s, 0) <= 0 THEN RETURN 'MOD(x.n, 1' || RPAD('0', LEAST(p, 18), '0') || ')'; END IF;
      RETURN 'MOD(x.n, 1' || RPAD('0', LEAST(p, 18), '0') || ') / 1' || RPAD('0', s, '0');   -- exact: NUMBER division
    END IF;
    IF ty = 'FLOAT' THEN RETURN 'x.n'; END IF;
    IF ty = 'BINARY_FLOAT' THEN RETURN 'TO_BINARY_FLOAT(x.n)'; END IF;
    IF ty = 'BINARY_DOUBLE' THEN RETURN 'TO_BINARY_DOUBLE(x.n)'; END IF;
    IF ty IN ('VARCHAR2', 'CHAR') THEN RETURN str(NVL(lenv, clen)); END IF;
    IF ty IN ('NVARCHAR2', 'NCHAR') THEN RETURN 'TO_NCHAR(' || str(NVL(lenv, clen)) || ')'; END IF;
    IF ty = 'CLOB' THEN RETURN 'TO_CLOB(' || str(NVL(lenv, GREATEST(lob, 38))) || ')'; END IF;
    IF ty = 'NCLOB' THEN RETURN 'TO_NCLOB(' || str(NVL(lenv, GREATEST(lob, 38))) || ')'; END IF;
    IF ty = 'RAW' THEN RETURN bin(NVL(lenv, clen)); END IF;
    IF ty = 'BLOB' THEN RETURN 'TO_BLOB(' || bin(NVL(lenv, GREATEST(lob, 16))) || ')'; END IF;
    IF ty = 'DATE' THEN RETURN 'DATE ''1970-01-01'' + MOD(x.n, 2932897)'; END IF;
    IF ty LIKE 'TIMESTAMP(%) WITH LOCAL TIME ZONE' THEN RETURN 'CAST(FROM_TZ(' || ts || ', ''+00:00'') AS TIMESTAMP WITH LOCAL TIME ZONE)'; END IF;
    IF ty LIKE 'TIMESTAMP(%) WITH TIME ZONE' THEN RETURN 'FROM_TZ(' || ts || ', ''+00:00'')'; END IF;
    IF ty LIKE 'TIMESTAMP(%)' THEN RETURN ts; END IF;
    IF ty LIKE 'INTERVAL YEAR(%) TO MONTH' THEN RETURN 'NUMTOYMINTERVAL(MOD(x.n, ' || (12 * (POWER(10, NVL(p, 2)) - 1)) || '), ''MONTH'')'; END IF;
    IF ty LIKE 'INTERVAL DAY(%) TO SECOND(%)' THEN RETURN 'NUMTODSINTERVAL(MOD(x.n, ' || (86400 * (POWER(10, NVL(p, 2)) - 1)) || '), ''SECOND'')'; END IF;
    IF ty = 'BOOLEAN' THEN RETURN 'CASE MOD(x.n, 2) WHEN 1 THEN TRUE ELSE FALSE END'; END IF;
    IF ty = 'JSON' THEN RETURN 'JSON_OBJECT(''n'' VALUE x.n RETURNING JSON)'; END IF;
    RETURN NULL;              -- ROWID, BFILE, LONG, XMLTYPE, SDO_GEOMETRY, VECTOR, ...: not handled
  END;
  -- n from a key column: integers as they are, a text key from its first 19 digits; NULL if it can't map back
  FUNCTION toint(t VARCHAR2, c VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    FOR r IN (SELECT data_type, data_scale, char_length FROM all_tab_cols WHERE owner = sch AND table_name = t AND column_name = c) LOOP
      IF r.data_type = 'NUMBER' AND NVL(r.data_scale, 0) = 0 THEN RETURN q(c); END IF;
      IF r.data_type IN ('VARCHAR2', 'CHAR') AND r.char_length >= 19 THEN RETURN 'TO_NUMBER(SUBSTR(' || q(c) || ', 1, 19))'; END IF;
    END LOOP;
    RETURN NULL;
  END;
  -- the table's key: the column to probe (lfc:by, or the key's first column), and n from a row (lfc:n, or the key)
  FUNCTION byc(t VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN NVL(tg(t, NULL, 'by'), q(g(pk1, t))); END;
  FUNCTION kint(t VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    IF tg(t, NULL, 'n') IS NOT NULL THEN RETURN tg(t, NULL, 'n'); END IF;
    IF g(npk, t) = '1' THEN RETURN toint(t, g(pk1, t)); END IF;
    RETURN NULL;
  END;
  -- the max and min of n: two probes of the key's index
  FUNCTION hiq(t VARCHAR2, hi BOOLEAN) RETURN VARCHAR2 IS d VARCHAR2(5) := CASE WHEN hi THEN ' DESC' END;
  BEGIN
    IF tg(t, NULL, 'n') IS NULL THEN
      RETURN '(SELECT ' || kint(t) || ' FROM ' || q(sch) || '.' || q(t) || ' ORDER BY ' || byc(t) || d || ' FETCH FIRST 1 ROW ONLY)';
    END IF;
    RETURN '(SELECT ' || CASE WHEN hi THEN 'MAX(' ELSE 'MIN(' END || kint(t) || ') FROM ' || q(sch) || '.' || q(t) || ' WHERE ' || byc(t) || ' = (SELECT '
           || byc(t) || ' FROM ' || q(sch) || '.' || q(t) || ' ORDER BY ' || byc(t) || d || ' FETCH FIRST 1 ROW ONLY))';
  END;
  -- n scrambled, non-linear, with a and b drawn once per column when the plan is made: (m * ((m + a) mod p) + b) mod p
  FUNCTION scr(v VARCHAR2) RETURN VARCHAR2 IS m VARCHAR2(200) := 'MOD(' || v || ', 2147483647)';
  BEGIN
    RETURN 'MOD(MOD(' || m || ' * MOD(' || m || ' + ' || TRUNC(DBMS_RANDOM.VALUE(1, 2147483646)) || ', 2147483647), 2147483647) + '
           || TRUNC(DBMS_RANDOM.VALUE(0, 2147483647)) || ', 2147483647)';
  END;
  -- print a statement in lines of about 2,000 characters, broken at a space outside a string literal
  PROCEDURE emit(s CLOB) IS
    n PLS_INTEGER := DBMS_LOB.GETLENGTH(s); pos PLS_INTEGER := 1; piece VARCHAR2(32767); line VARCHAR2(32767); ch VARCHAR2(1); inq BOOLEAN := FALSE;
  BEGIN
    WHILE pos <= n LOOP
      piece := DBMS_LOB.SUBSTR(s, 8000, pos); pos := pos + 8000;
      FOR j IN 1 .. LENGTH(piece) LOOP
        ch := SUBSTR(piece, j, 1);
        IF ch = '''' THEN inq := NOT inq; END IF;
        IF ch = ' ' AND NOT inq AND LENGTH(line) >= 2000 THEN DBMS_OUTPUT.PUT_LINE(line); line := NULL;
        ELSE line := line || ch; END IF;
      END LOOP;
    END LOOP;
    IF line IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE(line); END IF;
  END;
