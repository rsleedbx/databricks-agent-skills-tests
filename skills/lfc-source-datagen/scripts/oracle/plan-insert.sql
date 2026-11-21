-- { echo "DEFINE schema = 'TPCH' ..."; cat cast.sql plan-insert.sql; } | sqlplus -s ... > inserts.sql   (fill.sh does this)
-- Emits one INSERT per table in the schema that has a usable one-column primary key or an lfc:n tag (cast.sql):
--   mode = append: add rows rows above the current max key.
--   mode = fill: top the table up to its target (lfc:rows), at most rows per statement; rerun until status shows every
--     table at its target.
-- Rows come from CONNECT BY LEVEL on DUAL (INSERT ... SELECT, nothing else crosses the network); /*+ APPEND */ is a
-- direct-path insert, above the high-water mark (Oracle uses a conventional insert instead while the table has enabled
-- foreign keys). Commit after each statement (fill.sh runs them with AUTOCOMMIT ON): a session must commit a
-- direct-path insert before it reads the table again.
-- order = random (default, by ORA_HASH of n) | asc | desc; data = seq | scramble | random, plain_every, block: as in the
-- Postgres and MySQL planners (key, UNIQUE, foreign-key, lfc:v and lfc:e columns always keep their own value).
-- Foreign keys take values inside the parent's current key range. Each statement starts with /* lfc level N */:
--   parents are level 0, a table referencing them is one level higher; run the levels in order.
BEGIN
  init;
  DECLARE
    TYPE tl IS TABLE OF VARCHAR2(128);
    names tl;
    cols CLOB; exprs CLOB; body CLOB; tn VARCHAR2(128); pkn VARCHAR2(128); e VARCHAR2(32767); v VARCHAR2(32767); x VARCHAR2(32767); pt VARCHAR2(128); tgt NUMBER; nr NUMBER; first BOOLEAN;
    vv VARCHAR2(10) := CASE WHEN dat = 'random' THEN 'x.r' ELSE 'x.n' END;
  BEGIN
    SELECT table_name BULK COLLECT INTO names FROM all_tables WHERE owner = sch AND table_name NOT IN ('LFC_TAGS') AND temporary = 'N'
      AND nested = 'NO' AND secondary = 'N' AND dropped = 'NO' ORDER BY table_name;
    FOR lv IN 0 .. 50 LOOP
      FOR i IN 1 .. names.COUNT LOOP
        CONTINUE WHEN NVL(TO_NUMBER(g(lvl, names(i))), 0) <> lv OR kint(names(i)) IS NULL;
        tn := names(i);                                                                      -- (SQL takes variables, not calls)
        tgt := TO_NUMBER(tg(names(i), NULL, 'rows'));
        CONTINUE WHEN md = 'fill' AND tgt IS NULL;
        CONTINUE WHEN md <> 'fill' AND tg(names(i), NULL, 'fixed') IS NOT NULL;               -- an append skips a full table
        cols := NULL; exprs := NULL; first := TRUE;
        FOR c IN (SELECT column_name, data_type, data_precision, data_scale, char_length, identity_column FROM all_tab_cols
                  WHERE owner = sch AND table_name = tn AND hidden_column = 'NO' AND virtual_column = 'NO' ORDER BY column_id) LOOP
          CONTINUE WHEN c.identity_column = 'YES' AND g(pkc, names(i) || '.' || c.column_name) IS NULL;   -- the database numbers it
          e := tg(names(i), c.column_name, 'e'); v := tg(names(i), c.column_name, 'v'); pt := g(fkp, names(i) || '.' || c.column_name);
          x := cst(c.data_type, c.data_precision, c.data_scale, c.char_length, TO_NUMBER(tg(names(i), c.column_name, 'len')));
          IF e IS NOT NULL THEN x := e;                                                      -- an expression, as is
          ELSIF v IS NOT NULL THEN x := REPLACE(x, 'x.n', '(' || v || ')');                  -- a spec formula for this column
          ELSIF pt IS NOT NULL AND g(pkc, names(i) || '.' || c.column_name) IS NULL AND g(npk, pt) = '1' THEN
            -- foreign key: inside the parent's current key range, cast like the parent's key
            pkn := g(pk1, pt);
            FOR p IN (SELECT data_type, data_precision, data_scale, char_length FROM all_tab_cols WHERE owner = sch AND table_name = pt AND column_name = pkn) LOOP
              x := REPLACE(cst(p.data_type, p.data_precision, p.data_scale, p.char_length, NULL), 'x.n',
                     '(' || hiq(pt, FALSE) || ' + MOD(' || scr(vv) || ', ' || hiq(pt, TRUE) || ' - ' || hiq(pt, FALSE) || ' + 1))');
            END LOOP;
          ELSIF NOT (dat = 'seq' OR g(pkc, names(i) || '.' || c.column_name) IS NOT NULL OR g(unq, names(i) || '.' || c.column_name) IS NOT NULL) AND x IS NOT NULL THEN
            x := CASE WHEN pe > 0 THEN 'CASE WHEN MOD(TRUNC((x.n - 1) / ' || blk || '), ' || pe || ') = 0 THEN ' || x || ' ELSE ' END
                 || REPLACE(x, 'x.n', '(' || scr(vv) || ' + 1)') || CASE WHEN pe > 0 THEN ' END' END;
          END IF;
          cols := cols || CASE WHEN NOT first THEN ', ' END || q(c.column_name);
          exprs := exprs || CASE WHEN NOT first THEN ', ' END || NVL(x, 'NULL');
          first := FALSE;
        END LOOP;
        nr := CASE WHEN md = 'fill' THEN LEAST(rws, tgt) ELSE rws END;                      -- rows the generator makes
        DBMS_OUTPUT.PUT_LINE('/* lfc level ' || lv || ' */ INSERT /*+ APPEND */ INTO ' || q(sch) || '.' || q(names(i)) || ' (');
        body := cols || ') SELECT ' || exprs || ' FROM (SELECT b.base + g.i AS n'
          || CASE WHEN dat = 'random' THEN ', TRUNC(DBMS_RANDOM.VALUE(0, 2147483647)) AS r' END
          || ' FROM (SELECT COALESCE(hi, 0) AS base, '
          || CASE WHEN md = 'fill' THEN 'LEAST(' || rws || ', GREATEST(' || tgt || ' - COALESCE(hi - lo + 1, 0), 0))' ELSE TO_CHAR(rws) END
          || ' AS need FROM (SELECT ' || hiq(names(i), TRUE) || ' AS hi, ' || hiq(names(i), FALSE) || ' AS lo FROM DUAL) m) b, '
          || '(SELECT LEVEL AS i FROM DUAL CONNECT BY LEVEL <= ' || nr || ') g WHERE g.i <= b.need) x'
          || CASE ord WHEN 'asc' THEN ' ORDER BY x.n' WHEN 'desc' THEN ' ORDER BY x.n DESC' ELSE ' ORDER BY ORA_HASH(x.n)' END || ';';
        emit(body);
      END LOOP;
    END LOOP;
  END;
END;
/
