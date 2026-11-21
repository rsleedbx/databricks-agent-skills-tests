-- cat cast.sql plan-insert.sql | psql -v schema=lfc -v rows=10000 -v src=series [-v mode=fill] -At > inserts.sql
-- Emits one INSERT per table in :schema that has a usable one-column primary key.
--   mode=append (default): add :rows rows above the current max key.
--   mode=fill: top the table up to the row count in its comment ('lfc:rows=<n>'), at most :rows per statement;
--              rerun until status shows every table at its target.
--   order=random (default) | asc | desc: the order the new keys are inserted in (always exactly max+1 .. max+need).
--   data=seq (default) | scramble | random: non-key columns from n itself; from n scrambled per column,
--     (m * ((m + a) mod p) + b) mod p with m = n mod p, p = 2^31 - 1, and a, b drawn once per column at plan time
--     (non-linear, so neighbouring rows don't form a sequence; the same rows on every run);
--     or from one random value per row (random()), scrambled per column. Key and UNIQUE columns always use plain n;
--   plain_every=k, block=B (k = 0 is off): every k-th block of B keys ((n - 1) / B % k = 0) keeps plain values in every
--     column whatever data is, so those rows can be audited from the key alone (plan-audit.sql);
--   non-key identity and serial columns are left to the database.
-- Each INSERT reads the table's min and max key itself, so rerunning it never duplicates rows.
-- Foreign keys (one column, to the parent's primary key) get parent_min + scramble(n) % (parent_max - parent_min + 1), cast
-- to the parent key's type, with the parent's range probed when the statement runs: every value is an existing key.
-- Tables with a composite key or none qualify when their comment has lfc:n={...} (see cast.sql); a column's lfc:v={...}
-- replaces n in its cast. Each INSERT then probes the max of n among the rows with the highest lfc:by column.
-- Each statement starts with /* lfc level N */: parents are level 0, a table referencing them is one level higher.
-- Run the levels in order (tables within a level can run in parallel).
\if :{?mode} \else \set mode append \endif
\if :{?order} \else \set order random \endif
\if :{?plain_every} \else \set plain_every 0 \endif
\if :{?block} \else \set block 1000 \endif
\if :{?data} \else \set data seq \endif
WITH c AS (
  SELECT r.oid AS rel, s.nspname, r.relname, a.attnum, a.attname, a.attidentity <> '' AS ident,
         coalesce(a.attnum = ANY (pk.conkey), false) AS is_pk, array_position(pk.conkey, a.attnum) AS kpos,
         pg_temp.lfc_tag(obj_description(r.oid, 'pg_class'), 'n') AS nkey, pg_temp.lfc_tag(obj_description(r.oid, 'pg_class'), 'by') AS byc,
         CASE WHEN pg_temp.lfc_tag(col_description(r.oid, a.attnum), 'e') IS NOT NULL THEN            -- an expression, as is
           pg_temp.lfc_tag(col_description(r.oid, a.attnum), 'e')
              WHEN pg_temp.lfc_tag(col_description(r.oid, a.attnum), 'v') IS NOT NULL THEN            -- a spec formula for this column
           replace(pg_temp.lfc_cast(a.atttypid, pg_temp.lfc_mod(r.oid, a.attnum, a.atttypmod)), 'x.n', '(' || pg_temp.lfc_tag(col_description(r.oid, a.attnum), 'v') || ')')
              WHEN fk.confrelid IS NOT NULL AND NOT coalesce(a.attnum = ANY (pk.conkey), false) THEN   -- foreign key: inside the parent's current key range
           replace(pg_temp.lfc_cast(fk.ptyp, fk.pmod), 'x.n', format(
             '((SELECT %1$s FROM %3$I.%4$I ORDER BY %2$I LIMIT 1) + ((((%5$s %% 2147483647) * ((%5$s %% 2147483647 + %6$s) %% 2147483647)) %% 2147483647 + %7$s) %% 2147483647) %% ((SELECT %1$s FROM %3$I.%4$I ORDER BY %2$I DESC LIMIT 1) - (SELECT %1$s FROM %3$I.%4$I ORDER BY %2$I LIMIT 1) + 1))',
             pg_temp.lfc_to_int(fk.ptyp, fk.pmod, quote_ident(fk.pcol)), fk.pcol, fk.pns, fk.prel,
             CASE :'data' WHEN 'random' THEN 'x.r' ELSE 'x.n' END, 1 + floor(random() * 2147483645)::bigint, floor(random() * 2147483647)::bigint))
              WHEN :'data' = 'seq' OR coalesce(a.attnum = ANY (pk.conkey), false) OR EXISTS (SELECT 1 FROM pg_index u WHERE u.indrelid = r.oid AND u.indisunique AND u.indnkeyatts = 1
                                                  AND u.indkey[0] = a.attnum)          -- key and UNIQUE columns keep plain n
              THEN pg_temp.lfc_cast(a.atttypid, pg_temp.lfc_mod(r.oid, a.attnum, a.atttypmod))
              ELSE format(CASE WHEN :plain_every > 0 THEN format('CASE WHEN (x.n - 1) / %s %%%% %s = 0 THEN %%s ELSE %%s END', :block, :plain_every)
                               ELSE '%2$s' END, pg_temp.lfc_cast(a.atttypid, pg_temp.lfc_mod(r.oid, a.attnum, a.atttypmod)),
                   replace(pg_temp.lfc_cast(a.atttypid, pg_temp.lfc_mod(r.oid, a.attnum, a.atttypmod)), 'x.n',
                           format('((((%1$s %% 2147483647) * ((%1$s %% 2147483647 + %2$s) %% 2147483647)) %% 2147483647 + %3$s) %% 2147483647 + 1)',
                                  CASE :'data' WHEN 'random' THEN 'x.r' ELSE 'x.n' END,
                                  1 + floor(random() * 2147483645)::bigint, floor(random() * 2147483647)::bigint))) END AS expr,
         pg_temp.lfc_to_int(a.atttypid, a.atttypmod, quote_ident(a.attname)) AS to_int
  FROM pg_attribute a
  JOIN pg_class r ON r.oid = a.attrelid AND r.relkind = 'r'
  JOIN pg_namespace s ON s.oid = r.relnamespace AND s.nspname = :'schema'
  LEFT JOIN pg_constraint pk ON pk.conrelid = r.oid AND pk.contype = 'p'
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum      -- non-key identity and serial columns: the database numbers them
  LEFT JOIN LATERAL (                                                           -- a one-column foreign key to the parent's primary key
    SELECT f.confrelid, pa.attname AS pcol, pa.atttypid AS ptyp, pa.atttypmod AS pmod, ps.nspname AS pns, pr.relname AS prel
    FROM pg_constraint f
    JOIN pg_constraint ppk ON ppk.conrelid = f.confrelid AND ppk.contype = 'p' AND ppk.conkey = f.confkey
    JOIN pg_attribute pa ON pa.attrelid = f.confrelid AND pa.attnum = f.confkey[1]
    JOIN pg_class pr ON pr.oid = f.confrelid JOIN pg_namespace ps ON ps.oid = pr.relnamespace
    WHERE f.conrelid = r.oid AND f.contype = 'f' AND cardinality(f.conkey) = 1 AND f.conkey[1] = a.attnum LIMIT 1) fk ON true
  WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
    AND (coalesce(a.attnum = ANY (pk.conkey), false) OR (a.attidentity = '' AND NOT coalesce(pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%', false)))
), lv AS (                                                                       -- level: 0 for tables no FK points from, else 1 + parent's
  WITH RECURSIVE e AS (SELECT DISTINCT f.conrelid AS child, f.confrelid AS parent FROM pg_constraint f
                       JOIN pg_class r ON r.oid = f.conrelid JOIN pg_namespace s ON s.oid = r.relnamespace AND s.nspname = :'schema'
                       WHERE f.contype = 'f' AND f.conrelid <> f.confrelid),
  l(rel, lvl) AS (SELECT r.oid, 0 FROM pg_class r JOIN pg_namespace s ON s.oid = r.relnamespace AND s.nspname = :'schema' WHERE r.relkind = 'r'
                  UNION ALL SELECT e.child, l.lvl + 1 FROM e JOIN l ON l.rel = e.parent WHERE l.lvl < 50)
  SELECT rel, max(lvl) AS lvl FROM l GROUP BY rel
), tb AS (
  SELECT nspname, relname, max(lv.lvl) AS lvl,
         string_agg(quote_ident(attname), ', ' ORDER BY attnum) AS cols,
         CASE WHEN bool_or(ident) THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END AS ovr,
         string_agg(coalesce(expr, 'NULL'), ', ' ORDER BY attnum) AS exprs,
         CASE WHEN max(nkey) IS NOT NULL THEN max(nkey) WHEN count(*) FILTER (WHERE is_pk) = 1 THEN max(to_int) FILTER (WHERE is_pk) END AS pk_int,
         max(nkey) AS nkey, quote_ident(coalesce(max(byc), max(attname) FILTER (WHERE kpos = 1))) AS byc,
         max(attname) FILTER (WHERE is_pk) AS pk,
         substring(obj_description(c.rel, 'pg_class') from 'lfc:rows=([0-9]+)')::bigint AS target,
         coalesce(obj_description(c.rel, 'pg_class') ~ '(^|\s)lfc:fixed(\s|$)', false) AS fixed
  FROM c JOIN lv ON lv.rel = c.rel GROUP BY c.rel, nspname, relname
  HAVING max(nkey) IS NOT NULL OR (count(*) FILTER (WHERE is_pk) = 1 AND max(to_int) FILTER (WHERE is_pk) IS NOT NULL)
), pr AS (                                                                       -- the max and min of n: two index probes
  SELECT tb.*, CASE WHEN nkey IS NULL THEN format('(SELECT %s FROM %I.%I ORDER BY %I DESC LIMIT 1)', pk_int, nspname, relname, pk)
                    ELSE format('(SELECT max(%1$s) FROM %2$I.%3$I WHERE %4$s = (SELECT %4$s FROM %2$I.%3$I ORDER BY %4$s DESC LIMIT 1))', nkey, nspname, relname, byc) END AS hi_q,
               CASE WHEN nkey IS NULL THEN format('(SELECT %s FROM %I.%I ORDER BY %I LIMIT 1)', pk_int, nspname, relname, pk)
                    ELSE format('(SELECT min(%1$s) FROM %2$I.%3$I WHERE %4$s = (SELECT %4$s FROM %2$I.%3$I ORDER BY %4$s LIMIT 1))', nkey, nspname, relname, byc) END AS lo_q
  FROM tb
)
SELECT format('/* lfc level %s */ INSERT INTO %I.%I (%s)%s SELECT %s FROM (SELECT b.base + g.%s AS n%s FROM '
              '(SELECT coalesce(hi, 0) AS base, %s AS need FROM (SELECT %s AS hi, %s AS lo) m) b, %s%s OFFSET 0) x;',
         lvl, nspname, relname, cols, ovr, exprs,
         CASE WHEN :'order' = 'random' AND :'src' = 'seed' THEN 'r' ELSE 'i' END,
         CASE :'data' WHEN 'random' THEN ', floor(random() * 2147483647)::bigint AS r' ELSE '' END,
         CASE :'mode' WHEN 'fill' THEN format('least(%s, greatest(%s - coalesce(hi - lo + 1, 0), 0))', :rows, target) ELSE :'rows' END,
         hi_q, lo_q,
         CASE WHEN :'src' <> 'seed' THEN 'generate_series(1::bigint, b.need::bigint) g(i)'
              WHEN :'order' = 'random' THEN 'lfc_seed.seed g WHERE g.r <= b.need' ELSE 'lfc_seed.seed g WHERE g.i <= b.need' END,
         CASE :'order' WHEN 'asc' THEN ' ORDER BY g.i' WHEN 'desc' THEN ' ORDER BY g.i DESC'
                       ELSE CASE WHEN :'src' <> 'seed' THEN ' ORDER BY random()' ELSE '' END END)
FROM pr
WHERE (:'mode' <> 'fill' OR target IS NOT NULL) AND NOT (:'mode' <> 'fill' AND fixed)   -- an append skips a full table
ORDER BY lvl, relname;
