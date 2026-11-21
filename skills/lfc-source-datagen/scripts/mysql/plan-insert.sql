-- { echo "SET @db='lfc', @rows=10000, @src='series', @mode='append'"; cat cast.sql plan-insert.sql; } | mysql -N -B -r > inserts.sql
-- Emits one INSERT per table in @db with a usable one-column primary key (cast.sql builds lfc_cols first).
--   @mode='append': add @rows rows above the current max key.
--   @mode='fill': top the table up to the row count in its comment ('lfc:rows=<n>'), at most @rows per statement.
-- @src='series' uses a recursive CTE (keep @rows <= 100000); @src='seed' reads lfc_seed.seed.
-- @order='random' (default) | 'asc' | 'desc': the order the new keys are inserted in (always exactly max+1 .. max+need).
-- @data='seq' | 'scramble' | 'random': non-key columns from n; from n scrambled per column, (m * ((m + a) mod p) + b) mod p
--   with m = n mod p, p = 2^31 - 1, a and b drawn once per column at plan time (same rows on every run); or from one RAND()
--   value per row, scrambled per column. Unset @data means 'seq'. Key and UNIQUE columns always use plain n.
-- Foreign keys (cast.sql) take values inside the parent's current key range. Each statement starts with /* lfc level N */:
--   parents are level 0, a table referencing them is one level higher; run the levels in order.
-- Tables with a composite key or none qualify when their comment has lfc:n={...}, and a column's lfc:v={...} replaces n in
--   its cast (cast.sql); each INSERT then probes the max of n among the rows with the highest lfc:by column.
-- @plain_every=k, @block=B (unset or 0 is off): every k-th block of B keys ((n - 1) DIV B % k = 0) keeps plain values in
--   every column whatever @data is, so those rows can be audited from the key alone (plan-audit.sql).
WITH RECURSIVE e AS (SELECT DISTINCT table_name AS child, referenced_table_name AS parent FROM information_schema.key_column_usage
                     WHERE table_schema = @db AND referenced_table_schema = @db AND referenced_table_name <> table_name),
l (tbl, lvl) AS (SELECT table_name, 0 FROM information_schema.tables WHERE table_schema = @db
                 UNION ALL SELECT e.child, l.lvl + 1 FROM e JOIN l ON l.tbl = e.parent WHERE l.lvl < 50),
lv AS (SELECT tbl, MAX(lvl) AS lvl FROM l GROUP BY tbl)
SELECT CONCAT('/* lfc level ', lv.lvl, ' */ ', IF(@src = 'seed', '', CONCAT('SET SESSION cte_max_recursion_depth = ', GREATEST(@rows, 1000), '; ')),
         'INSERT INTO `', k.db, '`.`', k.tbl, '` (', k.cols, ') ',
         IF(@src = 'seed', '', CONCAT('WITH RECURSIVE g(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM g WHERE i < ', @rows, ') ')),
         'SELECT /*+ NO_MERGE(x) */ ', k.exprs, ' FROM (SELECT b.base + g.', IF(COALESCE(@order, 'random') = 'random' AND @src = 'seed', 'r', 'i'), ' AS n',
         IF(@data = 'random', ', FLOOR(RAND() * 2147483647) AS r', ''), ' FROM ',
         '(SELECT COALESCE(hi, 0) AS base, ',
         IF(@mode = 'fill', CONCAT('LEAST(', @rows, ', GREATEST(', t.target, ' - COALESCE(hi - lo + 1, 0), 0))'), @rows), ' AS need FROM ',
         IF(kk.has_n,
            CONCAT('(SELECT (SELECT MAX(', kk.pk_int, ') FROM `', k.db, '`.`', k.tbl, '` WHERE `', kk.pk, '` = (SELECT `', kk.pk, '` FROM `', k.db, '`.`', k.tbl,
                   '` ORDER BY `', kk.pk, '` DESC LIMIT 1)) AS hi, (SELECT MIN(', kk.pk_int, ') FROM `', k.db, '`.`', k.tbl, '` WHERE `', kk.pk, '` = (SELECT `',
                   kk.pk, '` FROM `', k.db, '`.`', k.tbl, '` ORDER BY `', kk.pk, '` LIMIT 1)) AS lo) m) b, '),
            CONCAT('(SELECT (SELECT ', kk.pk_int, ' FROM `', k.db, '`.`', k.tbl, '` ORDER BY `', kk.pk, '` DESC LIMIT 1) AS hi, ',
                   '(SELECT ', kk.pk_int, ' FROM `', k.db, '`.`', k.tbl, '` ORDER BY `', kk.pk, '` LIMIT 1) AS lo) m) b, ')),
         IF(@src = 'seed', 'lfc_seed.seed g', 'g'), ' WHERE g.', IF(COALESCE(@order, 'random') = 'random' AND @src = 'seed', 'r', 'i'), ' <= b.need',
         CASE COALESCE(@order, 'random') WHEN 'asc' THEN ' ORDER BY g.i' WHEN 'desc' THEN ' ORDER BY g.i DESC'
              ELSE IF(@src <> 'seed', ' ORDER BY RAND()', '') END, ') x;')
FROM (SELECT db, tbl, GROUP_CONCAT(CONCAT('`', col, '`') ORDER BY pos SEPARATOR ', ') AS cols,
             GROUP_CONCAT(COALESCE(IF(is_unique OR fk_tbl IS NOT NULL OR v IS NOT NULL OR e IS NOT NULL OR COALESCE(@data, 'seq') = 'seq', expr0,
                 CONCAT(IF(COALESCE(@plain_every, 0) > 0, CONCAT('CASE WHEN (x.n - 1) DIV ', COALESCE(@block, 1000), ' % ', @plain_every,
                                                                 ' = 0 THEN ', expr0, ' ELSE '), ''),
                 REPLACE(expr0, 'x.n', CONCAT('((((', @v := IF(@data = 'random', 'x.r', 'x.n'), ' % 2147483647) * ((', @v, ' % 2147483647 + ',
                   FLOOR(1 + RAND() * 2147483645), ') % 2147483647)) % 2147483647 + ', FLOOR(RAND() * 2147483647), ') % 2147483647 + 1)')),
                 IF(COALESCE(@plain_every, 0) > 0, ' END', ''))), 'NULL')
                 ORDER BY pos SEPARATOR ', ') AS exprs
      FROM lfc_cols GROUP BY db, tbl) k
JOIN lfc_keys kk ON kk.db = k.db AND kk.tbl = k.tbl
JOIN (SELECT table_schema, table_name, CAST(NULLIF(REGEXP_SUBSTR(table_comment, '(?<=lfc:rows=)[0-9]+'), '') AS UNSIGNED) AS target,
             CONCAT(' ', table_comment, ' ') LIKE '% lfc:fixed %' AS fixed
      FROM information_schema.tables WHERE table_schema = @db) t ON t.table_schema = k.db AND t.table_name = k.tbl
JOIN lv ON lv.tbl = k.tbl
WHERE (@mode <> 'fill' OR t.target IS NOT NULL) AND NOT (@mode <> 'fill' AND t.fixed)   -- an append skips a full table
ORDER BY lv.lvl, k.tbl;
