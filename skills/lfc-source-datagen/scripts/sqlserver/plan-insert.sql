-- { echo "DECLARE @schema sysname = N'dbo', @rows bigint = 1000000, @mode varchar(10) = 'fill', ...;"; cat cast.sql plan-insert.sql; } | sqlcmd -h -1 -y 0 | sed 's/$/\nGO/' > inserts.sql
-- Emits one INSERT per table in @schema that has a usable one-column primary key or an lfc:n tag (cast.sql), one per line.
-- Run each as its own batch (fill.sh adds GO after every line), so one that fails to compile doesn't stop the rest:
--   @mode = 'append': add @rows rows above the current max key.
--   @mode = 'fill': top the table up to its target (lfc:rows), at most @rows per statement; rerun until status shows
--     every table at its target.
-- Rows come from GENERATE_SERIES (SQL Server 2022+, database compatibility level 160): INSERT ... SELECT, nothing else
-- crosses the network. WITH (TABLOCK) lets an INSERT into an empty table be minimally logged (SIMPLE or BULK_LOGGED).
-- @order = 'asc' | 'desc' add ORDER BY; 'random' (default) adds none, since SQL Server sorts the rows by the clustered
--   key before inserting them anyway.
-- @data = 'seq' | 'scramble' | 'random', @plain_every, @block: as in the Postgres and MySQL planners (key, UNIQUE,
--   foreign-key, lfc:v and lfc:e columns always keep their own value).
-- Foreign keys (cast.sql) take values inside the parent's current key range. Each statement starts with /* lfc level N */:
--   parents are level 0, a table referencing them is one level higher; run the levels in order.
WITH e AS (SELECT DISTINCT f.parent_object_id AS child, f.referenced_object_id AS parent FROM sys.foreign_keys f
           WHERE OBJECT_SCHEMA_NAME(f.parent_object_id) = @schema AND f.parent_object_id <> f.referenced_object_id),
l AS (SELECT t.object_id AS oid, 0 AS lvl FROM sys.tables t WHERE t.schema_id = SCHEMA_ID(@schema)
      UNION ALL SELECT e.child, l.lvl + 1 FROM e JOIN l ON l.oid = e.parent WHERE l.lvl < 50),
lv AS (SELECT oid, MAX(lvl) AS lvl FROM l GROUP BY oid),
k AS (SELECT c.oid, MAX(CAST(c.ident AS int) * c.is_pk) AS ident,
             STRING_AGG(CAST(QUOTENAME(c.col) AS nvarchar(max)), ', ') WITHIN GROUP (ORDER BY c.pos) AS cols,
             STRING_AGG(COALESCE(IIF(c.is_unique = 1 OR c.is_pk = 1 OR c.fk_tbl IS NOT NULL OR c.v IS NOT NULL OR c.e IS NOT NULL OR COALESCE(@data, 'seq') = 'seq', c.expr0,
                 CONCAT(IIF(COALESCE(@plain_every, 0) > 0, CONCAT('CASE WHEN (x.n - 1) / ', COALESCE(@block, 1000), ' % ', @plain_every, ' = 0 THEN ', c.expr0, ' ELSE '), ''),
                        REPLACE(c.expr0, 'x.n', CONCAT('((((', IIF(@data = 'random', 'x.r', 'x.n'), ' % 2147483647) * ((', IIF(@data = 'random', 'x.r', 'x.n'), ' % 2147483647 + ',
                          ABS(CHECKSUM(NEWID())) % 2147483645 + 1, ') % 2147483647)) % 2147483647 + ', ABS(CHECKSUM(NEWID())) % 2147483647, ') % 2147483647 + 1)')),
                        IIF(COALESCE(@plain_every, 0) > 0, ' END', ''))), CAST('NULL' AS nvarchar(max))), ', ') WITHIN GROUP (ORDER BY c.pos) AS exprs
      FROM #lfc_cols c GROUP BY c.oid),
q AS (SELECT kk.*, k.cols, k.exprs, k.ident, lv.lvl, CONCAT(QUOTENAME(@schema), '.', QUOTENAME(kk.tbl)) AS t,
             IIF(kk.has_n = 1,
                 CONCAT('(SELECT MAX(', kk.pk_int, ') FROM ', QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' WHERE ', kk.pk, ' = (SELECT TOP (1) ', kk.pk, ' FROM ',
                        QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' ORDER BY ', kk.pk, ' DESC))'),
                 CONCAT('(SELECT TOP (1) ', kk.pk_int, ' FROM ', QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' ORDER BY ', kk.pk, ' DESC)')) AS hi_q,
             IIF(kk.has_n = 1,
                 CONCAT('(SELECT MIN(', kk.pk_int, ') FROM ', QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' WHERE ', kk.pk, ' = (SELECT TOP (1) ', kk.pk, ' FROM ',
                        QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' ORDER BY ', kk.pk, '))'),
                 CONCAT('(SELECT TOP (1) ', kk.pk_int, ' FROM ', QUOTENAME(@schema), '.', QUOTENAME(kk.tbl), ' ORDER BY ', kk.pk, ')')) AS lo_q
      FROM #lfc_keys kk JOIN k ON k.oid = kk.oid JOIN lv ON lv.oid = kk.oid)
SELECT CONCAT(CAST('/* lfc level ' AS nvarchar(max)), lvl, ' */ ', IIF(ident = 1, CONCAT('SET IDENTITY_INSERT ', t, ' ON; '), ''),
              'INSERT INTO ', t, ' WITH (TABLOCK) (', cols, ') SELECT ', exprs, ' FROM (SELECT b.base + g.value AS n',
              IIF(@data = 'random', ', CAST(ABS(CHECKSUM(NEWID())) AS bigint) AS r', ''),
              ' FROM (SELECT COALESCE(hi, 0) AS base, ',
              IIF(@mode = 'fill', CONCAT('LEAST(', @rows, ', GREATEST(', target, ' - COALESCE(hi - lo + 1, 0), 0))'), CAST(@rows AS varchar(20))),
              ' AS need FROM (SELECT ', hi_q, ' AS hi, ', lo_q, ' AS lo) m) b CROSS APPLY GENERATE_SERIES(CAST(1 AS bigint), CAST(b.need AS bigint), CAST(1 AS bigint)) g) x',
              CASE COALESCE(@order, 'random') WHEN 'asc' THEN ' ORDER BY x.n' WHEN 'desc' THEN ' ORDER BY x.n DESC' ELSE '' END, ';',
              IIF(ident = 1, CONCAT(' SET IDENTITY_INSERT ', t, ' OFF;'), ''))
FROM q
WHERE (@mode <> 'fill' OR target IS NOT NULL) AND NOT (@mode <> 'fill' AND fixed = 1)   -- an append skips a full table
ORDER BY lvl, tbl;
