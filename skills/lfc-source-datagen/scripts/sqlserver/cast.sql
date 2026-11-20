-- Builds the session's temp tables the SQL Server planners read: #lfc_tag (every table's and column's lfc tags, from
-- lfc_seed.tags, which statschema's ddl.py fills), #lfc_cols (for every column of every table in @schema that qualifies,
-- the expression that turns x.n (bigint, any n >= 1) into a valid value, and for the key, the reverse), and #lfc_keys
-- (each table's key probe). Run after the parameters, in the same batch:
--   DECLARE @schema sysname = N'dbo', @rows bigint = 1000000, @mode varchar(10) = 'fill', @order varchar(10) = 'random',
--           @data varchar(10) = 'scramble', @plain_every int = 4, @block int = 1000, @lob_len int = 100;
-- (fill.sh writes them). Non-key identity, computed, and rowversion columns are left to the database.
SET NOCOUNT ON;
-- Tags, in braces: a column's lfc:v={integer expression in x.n} replaces n in its cast; lfc:e={SQL} is its value as is;
-- lfc:len={L} its string length; a table's lfc:rows=N is its target, lfc:n={expression of its columns} gives n back from a
-- row (composite keys, tables without a key), and lfc:by={column} names the column n rises with.
IF OBJECT_ID('tempdb..#lfc_raw') IS NOT NULL DROP TABLE #lfc_raw;
IF OBJECT_ID('tempdb..#lfc_tag') IS NOT NULL DROP TABLE #lfc_tag;
CREATE TABLE #lfc_raw (tbl sysname, col sysname, tag nvarchar(max));
CREATE TABLE #lfc_tag (tbl sysname, col sysname, k varchar(16), val nvarchar(max));
IF OBJECT_ID('lfc_seed.tags') IS NOT NULL      -- (a query naming a missing table fails when it runs, so only then)
  EXEC sp_executesql N'INSERT INTO #lfc_raw SELECT tbl, col, tag FROM lfc_seed.tags WHERE sch = @s', N'@s sysname', @s = @schema;
-- split on 'lfc:' (formulas never contain it): 'v={...}' -> (v, ...), 'rows=5' -> (rows, 5), 'added' -> (added, '')
INSERT INTO #lfc_tag
SELECT r.tbl, r.col, p.k, CASE WHEN LEFT(p.v, 1) = '{' THEN SUBSTRING(p.v, 2, LEN(p.v) - 2) ELSE p.v END
FROM #lfc_raw r
CROSS APPLY STRING_SPLIT(REPLACE(r.tag, N'lfc:', NCHAR(1)), NCHAR(1)) s
CROSS APPLY (SELECT LTRIM(RTRIM(s.value)) AS t) a
CROSS APPLY (SELECT LEFT(a.t, CHARINDEX('=', a.t + '=') - 1) AS k, SUBSTRING(a.t, CHARINDEX('=', a.t + '=') + 1, LEN(a.t)) AS v) p
WHERE a.t <> '';
-- #lfc_tabs: the tables that qualify (a one-column primary key, or lfc:n), with their key formula and target
IF OBJECT_ID('tempdb..#lfc_tabs') IS NOT NULL DROP TABLE #lfc_tabs;
SELECT t.object_id AS oid, t.name AS tbl,
       (SELECT val FROM #lfc_tag g WHERE g.tbl = t.name AND g.col = '' AND g.k = 'n') AS nkey,
       (SELECT val FROM #lfc_tag g WHERE g.tbl = t.name AND g.col = '' AND g.k = 'by') AS byc,
       TRY_CAST((SELECT val FROM #lfc_tag g WHERE g.tbl = t.name AND g.col = '' AND g.k = 'rows') AS bigint) AS target,
       (SELECT COUNT(*) FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.object_id = t.object_id AND i.is_primary_key = 1) AS npk,
       (SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = t.object_id AND i.is_primary_key = 1 AND ic.key_ordinal = 1) AS pk1
INTO #lfc_tabs
FROM sys.tables t WHERE t.schema_id = SCHEMA_ID(@schema);
DELETE FROM #lfc_tabs WHERE npk <> 1 AND nkey IS NULL;
IF OBJECT_ID('tempdb..#lfc_cols') IS NOT NULL DROP TABLE #lfc_cols;
SELECT tb.oid, tb.tbl, c.name AS col, c.column_id AS pos, c.is_identity AS ident,
       CASE WHEN c.system_type_id = 240 THEN TYPE_NAME(c.user_type_id) ELSE TYPE_NAME(c.system_type_id) END AS ty,
       c.precision AS prec, c.scale,
       CASE WHEN TYPE_NAME(c.system_type_id) IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN c.max_length / 2 ELSE c.max_length END AS clen,
       CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                         WHERE i.object_id = c.object_id AND i.is_primary_key = 1 AND ic.column_id = c.column_id) THEN 1 ELSE 0 END AS is_pk,
       CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                         WHERE i.object_id = c.object_id AND i.is_unique = 1 AND ic.column_id = c.column_id AND ic.key_ordinal > 0
                           AND (SELECT COUNT(*) FROM sys.index_columns k WHERE k.object_id = i.object_id AND k.index_id = i.index_id AND k.key_ordinal > 0) = 1)
            THEN 1 ELSE 0 END AS is_unique,
       fk.ptbl AS fk_tbl,
       (SELECT val FROM #lfc_tag g WHERE g.tbl = tb.tbl AND g.col = c.name AND g.k = 'v') AS v,
       (SELECT val FROM #lfc_tag g WHERE g.tbl = tb.tbl AND g.col = c.name AND g.k = 'e') AS e,
       TRY_CAST((SELECT val FROM #lfc_tag g WHERE g.tbl = tb.tbl AND g.col = c.name AND g.k = 'len') AS int) AS lenv,
       CAST(NULL AS int) AS len, CAST(NULL AS nvarchar(max)) AS expr0, CAST(NULL AS nvarchar(max)) AS to_int
INTO #lfc_cols
FROM #lfc_tabs tb
JOIN sys.columns c ON c.object_id = tb.oid
OUTER APPLY (                                          -- a one-column foreign key to the parent's one-column primary key
  SELECT TOP (1) OBJECT_NAME(f.referenced_object_id) AS ptbl
  FROM sys.foreign_keys f JOIN sys.foreign_key_columns fc ON fc.constraint_object_id = f.object_id
  WHERE f.parent_object_id = c.object_id AND fc.parent_column_id = c.column_id
    AND (SELECT COUNT(*) FROM sys.foreign_key_columns x WHERE x.constraint_object_id = f.object_id) = 1) fk
WHERE c.is_computed = 0 AND TYPE_NAME(c.system_type_id) <> 'timestamp'
  AND NOT (c.is_identity = 1 AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id WHERE i.object_id = c.object_id AND i.is_primary_key = 1 AND ic.column_id = c.column_id));
-- length to generate: the declared length, or @lob_len (at least 38) for max and text types (or lfc:len={L})
UPDATE #lfc_cols SET len = COALESCE(lenv, CASE WHEN clen = -1 OR ty IN ('text', 'ntext') THEN GREATEST(COALESCE(@lob_len, 100), 38)
                                               ELSE clen END)
WHERE ty IN ('char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext', 'binary', 'varbinary', 'image');
-- a fixed 19-digit zero-padded n, its MD5 in lower-case hex, n's 8 bytes, and n's MD5 bytes
DECLARE @s19 nvarchar(200) = N'RIGHT(REPLICATE(''0'', 19) + CAST(x.n AS varchar(20)), 19)',
        @md5 nvarchar(200) = N'LOWER(CONVERT(varchar(32), HASHBYTES(''MD5'', CAST(x.n AS varchar(20))), 2))',
        @n8 nvarchar(200) = N'CAST(x.n AS binary(8))';
UPDATE #lfc_cols SET expr0 = CASE ty
    -- integers: wrapped to the type's positive range (tinyint is unsigned)
    WHEN 'bit'      THEN 'CAST(x.n % 2 AS bit)'
    WHEN 'tinyint'  THEN 'x.n % 255'
    WHEN 'smallint' THEN 'x.n % 32767'
    WHEN 'int'      THEN 'x.n % 2147483647'
    WHEN 'bigint'   THEN 'x.n'
    -- exact numeric: p digits of n times an exact 10^-s
    WHEN 'decimal'  THEN CONCAT('CAST((x.n % 1', REPLICATE('0', LEAST(prec, 18)), ')', IIF(scale > 0, CONCAT(' * 0.', REPLICATE('0', scale - 1), '1'), ''),
                                ' AS decimal(', prec, ',', scale, '))')
    WHEN 'numeric'  THEN CONCAT('CAST((x.n % 1', REPLICATE('0', LEAST(prec, 18)), ')', IIF(scale > 0, CONCAT(' * 0.', REPLICATE('0', scale - 1), '1'), ''),
                                ' AS numeric(', prec, ',', scale, '))')
    WHEN 'money'      THEN 'CAST((x.n % 100000000000) / 100.0 AS money)'
    WHEN 'smallmoney' THEN 'CAST((x.n % 20000000) / 100.0 AS smallmoney)'
    WHEN 'float'    THEN 'CAST(x.n AS float)'
    WHEN 'real'     THEN 'CAST(x.n AS real)'
    -- date/time: 1970-01-01 plus n days or seconds (DATEADD takes an int, so seconds split into days and seconds)
    WHEN 'date'     THEN 'DATEADD(day, CAST(x.n % 2932897 AS int), CAST(''1970-01-01'' AS date))'
    WHEN 'datetime2' THEN 'DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST(''1970-01-01'' AS datetime2(0))))'
    WHEN 'datetime' THEN 'DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST(''1970-01-01'' AS datetime2(0))))'
    WHEN 'datetimeoffset' THEN 'TODATETIMEOFFSET(DATEADD(second, CAST(x.n % 86400 AS int), DATEADD(day, CAST((x.n / 86400) % 2932897 AS int), CAST(''1970-01-01'' AS datetime2(0)))), ''+00:00'')'
    WHEN 'smalldatetime' THEN 'DATEADD(minute, CAST(x.n % 94063680 AS int), CAST(''1900-01-01'' AS smalldatetime))'
    WHEN 'time'     THEN 'CAST(DATEADD(second, CAST(x.n % 86400 AS int), CAST(''00:00'' AS time(0))) AS time)'
    -- strings: the padded n first (so a key maps back to n) and, from 38 characters, last; MD5 filler between; to the length
    WHEN 'char' THEN 's' WHEN 'varchar' THEN 's' WHEN 'nchar' THEN 's' WHEN 'nvarchar' THEN 's' WHEN 'text' THEN 's' WHEN 'ntext' THEN 's'
    -- binary: n's 8 bytes first and (from 16 bytes) last, MD5 between
    WHEN 'binary' THEN 'b' WHEN 'varbinary' THEN 'b' WHEN 'image' THEN 'b'
    -- n in a uuid's last 6 bytes, which SQL Server sorts first: keys stay in n order below 2^48
    WHEN 'uniqueidentifier' THEN 'CAST(''00000000-0000-0000-'' + LEFT(CONVERT(varchar(16), CAST(x.n AS binary(8)), 2), 4) + ''-'' + RIGHT(CONVERT(varchar(16), CAST(x.n AS binary(8)), 2), 12) AS uniqueidentifier)'
    WHEN 'xml'      THEN 'CAST(''<n>'' + CAST(x.n AS varchar(20)) + ''</n>'' AS xml)'
    WHEN 'sql_variant' THEN 'CAST(x.n AS sql_variant)'
    WHEN 'geometry' THEN 'geometry::Point(x.n % 1000, x.n % 997, 0)'
    WHEN 'geography' THEN 'geography::Point(x.n % 90, x.n % 180, 4326)'
    WHEN 'hierarchyid' THEN 'hierarchyid::Parse(''/'' + CAST(x.n % 2147483647 AS varchar(20)) + ''/'')'
  END,
  to_int = CASE WHEN ty IN ('tinyint', 'smallint', 'int', 'bigint') OR (ty IN ('decimal', 'numeric') AND scale = 0) THEN CONCAT('CAST(', QUOTENAME(col), ' AS bigint)')
                WHEN ty IN ('char', 'varchar', 'nchar', 'nvarchar') AND clen >= 19 THEN CONCAT('CAST(LEFT(', QUOTENAME(col), ', 19) AS bigint)') END;
UPDATE #lfc_cols SET expr0 = CASE WHEN len < 19 THEN CONCAT('RIGHT(', @s19, ', ', len, ')')
                                  WHEN len < 38 THEN CONCAT('LEFT(', @s19, ' + ', @md5, ', ', len, ')')
                                  ELSE CONCAT(@s19, ' + LEFT(REPLICATE(CAST(', @md5, ' AS varchar(max)), ', GREATEST(CEILING((len - 38) / 32.0), 1), '), ', len - 38, ') + ', @s19) END
WHERE expr0 = 's';
UPDATE #lfc_cols SET expr0 = CASE WHEN len < 16 THEN CONCAT('SUBSTRING(', @n8, ' + HASHBYTES(''MD5'', CAST(x.n AS varchar(20))), 1, ', len, ')')
                                  ELSE CONCAT(@n8, ' + SUBSTRING(CAST(REPLICATE(CAST(', @md5, ' AS varchar(max)), ', GREATEST(CEILING((len - 16) / 32.0), 1),
                                              ') AS varbinary(max)), 1, ', len - 16, ') + ', @n8) END
WHERE expr0 = 'b';
-- a column with an expression (lfc:e): the expression as is; with a formula (lfc:v): its cast of the formula
UPDATE #lfc_cols SET expr0 = e WHERE e IS NOT NULL;
UPDATE #lfc_cols SET expr0 = REPLACE(expr0, 'x.n', CONCAT('(', v, ')')) WHERE v IS NOT NULL AND e IS NULL;
-- each table's key: the column to probe, n from a row (the key decoded, or lfc:n), and the cast from n for a one-column key
IF OBJECT_ID('tempdb..#lfc_keys') IS NOT NULL DROP TABLE #lfc_keys;
SELECT tb.oid, tb.tbl, tb.target, COALESCE(tb.byc, QUOTENAME(tb.pk1)) AS pk, COALESCE(tb.nkey, c.to_int) AS pk_int,
       IIF(tb.npk = 1, c.expr0, NULL) AS pk_expr, IIF(tb.nkey IS NULL, 0, 1) AS has_n
INTO #lfc_keys
FROM #lfc_tabs tb LEFT JOIN #lfc_cols c ON c.oid = tb.oid AND c.col = tb.pk1
WHERE tb.nkey IS NOT NULL OR c.to_int IS NOT NULL;
UPDATE #lfc_keys SET pk = QUOTENAME(pk) WHERE LEFT(pk, 1) <> '[';
-- foreign keys: parent_min + scramble(n) % (parent_max - parent_min + 1), cast to the parent key's type, with the parent's
-- range probed when the INSERT runs, so every value is an existing key
UPDATE c SET c.expr0 = REPLACE(k.pk_expr, 'x.n', CONCAT(
      '((SELECT TOP (1) ', k.pk_int, ' FROM ', QUOTENAME(@schema), '.', QUOTENAME(k.tbl), ' ORDER BY ', k.pk, ') + ((((x.n % 2147483647) * ((x.n % 2147483647 + ',
      ABS(CHECKSUM(NEWID())) % 2147483645 + 1, ') % 2147483647)) % 2147483647 + ', ABS(CHECKSUM(NEWID())) % 2147483647, ') % 2147483647) % ((SELECT TOP (1) ', k.pk_int,
      ' FROM ', QUOTENAME(@schema), '.', QUOTENAME(k.tbl), ' ORDER BY ', k.pk, ' DESC) - (SELECT TOP (1) ', k.pk_int, ' FROM ', QUOTENAME(@schema), '.', QUOTENAME(k.tbl),
      ' ORDER BY ', k.pk, ') + 1))'))
FROM #lfc_cols c JOIN #lfc_keys k ON k.tbl = c.fk_tbl AND k.pk_expr IS NOT NULL
WHERE c.v IS NULL AND c.e IS NULL AND c.is_pk = 0;
