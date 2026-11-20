-- Builds the session table lfc_cols: for every column of every table in @db with a one-column primary key, the SQL
-- expression that turns x.n (BIGINT, any n >= 1) into a valid value in strict mode, and for the key, the reverse.
-- Non-key AUTO_INCREMENT and generated columns are left out; is_unique marks columns that must keep plain n.
-- The insert, status, and boundary queries all read lfc_cols; it disappears when the session ends.
SET SESSION group_concat_max_len = 16777216;
-- Tags longer than a MySQL comment holds (1,024 characters for a column) live in lfc_seed.tags, one row per table ('') or
-- column; a table's or column's tags are its comment and that row together.
CREATE DATABASE IF NOT EXISTS lfc_seed;
CREATE TABLE IF NOT EXISTS lfc_seed.tags (db VARCHAR(64), tbl VARCHAR(64), col VARCHAR(64), tag MEDIUMTEXT, PRIMARY KEY (db, tbl, col));
-- Tags in comments, in braces: a column's lfc:v={integer expression in x.n} replaces n in its cast (spec formulas: sparse
-- keys, composite keys, value ranges); a table's lfc:n={expression of its columns} gives n back from a row, for composite
-- keys or tables without a primary key, and lfc:by={column} names the column n rises with (default: the first key column).
-- lfc_tabs: the tables that qualify (a one-column primary key, or lfc:n), with their key formula.
DROP TEMPORARY TABLE IF EXISTS lfc_tabs;
CREATE TEMPORARY TABLE lfc_tabs AS
SELECT t.table_schema AS db, t.table_name AS tbl, NULLIF(REGEXP_SUBSTR(CONCAT(t.table_comment, ' ', COALESCE((SELECT g.tag FROM lfc_seed.tags g WHERE g.db = t.table_schema AND g.tbl = t.table_name AND g.col = ''), '')), '(?<=lfc:n=\\{)[^}]*'), '') AS nkey,
       NULLIF(REGEXP_SUBSTR(CONCAT(t.table_comment, ' ', COALESCE((SELECT g.tag FROM lfc_seed.tags g WHERE g.db = t.table_schema AND g.tbl = t.table_name AND g.col = ''), '')), '(?<=lfc:by=\\{)[^}]*'), '') AS byc,
       (SELECT COUNT(*) FROM information_schema.statistics s WHERE s.table_schema = t.table_schema AND s.table_name = t.table_name
          AND s.index_name = 'PRIMARY') AS npk,
       (SELECT s.column_name FROM information_schema.statistics s WHERE s.table_schema = t.table_schema AND s.table_name = t.table_name
          AND s.index_name = 'PRIMARY' AND s.seq_in_index = 1) AS pk1
FROM information_schema.tables t WHERE t.table_schema = @db AND t.table_type = 'BASE TABLE'
HAVING npk = 1 OR nkey IS NOT NULL;
DROP TEMPORARY TABLE IF EXISTS lfc_cols;
CREATE TEMPORARY TABLE lfc_cols AS
SELECT c.table_schema AS db, c.table_name AS tbl, c.column_name AS col, c.ordinal_position AS pos, c.column_key = 'PRI' AS is_pk,
  c.column_key IN ('PRI', 'UNI') AS is_unique, fk.referenced_table_schema AS fk_db, fk.referenced_table_name AS fk_tbl,
  c.cm LIKE '%lfc:added%' AS added, NULLIF(REGEXP_SUBSTR(c.cm, '(?<=lfc:v=\\{)[^}]*'), '') AS v,
  NULLIF(REGEXP_SUBSTR(c.cm, '(?<=lfc:e=\\{)[^}]*'), '') AS e,
  CASE c.data_type
    -- integers (signed / unsigned)
    WHEN 'tinyint'   THEN IF(c.column_type LIKE '%unsigned%', 'x.n % 255', 'x.n % 127')
    WHEN 'smallint'  THEN IF(c.column_type LIKE '%unsigned%', 'x.n % 65535', 'x.n % 32767')
    WHEN 'mediumint' THEN IF(c.column_type LIKE '%unsigned%', 'x.n % 16777215', 'x.n % 8388607')
    WHEN 'int'       THEN IF(c.column_type LIKE '%unsigned%', 'x.n % 4294967295', 'x.n % 2147483647')
    WHEN 'bigint'    THEN 'x.n'
    -- exact numeric: p digits of n times an exact 10^-s (no division: it would round to 4 decimals)
    WHEN 'decimal'   THEN CONCAT('CAST((x.n % 1', REPEAT('0', LEAST(c.numeric_precision, 18)), ')',
                                 IF(c.numeric_scale > 0, CONCAT(' * 0.', REPEAT('0', c.numeric_scale - 1), '1'), ''),
                                 ' AS DECIMAL(', c.numeric_precision, ',', c.numeric_scale, '))')
    WHEN 'float'     THEN 'CAST(x.n AS FLOAT)'
    WHEN 'double'    THEN 'CAST(x.n AS DOUBLE)'
    WHEN 'bit'       THEN IF(c.numeric_precision = 64, 'x.n', CONCAT('x.n % ', CAST(POW(2, c.numeric_precision) AS UNSIGNED)))
    -- date/time: epoch plus n days or seconds, wrapped to stay inside the type's range
    WHEN 'date'      THEN 'DATE_ADD(''1970-01-01'', INTERVAL x.n % 2932897 DAY)'
    WHEN 'datetime'  THEN 'DATE_ADD(''1970-01-01 00:00:00'', INTERVAL x.n % 253402300800 SECOND)'
    WHEN 'timestamp' THEN 'FROM_UNIXTIME(1 + x.n % 2147483646)'
    WHEN 'time'      THEN 'SEC_TO_TIME(x.n % 86400)'
    WHEN 'year'      THEN '1901 + x.n % 255'
    -- strings: 19-digit zero-padded n first (so a key maps back to n), then MD5 filler, cut to the length
    WHEN 'char' THEN 's' WHEN 'varchar' THEN 's' WHEN 'tinytext' THEN 's' WHEN 'text' THEN 's'
    WHEN 'mediumtext' THEN 's' WHEN 'longtext' THEN 's'
    -- binary: 8-byte big-endian n at the start and (from 16 bytes) the end, MD5 bytes between, to the length
    WHEN 'binary' THEN 'b' WHEN 'varbinary' THEN 'b' WHEN 'tinyblob' THEN 'b' WHEN 'blob' THEN 'b'
    WHEN 'mediumblob' THEN 'b' WHEN 'longblob' THEN 'b'
    -- other scalars
    WHEN 'json'      THEN 'JSON_OBJECT(''n'', x.n)'
    WHEN 'enum'      THEN CONCAT('1 + x.n % ', LENGTH(c.column_type) - LENGTH(REPLACE(c.column_type, ',', '')) + 1)
    WHEN 'set'       THEN CONCAT('x.n % ', CAST(POW(2, LENGTH(c.column_type) - LENGTH(REPLACE(c.column_type, ',', '')) + 1) AS UNSIGNED))
    -- spatial: coordinates 0..89, valid as latitude and longitude, in the column's SRID
    WHEN 'point'      THEN CONCAT('ST_GeomFromText(CONCAT(''POINT('', x.n % 90, '' '', x.n % 89, '')''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'geometry'   THEN CONCAT('ST_GeomFromText(CONCAT(''POINT('', x.n % 90, '' '', x.n % 89, '')''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'linestring' THEN CONCAT('ST_GeomFromText(CONCAT(''LINESTRING(0 0, '', 1 + x.n % 89, '' '', 1 + x.n % 89, '')''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'polygon'    THEN CONCAT('ST_GeomFromText(CONCAT(''POLYGON((0 0, '', 1 + x.n % 89, '' 0, '', 1 + x.n % 89, '' '', 1 + x.n % 89, '', 0 '', 1 + x.n % 89, '', 0 0))''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'multipoint' THEN CONCAT('ST_GeomFromText(CONCAT(''MULTIPOINT(0 0, '', x.n % 90, '' '', x.n % 89, '')''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'multilinestring' THEN CONCAT('ST_GeomFromText(CONCAT(''MULTILINESTRING((0 0, '', 1 + x.n % 89, '' '', 1 + x.n % 89, ''))''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'multipolygon' THEN CONCAT('ST_GeomFromText(CONCAT(''MULTIPOLYGON(((0 0, '', 1 + x.n % 89, '' 0, '', 1 + x.n % 89, '' '', 1 + x.n % 89, '', 0 '', 1 + x.n % 89, '', 0 0)))''), ', COALESCE(c.srs_id, 0), ')')
    WHEN 'geomcollection' THEN CONCAT('ST_GeomFromText(CONCAT(''GEOMETRYCOLLECTION(POINT('', x.n % 90, '' '', x.n % 89, ''))''), ', COALESCE(c.srs_id, 0), ')')
  END AS expr0,
  -- audit: the column decoded back to n, compared with the key's n ({k}) modulo the cast's wrap; NULL if not exact
  CASE c.data_type
    WHEN 'tinyint'   THEN CONCAT('`', c.column_name, '` = {k} % ', IF(c.column_type LIKE '%unsigned%', 255, 127))
    WHEN 'smallint'  THEN CONCAT('`', c.column_name, '` = {k} % ', IF(c.column_type LIKE '%unsigned%', 65535, 32767))
    WHEN 'mediumint' THEN CONCAT('`', c.column_name, '` = {k} % ', IF(c.column_type LIKE '%unsigned%', 16777215, 8388607))
    WHEN 'int'       THEN CONCAT('`', c.column_name, '` = {k} % ', IF(c.column_type LIKE '%unsigned%', 4294967295, 2147483647))
    WHEN 'bigint'    THEN CONCAT('`', c.column_name, '` = {k}')
    WHEN 'decimal'   THEN CONCAT('`', c.column_name, '` * 1', REPEAT('0', c.numeric_scale), ' = {k} % 1', REPEAT('0', LEAST(c.numeric_precision, 18)))
    WHEN 'double'    THEN CONCAT('`', c.column_name, '` = {k}')
    WHEN 'bit'       THEN CONCAT('`', c.column_name, '` + 0 = ', IF(c.numeric_precision = 64, '{k}', CONCAT('{k} % ', CAST(POW(2, c.numeric_precision) AS UNSIGNED))))
    WHEN 'date'      THEN CONCAT('DATEDIFF(`', c.column_name, '`, ''1970-01-01'') = {k} % 2932897')
    WHEN 'datetime'  THEN CONCAT('TIMESTAMPDIFF(SECOND, ''1970-01-01 00:00:00'', `', c.column_name, '`) = {k} % 253402300800')
    WHEN 'timestamp' THEN CONCAT('UNIX_TIMESTAMP(`', c.column_name, '`) = 1 + {k} % 2147483646')
    WHEN 'time'      THEN CONCAT('TIME_TO_SEC(`', c.column_name, '`) = {k} % 86400')
    WHEN 'year'      THEN CONCAT('`', c.column_name, '` - 1901 = {k} % 255')
    WHEN 'json'      THEN CONCAT('JSON_EXTRACT(`', c.column_name, '`, ''$.n'') = {k}')
    WHEN 'enum'      THEN CONCAT('`', c.column_name, '` + 0 - 1 = {k} % ', LENGTH(c.column_type) - LENGTH(REPLACE(c.column_type, ',', '')) + 1)
    WHEN 'set'       THEN CONCAT('`', c.column_name, '` + 0 = {k} % ', CAST(POW(2, LENGTH(c.column_type) - LENGTH(REPLACE(c.column_type, ',', '')) + 1) AS UNSIGNED))
    ELSE CASE
      -- strings and binaries: decided after len is known (see the UPDATE below)
      WHEN c.data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext') THEN 's'
      WHEN c.data_type IN ('binary', 'varbinary', 'tinyblob', 'blob', 'mediumblob', 'longblob') THEN 'b'
    END
  END AS chk,
  -- length to generate: the declared length, or @lob_len (default 100) for text and blob types, capped at the type's max
  -- (or the column's lfc:len={L}, for a source's average width)
  COALESCE(CAST(NULLIF(REGEXP_SUBSTR(c.cm, '(?<=lfc:len=\\{)[0-9]+'), '') AS UNSIGNED),   -- '' on no match (MariaDB) would fail strict CAST
           CASE WHEN c.data_type IN ('char', 'varchar', 'binary', 'varbinary') THEN c.character_maximum_length
                ELSE LEAST(GREATEST(COALESCE(@lob_len, 100), 38), c.character_maximum_length) END) AS len,
  CASE WHEN c.data_type IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint') THEN CONCAT('`', c.column_name, '`')
       WHEN c.data_type IN ('char', 'varchar') AND c.character_maximum_length >= 19 THEN CONCAT('CAST(LEFT(`', c.column_name, '`, 19) AS UNSIGNED)')
  END AS to_int
FROM (SELECT c0.*, CONCAT(c0.column_comment, ' ', COALESCE((SELECT g.tag FROM lfc_seed.tags g WHERE g.db = c0.table_schema
        AND g.tbl = c0.table_name AND g.col = c0.column_name), '')) AS cm FROM information_schema.columns c0 WHERE c0.table_schema = @db) c
JOIN lfc_tabs tb ON tb.db = c.table_schema AND tb.tbl = c.table_name
LEFT JOIN (SELECT table_schema, table_name, column_name, MAX(referenced_table_schema) AS referenced_table_schema,     -- one-column FKs
                  MAX(referenced_table_name) AS referenced_table_name
           FROM information_schema.key_column_usage k
           WHERE table_schema = @db AND referenced_table_name IS NOT NULL
             AND (SELECT COUNT(*) FROM information_schema.key_column_usage k2 WHERE k2.constraint_schema = k.constraint_schema
                    AND k2.constraint_name = k.constraint_name AND k2.table_name = k.table_name) = 1
           GROUP BY table_schema, table_name, column_name) fk
  ON fk.table_schema = c.table_schema AND fk.table_name = c.table_name AND fk.column_name = c.column_name AND c.column_key <> 'PRI'
WHERE c.table_schema = @db AND COALESCE(c.generation_expression, '') = ''
  AND NOT (c.extra LIKE '%auto_increment%' AND c.column_key <> 'PRI');     -- the database numbers a non-key AUTO_INCREMENT column
-- strings: zero-padded n at the start and (from 38 characters) the end, MD5 filler between, to the length;
-- the audit checks both ends and the length, so a truncated value shows up. A non-digit where n should be is a mismatch.
UPDATE lfc_cols SET
  expr0 = CASE WHEN len < 19 THEN CONCAT('RIGHT(LPAD(x.n, 19, ''0''), ', len, ')')
               WHEN len < 38 THEN CONCAT('LEFT(CONCAT(LPAD(x.n, 19, ''0''), MD5(x.n)), ', len, ')')
               ELSE CONCAT('CONCAT(LPAD(x.n, 19, ''0''), LEFT(REPEAT(MD5(x.n), ', GREATEST(CEIL((len - 38) / 32), 1), '), ', len - 38,
                           '), LPAD(x.n, 19, ''0''))') END,
  chk = CASE WHEN len < 19 THEN CONCAT('IF(`', col, '` REGEXP ''^[0-9]+$'', CAST(`', col, '` AS UNSIGNED), NULL) = {k} % 1', REPEAT('0', len))
             -- from 38 characters both ends carry n, which is enough to catch truncation and keeps values written before
             -- a column was widened valid; shorter strings must have the exact length
             WHEN len >= 38 THEN CONCAT('IF(LEFT(`', col, '`, 19) REGEXP ''^[0-9]{19}$'', CAST(LEFT(`', col, '`, 19) AS UNSIGNED), NULL) = {k} AND RIGHT(`', col, '`, 19) = LPAD({k}, 19, ''0'')')
             ELSE CONCAT('IF(LEFT(`', col, '`, 19) REGEXP ''^[0-9]{19}$'', CAST(LEFT(`', col, '`, 19) AS UNSIGNED), NULL) = {k} AND CHAR_LENGTH(`', col, '`) = ', len) END
WHERE expr0 = 's';
UPDATE lfc_cols SET
  expr0 = IF(len < 16, CONCAT('LEFT(CONCAT(UNHEX(LPAD(HEX(x.n), 16, ''0'')), UNHEX(MD5(x.n))), ', len, ')'),
             CONCAT('CONCAT(UNHEX(LPAD(HEX(x.n), 16, ''0'')), LEFT(REPEAT(UNHEX(MD5(x.n)), ', GREATEST(CEIL((len - 16) / 16), 1), '), ', len - 16,
                    '), UNHEX(LPAD(HEX(x.n), 16, ''0'')))')),
  chk = IF(len < 8, NULL, CONCAT('CAST(CONV(HEX(LEFT(`', col, '`, 8)), 16, 10) AS UNSIGNED) = {k} AND LENGTH(`', col, '`) = ', len,
                                 IF(len >= 16, CONCAT(' AND RIGHT(`', col, '`, 8) = UNHEX(LPAD(HEX({k}), 16, ''0''))'), '')))
WHERE expr0 = 'b';
-- a column added after rows existed (tagged 'lfc:added' by plan-ddl.sql) may be NULL in the older rows
UPDATE lfc_cols SET chk = CONCAT('(`', col, '` IS NULL OR ', chk, ')') WHERE added AND chk IS NOT NULL;
-- a column with an expression (lfc:e): the expression as is, checked against it at n
UPDATE lfc_cols SET expr0 = e, chk = CONCAT('`', col, '` <=> (', REPLACE(e, 'x.n', '{k}'), ')') WHERE e IS NOT NULL;
-- a column with a formula (lfc:v): its cast of the formula, checked against the formula at n
UPDATE lfc_cols SET expr0 = REPLACE(expr0, 'x.n', CONCAT('(', v, ')')), chk = REPLACE(chk, '{k}', CONCAT('(', REPLACE(v, 'x.n', '{k}'), ')'))
WHERE v IS NOT NULL AND e IS NULL;
-- each table's key: the column to probe, n from a row (the key decoded, or lfc:n), and the cast from n for a one-column key
-- (a temporary table can't appear twice in one MySQL query, so the FK, audit, and delete queries join this copy)
DROP TEMPORARY TABLE IF EXISTS lfc_keys;
CREATE TEMPORARY TABLE lfc_keys AS
SELECT tb.db, tb.tbl, COALESCE(tb.byc, tb.pk1) AS pk, COALESCE(tb.nkey, c.to_int) AS pk_int, IF(tb.npk = 1, c.expr0, NULL) AS pk_expr,
       tb.nkey IS NOT NULL AS has_n
FROM lfc_tabs tb LEFT JOIN lfc_cols c ON c.db = tb.db AND c.tbl = tb.tbl AND c.col = tb.pk1
WHERE tb.nkey IS NOT NULL OR c.to_int IS NOT NULL;
-- foreign keys: parent_min + scramble(n) % (parent_max - parent_min + 1), cast to the parent key's type, with the parent's
-- range probed when the INSERT runs, so every value is an existing key (NULL while the parent is empty)
UPDATE lfc_cols c JOIN lfc_keys k ON k.db = c.fk_db AND k.tbl = c.fk_tbl AND k.pk_expr IS NOT NULL AND c.v IS NULL AND c.e IS NULL
SET c.expr0 = REPLACE(k.pk_expr, 'x.n', CONCAT(
      '((SELECT ', k.pk_int, ' FROM `', k.db, '`.`', k.tbl, '` ORDER BY `', k.pk, '` LIMIT 1) + ((((x.n % 2147483647) * ((x.n % 2147483647 + ',
      FLOOR(1 + RAND() * 2147483645), ') % 2147483647)) % 2147483647 + ', FLOOR(RAND() * 2147483647), ') % 2147483647) % ((SELECT ', k.pk_int,
      ' FROM `', k.db, '`.`', k.tbl, '` ORDER BY `', k.pk, '` DESC LIMIT 1) - (SELECT ', k.pk_int, ' FROM `', k.db, '`.`', k.tbl,
      '` ORDER BY `', k.pk, '` LIMIT 1) + 1))')),
    c.chk = NULL;                                                 -- FK values depend on the parent, so the audit skips them
