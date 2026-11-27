-- Primary and foreign keys of __SCHEMA__'s tables from pg_catalog (PostgreSQL, YugabyteDB, CockroachDB), the same rows
-- as infoschema-keys.sql. information_schema's constraint views show only tables the login owns, so a read-only
-- login sees none there; pg_catalog shows every table's.
SELECT CASE con.contype WHEN 'p' THEN 'P' ELSE 'F' END AS kind, cl.relname AS table_name, con.conname AS constraint_name,
       a.attname AS column_name, k.ord AS ordinal_position, fcl.relname AS ref_table, fa.attname AS ref_column,
       CASE con.confdeltype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT'
                            WHEN 'r' THEN 'RESTRICT' WHEN 'a' THEN 'NO ACTION' END AS on_delete,
       CASE con.confupdtype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT'
                            WHEN 'r' THEN 'RESTRICT' WHEN 'a' THEN 'NO ACTION' END AS on_update
  FROM pg_constraint con
  JOIN pg_class cl ON cl.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = cl.relnamespace
  CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
  LEFT JOIN pg_class fcl ON fcl.oid = con.confrelid
  LEFT JOIN pg_attribute fa ON fa.attrelid = con.confrelid AND fa.attnum = con.confkey[k.ord]
 WHERE n.nspname = '__SCHEMA__' AND ('__DB__' = '' OR current_database() = '__DB__') AND con.contype IN ('p', 'f')
