-- Every base table's columns in __SCHEMA__ (information_schema: PostgreSQL, YugabyteDB, CockroachDB, SQL Server).
-- No double quotes: it runs inside remote_query("...").
SELECT c.table_name, c.ordinal_position, c.column_name, c.data_type, c.character_maximum_length,
       c.numeric_precision, c.numeric_scale, c.datetime_precision, c.is_nullable, c.column_default
  FROM information_schema.columns c
  JOIN information_schema.tables t ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema
                               AND t.table_name = c.table_name
 WHERE c.table_schema = '__SCHEMA__' AND ('__DB__' = '' OR c.table_catalog = '__DB__') AND t.table_type = 'BASE TABLE'
