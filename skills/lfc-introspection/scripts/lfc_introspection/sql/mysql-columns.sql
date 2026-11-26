-- Every base table's columns in the database __SCHEMA__ (MySQL, MariaDB); column_type is the whole declared type.
SELECT c.table_name, c.ordinal_position, c.column_name, c.column_type AS data_type, NULL AS character_maximum_length,
       NULL AS numeric_precision, NULL AS numeric_scale, NULL AS datetime_precision, c.is_nullable, c.column_default
  FROM information_schema.columns c
  JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
 WHERE c.table_schema = '__SCHEMA__' AND t.table_type = 'BASE TABLE'
