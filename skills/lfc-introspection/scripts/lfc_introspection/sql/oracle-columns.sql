-- Every table's columns of the schema (owner) __SCHEMA__ in Oracle: the same columns as infoschema-columns.sql.
-- Run in the PDB (a UC JDBC connection to it, through spark.read.jdbc): the all_* views see only the session's own
-- container, so from CDB$ROOT (a standard ORACLE connection whose service is the CDB's) they find no tables.
-- Oracle names are upper case unless quoted; nullable is Y/N. A column's default (a LONG) isn't read here.
SELECT c.table_name, c.column_id AS ordinal_position, c.column_name, c.data_type, c.char_length AS character_maximum_length,
       c.data_precision AS numeric_precision, c.data_scale AS numeric_scale,
       CASE WHEN c.data_type LIKE 'TIMESTAMP%' THEN c.data_scale END AS datetime_precision,
       CASE c.nullable WHEN 'N' THEN 'NO' ELSE 'YES' END AS is_nullable, NULL AS column_default
  FROM all_tab_columns c
  JOIN all_tables t ON t.owner = c.owner AND t.table_name = c.table_name
 WHERE c.owner = '__SCHEMA__'
