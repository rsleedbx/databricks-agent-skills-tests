-- Primary keys and foreign keys of the database __SCHEMA__ (MySQL, MariaDB): key_column_usage names the parent itself.
SELECT CASE WHEN k.constraint_name = 'PRIMARY' THEN 'P' ELSE 'F' END AS kind, k.table_name, k.constraint_name,
       k.column_name, k.ordinal_position, k.referenced_table_name AS ref_table, k.referenced_column_name AS ref_column,
       rc.delete_rule AS on_delete, rc.update_rule AS on_update
  FROM information_schema.key_column_usage k
  LEFT JOIN information_schema.referential_constraints rc ON rc.constraint_schema = k.constraint_schema
   AND rc.constraint_name = k.constraint_name AND rc.table_name = k.table_name
 WHERE k.table_schema = '__SCHEMA__' AND (k.constraint_name = 'PRIMARY' OR k.referenced_table_name IS NOT NULL)
