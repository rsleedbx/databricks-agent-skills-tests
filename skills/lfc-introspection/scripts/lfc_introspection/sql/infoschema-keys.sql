-- Primary keys and foreign keys of __SCHEMA__'s tables, one row a key column: kind (P or F), table, constraint,
-- column, its position, and for a foreign key the parent table and column (matched by position) and its actions.
SELECT 'P' AS kind, k.table_name, k.constraint_name, k.column_name, k.ordinal_position,
       NULL AS ref_table, NULL AS ref_column, NULL AS on_delete, NULL AS on_update
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage k ON k.constraint_schema = tc.constraint_schema
   AND k.constraint_name = tc.constraint_name AND k.table_name = tc.table_name
 WHERE tc.table_schema = '__SCHEMA__' AND ('__DB__' = '' OR tc.table_catalog = '__DB__') AND tc.constraint_type = 'PRIMARY KEY'
UNION ALL
SELECT 'F', k.table_name, k.constraint_name, k.column_name, k.ordinal_position,
       p.table_name, p.column_name, rc.delete_rule, rc.update_rule
  FROM information_schema.referential_constraints rc
  JOIN information_schema.key_column_usage k ON k.constraint_schema = rc.constraint_schema
   AND k.constraint_name = rc.constraint_name
  JOIN information_schema.key_column_usage p ON p.constraint_schema = rc.unique_constraint_schema
   AND p.constraint_name = rc.unique_constraint_name AND p.ordinal_position = k.ordinal_position
 WHERE k.table_schema = '__SCHEMA__' AND ('__DB__' = '' OR k.table_catalog = '__DB__')
