-- Primary and foreign keys of the schema (owner) __SCHEMA__'s tables in Oracle, the same rows as infoschema-keys.sql:
-- a foreign key's parent columns matched by position through the constraint it references. In the PDB
-- (oracle-columns.sql).
SELECT 'P' AS kind, c.table_name, c.constraint_name, cc.column_name, cc.position AS ordinal_position,
       NULL AS ref_table, NULL AS ref_column, NULL AS on_delete, NULL AS on_update
  FROM all_constraints c
  JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
 WHERE c.owner = '__SCHEMA__' AND c.constraint_type = 'P'
UNION ALL
SELECT 'F', c.table_name, c.constraint_name, cc.column_name, cc.position, p.table_name, pc.column_name,
       c.delete_rule, NULL
  FROM all_constraints c
  JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
  JOIN all_constraints p ON p.owner = c.r_owner AND p.constraint_name = c.r_constraint_name
  JOIN all_cons_columns pc ON pc.owner = p.owner AND pc.constraint_name = p.constraint_name
                          AND pc.position = cc.position
 WHERE c.owner = '__SCHEMA__' AND c.constraint_type = 'R'
