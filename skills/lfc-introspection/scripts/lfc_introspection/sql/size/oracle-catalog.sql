-- oracle.sql for a login without the DBA_ views (a UC JDBC connection's read/write user, in the PDB): the same columns
-- from ALL_TABLES and ALL_TAB_MODIFICATIONS. Rows from optimizer statistics (NULL until gathered) plus the changes since;
-- bytes estimated as num_rows * avg_row_len (no segment sizes without DBA_SEGMENTS); LOB, index, database bytes NULL.
SELECT t.owner AS table_schema, t.table_name, t.num_rows AS rows_est,
       CASE WHEN t.num_rows IS NOT NULL THEN t.num_rows + NVL(m.inserts, 0) - NVL(m.deletes, 0) END AS rows_alt,
       NULL AS rows_size,
       t.last_analyzed AS stats_time,
       CASE WHEN t.num_rows IS NOT NULL AND t.avg_row_len > 0 THEN t.num_rows * t.avg_row_len END AS row_bytes,
       NULL AS lob_bytes, NULL AS index_bytes, NULL AS total_bytes,
       t.avg_row_len AS logical_row_bytes,
       NULL AS database_bytes
  FROM all_tables t
  LEFT JOIN (SELECT table_owner, table_name, SUM(inserts) AS inserts, SUM(deletes) AS deletes FROM all_tab_modifications
              GROUP BY table_owner, table_name) m ON m.table_owner = t.owner AND m.table_name = t.table_name
 WHERE t.owner NOT IN (SELECT username FROM all_users WHERE oracle_maintained = 'Y') AND t.nested = 'NO' AND t.temporary = 'N'
   AND ('__SCHEMA__' = '' OR t.owner = '__SCHEMA__')
