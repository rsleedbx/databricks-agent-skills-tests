-- mysql.sql for a login that can't read mysql.innodb_table_stats (a UC JDBC connection's read/write user): the same
-- columns from information_schema.tables alone; rows_alt and stats_time NULL (no persistent statistics).
SELECT t.table_schema, t.table_name, t.table_rows AS rows_est,
       NULL AS rows_alt,
       CASE WHEN t.avg_row_length > 0 THEN FLOOR(t.data_length / t.avg_row_length) END AS rows_size,
       NULL AS stats_time,
       t.data_length AS row_bytes,
       0 AS lob_bytes,
       t.index_length AS index_bytes,
       t.data_length + t.index_length + t.data_free AS total_bytes,
       t.avg_row_length AS logical_row_bytes,
       (SELECT SUM(x.data_length + x.index_length + x.data_free) FROM information_schema.tables x
         WHERE x.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')) AS database_bytes
  FROM information_schema.tables t
 WHERE t.table_type = 'BASE TABLE' AND t.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
   AND ('__SCHEMA__' = '' OR t.table_schema = '__SCHEMA__')
