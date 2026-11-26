-- One row per table in every user database, with the PROCESS privilege (for INFORMATION_SCHEMA.INNODB_TABLESPACES). Catalog only. information_schema.tables is an estimate cached for
-- information_schema_stats_expiry seconds (default a day); the tablespace file size is current. Rows:
--   rows_est   table_rows (InnoDB's sampled estimate)
--   rows_alt   n_rows from InnoDB's persistent statistics (mysql.innodb_table_stats), with its own timestamp
--   rows_size  the tablespace file's current size / the average row length (file-per-table tables only)
SELECT t.table_schema, t.table_name, t.table_rows AS rows_est,
       s.n_rows AS rows_alt,
       CASE WHEN t.avg_row_length > 0 THEN FLOOR(f.file_size / t.avg_row_length) END AS rows_size,
       s.last_update AS stats_time,
       t.data_length AS row_bytes,                                                               -- the clustered index: rows and large values
       0 AS lob_bytes,
       t.index_length AS index_bytes,
       COALESCE(f.allocated_size, t.data_length + t.index_length + t.data_free) AS total_bytes,  -- the file on disk, when it has its own
       t.avg_row_length AS logical_row_bytes,
       (SELECT SUM(x.data_length + x.index_length + x.data_free) FROM information_schema.tables x
         WHERE x.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')) AS database_bytes
  FROM information_schema.tables t
  LEFT JOIN mysql.innodb_table_stats s ON s.database_name = t.table_schema AND s.table_name = t.table_name
  LEFT JOIN information_schema.innodb_tablespaces f ON f.name = CONCAT(t.table_schema, '/', t.table_name)
 WHERE t.table_type = 'BASE TABLE' AND t.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
