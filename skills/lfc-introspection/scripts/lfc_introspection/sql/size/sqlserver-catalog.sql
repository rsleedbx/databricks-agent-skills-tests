-- sqlserver.sql for a login without VIEW DATABASE PERFORMANCE STATE (a UC JDBC connection's read/write user): the same
-- columns from sys.partitions and sys.allocation_units, readable with metadata access alone. rows is the engine's own
-- per-partition count (as near exact as dm_db_partition_stats); used pages split the same way; database_bytes NULL.
SELECT s.name AS table_schema, t.name AS table_name,
       SUM(CASE WHEN p.index_id IN (0, 1) AND a.type = 1 THEN p.rows ELSE 0 END) AS rows_est,
       SUM(CASE WHEN p.index_id IN (0, 1) AND a.type = 1 THEN p.rows ELSE 0 END) AS rows_alt,
       SUM(CASE WHEN p.index_id IN (0, 1) AND a.type = 1 THEN p.rows ELSE 0 END) AS rows_size,
       NULL AS stats_time,
       SUM(CASE WHEN p.index_id IN (0, 1) AND a.type = 1 THEN a.used_pages ELSE 0 END) * 8192 AS row_bytes,
       SUM(CASE WHEN p.index_id IN (0, 1) AND a.type IN (2, 3) THEN a.used_pages ELSE 0 END) * 8192 AS lob_bytes,
       SUM(CASE WHEN p.index_id > 1 THEN a.used_pages ELSE 0 END) * 8192 AS index_bytes,
       SUM(a.total_pages) * 8192 AS total_bytes,
       NULL AS logical_row_bytes,
       NULL AS database_bytes
  FROM sys.partitions p
  JOIN sys.allocation_units a ON a.container_id = CASE a.type WHEN 2 THEN p.partition_id ELSE p.hobt_id END
  JOIN sys.tables t ON t.object_id = p.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id
 WHERE t.is_ms_shipped = 0 AND ('__SCHEMA__' = '' OR s.name = '__SCHEMA__') AND ('__DB__' = '' OR DB_NAME() = '__DB__')
 GROUP BY s.name, t.name
