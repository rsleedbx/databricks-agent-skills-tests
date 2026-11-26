-- One row per user table in the current database. Catalog only. row_count in sys.dm_db_partition_stats is maintained
-- by the engine on every change (near exact, no statistics needed), so rows_alt and rows_size repeat it.
-- Data = the heap or clustered index, split into in-row pages and large-value (LOB, row-overflow) pages; indexes = nonclustered.
SELECT s.name AS table_schema, t.name AS table_name,
       SUM(CASE WHEN p.index_id IN (0, 1) THEN p.row_count ELSE 0 END) AS rows_est,
       SUM(CASE WHEN p.index_id IN (0, 1) THEN p.row_count ELSE 0 END) AS rows_alt,
       SUM(CASE WHEN p.index_id IN (0, 1) THEN p.row_count ELSE 0 END) AS rows_size,
       NULL AS stats_time,
       SUM(CASE WHEN p.index_id IN (0, 1) THEN p.in_row_used_page_count ELSE 0 END) * 8192 AS row_bytes,
       SUM(CASE WHEN p.index_id IN (0, 1) THEN p.lob_used_page_count + p.row_overflow_used_page_count ELSE 0 END) * 8192 AS lob_bytes,
       SUM(CASE WHEN p.index_id > 1 THEN p.used_page_count ELSE 0 END) * 8192 AS index_bytes,
       SUM(p.reserved_page_count) * 8192 AS total_bytes,
       NULL AS logical_row_bytes,
       (SELECT SUM(CAST(f.size AS bigint)) * 8192 FROM sys.database_files f) AS database_bytes   -- data and log files
  FROM sys.dm_db_partition_stats p
  JOIN sys.tables t ON t.object_id = p.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id
 WHERE t.is_ms_shipped = 0
 GROUP BY s.name, t.name
