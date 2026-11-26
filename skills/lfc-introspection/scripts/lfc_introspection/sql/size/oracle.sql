-- One row per table in every non-Oracle schema of the PDB (connect with service_name => the PDB). Catalog only. Rows:
--   rows_est   num_rows from optimizer statistics (NULL until gathered)
--   rows_alt   num_rows + inserts - deletes since the statistics (DBA_TAB_MODIFICATIONS; flushed every few minutes,
--              or at once with DBMS_STATS.FLUSH_DATABASE_MONITORING_INFO)
--   rows_size  the table segment's current bytes / avg_row_len
-- Bytes from segments (current). Needs SELECT on the DBA_ views (the Databricks setup package grants it).
SELECT t.owner AS table_schema, t.table_name, t.num_rows AS rows_est,
       CASE WHEN t.num_rows IS NOT NULL THEN t.num_rows + NVL(m.inserts, 0) - NVL(m.deletes, 0) END AS rows_alt,
       CASE WHEN t.avg_row_len > 0 THEN FLOOR(seg.bytes / t.avg_row_len) END AS rows_size,
       t.last_analyzed AS stats_time,
       NVL(seg.bytes, 0) AS row_bytes,
       (SELECT NVL(SUM(s.bytes), 0) FROM dba_lobs l JOIN dba_segments s ON s.owner = l.owner AND s.segment_name IN (l.segment_name, l.index_name)
         WHERE l.owner = t.owner AND l.table_name = t.table_name) AS lob_bytes,
       (SELECT NVL(SUM(s.bytes), 0) FROM dba_indexes i JOIN dba_segments s ON s.owner = i.owner AND s.segment_name = i.index_name
         WHERE i.table_owner = t.owner AND i.table_name = t.table_name AND i.index_type <> 'LOB') AS index_bytes,
       NULL AS total_bytes,
       t.avg_row_len AS logical_row_bytes,
       (SELECT SUM(bytes) FROM dba_data_files) AS database_bytes
  FROM dba_tables t
  LEFT JOIN (SELECT owner, segment_name, SUM(bytes) AS bytes FROM dba_segments WHERE segment_type LIKE 'TABLE%' GROUP BY owner, segment_name) seg
    ON seg.owner = t.owner AND seg.segment_name = t.table_name
  LEFT JOIN (SELECT table_owner, table_name, SUM(inserts) AS inserts, SUM(deletes) AS deletes FROM dba_tab_modifications GROUP BY table_owner, table_name) m
    ON m.table_owner = t.owner AND m.table_name = t.table_name
 WHERE t.owner NOT IN (SELECT username FROM dba_users WHERE oracle_maintained = 'Y') AND t.nested = 'NO' AND t.temporary = 'N'
