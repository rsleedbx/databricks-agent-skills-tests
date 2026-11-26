-- One row per user table in the connected database, on Db2 LUW (run by hand: size.sh has no Db2 connection type).
-- Catalog only: no table scans, no COUNT(*). Same columns as postgres.sql, in the same order, for a side-by-side read:
--   rows_est      CARD: the optimizer's row count from the last RUNSTATS (-1 and NULL: never run)
--   row_bytes     NPAGES (the table's data pages) x the tablespace's page size
--   index_bytes   the leaf pages of its indexes (NLEAF) x the page size: an approximation (non-leaf pages not counted)
--   total_bytes   FPAGES (the pages the table has allocated) x the page size
--   database_bytes  the database's tablespace files (SYSIBMADM.TBSP_UTILIZATION): the infrastructure figure
-- Large objects (LOB columns in a LOB tablespace) are not in NPAGES: lob_bytes is 0 here. Run RUNSTATS first for rows.
SELECT t.tabschema AS table_schema, t.tabname AS table_name,
       CASE WHEN t.card >= 0 THEN t.card END AS rows_est,
       CAST(NULL AS BIGINT) AS rows_alt,
       CAST(NULL AS BIGINT) AS rows_size,
       t.stats_time AS stats_time,
       CASE WHEN t.npages > 0 THEN t.npages * p.pagesize ELSE 0 END AS row_bytes,
       0 AS lob_bytes,
       COALESCE(i.leaf_pages, 0) * p.pagesize AS index_bytes,
       CASE WHEN t.fpages > 0 THEN t.fpages * p.pagesize ELSE 0 END AS total_bytes,
       CAST(NULL AS BIGINT) AS logical_row_bytes,
       (SELECT SUM(TBSP_TOTAL_SIZE_KB) * 1024 FROM SYSIBMADM.TBSP_UTILIZATION) AS database_bytes
  FROM syscat.tables t
  JOIN syscat.tablespaces p ON p.tbspaceid = t.tbspaceid
  LEFT JOIN (SELECT tabschema, tabname, SUM(CASE WHEN nleaf > 0 THEN nleaf ELSE 0 END) AS leaf_pages
               FROM syscat.indexes GROUP BY tabschema, tabname) i
         ON i.tabschema = t.tabschema AND i.tabname = t.tabname
 WHERE t.type = 'T'
   AND t.tabschema NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSIBMINTERNAL', 'SYSIBMTS', 'SYSCAT', 'SYSFUN', 'SYSPROC', 'SYSSTAT', 'SYSTOOLS', 'NULLID')
 ORDER BY 1, 2
