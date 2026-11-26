-- One row per user table in the database named by the DATABASE line above it, on Informix (run by hand, in dbaccess:
-- size.sh has no Informix connection type). Catalog and partition headers only: no table scans, no COUNT(*).
--   rows_est    systables.nrows: the count from the last UPDATE STATISTICS (NULL / -1 until one runs)
--   row_bytes   the table's data partition: pages in use (sysmaster:sysptnhdr.npused) x the page size. Large values in
--               blobspaces (TEXT, BYTE) are not in it: lob_bytes is not reported
--   index_bytes the index partitions (sysfragments, fragtype I) in use, same way
--   total_bytes the table's data partition's allocated pages (nptotal) x the page size
-- Run: dbaccess <db> sizing.sql, or the query on its own after DATABASE <db>;
SELECT TRIM(t.owner) AS table_schema, TRIM(t.tabname) AS table_name,
       t.nrows AS rows_est,
       h.npused * h.pagesize AS row_bytes,
       (SELECT SUM(x.npused * x.pagesize) FROM sysfragments f JOIN sysmaster:sysptnhdr x ON x.partnum = f.partn
         WHERE f.tabid = t.tabid AND f.fragtype = 'I') AS index_bytes,
       h.nptotal * h.pagesize AS total_bytes
  FROM systables t JOIN sysmaster:sysptnhdr h ON h.partnum = t.partnum
 WHERE t.tabid >= 100 AND t.tabtype = 'T'
 ORDER BY 1, 2;
