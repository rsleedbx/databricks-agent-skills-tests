-- One row per table in the current database. Catalog only: no table scans, no count(*). No double quotes, so it fits remote_query("...").
-- Bytes come from the files (always current). Rows come from three cheap sources that go stale differently:
--   rows_est   reltuples, from the last ANALYZE (NULL: never analyzed)
--   rows_alt   n_live_tup, kept by the statistics collector on every insert and delete, without ANALYZE
--   rows_size  the planner's own extrapolation: rows per page at the last ANALYZE x the pages the file has now
SELECT n.nspname AS table_schema, c.relname AS table_name,
       CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END AS rows_est,
       st.n_live_tup AS rows_alt,
       CASE WHEN c.relpages > 0 AND c.reltuples > 0 THEN (c.reltuples / c.relpages * (pg_relation_size(c.oid) / 8192))::bigint END AS rows_size,
       greatest(st.last_analyze, st.last_autoanalyze) AS stats_time,
       pg_relation_size(c.oid) AS row_bytes,                                                    -- the table's own file
       pg_table_size(c.oid) - pg_relation_size(c.oid) AS lob_bytes,                             -- TOAST (large values) and free-space / visibility maps
       pg_indexes_size(c.oid) AS index_bytes,
       pg_total_relation_size(c.oid) AS total_bytes,
       (SELECT sum(s.avg_width) FROM pg_stats s WHERE s.schemaname = n.nspname AND s.tablename = c.relname)::bigint AS logical_row_bytes,
       pg_database_size(current_database()) AS database_bytes
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
 WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'
