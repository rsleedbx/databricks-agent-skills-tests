-- One row per table in the current database, on YugabyteDB (YSQL). The PostgreSQL query (postgres.sql) reads 0 for a
-- table's own file and NULL for the database size: YugabyteDB keeps the rows in DocDB, not in a heap file. So this
-- query reports the table's size as one figure (pg_table_size: rows and large values, no split) and the indexes apart.
-- Catalog only: no table scans, no count(*). No double quotes, so it fits remote_query("...").
-- Rows come from the same three sources as postgres.sql (reltuples at the last ANALYZE, n_live_tup, rows per page).
SELECT n.nspname AS table_schema, c.relname AS table_name,
       CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END AS rows_est,
       st.n_live_tup AS rows_alt,
       CASE WHEN c.relpages > 0 AND c.reltuples > 0 THEN (c.reltuples / c.relpages * (pg_table_size(c.oid) / 8192))::bigint END AS rows_size,
       greatest(st.last_analyze, st.last_autoanalyze) AS stats_time,
       pg_table_size(c.oid) AS row_bytes,                                                       -- the table's data, one figure (DocDB)
       0 AS lob_bytes,                                                                          -- not separable on YugabyteDB
       pg_indexes_size(c.oid) AS index_bytes,
       pg_total_relation_size(c.oid) AS total_bytes,
       (SELECT sum(s.avg_width) FROM pg_stats s WHERE s.schemaname = n.nspname AND s.tablename = c.relname)::bigint AS logical_row_bytes,
       (SELECT sum(pg_total_relation_size(x.oid))::bigint FROM pg_class x JOIN pg_namespace xn ON xn.oid = x.relnamespace
         WHERE x.relkind IN ('r', 'm') AND xn.nspname NOT IN ('pg_catalog', 'information_schema') AND xn.nspname NOT LIKE 'pg_toast%') AS database_bytes
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
 WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'
