-- One row per table in database __DB__ (size.sh puts the --database name here), on CockroachDB. The PostgreSQL query
-- (postgres.sql) doesn't run on CockroachDB: it needs pg_relation_size and pg_stats. Here: rows from the table
-- statistics (crdb_internal.table_row_statistics, which counts from statistics and is NULL-free only after the first
-- automatic statistics run); bytes from SHOW RANGES, the ranges' sizes (approximate_disk_bytes is in span_stats).
-- A range can hold several tables and their indexes: the bytes count once, for the table the range starts in, so the
-- per-table figures are approximate (a range that holds two tables, as a small database's one range does, is all
-- the first one's: the test had orders 24 MB and items 0); the database total (sum of the ranges) is exact.
-- Catalog and range metadata only; tested as the connection's user (lfc_user) through remote_query.
WITH rg AS (
  SELECT substring(start_key FROM '/Table/([0-9]+)')::INT AS tid, range_size::FLOAT AS sz
    FROM [SHOW RANGES FROM DATABASE __DB__ WITH DETAILS]
), tb AS (
  SELECT t.table_id AS tid, t.schema_name, t.name FROM crdb_internal.tables t
    JOIN information_schema.tables i ON i.table_schema = t.schema_name AND i.table_name = t.name
   WHERE t.database_name = '__DB__' AND i.table_type = 'BASE TABLE'
)
SELECT tb.schema_name AS table_schema, tb.name AS table_name,
       st.estimated_row_count AS rows_est,
       NULL AS rows_alt,
       NULL AS rows_size,
       NULL AS stats_time,
       coalesce(sum(rg.sz), 0)::BIGINT AS row_bytes,
       0 AS lob_bytes,
       0 AS index_bytes,
       coalesce(sum(rg.sz), 0)::BIGINT AS total_bytes,
       NULL AS logical_row_bytes,
       (SELECT coalesce(sum(sz), 0)::BIGINT FROM rg) AS database_bytes
  FROM tb
  LEFT JOIN rg ON rg.tid = tb.tid
  LEFT JOIN crdb_internal.table_row_statistics st ON st.table_id = tb.tid
 GROUP BY tb.schema_name, tb.name, st.estimated_row_count
