#!/usr/bin/env bash
# size.sh <profile> <connection> <postgres|mysql|mariadb|sqlserver|oracle|cockroachdb|yugabytedb> [--database D | --service S] [--out DIR] [--top N]
#   How big is the source behind a UC connection? Runs the engine's catalog query through the connection with
#   remote_query (read-only, from serverless; no table scans, no count(*)), saves one row per table to DIR/tables.csv,
#   and prints: the database total (for the infrastructure team), the data Lakeflow Connect moves (table data and
#   large values, no indexes), rows and average row size, the largest tables, and tables whose row sources disagree.
#   PostgreSQL, SQL Server, CockroachDB and YugabyteDB need --database; Oracle --service (the PDB). MySQL covers every user database at once.
#   MariaDB uses the MySQL query (its connection is a MYSQL one; no PROCESS variant: MariaDB has no innodb_tablespaces).
#   CockroachDB and YugabyteDB: a POSTGRESQL connection and the PostgreSQL query (--database, as for PostgreSQL).
#   Db2 and Informix have no Unity Catalog connection type: run lfc-introspection's scripts/sql/size/db2.sql or informix.sql with the engine's client.
set -uo pipefail
P=$1; CONN=$2; E=$3; shift 3; DB=; SVC=; OUT=.; TOP=15
while [ $# -gt 0 ]; do case $1 in --database) DB=$2; shift 2;; --service) SVC=$2; shift 2;; --out) OUT=$2; shift 2;; --top) TOP=$2; shift 2;; *) echo "unknown $1"; exit 2;; esac; done
HERE=$(cd "$(dirname "$0")" && pwd); mkdir -p "$OUT"
Q=(python3 "$HERE/size_query.py" "$CONN" "$E"); [ -n "$DB" ] && Q+=(--database "$DB"); [ -n "$SVC" ] && Q+=(--service "$SVC")
"${Q[@]}" > /dev/null || exit 2                     # the engine and its --database / --service (size_query.py says which)
runq() { "${Q[@]}" ${1:+--file "$1"} > "$OUT/size.sql" || return 2
        databricks experimental aitools tools query --file "$OUT/size.sql" --profile "$P" -o csv > "$OUT/tables.csv" 2> "$OUT/err.txt"; }
if [ $E = mysql ]; then
  if runq mysql-process; then echo "(MySQL: with current tablespace file sizes)"
  elif grep -q PROCESS "$OUT/err.txt"; then runq mysql || { echo "query failed: $(head -c 400 "$OUT/err.txt")"; exit 1; }
    echo "(MySQL: no PROCESS privilege, so no current file sizes: rows and bytes are information_schema estimates; GRANT PROCESS ON *.* adds them)"
  else echo "query failed: $(head -c 400 "$OUT/err.txt")"; exit 1; fi
else runq || { echo "query failed: $(head -c 400 "$OUT/err.txt")"; exit 1; }; fi
duckdb -box -c "
CREATE TABLE t AS SELECT * FROM read_csv('$OUT/tables.csv', header = true, all_varchar = true);
CREATE TABLE r AS SELECT table_schema AS s, table_name AS n,
  TRY_CAST(rows_est AS DOUBLE) AS est, TRY_CAST(rows_alt AS DOUBLE) AS alt, TRY_CAST(rows_size AS DOUBLE) AS sz, stats_time,
  coalesce(TRY_CAST(row_bytes AS DOUBLE), 0) AS rowb, coalesce(TRY_CAST(lob_bytes AS DOUBLE), 0) AS lobb,
  coalesce(TRY_CAST(index_bytes AS DOUBLE), 0) AS idxb,
  coalesce(TRY_CAST(total_bytes AS DOUBLE), coalesce(TRY_CAST(row_bytes AS DOUBLE), 0) + coalesce(TRY_CAST(lob_bytes AS DOUBLE), 0) + coalesce(TRY_CAST(index_bytes AS DOUBLE), 0)) AS totb,
  TRY_CAST(logical_row_bytes AS DOUBLE) AS lrb, TRY_CAST(database_bytes AS DOUBLE) AS dbb FROM t;
CREATE TABLE x AS SELECT *, coalesce(alt, est, sz) AS rows, rowb + lobb AS datab,
  greatest(coalesce(est, 0), coalesce(alt, 0), coalesce(sz, 0)) / nullif(least(coalesce(est, alt, sz), coalesce(alt, est, sz), coalesce(sz, alt, est)), 0) AS spread FROM r;
SELECT 'database total (files, incl. indexes, logs/WAL where reported)' AS what, round(max(dbb) / 1e9, 3) AS gb, round(max(dbb) / 1e6, 1) AS mb FROM x
UNION ALL SELECT 'tables + indexes + large values', round(sum(totb) / 1e9, 3), round(sum(totb) / 1e6, 1) FROM x
UNION ALL SELECT 'data Lakeflow Connect moves (table data + large values)', round(sum(datab) / 1e9, 3), round(sum(datab) / 1e6, 1) FROM x
UNION ALL SELECT '  of which large values (TOAST / LOB / overflow)', round(sum(lobb) / 1e9, 3), round(sum(lobb) / 1e6, 1) FROM x
UNION ALL SELECT 'indexes (not moved)', round(sum(idxb) / 1e9, 3), round(sum(idxb) / 1e6, 1) FROM x
UNION ALL SELECT 'logical data from row widths (rows x avg width)', round(sum(rows * lrb) / 1e9, 3), round(sum(rows * lrb) / 1e6, 1) FROM x;
SELECT count(*) AS tables, format('{:,}', CAST(sum(rows) AS BIGINT)) AS rows, CAST(sum(datab) / nullif(sum(rows), 0) AS BIGINT) AS avg_row_bytes_on_disk,
  CAST(sum(rows * lrb) / nullif(sum(rows), 0) AS BIGINT) AS avg_row_bytes_logical FROM x;
SELECT s || '.' || n AS largest_tables, format('{:,}', CAST(rows AS BIGINT)) AS rows, round(datab / 1e9, 3) AS data_gb, round(idxb / 1e9, 3) AS index_gb,
  CAST(datab / nullif(rows, 0) AS BIGINT) AS bytes_per_row FROM x ORDER BY datab DESC LIMIT $TOP;
SELECT s || '.' || n AS check_rows, CAST(est AS BIGINT) AS from_stats, CAST(alt AS BIGINT) AS second_source, CAST(sz AS BIGINT) AS from_size, stats_time,
  CASE WHEN est IS NULL AND alt IS NULL THEN 'no statistics' ELSE 'sources differ ' || round(spread, 1) || 'x' END AS why
  FROM x WHERE (est IS NULL AND alt IS NULL) OR spread > 1.5 ORDER BY datab DESC LIMIT $TOP;
"
echo "per-table detail: $OUT/tables.csv"
