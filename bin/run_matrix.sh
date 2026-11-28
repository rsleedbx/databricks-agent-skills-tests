#!/usr/bin/env bash
# Run the full Lakeflow Connect matrix (see MATRIX.md) against one workspace. LFC_SKIP_LAKEBASE=1 leaves the Lakebase
# tests out (a workspace with no Lakebase project for them).
#
# Usage:
#   LFC_ENV=<name> source env.sh
#   bin/run_matrix.sh                 # all batches, in order
#   bin/run_matrix.sh qbc fcqbc       # only some batches
#
# Engines: MySQL, PostgreSQL, SQL Server and Oracle (the -mysql/-pg/-sqlserver/-oracle prompts). No MariaDB batch:
# the MySQL connector's source-setup page lists MySQL 5.7 and later on RDS, Aurora, Azure, EC2 and Cloud SQL, and no
# MariaDB (Databricks docs, lakeflow-connect/mysql-source-setup, checked 2026-10-27). Db2, Informix, CockroachDB and
# YugabyteDB have no Lakeflow Connect connector: the CDC overview (lakeflow-connect/cdc-overview) lists four database
# connectors. Test those engines with lfc-source-sizing and zerobus-debezium-test instead.
# Batches run one after another so tests on the same connection never race for the same
# default pipeline name. Order matters: qbc creates the Lakebase project and connection that
# fcqbc-lakebase and dml-lakebase use.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
M="$ROOT/prompts/lfc-matrix"
if [ $# -gt 0 ]; then batches=("$@"); else batches=(qbc fcqbc icdc scdc dml); fi
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)          # for the compute readback at the end

for b in "${batches[@]}"; do
  case $b in
    qbc|fcqbc|icdc) files=("$M/$b-lakebase.txt" "$M/$b-pg.txt" "$M/$b-mysql.txt" "$M/$b-sqlserver.txt")
                    [ -n "${LFC_ORACLE_CONNECTION:-}" ] && files+=("$M/$b-oracle.txt");;   # Oracle: when the env names its connections
    scdc) files=("$M/$b-lakebase.txt" "$M/$b-pg.txt" "$M/$b-mysql.txt" "$M/$b-sqlserver.txt");;
    dml) files=("$M/dml-lakebase.txt");;
    *) echo "unknown batch: $b (qbc, fcqbc, icdc, scdc, dml)"; exit 1;;
  esac
  [ -n "${LFC_SKIP_LAKEBASE:-}" ] && files=("${files[@]/*-lakebase.txt/}") && files=($(printf '%s\n' "${files[@]}" | grep .))
  echo "### batch $b  $(date -u +%H:%M:%S)"
  "$ROOT/bin/run_batch.sh" "${files[@]}"
done
echo "### done  $(date -u +%H:%M:%S)  results: ${RESULTS_ROOT:-/tmp}"

# Read back, right away, what compute each pipeline of this run asked for and got: whether it started and reached the
# source (the records' "compute" field). Sandbox workspaces can disappear within hours, and a classic update takes about
# 8 minutes before it fails, so a pipeline still WAITING_FOR_RESOURCES here shows started=unknown: rerun the same command.
if [ -n "${LFC_NAME_PREFIX:-}" ]; then
  out="${RESULTS_ROOT:-/tmp}/compute.tsv"
  echo "### compute readback  $(date -u +%H:%M:%S)  (serverless asked: ${LFC_SERVERLESS:-?})"
  WS_CFG=$(python3 "$ROOT/skills/account-token-fallback/scripts/ws_token.py" "$PIPELINE_WS")     # the login may have expired by now
  ${WS_CFG:+env DATABRICKS_CONFIG_FILE=$WS_CFG} \
    "$ROOT/bin/compute_report.sh" "$PIPELINE_WS" "$LFC_NAME_PREFIX-" "$START" | tee "$out" | column -t -s $'\t'
  echo "saved $out; again later: bin/compute_report.sh $PIPELINE_WS $LFC_NAME_PREFIX- $START"
else
  echo "### compute readback skipped: LFC_NAME_PREFIX is unset"
fi
