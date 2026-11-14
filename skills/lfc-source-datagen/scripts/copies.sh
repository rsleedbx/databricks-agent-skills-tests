#!/usr/bin/env bash
# copies.sh <ms|pg> <model.sql> <model> <first> <last> [parallel]: build copies <first>..<last> of a model, each in its own
# schema <model>_NNNN (4 digits, so up to 9,999 sort in order), and fill them with fill.sh. Runs anywhere a SQL client
# does (a laptop, the database's VM, its container): it needs only bash, sed and the client, no Python.
#
#   model.sql  the model's DDL for the engine, from lfc-source-statschema ddl.py (tables unqualified; tags keyed by
#              SCHEMA_NAME()). Each copy gets it with its tables qualified by the copy's schema (pg: run with the
#              copy's schema as the search_path, which the COMMENT ON tags follow).
#   MSSQL      ms: the client, a command that reads SQL on stdin, connected to the target database, output with -y 0
#              (as fill.sh needs). Default: ./sq in this folder.
#   PSQL       pg: the client, a command that reads SQL on stdin, connected to the target database, with psql's own
#              output (-At, | between fields, as fill.sh needs). Default: psql. SRC (fill.sh's row source) defaults
#              to series (generate_series), so a database without lfc_seed.seed fills.
#   DIR        the planner files (default: sqlserver/ or postgres/ next to this script)
#
# Prints one line per copy, "done <schema> <seconds>s" or "failed <schema> <reason>", then "all done" or
# "finished with failures"; exits 1 if any copy failed. A copy already filled is filled again only to its target
# (fill.sh resumes), and a schema that exists is reused, so a rerun after an interruption continues.
# SQL Server (ms) and PostgreSQL (pg) so far; the other engines' fill.sh paths are the same and can be added here.
set -uo pipefail
E=$1 MODEL_SQL=$2 MODEL=$3 FIRST=$4 LAST=$5 PAR=${6:-4}
HERE=$(cd "$(dirname "$0")" && pwd)
case $E in
  ms) export MSSQL=${MSSQL:-$HERE/sq} DIR=${DIR:-$HERE/sqlserver}
      export RE=$(grep -o -E '^CREATE TABLE \[[A-Za-z_0-9]+\]' "$MODEL_SQL" | sed -E 's/.*\[(.*)\]/\1/' | paste -sd'|' -) ;;
  pg) export PSQL=${PSQL:-psql} DIR=${DIR:-$HERE/postgres} SRC=${SRC:-series}
      export RE=$(grep -o -E '^CREATE TABLE "?[A-Za-z_0-9]+"?' "$MODEL_SQL" | sed -E 's/^CREATE TABLE "?([A-Za-z_0-9]+)"?/\1/' | paste -sd'|' -) ;;
  *) echo "copies.sh: ms or pg so far"; exit 2 ;;
esac
export E HERE MODEL MODEL_SQL
[ -n "$RE" ] || { echo "copies.sh: no CREATE TABLE in $MODEL_SQL"; exit 2; }

one() {
  local s t0 n; s=$(printf '%s_%04d' "$MODEL" "$1"); t0=$(date +%s)
  # the schema and its tables, unless a previous run made them
  if [ "$E" = pg ]; then
    if [ "$(printf "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s';\n" "$s" | $PSQL -X -At | tr -d ' ')" = 0 ]; then
      { printf 'CREATE SCHEMA IF NOT EXISTS %s;\nSET search_path = %s;\n' "$s" "$s"; cat "$MODEL_SQL"; } | $PSQL -X -q -v ON_ERROR_STOP=1 > "/tmp/copies-$s.ddl.out" 2>&1 \
        || { echo "failed $s DDL: $(grep -m1 ERROR "/tmp/copies-$s.ddl.out")"; return 1; }
    fi
  elif [ "$(printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE SCHEMA_NAME(schema_id) = '%s';\n" "$s" | $MSSQL | grep -E '^ *[0-9]+ *$' | tr -d ' ')" = 0 ]; then
    printf "IF SCHEMA_ID('%s') IS NULL EXEC('CREATE SCHEMA %s');\n" "$s" "$s" | $MSSQL >/dev/null
    sed -E "s/(CREATE TABLE|ALTER TABLE|REFERENCES) \[($RE)\]/\1 [$s].[\2]/g; s/SCHEMA_NAME\(\)/N'$s'/g" "$MODEL_SQL" | $MSSQL > "/tmp/copies-$s.ddl.out" 2>&1 \
      || { echo "failed $s DDL: $(grep -m1 Msg "/tmp/copies-$s.ddl.out")"; return 1; }
  fi
  for n in 1 2 3; do
    bash "$HERE/fill.sh" "$E" "$s" > "/tmp/copies-$s.fill.out" 2>&1 && { echo "done $s $(( $(date +%s) - t0 ))s"; return 0; }
    sleep $((n * 20))
  done
  echo "failed $s fill: $(grep -m1 -E 'Msg [0-9]|ERROR|no rows added' "/tmp/copies-$s.fill.out")"; return 1
}
export -f one
if seq "$FIRST" "$LAST" | xargs -P "$PAR" -I{} bash -c 'one {}'; then echo "all done"; else echo "finished with failures"; exit 1; fi
