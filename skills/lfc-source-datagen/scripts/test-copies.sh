#!/usr/bin/env bash
# test-copies.sh: offline checks of copies.sh pg with a fake psql and a stubbed fill (FILL): the table names it reads
# from quoted and unquoted CREATE TABLE, the DDL it sends for a new copy (schema, then search_path, then the model), a
# copy whose schema already has tables skips the DDL, and a failed DDL is reported. Prints "all passed" or the failures.
#   bash skills/lfc-source-datagen/scripts/test-copies.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@" > "$T/out" 2>&1; then ok "$name"; else no "$name"; sed 's/^/        /' "$T/out" | tail -5; fi; }

printf 'CREATE TABLE "nation" (n_nationkey INT);\nCREATE TABLE region (r_regionkey INT);\nCOMMENT ON TABLE region IS '"'"'x'"'"';\n' > "$T/model.sql"
# fake psql: logs each call's stdin; answers the table count with $TABLES_IN; fails a DDL when FAIL_DDL is set
cat > "$T/psql" <<'FAKE'
#!/usr/bin/env bash
# fake psql for test-copies.sh
in=$(cat); { echo "CALL $*"; echo "$in"; } >> "$LOG"
case $in in
  *information_schema.tables*) echo "${TABLES_IN:-0}" ;;
  *"CREATE TABLE"*) [ -z "${FAIL_DDL:-}" ] || { echo 'ERROR:  relation "nation" already exists'; exit 3; } ;;
esac
FAKE
# stub fill: logs its arguments and the table list copies.sh exports
printf '#!/usr/bin/env bash\necho "FILL $* RE=$RE" >> "$LOG"\n' > "$T/fill"
chmod +x "$T/psql" "$T/fill"

run() {   # run <log> [VAR=value ...]: copies.sh pg on the fixture, copies 1 and 2
  local log=$1; shift; : > "$log"
  env LOG="$log" PSQL="$T/psql" FILL="$T/fill" "$@" bash "$HERE/copies.sh" pg "$T/model.sql" m 1 2 1
}
new_copies() {
  run "$T/new" > "$T/new.out" || return 1
  grep -qx 'all done' "$T/new.out" && grep -q '^done m_0001 ' "$T/new.out" && grep -q '^done m_0002 ' "$T/new.out" || return 1
  grep -qx 'FILL pg m_0001 RE=nation|region' "$T/new" || return 1
  # copy 1's DDL: the schema, then the search_path, then the model's tables, in that order
  awk '/CREATE SCHEMA IF NOT EXISTS m_0001;/ {s=NR} /SET search_path = m_0001;/ {p=NR} /CREATE TABLE "nation"/ && !t {t=NR}
       END {exit !(s && p > s && t > p)}' "$T/new"
}
existing_copy() {   # the schema has its tables: no DDL, straight to the fill
  run "$T/old" TABLES_IN=2 > "$T/old.out" || return 1
  ! grep -q 'CREATE' "$T/old" && grep -qx 'FILL pg m_0002 RE=nation|region' "$T/old"
}
failed_ddl() {
  run "$T/bad" FAIL_DDL=1 > "$T/bad.out" && return 1
  grep -q '^failed m_0001 DDL: ERROR:  relation "nation" already exists' "$T/bad.out" && grep -qx 'finished with failures' "$T/bad.out" \
    && ! grep -q '^FILL' "$T/bad"
}
check "pg: new copies get CREATE SCHEMA, SET search_path, then the model; the fill gets nation|region" new_copies
check "pg: a copy whose schema has tables skips the DDL and only fills" existing_copy
check "pg: a failed DDL is reported with the error and the copy isn't filled" failed_ddl

rm -rf "${T:?}"; rm -f /tmp/copies-m_0001.ddl.out /tmp/copies-m_0001.fill.out /tmp/copies-m_0002.ddl.out /tmp/copies-m_0002.fill.out   # copies.sh's logs
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
