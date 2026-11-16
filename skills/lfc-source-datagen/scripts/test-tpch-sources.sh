#!/usr/bin/env bash
# test-tpch-sources.sh: offline checks of tpch-sources.sh with a fake sql_cmd: bad arguments stop before any SQL; the
# publication by schema on Postgres 15 on and by table name before; an existing publication altered; the slot created
# only when missing, and dropped. Prints "all passed" or the failures.
#   bash skills/lfc-source-datagen/scripts/test-tpch-sources.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); TS=$HERE/tpc/tpch-sources.sh
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }

# fake sql_cmd: logs each call's stdin; every schema exists (no fill), answers from VER, PUB_IN and SLOT_IN
cat > "$T/sql" <<'FAKE'
#!/usr/bin/env bash
# fake sql_cmd for test-tpch-sources.sh
in=$(cat); { echo "CALL $*"; echo "$in"; } >> "$LOG"
case $in in
  *"FROM pg_database"*|*"FROM information_schema.schemata"*) echo 1 ;;
  *server_version_num*) echo "$VER" ;;
  *string_agg*) echo "a.lineitem, b.lineitem" ;;
  *"FROM pg_publication WHERE"*) echo "${PUB_IN:-0}" ;;
  *"SELECT COUNT(*) FROM pg_replication_slots"*) echo "${SLOT_IN:-0}" ;;
esac
FAKE
chmod +x "$T/sql"
printf '{"contract": 1, "name": "t", "engine": "postgres", "host": "h", "port": 5432, "user": "lfc_cdc", "sql_cmd": "%s"}\n' "$T/sql" > "$T/src.json"
run() { : > "$T/log"; LOG=$T/log VER=${VER:-170000} PUB_IN=${PUB_IN:-0} SLOT_IN=${SLOT_IN:-0} SOURCE_JSON=$T/src.json bash "$TS" "$@" > "$T/out" 2>&1; }

for bad in "Bad:1" "a:x" "a:0.03:1,2" "a:0.03:1,0,0,1,5,0,0,1,2" "a:0.03 a:0.03"; do
  # shellcheck disable=SC2086
  run db $bad; rc=$?
  [ $rc = 2 ] && [ ! -s "$T/log" ] && ok "refused before any SQL: $bad" || no "refused before any SQL: $bad (rc $rc)"
done
run --labels other db a:0.03; [ $? = 2 ] && ok "refused: --labels other" || no "refused: --labels other"

VER=170000 run db a:0.03 b:0.03
grep -q 'CREATE PUBLICATION db_pub FOR TABLES IN SCHEMA a,b;' "$T/log" && grep -q 'exists: kept' "$T/out" \
  && ok "Postgres 17: publication by schema; existing schemas kept" || no "Postgres 17: publication by schema"
VER=140000 run --publication p db a:0.03 b:0.03
grep -q 'CREATE PUBLICATION p FOR TABLE a.lineitem, b.lineitem;' "$T/log" && ok "Postgres 14: publication by table name" \
  || no "Postgres 14: publication by table name"
PUB_IN=1 run db a:0.03 b:0.03
grep -q 'ALTER PUBLICATION db_pub SET TABLES IN SCHEMA a,b;' "$T/log" && ok "existing publication altered" || no "existing publication altered"

SLOT_IN=0 run slot db s1; grep -q "pg_create_logical_replication_slot('s1', 'pgoutput')" "$T/log" && ok "slot created when missing" || no "slot created when missing"
SLOT_IN=1 run slot db s1; grep -q pg_create_logical_replication_slot "$T/log" && no "existing slot kept" || ok "existing slot kept"
run slot db s1 --drop; grep -q "pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 's1' AND NOT active" "$T/log" \
  && ok "slot dropped when not active" || no "slot dropped when not active"

rm -rf "${T:?}"
[ $fails = 0 ] && echo "all passed" || { echo "$fails failed"; exit 1; }
