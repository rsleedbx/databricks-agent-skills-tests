#!/usr/bin/env bash
# sqlserver-same-name.sh: can one pipeline ingest two source tables that have the same name in different source schemas
# (lfcddemo.orders and lfcdb.orders)? The SQL Server and PostgreSQL limits pages say no ("Databricks can't ingest two or
# more tables with the same name in the same pipeline, even if they come from different source schemas"); the MySQL
# page says yes, to different destination schemas, with a multi-destination pipeline (the default publishing mode).
#
# lfcddemo.orders has 3 rows (v a1..a3), lfcdb.orders 5 rows (v b1..b5), so each destination row shows which source it
# came from. After the first load, one update, one insert and one delete on each; then each destination must hold its
# own source's rows and nothing of the other's.
#
#   CASE (two-schemas): how the two tables are mapped
#     two-schemas   table specs, lfcddemo.orders -> $S.orders, lfcdb.orders -> ${S}_b.orders (a multi-destination pipeline)
#     schema-specs  schema specs, lfcddemo -> $S, lfcdb -> ${S}_b (every table of each source schema)
#     one-schema    table specs, both to $S.orders (expected to be refused)
#     rename        table specs, both to $S, as orders_a and orders_b (destination_table)
#     legacy        two-schemas, in a pipeline created in the legacy publishing mode ("target" in place of "schema")
#     flowname      lfcddemo.orders -> ${S}_raw.orders, lfcdb.orders -> $S.raw_orders: different destination tables whose
#                   names are the same once "." becomes "_" (<catalog>_<S>_raw_orders), the way flow names are built:
#                   the create is accepted, then every update fails with FLOW_DEFINED_IN_MULTIPLE_DATASETS and the
#                   pipeline retries on its own (2026-10-23), so the run stops the pipeline after the first update
#   TRACK (ct): ct (change tracking) or cdc, on both tables
#   Needs and options: as sqlserver-add-column.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
CASE=${CASE:-two-schemas}; TRACK=${TRACK:-ct}
case $CASE in two-schemas|schema-specs|one-schema|rename|legacy|flowname) ;; *) echo "CASE: two-schemas schema-specs one-schema rename legacy flowname"; exit 2;; esac
case $TRACK in ct|cdc) ;; *) echo "TRACK must be ct or cdc"; exit 2;; esac
lfc_init samename
SB=${S}_b; [ "$CASE" = flowname ] && SB=${S}_raw; TB="\`$CATALOG\`.$SB"
conf case "$CASE"; conf track "$TRACK"
trap 'cleanup; [ "${KEEP:-0}" = 1 ] || dbx schemas delete "$CATALOG.$SB" --force 2>/dev/null' EXIT
src_db <<'EOF'
CREATE TABLE lfcddemo.orders (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
GO
CREATE SCHEMA lfcdb;
GO
CREATE TABLE lfcdb.orders (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
INSERT INTO lfcddemo.orders (id, v) VALUES (1, 'a1'), (2, 'a2'), (3, 'a3');
INSERT INTO lfcdb.orders (id, v) VALUES (1, 'b1'), (2, 'b2'), (3, 'b3'), (4, 'b4'), (5, 'b5');
EOF
src_util
proc=lakeflowSetupChangeTracking ddl=", @CreateDdlSupportingObjects = 1"   # change tracking: the DDL audit objects are opt-in in 1.7
[ "$TRACK" = cdc ] && { proc=lakeflowSetupChangeDataCapture; ddl=""; }
echo "EXEC dbo.$proc @Tables = 'lfcddemo.orders,lfcdb.orders', @User = '$CONN_USER'$ddl;" | src > "$W/track.out" 2>&1
tv="(SELECT object_id FROM sys.change_tracking_tables)"; [ "$TRACK" = cdc ] && tv="(SELECT object_id FROM sys.tables WHERE is_tracked_by_cdc = 1)"
n=$(printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE name = 'orders' AND object_id IN %s;\n" "$tv" | src | tr -d '[:space:]')
[ "$n" = 2 ] || { echo "could not set up $TRACK on both tables (tracked: $n): $(grep -i -m1 -E 'error|msg [0-9]' "$W/track.out" | cut -c1-160)"; exit 2; }
for s in lfcddemo lfcdb; do echo "EXEC dbo.lakeflowFixPermissions @User = '$CONN_USER', @Tables = 'SCHEMAS:$s';" | src >/dev/null; done

# one table spec: <source schema> <destination schema> [destination table]
tspec() { echo "{\"table\":{\"source_catalog\":\"$DB\",\"source_schema\":\"$1\",\"source_table\":\"orders\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$2\"${3:+,\"destination_table\":\"$3\"},\"table_configuration\":{\"scd_type\":\"SCD_TYPE_1\"}}}"; }
sspec() { echo "{\"schema\":{\"source_catalog\":\"$DB\",\"source_schema\":\"$1\",\"destination_catalog\":\"$CATALOG\",\"destination_schema\":\"$2\",\"table_configuration\":{\"scd_type\":\"SCD_TYPE_1\"}}}"; }
# where each source table lands: A_T (lfcddemo.orders), B_T (lfcdb.orders)
A_T=$T.orders; B_T=$TB.orders; FP_TABLES=orders
case $CASE in
  two-schemas|legacy) OBJS="$(tspec lfcddemo "$S"),$(tspec lfcdb "$SB")" ;;
  schema-specs)       OBJS="$(sspec lfcddemo "$S"),$(sspec lfcdb "$SB")" ;;
  one-schema)         OBJS="$(tspec lfcddemo "$S"),$(tspec lfcdb "$S")"; B_T=$A_T ;;
  flowname)           OBJS="$(tspec lfcddemo "$SB"),$(tspec lfcdb "$S" raw_orders)"; A_T=$TB.orders; B_T=$T.raw_orders; FP_TABLES=raw_orders ;;
  rename)             OBJS="$(tspec lfcddemo "$S" orders_a),$(tspec lfcdb "$S" orders_b)"; A_T=$T.orders_a; B_T=$T.orders_b; FP_TABLES="orders_a orders_b" ;;
esac
[ "$CASE" = one-schema ] || [ "$CASE" = rename ] || dbx schemas create "$SB" "$CATALOG" >/dev/null || exit 2
if [ "$CASE" = legacy ]; then
  # pipelines_create with "target" in place of "schema": the legacy publishing mode
  dbx() { if [ "${1:-}" = pipelines ] && [ "${2:-}" = create ] && [[ "$*" == *ingestion_definition* ]]; then
            local a=("$@"); a[5]=$(echo "${a[5]}" | sed 's/"schema":"/"target":"/'); databricks "${a[@]}" --profile "$PROFILE"
          else databricks "$@" --profile "$PROFILE"; fi; }
fi
CREATE_SOFT=1 pipelines_create "$OBJS"; rc=$?
[ "$CASE" = legacy ] && observe legacy-mode "created with target: $( [ $rc = 0 ] && dbx pipelines get "$AP" -o json | yq -o json -I0 '{"schema": .spec.schema, "target": .spec.target}')"
if [ $rc != 0 ]; then
  observe create-refused "$CREATE_ERR"
  claim pipeline-created FAIL "create refused: $CREATE_ERR"
  result_md "sqlserver-same-name ($CASE, $TRACK)"; exit $FAILED
fi
# the create doesn't check the mapping (2026-10-23): both to one destination table is refused at the first update
claim pipeline-created PASS "the create accepted both tables ($CASE)"

# rows of each destination as "id:v ..." (sorted)
# (COLLECT_LIST doesn't keep a subquery's order: sort the array; ids stay single-digit, so text order is id order)
rows() { q "SELECT ARRAY_JOIN(SORT_ARRAY(COLLECT_LIST(CONCAT(id, ':', v))), ' ') FROM $1" 2>&1 | head -1 | cut -c1-200; }
both() { echo "$(rows "$A_T") | $(rows "$B_T")"; }
WANT1="1:a1 2:a2 3:a3 | 1:b1 2:b2 3:b3 4:b4 5:b5"
if [ "$CASE" = flowname ]; then
  # one update, not the chain: the pipeline retries the same failure on its own (RETRY_ON_FAILURE)
  dbx pipelines start-update "$AP" --json '{}' >/dev/null; sleep 30
  for _ in $(seq 1 45); do u=$(newest); STATE=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state')
    case $STATE in COMPLETED|FAILED|CANCELED) break;; esac; sleep 20; done
  sleep 60; dbx pipelines stop "$AP" >/dev/null 2>&1
  n=$(dbx pipelines list-updates "$AP" -o json | yq '[.updates[] | select(.cause == "RETRY_ON_FAILURE")] | length')
  m=$(dbx api get "/api/2.0/pipelines/$AP/events?max_results=100" -o json | yq -r '[.events[] | .error.exceptions[]? | select(.error_class) | .error_class + ": " + .message][0] // ""' \
      | sed -E "s/$S/<S>/g; s/$CATALOG/<catalog>/g" | cut -c1-300)
  [[ "$m" == *FLOW_DEFINED_IN_MULTIPLE_DATASETS* ]] \
    && claim flowname-refused PASS "the first update failed ($STATE): $m; $n automatic retries before the stop" \
    || claim flowname-refused FAIL "first update $STATE; errors: ${m:-none}"
  result_md "sqlserver-same-name ($CASE, $TRACK)"; exit $FAILED
fi
for i in 1 2 3 4 5; do [ $i = 1 ] || sleep "$WAIT"; update; [ "$(both)" = "$WANT1" ] && break; [ -n "${ERRS// /}" ] && break; done
GOT1=$(both)
observe first-load "$GOT1 (updates: $CHAIN; errors: ${ERRS:-none})"
[ -n "${ERRS// /}" ] && observe first-load-errors "$(yq -r '.[] | .error.exceptions[]? | select(.error_class) | .error_class + ": " + .message' "$W/ev_last.json" | sort -u | cut -c1-300 | head -4 | tr '\n' ' ')"
if [ "$CASE" = one-schema ]; then
  [[ "$ERRS" == *CDC_MULTIPLE_SOURCE_TABLES_MAPPED_TO_SAME_DESTINATION_TABLE* ]] \
    && claim one-destination-refused PASS "the first update fails with CDC_MULTIPLE_SOURCE_TABLES_MAPPED_TO_SAME_DESTINATION_TABLE; no destination table: $(rows "$A_T" | cut -c1-60)" \
    || claim one-destination-refused FAIL "errors: ${ERRS:-none}; destination: $GOT1"
  result_md "sqlserver-same-name ($CASE, $TRACK)"; exit $FAILED
fi
[ "$GOT1" = "$WANT1" ] && claim first-load PASS "each destination has its own source's rows: $GOT1" \
  || claim first-load FAIL "want $WANT1, got $GOT1"

src >/dev/null <<'EOF'
UPDATE lfcddemo.orders SET v = 'a1-upd' WHERE id = 1; INSERT INTO lfcddemo.orders (id, v) VALUES (4, 'a4-new'); DELETE FROM lfcddemo.orders WHERE id = 3;
UPDATE lfcdb.orders SET v = 'b3-upd' WHERE id = 3; INSERT INTO lfcdb.orders (id, v) VALUES (6, 'b6-new'); DELETE FROM lfcdb.orders WHERE id = 2;
EOF
WANT2="1:a1-upd 2:a2 4:a4-new | 1:b1 3:b3-upd 4:b4 5:b5 6:b6-new"
for _ in 1 2 3 4; do sleep "$WAIT"; update; [ "$(both)" = "$WANT2" ] && break; [ -n "${ERRS// /}" ] && break; done
GOT2=$(both)
[ "$GOT2" = "$WANT2" ] && claim changes-applied PASS "each destination got only its own source's changes: $GOT2" \
  || claim changes-applied FAIL "want $WANT2, got $GOT2 (errors: ${ERRS:-none})"
example "lfcddemo.orders -> $(echo "$A_T" | sed 's/.*\.//; s/`//g') (after the changes)" "id|v" "SELECT CONCAT_WS(CHAR(9), id, v) FROM lfcddemo.orders ORDER BY id;" "id|v" "SELECT id, v FROM $A_T ORDER BY id"
example "lfcdb.orders -> $(echo "$B_T" | sed 's/.*\.//; s/`//g') (after the changes)" "id|v" "SELECT CONCAT_WS(CHAR(9), id, v) FROM lfcdb.orders ORDER BY id;" "id|v" "SELECT id, v FROM $B_T ORDER BY id"
observe destinations "lfcddemo.orders -> ${A_T//\`/}, lfcdb.orders -> ${B_T//\`/}"
observe flows "$(dbx api get "/api/2.0/pipelines/$AP/events?max_results=250" -o json | yq -r '[.events[] | .origin.flow_name // ""] | unique | map(select(. != "")) | join(" ")' | cut -c1-400)"
result_md "sqlserver-same-name ($CASE, $TRACK)"
exit $FAILED
