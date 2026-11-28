#!/usr/bin/env bash
# sqlserver-cii-deny.sh: on a CDC table, does a column added after the first load need the login's UPDATE on the
# utility objects' lakeflowCaptureInstanceInfo table? Answers a field question (a cause listed for
# TABLE_METADATA_NOT_READY_INTERNAL_ERROR). One CDC table, 10 rows; after the load a row, then ADD COLUMN and a row.
# Prints the destination rows and columns after each step, the source's capture instances and the info table's row
# (old/new capture instance, committedCursor: what the gateway writes when it moves to the new capture instance).
#
#   CII_DENY=1 (default) denies the login UPDATE on lakeflowCaptureInstanceInfo after setup; CII_DENY=0 is the control.
#   Needs and options: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init cii
CII_DENY=${CII_DENY:-1}; conf cii_deny "$CII_DENY"
FP_TABLES=p_cii
echo "CREATE TABLE lfcddemo.p_cii (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));" | src_db
fill p_cii 10; src_util; track cdc p_cii
CII=$(printf "SET NOCOUNT ON; SELECT TOP 1 name FROM sys.tables WHERE name LIKE 'lakeflowCaptureInstanceInfo%%';\n" | src | tr -d '[:space:]')
# the info table must exist, and in deny mode the DENY must show in the permissions: else the run would be the control
[[ "$CII" =~ ^lakeflowCaptureInstanceInfo[A-Za-z0-9_]*$ ]] || { claim setup FAIL "no lakeflowCaptureInstanceInfo table found (got '${CII:0:120}')"; result_md sqlserver-cii-deny; exit 1; }
[ "$CII_DENY" = 1 ] && echo "DENY UPDATE ON dbo.$CII TO $CONN_USER;" | src >/dev/null
PERM=$(printf "SET NOCOUNT ON; SELECT ISNULL(MAX(state_desc), 'none') FROM sys.database_permissions WHERE major_id = OBJECT_ID('dbo.%s') AND permission_name = 'UPDATE' AND grantee_principal_id = USER_ID('%s');\n" "$CII" "$CONN_USER" | src 2>&1 | tr -d '[:space:]')
[ "$CII_DENY" = 1 ] && [ "$PERM" != DENY ] && { claim setup FAIL "DENY UPDATE on dbo.$CII to $CONN_USER didn't take (permission state: ${PERM:0:120})"; result_md sqlserver-cii-deny; exit 1; }
observe setup "CDC on p_cii; UPDATE on dbo.$CII $( [ "$CII_DENY" = 1 ] && echo denied || echo granted) to the login (permission state: $PERM)"
pipelines_create "$(obj p_cii)"
update; [ "$(q "SELECT COUNT(*) FROM $T.p_cii" 2>&1)" = 10 ] || update
cols() { q "SELECT CONCAT_WS(',', COLLECT_LIST(column_name)) FROM \`$CATALOG\`.information_schema.columns WHERE table_schema = '$S' AND table_name = 'p_cii'" 2>&1; }
info() { printf "SET NOCOUNT ON; SELECT CONCAT(oldCaptureInstance, ' -> ', newCaptureInstance, ', committedCursor ', ISNULL(committedCursor, 'NULL'), ', triggerReinit ', triggerReinit) FROM dbo.%s WHERE tableName = 'p_cii';\nSELECT STRING_AGG(capture_instance, ' ') FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('lfcddemo.p_cii');\n" "$CII" | src 2>&1 | paste -sd';' - | sed 's/;/; capture instances: /'; }
step() {   # <rows wanted> <name>: up to 4 updates until the rows arrive or the table has an error
  settle_rows "SELECT COUNT(*) FROM $T.p_cii" "$1" p_cii
  observe "$2" "rows $ROWS (source $1); ids $(q "SELECT CONCAT_WS(',', COLLECT_LIST(id)) FROM (SELECT id FROM $T.p_cii WHERE id > 10 ORDER BY id)" 2>&1); columns $(cols); errors: ${TERRS:-none}$( [ -n "${TERRS// /}" ] && echo "; $(msgs_for p_cii | head -1 | cut -c1-200)"); source: $(info)"
  [ "$SETTLED" = 1 ]
}
observe after-load "rows $(q "SELECT COUNT(*) FROM $T.p_cii" 2>&1); columns $(cols); source: $(info)"
echo "INSERT INTO lfcddemo.p_cii (id, v) VALUES (11, 'after');" | src >/dev/null
# the precondition: a plain change after the load arrives; else a missing row later says nothing about the deny
if ! step 11 row-after-load; then
  claim row-after-load FAIL "a plain row after the load didn't arrive (rows $ROWS; errors ${TERRS:-none}): the ADD COLUMN step can't be judged"
  result_md "sqlserver-cii-deny (CII_DENY=$CII_DENY)"; exit $FAILED
fi
echo "ALTER TABLE lfcddemo.p_cii ADD c INT NULL;" | src >/dev/null
echo "INSERT INTO lfcddemo.p_cii (id, v, c) VALUES (12, 'ddl', 1);" | src >/dev/null
# the expected answer depends on the mode: with UPDATE granted the row arrives; denied, the gateway can't move to the
# new capture instance and the row doesn't arrive, with no error. Denied, the claim passes only when the
# gateway is alive (column c arrived), the table has no error and committedCursor is still NULL: a stall for another
# reason, or an error instead of the silent stop, fails it
if step 12 column-after-load; then arrived=1; else arrived=0; fi
C=$(cols); I=$(info)
if [ "$CII_DENY" = 0 ]; then
  [ $arrived = 1 ] && claim column-row-arrives PASS "after ADD COLUMN the new row arrived (UPDATE on $CII granted)" \
    || claim column-row-arrives FAIL "after ADD COLUMN the new row didn't arrive in 4 updates (UPDATE on $CII granted); errors ${TERRS:-none}; source: $I"
elif [ $arrived = 1 ]; then
  claim column-row-stops-when-denied FAIL "after ADD COLUMN the new row arrived although UPDATE on $CII is denied: the documented behavior changed"
elif [ -n "${TERRS// /}" ]; then
  claim column-row-stops-when-denied FAIL "the row didn't arrive, but with an error on p_cii ($TERRS): no longer silent; $(msgs_for p_cii | head -1 | cut -c1-200)"
elif [[ ",$C," != *,c,* ]] || [[ "$I" != *"committedCursor NULL"* ]]; then
  claim column-row-stops-when-denied FAIL "the row didn't arrive, but not the documented way (columns $C; source: $I): the stop is another cause"
else
  claim column-row-stops-when-denied PASS "after ADD COLUMN column c arrived but the new row didn't (4 updates), no error, committedCursor NULL (UPDATE on $CII denied), as documented; source: $I"
fi
result_md "sqlserver-cii-deny (CII_DENY=$CII_DENY)"
exit $FAILED
