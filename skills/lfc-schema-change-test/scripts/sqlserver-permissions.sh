#!/usr/bin/env bash
# sqlserver-permissions.sh: what does the pipeline report when the gateway can't read a table's data or its change
# metadata? Answers a field question (TABLE_METADATA_NOT_READY_INTERNAL_ERROR: which source problems lead to it) and shows
# #14's per-table stop: one table fails, the others keep loading.
#
#   p_ok        change tracking, the login can read it          (control)
#   p_noselect  change tracking, SELECT denied to the login
#   p_notrack   neither change tracking nor CDC
#   p_cdcdeny   CDC, SELECT on its change table denied to the login
#   p_ciideny   CDC, UPDATE on the utility objects' lakeflowCaptureInstanceInfo table denied to the login
#   p_locked    change tracking, a schema lock held on the table while the pipeline starts (released after LOCK_MIN,
#               default 15 minutes); checked held (sys.dm_tran_locks) before and after the first update, and sampled
#               for sessions it blocks (the gateway's reads) during it
#
#   After the first load, p_ciideny gets a new row, then a column (with a row): does the capture-instance refresh need
#   the UPDATE on lakeflowCaptureInstanceInfo that the login is denied?
#
#   Needs and options: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init perm
TABLES="p_ok p_noselect p_notrack p_cdcdeny p_ciideny p_locked"; FP_TABLES=$TABLES
{ for t in $TABLES; do echo "CREATE TABLE lfcddemo.$t (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));"; done; } | src_db
for t in $TABLES; do fill $t 10; done
src_util
track ct p_ok p_noselect p_locked; track cdc p_cdcdeny p_ciideny
echo "GRANT SELECT ON lfcddemo.p_notrack TO $CONN_USER;" | src >/dev/null
# after lakeflowFixPermissions (inside track) has granted everything: take two of the grants away again
src >/dev/null <<EOF
DENY SELECT ON lfcddemo.p_noselect TO $CONN_USER;
DECLARE @ct sysname = (SELECT OBJECT_SCHEMA_NAME(object_id) + '.' + name FROM sys.tables
  WHERE object_id = (SELECT object_id FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('lfcddemo.p_cdcdeny')));
EXEC ('DENY SELECT ON ' + @ct + ' TO $CONN_USER');
DECLARE @cii sysname = (SELECT TOP 1 name FROM sys.tables WHERE name LIKE 'lakeflowCaptureInstanceInfo%');
IF @cii IS NOT NULL EXEC ('DENY UPDATE ON dbo.' + @cii + ' TO $CONN_USER');
EOF
# a schema-modification lock on p_locked, held by a session on the VM for 15 minutes (BEGIN TRAN + ALTER, then rollback)
LOCK_MIN=${LOCK_MIN:-15}
# the table's id now: once the lock is held, OBJECT_ID('lfcddemo.p_locked') itself waits for it
PLID=$(printf "SET NOCOUNT ON; SELECT OBJECT_ID('lfcddemo.p_locked');\n" | src | tr -d '[:space:]')
printf "BEGIN TRAN; ALTER TABLE lfcddemo.p_locked ADD lockcol INT NULL; WAITFOR DELAY '%02d:%02d:00'; ROLLBACK;\n" $((LOCK_MIN / 60)) $((LOCK_MIN % 60)) | src >/dev/null 2>&1 &
LOCKER=$!
# Sch-M locks granted on p_locked now, and the sessions waiting on p_locked's lock (login, wait type), one line each
lock_held() { printf "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_tran_locks WHERE resource_type = 'OBJECT' AND resource_database_id = DB_ID() AND resource_associated_entity_id = $PLID AND request_mode = 'Sch-M' AND request_status = 'GRANT';\n" | src 2>/dev/null | tr -d '[:space:]'; }
# every session the lock holder blocks (on the table or on the metadata rows its ALTER changed): login, program, database
lock_waiters() { [ -n "$LOCKSPID" ] || return 0; printf "SET NOCOUNT ON; SELECT s.login_name + ' ' + s.program_name + ' ' + DB_NAME(r.database_id) + ' ' + r.wait_type FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id WHERE r.blocking_session_id = %s;\n" "$LOCKSPID" | $SRC_SQL master 2>/dev/null; }
# the session holding the granted Sch-M on p_locked in this database (object ids repeat across databases), waited for
# up to 60 s: a slow connect would leave it empty, and the sampler would then find no one blocked
LOCKSPID=""
for _ in 1 2 3 4 5 6; do
  sleep 10
  LOCKSPID=$(printf "SET NOCOUNT ON; SELECT TOP 1 request_session_id FROM sys.dm_tran_locks WHERE resource_type = 'OBJECT' AND resource_database_id = DB_ID() AND resource_associated_entity_id = %s AND request_mode = 'Sch-M' AND request_status = 'GRANT' ORDER BY request_session_id;\n" "$PLID" | src 2>/dev/null | tr -d '[:space:]')
  [[ "$LOCKSPID" =~ ^[0-9]+$ ]] && break; LOCKSPID=""
done
LOCK0=$(lock_held)
( while kill -0 $LOCKER 2>/dev/null; do lock_waiters >> "$W/lock-waiters.txt"; sleep 30; done ) &
SAMPLER=$!
pipelines_create "$(for t in $TABLES; do printf '%s,' "$(obj $t)"; done | sed 's/,$//')"
update; [ "$(q "SELECT COUNT(*) FROM $T.p_ok" 2>&1)" = 10 ] || update
LOCK1=$(lock_held)
observe lock-held "Sch-M on p_locked: ${LOCK0:-?} before the pipeline started (holder session ${LOCKSPID:-not found in 60 s, so no sessions sampled}), ${LOCK1:-?} after the first load ($LOCK_MIN-minute lock); sessions it blocked (sampled every 30 s): $(sort "$W/lock-waiters.txt" 2>/dev/null | uniq -c | sed 's/^ *//' | paste -sd';' - | sed 's/;/; /g')"
for t in $TABLES; do
  e=$(errs_for $t)
  observe "$t" "rows $(q "SELECT COUNT(*) FROM $T.$t" 2>&1); errors: ${e:-none}$( [ -n "${e// /}" ] && echo "; $(msgs_for $t | head -1 | cut -c1-220)")"
done
observe update-chain "state $STATE; all errors: ${ERRS:-none}; $CHAIN"
# the control keeps loading while the others fail
echo "INSERT INTO lfcddemo.p_ok (id, v) VALUES (11, 'after');" | src >/dev/null
settle_rows "SELECT COUNT(*) FROM $T.p_ok" 11
[ "$SETTLED" = 1 ] && claim others-keep-loading PASS "p_ok picked up a new row while the other tables failed (errors: ${ERRS:-none})" \
  || claim others-keep-loading FAIL "p_ok rows $(q "SELECT COUNT(*) FROM $T.p_ok" 2>&1); errors: ${ERRS:-none}"
# p_ciideny after the load, with UPDATE on lakeflowCaptureInstanceInfo still denied: a row, then a column and a row
ciideny() {   # <rows wanted> <name>: up to 4 updates until the rows arrive or the table has an error
  settle_rows "SELECT COUNT(*) FROM $T.p_ciideny" "$1" p_ciideny
  observe "$2" "rows $ROWS (source $1); columns $(q "SELECT CONCAT_WS(',', COLLECT_LIST(column_name)) FROM \`$CATALOG\`.information_schema.columns WHERE table_schema = '$S' AND table_name = 'p_ciideny'" 2>&1); errors: ${TERRS:-none}$( [ -n "${TERRS// /}" ] && echo "; $(msgs_for p_ciideny | head -1 | cut -c1-220)")"
}
echo "INSERT INTO lfcddemo.p_ciideny (id, v) VALUES (11, 'after');" | src >/dev/null
ciideny 11 ciideny-row-after-load
echo "ALTER TABLE lfcddemo.p_ciideny ADD c INT NULL;" | src >/dev/null
echo "INSERT INTO lfcddemo.p_ciideny (id, v, c) VALUES (12, 'ddl', 1);" | src >/dev/null
ciideny 12 ciideny-column-after-load
observe ciideny-capture-instances "$(printf "SET NOCOUNT ON; SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('lfcddemo.p_ciideny');\n" | src 2>&1 | paste -sd' ' -)"
# fix the source, then a full refresh of the failed tables recovers them
wait $LOCKER 2>/dev/null   # the lock is released when its LOCK_MIN minutes are up
kill $SAMPLER 2>/dev/null
src >/dev/null <<EOF
REVOKE SELECT ON lfcddemo.p_noselect TO $CONN_USER;
DECLARE @ct sysname = (SELECT OBJECT_SCHEMA_NAME(object_id) + '.' + name FROM sys.tables
  WHERE object_id = (SELECT object_id FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('lfcddemo.p_cdcdeny')));
EXEC ('REVOKE SELECT ON ' + @ct + ' TO $CONN_USER');
DECLARE @cii sysname = (SELECT TOP 1 name FROM sys.tables WHERE name LIKE 'lakeflowCaptureInstanceInfo%');
IF @cii IS NOT NULL EXEC ('REVOKE UPDATE ON dbo.' + @cii + ' TO $CONN_USER');
EOF
track ct p_notrack
sleep "$WAIT"
want() { [ "$1" = p_ciideny ] && echo 12 || echo 10; }   # p_ciideny got two rows after the load
FR=""; for t in p_noselect p_notrack p_cdcdeny p_ciideny p_locked; do [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = "$(want $t)" ] || FR+="${FR:+,}$t"; done
if [ -n "$FR" ]; then
  update "$FR"
  # standard CDC: the gateway snapshots the refreshed tables again, so the tables can arrive a few updates later
  for _ in 1 2 3; do
    bad=""; for t in ${FR//,/ }; do [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = "$(want $t)" ] || bad+=" $t"; done
    [ -z "$bad" ] && break; sleep "$WAIT"; update
  done
  bad=""; for t in ${FR//,/ }; do r=$(q "SELECT COUNT(*) FROM $T.$t" 2>&1 | head -1 | cut -c1-40); [ "$r" = "$(want $t)" ] || bad+=" $t(rows $r; errors $(errs_for $t | tr -d ' '))"; done
  [ -z "$bad" ] && claim fix-then-refresh-recovers PASS "after fixing the source, a full refresh of $FR loads each table's source rows" \
    || claim fix-then-refresh-recovers FAIL "after fixing and refreshing $FR, still off:$bad"
fi
result_md sqlserver-permissions
exit $FAILED
