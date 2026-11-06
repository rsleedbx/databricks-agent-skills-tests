#!/usr/bin/env bash
# sqlserver-add-column-staged.sh: can a column with a default be added to a SQL Server table without a full refresh, by
# staging it? ADD COLUMN with a default needs a backfill (CDC_INCOMPATIBLE_SCHEMA_CHANGES.ADD_COLUMN_WITH_BACKFILL), so
# the staged way adds the column nullable, fills it with UPDATEs (which reach the destination as ordinary changes), and
# adds the default afterwards as a constraint. Two tables per mode (CDC and change tracking):
#
#   direct_<m>   ALTER TABLE ADD c INT NOT NULL CONSTRAINT DEFAULT 7, one new row            (the control)
#   staged_<m>   1. ALTER TABLE ADD c INT NULL, one new row with c = 7
#                2. UPDATE SET c = 7 in two batches (ids 1-5, then the rest)
#                3. ALTER TABLE ADD CONSTRAINT DEFAULT 7 FOR c, one new row without c
#                4. ALTER TABLE ALTER COLUMN c INT NOT NULL, one new row without c            (observed)
#
#   Needs and options: as sqlserver-add-column.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER, or SOURCE_JSON; MODE,
#   KEEP, KEEP_DAYS, OUT). About 40 minutes on standard CDC.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init addcolstg
MODES="cdc ct"
TABLES=""; for m in $MODES; do TABLES+=" direct_$m staged_$m"; done
FP_TABLES=$TABLES

{ for t in $TABLES; do echo "CREATE TABLE lfcddemo.$t (id INT NOT NULL, v VARCHAR(20), x INT, CONSTRAINT pk_$t PRIMARY KEY (id));"; done; } | src_db
for t in $TABLES; do fill $t 10; done
src_util
track cdc direct_cdc staged_cdc
track ct direct_ct staged_ct

OBJS=""; for t in $TABLES; do OBJS+="${OBJS:+,}$(obj $t)"; done
pipelines_create "$OBJS"
count() { q "SELECT COUNT(*) FROM $T.$1" 2>&1; }
cval() { q "SELECT COALESCE(CAST(SUM(CASE WHEN c = 7 THEN 1 ELSE 0 END) AS STRING), 'no c') FROM $T.$1" 2>&1; }   # rows with c = 7
cols() { q "SELECT CONCAT_WS(',', COLLECT_LIST(column_name)) FROM \`$CATALOG\`.information_schema.columns WHERE table_schema = '$S' AND table_name = '$1'" 2>&1; }
# up to 4 updates until each table has <want> rows or an error that needs a full refresh (hand-off errors such as
# FLOW_SCHEMA_CHANGED clear on a later update)
wait_rows() {   # wait_rows <want> <table...>
  local want=$1 n t; shift
  for _ in 1 2 3 4; do
    sleep "$WAIT"; update
    n=0; for t in "$@"; do { needs_refresh $t || [ "$(count $t)" = "$want" ]; } && n=$((n + 1)); done
    [ $n -eq $# ] && return 0
  done
}
# up to 4 updates until each table has <want> rows with c = 7, or an error that needs a full refresh
wait_c() {   # wait_c <want> <table...>
  local want=$1 n t; shift
  for _ in 1 2 3 4; do
    sleep "$WAIT"; update
    n=0; for t in "$@"; do { needs_refresh $t || [ "$(cval $t)" = "$want" ]; } && n=$((n + 1)); done
    [ $n -eq $# ] && return 0
  done
}
# the baseline: a classic gateway (GATEWAY_COMPUTE=classic) waits minutes for its cluster before the snapshot
update; for _ in 1 2 3 4 5; do ok=1; for t in $TABLES; do [ "$(count $t)" = 10 ] || ok=0; done; [ $ok = 1 ] && break; sleep "$WAIT"; update; done
[ $ok = 1 ] || { echo "baseline rows wrong; $ERRS"; exit 1; }

# --- 0 (control) and 1: the column -------------------------------------------------------------------------------------
for m in $MODES; do
  src <<EOF
ALTER TABLE lfcddemo.direct_$m ADD c INT NOT NULL CONSTRAINT df_direct_$m DEFAULT 7;
GO
INSERT INTO lfcddemo.direct_$m (id, v) VALUES (11, 'after');
ALTER TABLE lfcddemo.staged_$m ADD c INT NULL;
GO
INSERT INTO lfcddemo.staged_$m (id, v, c) VALUES (11, 'after', 7);
EOF
done
wait_rows 11 $TABLES
for m in $MODES; do
  e=$(errs_for direct_$m)
  if [[ "$e" == *ADD_COLUMN_WITH_BACKFILL* ]]; then claim "direct-$m-needs-refresh" PASS "ADD c NOT NULL DEFAULT 7: ${e% }"
  else observe "direct-$m-needs-refresh" "errors: ${e:-none}; rows $(count direct_$m); columns $(cols direct_$m)"; fi
  e=$(errs_for staged_$m)
  ! needs_refresh staged_$m && [ "$(count staged_$m)" = 11 ] && [[ ",$(cols staged_$m)," == *,c,* ]] \
    && claim "staged-$m-1-add-nullable" PASS "no refresh needed; 11 rows; c in the destination; rows with c = 7: $(cval staged_$m)" \
    || claim "staged-$m-1-add-nullable" FAIL "errors: ${e:-none} $(msgs_for staged_$m | head -1); rows $(count staged_$m); columns $(cols staged_$m)"
done

# --- 2: fill it with UPDATEs ---------------------------------------------------------------------------------------
for m in $MODES; do
  src <<EOF
UPDATE lfcddemo.staged_$m SET c = 7 WHERE id BETWEEN 1 AND 5;
UPDATE lfcddemo.staged_$m SET c = 7 WHERE c IS NULL;
EOF
done
wait_c 11 staged_cdc staged_ct
for m in $MODES; do
  e=$(errs_for staged_$m)
  ! needs_refresh staged_$m && [ "$(cval staged_$m)" = 11 ] \
    && claim "staged-$m-2-backfill-by-update" PASS "no refresh needed; all 11 rows have c = 7" \
    || claim "staged-$m-2-backfill-by-update" FAIL "errors: ${e:-none} $(msgs_for staged_$m | head -1); rows with c = 7: $(cval staged_$m) of $(count staged_$m)"
done

# --- 3: the default, afterwards ------------------------------------------------------------------------------------
for m in $MODES; do
  src <<EOF
ALTER TABLE lfcddemo.staged_$m ADD CONSTRAINT df_staged_$m DEFAULT 7 FOR c;
GO
INSERT INTO lfcddemo.staged_$m (id, v) VALUES (12, 'default');
EOF
done
wait_rows 12 staged_cdc staged_ct
for m in $MODES; do
  e=$(errs_for staged_$m)
  ! needs_refresh staged_$m && [ "$(count staged_$m)" = 12 ] && [ "$(cval staged_$m)" = 12 ] \
    && claim "staged-$m-3-default-later" PASS "no refresh needed; 12 rows, all with c = 7 (the new row from the default)" \
    || claim "staged-$m-3-default-later" FAIL "errors: ${e:-none} $(msgs_for staged_$m | head -1); rows $(count staged_$m); with c = 7: $(cval staged_$m)"
done

# --- 4 (observed): NOT NULL ----------------------------------------------------------------------------------------
for m in $MODES; do
  src <<EOF
ALTER TABLE lfcddemo.staged_$m ALTER COLUMN c INT NOT NULL;
GO
INSERT INTO lfcddemo.staged_$m (id, v) VALUES (13, 'notnull');
EOF
done
wait_rows 13 staged_cdc staged_ct
for m in $MODES; do
  e=$(errs_for staged_$m)
  observe "staged-$m-4-not-null" "errors: ${e:-none}$( [ -n "${e// /}" ] && echo "; $(msgs_for staged_$m | head -1 | cut -c1-160)"); rows $(count staged_$m); with c = 7: $(cval staged_$m)"
done

result_md sqlserver-add-column-staged
exit $FAILED
