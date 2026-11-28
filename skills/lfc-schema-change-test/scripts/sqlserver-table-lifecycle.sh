#!/usr/bin/env bash
# sqlserver-table-lifecycle.sh: what a SQL Server source table's lifecycle does to a Lakeflow Connect pipeline, for a
# change-tracking and a CDC table each. Answers four field questions:
#   #3  is change data feed on for the destination streaming tables, and can table_changes() read it?
#   #6  DELETE every row + INSERT again: applied incrementally?
#   #4  add a table to the ingestion definition: does it need a full refresh, and are the other tables reset?
#   #6  TRUNCATE: refused on a CDC table by SQL Server; on change tracking, what does the pipeline do?
#   #5  DROP and recreate the table (with its tracking set up again): what does the pipeline do, and does a full refresh
#       of that table recover it?
#
#   Needs and options: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
#
# Prints one row per claim (PASS / FAIL / OBSERVED with what was seen) and exits 1 if any claim failed.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init life
FP_TABLES="ctdel cdcdel cttr cdctr ctdrop cdcdrop"

# --- source: one change-tracking and one CDC table per step -----------------------------------------------------------
src_db <<'EOF'
CREATE TABLE lfcddemo.ctdel (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cdcdel (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cttr (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cdctr (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.ctdrop (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cdcdrop (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.ctnew (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
EOF
for t in ctdel cdcdel cttr cdctr ctdrop cdcdrop ctnew; do fill $t 10; done
src_util
track ct ctdel cttr ctdrop ctnew
track cdc cdcdel cdctr cdcdrop
SEEN=$(rq "SELECT (SELECT COUNT(*) FROM lfcddemo.ctdel) AS a, (SELECT COUNT(*) FROM lfcddemo.cdcdrop) AS b" 2>&1)
[ "$SEEN" = "$(printf '10\t10')" ] || { echo "serverless can't read the source through $CONNECTION: $SEEN"; exit 2; }

OBJS=""; for t in $FP_TABLES; do OBJS+="${OBJS:+,}$(obj $t)"; done
pipelines_create "$OBJS"
counts() { q "SELECT $(for t in "$@"; do printf '(SELECT COUNT(*) FROM %s.%s),' "$T" "$t"; done | sed 's/,$//')" 2>&1 | tr '\t' ' '; }
update; R=$(counts $FP_TABLES); [ "$R" = "10 10 10 10 10 10" ] || { update; R=$(counts $FP_TABLES); }
[ "$R" = "10 10 10 10 10 10" ] || { echo "baseline row counts wrong ($FP_TABLES): $R; $ERRS"; exit 1; }

# --- #3: change data feed on the destination --------------------------------------------------------------------------
CDF=$(q "SHOW TBLPROPERTIES $T.ctdel ('delta.enableChangeDataFeed')" 2>&1 | cut -f2)
observe cdf-property "delta.enableChangeDataFeed on a destination table: ${CDF:-not set}"
R=$(q "SELECT _change_type, COUNT(*) FROM table_changes('\`$CATALOG\`.$S.ctdel', 0) GROUP BY 1 ORDER BY 1" 2>&1 | tr '\t\n' ': ' | cut -c1-200)
observe cdf-table-changes "table_changes() on ctdel after the first load: ${R:-no rows}"
# version 0 (the pipeline's create) has no change data (DELTA_MISSING_CHANGE_DATA, 2026-10-21): read from the first
# version after change data feed was turned on, as DESCRIBE HISTORY shows
CDF_FROM=$(q "SELECT MIN(version) FROM (DESCRIBE HISTORY $T.ctdel) WHERE version > 0" 2>&1)
R=$(q "SELECT _change_type, COUNT(*) FROM table_changes('\`$CATALOG\`.$S.ctdel', ${CDF_FROM:-1}) GROUP BY 1 ORDER BY 1" 2>&1 | tr '\t\n' ': ' | cut -c1-200)
observe cdf-table-changes-from-v${CDF_FROM:-1} "table_changes() on ctdel from version ${CDF_FROM:-1}: ${R:-no rows}; history: $(q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.ctdel) ORDER BY version)" 2>&1 | cut -c1-200)"

# --- #6: DELETE every row, then INSERT them again --------------------------------------------------------------------
src >/dev/null <<'EOF'
DELETE FROM lfcddemo.ctdel; DELETE FROM lfcddemo.cdcdel;
INSERT INTO lfcddemo.ctdel (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'reload' FROM sys.all_objects;
INSERT INTO lfcddemo.cdcdel (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'reload' FROM sys.all_objects;
EOF
WANT="$(printf '10\t10')"
settle_rows "SELECT (SELECT SUM(CASE WHEN v = 'reload' THEN 1 ELSE 0 END) FROM $T.ctdel), (SELECT SUM(CASE WHEN v = 'reload' THEN 1 ELSE 0 END) FROM $T.cdcdel)" "$WANT"
E_DEL="$(errs_for ctdel)$(errs_for cdcdel)"
[ "$SETTLED" = 1 ] && [ -z "${E_DEL// /}" ] && claim delete-all-insert-incremental PASS "ctdel and cdcdel: 10 rows each now 'reload', no error and no full refresh ($CHAIN)" \
  || claim delete-all-insert-incremental FAIL "reload rows: $(q "SELECT (SELECT COUNT(*) FROM $T.ctdel WHERE v = 'reload'), (SELECT COUNT(*) FROM $T.cdcdel WHERE v = 'reload')" 2>&1 | tr '\t' ' '); errors: ${E_DEL:-none}"
R=$(q "SELECT _change_type, COUNT(*) FROM table_changes('\`$CATALOG\`.$S.cdcdel', 0) GROUP BY 1 ORDER BY 1" 2>&1 | tr '\t\n' ': ' | cut -c1-200)
observe cdf-after-reload "table_changes() on cdcdel after the reload: ${R:-no rows}"

# --- #4: add a table to the ingestion definition ---------------------------------------------------------------------
echo "INSERT INTO lfcddemo.ctdel (id, v) VALUES (11, 'before_add');" | src >/dev/null
OBJS+=",$(obj ctnew)"; set_objects "$OBJS"; FP_TABLES+=" ctnew"
T0=$(date -u +%FT%TZ)
update
R=$(expect "$(printf '10\t11')" "SELECT (SELECT COUNT(*) FROM $T.ctnew), (SELECT COUNT(*) FROM $T.ctdel)")
[ "$R" = "$(printf '10\t11')" ] && claim add-table-snapshots-new-table PASS "ctnew loaded its 10 rows with no full refresh requested; ctdel picked up its new row (11 rows) ($CHAIN)" \
  || claim add-table-snapshots-new-table FAIL "ctnew, ctdel rows: ${R//$'\t'/ }; errors: ${ERRS:-none} ($CHAIN)"
# the other tables weren't reset: their Delta history since the definition change has no overwrite/truncate
OPS=$(q "SELECT CONCAT_WS(',', COLLECT_SET(operation)) FROM (DESCRIBE HISTORY $T.cdcdrop) WHERE timestamp >= '$T0'" 2>&1)
R=$(q "SELECT COUNT(*) FROM $T.cdcdrop" 2>&1)
[[ "$OPS" != *"CREATE OR REPLACE"* && "$OPS" != *TRUNCATE* && "$R" = 10 ]] && claim add-table-others-not-reset PASS "cdcdrop kept its 10 rows; its operations since the change: ${OPS:-none}" \
  || claim add-table-others-not-reset FAIL "cdcdrop rows $R; operations since the change: ${OPS:-none}"
echo "INSERT INTO lfcddemo.ctnew (id, v) VALUES (11, 'after_add');" | src >/dev/null
settle_rows "SELECT COUNT(*) FROM $T.ctnew" 11
[ "$SETTLED" = 1 ] && claim add-table-then-incremental PASS "a row inserted into ctnew after its snapshot arrived incrementally (11 rows)" \
  || claim add-table-then-incremental FAIL "ctnew rows: $(q "SELECT COUNT(*) FROM $T.ctnew" 2>&1); errors: ${ERRS:-none}"

# --- #6: TRUNCATE --------------------------------------------------------------------------------------------------
OUT_TR=$(echo "TRUNCATE TABLE lfcddemo.cdctr;" | src 2>&1); RC=$?
[ $RC -ne 0 ] && claim truncate-cdc-refused-by-sqlserver PASS "SQL Server refuses TRUNCATE on a CDC table: $(echo "$OUT_TR" | grep -i -m1 'truncate' | cut -c1-160)" \
  || claim truncate-cdc-refused-by-sqlserver FAIL "TRUNCATE on cdctr succeeded"
OUT_TR=$(printf "TRUNCATE TABLE lfcddemo.cttr;\nINSERT INTO lfcddemo.cttr (id, v) SELECT TOP 5 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'after_truncate' FROM sys.all_objects;\n" | src 2>&1); RC=$?
observe truncate-ct-allowed "TRUNCATE on the change-tracking table cttr: $([ $RC -eq 0 ] && echo allowed || echo "refused: $(echo "$OUT_TR" | head -1 | cut -c1-160)")"
settle_rows "SELECT COUNT(*), SUM(CASE WHEN v = 'after_truncate' THEN 1 ELSE 0 END) FROM $T.cttr" "$(printf '5\t5')"
E_TR=$(errs_for cttr)
observe truncate-ct-pipeline "cttr after TRUNCATE + 5 inserts: rows, after_truncate rows: $(q "SELECT COUNT(*), SUM(CASE WHEN v = 'after_truncate' THEN 1 ELSE 0 END) FROM $T.cttr" 2>&1 | tr '\t' ' '); errors: ${E_TR:-none}$( [ -n "${E_TR// /}" ] && echo "; $(msgs_for cttr | head -1)")"
# the documented way on CDC: disable CDC, TRUNCATE, set CDC up again
src >/dev/null <<'EOF'
EXEC sys.sp_cdc_disable_table @source_schema = 'lfcddemo', @source_name = 'cdctr', @capture_instance = 'all';
TRUNCATE TABLE lfcddemo.cdctr;
INSERT INTO lfcddemo.cdctr (id, v) SELECT TOP 5 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'after_truncate' FROM sys.all_objects;
EOF
track cdc cdctr
settle_rows "SELECT COUNT(*), SUM(CASE WHEN v = 'after_truncate' THEN 1 ELSE 0 END) FROM $T.cdctr" "$(printf '5\t5')"
E_TR=$(errs_for cdctr)
observe truncate-cdc-reenabled-pipeline "cdctr after disable CDC, TRUNCATE, 5 inserts, CDC again: rows, after_truncate rows: $(q "SELECT COUNT(*), SUM(CASE WHEN v = 'after_truncate' THEN 1 ELSE 0 END) FROM $T.cdctr" 2>&1 | tr '\t' ' '); errors: ${E_TR:-none}$( [ -n "${E_TR// /}" ] && echo "; $(msgs_for cdctr | head -1)")"
FR=""; for t in cttr cdctr; do [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = 5 ] || FR+="${FR:+,}$t"; done
if [ -n "$FR" ]; then
  update "$FR"
  R=$(expect "$(printf '5\t5')" "SELECT (SELECT COUNT(*) FROM $T.cttr), (SELECT COUNT(*) FROM $T.cdctr)")
  [ "$R" = "$(printf '5\t5')" ] && claim truncate-full-refresh-recovers PASS "a full refresh of $FR leaves 5 rows each, as in the source" \
    || claim truncate-full-refresh-recovers FAIL "cttr, cdctr rows after refreshing $FR: ${R//$'\t'/ }; errors: ${ERRS:-none}"
fi

# --- #5 / #6: DROP and recreate, tracking set up again --------------------------------------------------------------
src >/dev/null <<'EOF'
EXEC sys.sp_cdc_disable_table @source_schema = 'lfcddemo', @source_name = 'cdcdrop', @capture_instance = 'all';
DROP TABLE lfcddemo.ctdrop; DROP TABLE lfcddemo.cdcdrop;
GO
CREATE TABLE lfcddemo.ctdrop (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cdcdrop (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
GO
INSERT INTO lfcddemo.ctdrop (id, v) SELECT TOP 5 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'recreated' FROM sys.all_objects;
INSERT INTO lfcddemo.cdcdrop (id, v) SELECT TOP 5 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'recreated' FROM sys.all_objects;
EOF
track ct ctdrop; track cdc cdcdrop
settle_rows "SELECT (SELECT COUNT(*) FROM $T.ctdrop), (SELECT COUNT(*) FROM $T.cdcdrop)" "$(printf '5\t5')"
for t in ctdrop cdcdrop; do
  e=$(errs_for $t)
  observe "drop-recreate-$t" "after DROP, CREATE, 5 inserts, tracking again: rows, recreated rows: $(q "SELECT COUNT(*), SUM(CASE WHEN v = 'recreated' THEN 1 ELSE 0 END) FROM $T.$t" 2>&1 | tr '\t' ' '); errors: ${e:-none}$( [ -n "${e// /}" ] && echo "; $(msgs_for $t | head -1)")"
done
observe drop-recreate-chain "updates after the drop: $CHAIN"
update ctdrop,cdcdrop
R=$(expect "$(printf '5\t5')" "SELECT (SELECT SUM(CASE WHEN v = 'recreated' THEN 1 ELSE 0 END) FROM $T.ctdrop), (SELECT SUM(CASE WHEN v = 'recreated' THEN 1 ELSE 0 END) FROM $T.cdcdrop)")
ok_state && [ "$R" = "$(printf '5\t5')" ] && claim drop-recreate-full-refresh-recovers PASS "a full refresh of ctdrop and cdcdrop leaves the recreated tables' 5 rows each" \
  || claim drop-recreate-full-refresh-recovers FAIL "after the refresh ($STATE): recreated rows ${R//$'\t'/ }; errors: ${ERRS:-none}"
echo "INSERT INTO lfcddemo.ctdrop (id, v) VALUES (6, 'after_refresh'); INSERT INTO lfcddemo.cdcdrop (id, v) VALUES (6, 'after_refresh');" | src >/dev/null
settle_rows "SELECT (SELECT COUNT(*) FROM $T.ctdrop), (SELECT COUNT(*) FROM $T.cdcdrop)" "$(printf '6\t6')"
[ "$SETTLED" = 1 ] && claim drop-recreate-then-incremental PASS "after the refresh, a new row in each recreated table arrives incrementally" \
  || claim drop-recreate-then-incremental FAIL "rows: $(q "SELECT (SELECT COUNT(*) FROM $T.ctdrop), (SELECT COUNT(*) FROM $T.cdcdrop)" 2>&1 | tr '\t' ' '); errors: ${ERRS:-none}"

result_md sqlserver-table-lifecycle
exit $FAILED
