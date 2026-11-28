#!/usr/bin/env bash
# sqlserver-cdf.sh: can the destination streaming tables' change data feed be read, and what does it show for an
# insert, an update and a delete? Answers a field question (the Databricks side). An SCD type 1 table (change tracking) and
# an SCD type 2 table (CDC), 5 rows each; then one insert, one update and one delete on each.
#
#   Needs and options: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init cdf
FP_TABLES="cdf1 cdf2"
src_db <<'EOF'
CREATE TABLE lfcddemo.cdf1 (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
CREATE TABLE lfcddemo.cdf2 (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
EOF
fill cdf1 5; fill cdf2 5; src_util; track ct cdf1; track cdc cdf2
pipelines_create "$(obj cdf1),$(SCD=SCD_TYPE_2 obj cdf2)"
update; [ "$(q "SELECT COUNT(*) FROM $T.cdf1" 2>&1)" = 5 ] || update

hist() { q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.$1) ORDER BY version)" 2>&1 | cut -c1-240; }
for t in cdf1 cdf2; do
  observe "$t-cdf-property" "delta.enableChangeDataFeed: $(q "SHOW TBLPROPERTIES $T.$t ('delta.enableChangeDataFeed')" 2>&1 | cut -f2); history: $(hist $t)"
  observe "$t-from-v0" "$(q "SELECT COUNT(*) FROM table_changes('\`$CATALOG\`.$S.$t', 0)" 2>&1 | cut -c1-160)"
done
V0=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.cdf1)" 2>&1); V2=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.cdf2)" 2>&1)

src >/dev/null <<'EOF'
INSERT INTO lfcddemo.cdf1 (id, v) VALUES (6, 'new'); UPDATE lfcddemo.cdf1 SET v = 'changed' WHERE id = 2; DELETE FROM lfcddemo.cdf1 WHERE id = 3;
INSERT INTO lfcddemo.cdf2 (id, v) VALUES (6, 'new'); UPDATE lfcddemo.cdf2 SET v = 'changed' WHERE id = 2; DELETE FROM lfcddemo.cdf2 WHERE id = 3;
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.cdf1), (SELECT COUNT(*) FROM $T.cdf1 WHERE v = 'changed')" "$(printf '5\t1')"
changes() { q "SELECT CONCAT_WS(', ', COLLECT_LIST(c)) FROM (SELECT CONCAT(_change_type, ' id ', id, ' ', COALESCE(v, 'null')) AS c FROM table_changes('\`$CATALOG\`.$S.$1', $(( $2 + 1 ))) ORDER BY _commit_version, id, _change_type)" 2>&1 | cut -c1-400; }
C1=$(changes cdf1 "$V0"); C2=$(changes cdf2 "$V2")
observe cdf1-changes "SCD type 1, change tracking, from the version after the load: $C1"
observe cdf2-changes "SCD type 2, CDC, from the version after the load: $C2"
[[ "$C1" == *insert*6* && "$C1" == *update_postimage*2*changed* && "$C1" == *delete*3* ]] \
  && claim cdf-scd1-readable PASS "table_changes() shows the insert, the update (pre/post image) and the delete" \
  || claim cdf-scd1-readable FAIL "cdf1 changes: ${C1:-none}"
observe history-after "cdf1: $(hist cdf1); cdf2: $(hist cdf2)"
result_md sqlserver-cdf
exit $FAILED
