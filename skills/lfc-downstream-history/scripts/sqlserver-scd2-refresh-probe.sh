#!/usr/bin/env bash
# sqlserver-scd2-refresh-probe.sh: what a reader of a Lakeflow Connect SCD type 2 destination can still read after a
# full refresh of it, to choose how a downstream copy catches up on the versions made since its last read (the gap).
# No history pipeline: the probe reads the destination's change feed and its Delta history directly.
#
#   1. load 3 rows (id, v, n INT NULL); the last read point A (what a downstream copy would have read)
#   2. after A: update 2, delete 3, then the gap: update 1 twice in two ingestion updates, so 1=a2 is a version that
#      only the destination has, then replaced by 1=a3
#   3. full refresh of the destination (R: its DLT REFRESH commit)
#   4. claims: table_changes from A to R-1 (a range that ends before the refresh) still reads after it, with the rows
#      it had before; the table VERSION AS OF R-1 holds every version it had before the refresh; table_changes after
#      the reload reads, and a change after the refresh comes through it; before the refresh the change feed had no
#      delete rows (a source delete closes the version)
#
#   Settings (each also changes what step 4 expects):
#     GAP=long        four more ingestion updates in the gap: keys 1 and 2 updated in each, a key inserted in each and
#                     deleted in the next, the deleted key 3 inserted again
#     DDL=...         a schema change on the source in the gap, between 1=a2 and 1=a3 (which sets the column):
#                     add-column (ADD c INT NULL) and drop-column (DROP n) applied without a refresh;
#                     int-to-string (n to NVARCHAR(20)) stops the table until the refresh, so 1=a3 arrives only with
#                     the reload; int-to-bigint (n) either (recorded: on an SCD type 2 destination it stopped,
#                     UNSUPPORTED_DATA_TYPE_CHANGE, dp-20261106; the DDL matrix saw it applied on dp-20261015). A column rename and a primary key drop are refused by SQL Server on a CDC
#                     table (lfc-schema-change-test references/ddl-matrix-results.md): they can't happen in a gap.
#     REFRESHES=2     after the first refresh: a change (2=x1), then a second full refresh before any read; the reads
#                     before each refresh, and between them, are checked after the second
#     BULK_ROWS=<n>   n more rows (ids from 1000) loaded before A, and every tenth of them updated in the gap: the
#                     claims compare the change feed's and the table's row counts as well as the small keys' versions
#     CONT=1          the ingestion pipeline runs continuously: changes land without updates; the full refresh is a
#                     restart with full_refresh (every table: "Refresh selection is not supported for the continuous
#                     mode"), standard CDC only
#   Needs: as lfc-schema-change-test lib.sh (PROFILE CATALOG, and SOURCE_JSON or CONNECTION SRC_SQL CONN_USER).
#   MODE=integrated for integrated CDC.
set -uo pipefail
. "$(dirname "$0")/../../lfc-schema-change-test/scripts/lib.sh"
GAP=${GAP:-short}; DDL=${DDL:-none}; REFRESHES=${REFRESHES:-1}; BULK=${BULK_ROWS:-0}; CONT=${CONT:-0}
case $GAP in short|long) ;; *) echo "GAP must be short or long"; exit 2;; esac
case $DDL in none|add-column|drop-column|int-to-bigint|int-to-string) ;; *) echo "DDL: none, add-column, drop-column, int-to-bigint or int-to-string"; exit 2;; esac
case $REFRESHES in 1|2) ;; *) echo "REFRESHES must be 1 or 2"; exit 2;; esac
[[ "$BULK" =~ ^[0-9]+$ ]] || { echo "BULK_ROWS must be a whole number"; exit 2; }
case $CONT in 0) ;; 1) [ "$MODE" = standard ] || { echo "CONT=1 needs MODE=standard"; exit 2; }; export PIPELINE_CONTINUOUS=1;; *) echo "CONT must be 0 or 1"; exit 2;; esac
LANDS=1; [ "$DDL" = int-to-string ] && LANDS=0   # the schema change stops the table: nothing lands until the refresh
[ "$DDL" = int-to-bigint ] && LANDS=either        # applied or stopped, by build: what happened is recorded
export SCD=SCD_TYPE_2
lfc_init scd2probe
echo "settings: MODE=$MODE GAP=$GAP DDL=$DDL REFRESHES=$REFRESHES BULK_ROWS=$BULK CONT=$CONT"
SCENARIO=${SCENARIO:-sqlserver-scd2-refresh-probe-$MODE-$GAP-$DDL-r$REFRESHES$([ "$BULK" != 0 ] && echo "-rows$BULK")$([ "$CONT" = 1 ] && echo -continuous)}
TB=probe_$E; FP_TABLES=$TB
src_db <<<"SELECT 1;"; src_util
src >/dev/null <<EOF || { echo "source setup failed"; exit 2; }
CREATE TABLE lfcddemo.$TB (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), n INT NULL);
INSERT INTO lfcddemo.$TB (id, v) VALUES (1, 'a1'), (2, 'b1'), (3, 'c1');
EOF
[ "$BULK" != 0 ] && fill "$TB" "$BULK" 1000   # rows 'before', ids from 1000
track cdc "$TB"   # SCD type 2 needs CDC on SQL Server
pipelines_create "$(obj "$TB")"

# rows: the small keys' versions (id < 1000), id=v and (closed), in key and start order (sorted in the array: an ORDER
# BY in the subquery doesn't order COLLECT_LIST); \$1: a VERSION AS OF clause or empty
rows() { q "SELECT CONCAT_WS(' ', TRANSFORM(ARRAY_SORT(COLLECT_LIST(STRUCT(id, COALESCE(__START_AT.__cdc_internal_value, '') AS s, x))), e -> e.x))
  FROM (SELECT id, __START_AT, CONCAT(id, '=', v, IF(__END_AT IS NULL, '', '(closed)')) x FROM $T.$TB ${1:-} WHERE id < 1000)" 2>&1 | cut -c1-900; }
count() { q "SELECT COUNT(*) FROM $T.$TB ${1:-}" 2>&1 | head -1; }
maxv() { q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1; }
hist() { q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.$TB) ORDER BY version)" 2>&1 | cut -c1-900; }
# feed <from> [to]: the small keys' change-feed rows as type:id=v in commit order, then "| types:" and every change
# type in the range (all rows); with BULK_ROWS, a second line with the count of every row by type. Or the error.
feed() { q "SELECT CONCAT_WS(' ', TRANSFORM(FILTER(ARRAY_SORT(COLLECT_LIST(STRUCT(_commit_version AS cv, id, _change_type AS t, x))), e -> e.id < 1000), e -> e.x))
    || ' | types: ' || CONCAT_WS(' ', ARRAY_SORT(COLLECT_SET(_change_type)))
    FROM (SELECT _commit_version, id, _change_type, CONCAT(_change_type, ':', id, '=', v) x FROM table_changes('$CATALOG.$S.$TB', $1${2:+, $2}))" 2>&1 | head -1 | cut -c1-900
  [ "$BULK" != 0 ] && q "SELECT CONCAT_WS(' ', TRANSFORM(ARRAY_SORT(COLLECT_LIST(STRUCT(t, c))), e -> CONCAT(e.t, '=', e.c))) FROM (SELECT _change_type t, COUNT(*) c FROM table_changes('$CATALOG.$S.$TB', $1${2:+, $2}) GROUP BY 1)" 2>&1 | head -1 | sed 's/^/counts /'
  return 0; }
tid() { dbx tables get "$CATALOG.$S.$TB" -o json | yq '.table_id'; }   # empty or null: the claim fails
# wait_rows <sql> <want> [minutes]: a continuous pipeline lands changes on its own: poll until <sql> gives <want>
wait_rows() { local end=$(( $(date +%s) + ${3:-15} * 60 )); SETTLED=0
  while [ "$(date +%s)" -lt "$end" ]; do ROWS_GOT=$(q "$1" 2>&1); [ "$ROWS_GOT" = "$2" ] && { SETTLED=1; return 0; }; sleep 20; done
  return 1; }
# land <sql> <want>: settle_rows up to three times (a schema change restarts the update: FLOW_SCHEMA_CHANGED);
# continuous: wait_rows
land() {
  if [ "$CONT" = 1 ]; then wait_rows "$1" "$2" && return 0
  else for _ in 1 2 3; do settle_rows "$1" "$2"; [ "$SETTLED" = 1 ] && return 0; done; fi
  echo "not landed: $1 (want $2)"; return 1; }
REFQ() { echo "SELECT MIN(version) FROM (DESCRIBE HISTORY $T.$TB) WHERE operation = 'DLT REFRESH' AND version > $1"; }
# refreshed_after <version>: the first DLT REFRESH after it, after a full refresh (integrated CDC: the next update
# applies it; continuous: a restart with the table selected, then wait for the refresh and a write after it)
refreshed_after() {
  local r
  if [ "$CONT" = 1 ]; then
    dbx pipelines stop "$AP" >/dev/null 2>&1
    # a selection is refused in continuous mode ("Refresh selection is not supported for the continuous mode"):
    # the whole pipeline (one table here)
    dbx pipelines start-update "$AP" --json '{"full_refresh":true}' -o json > "$W/cont.out" 2>&1 \
      || echo "continuous restart with a full refresh refused: $(head -1 "$W/cont.out")"
    wait_rows "SELECT COUNT(*) > 0 FROM (DESCRIBE HISTORY $T.$TB) WHERE operation IN ('MERGE', 'WRITE', 'STREAMING UPDATE') AND version > ($(REFQ "$1"))" true 30
    r=$(q "$(REFQ "$1")" 2>&1)
  else
    EARLY_STOP=0 update "$TB"   # integrated CDC: no early stop while it re-reads the source
    r=$(q "$(REFQ "$1")" 2>&1)
    [[ "$r" =~ ^[0-9]+$ ]] || { update; r=$(q "$(REFQ "$1")" 2>&1); }
    # a stopped table (int-to-string): the refresh's update may need one more to apply the source's current rows
  fi
  echo "$r" > "$W/refresh"
}
check_refresh() {   # check_refresh <name> <after>: R from refreshed_after, or stop the run
  R=$(cat "$W/refresh")
  [[ "$R" =~ ^[0-9]+$ ]] && claim "$1" PASS "DLT REFRESH at version $R (after $2)" \
    || { claim "$1" FAIL "no DLT REFRESH after version $2 ($R): nothing below is about a refresh"; result_md sqlserver-scd2-refresh-probe; exit 1; }
}

if [ "$CONT" = 1 ]; then wait_rows "SELECT COUNT(*) FROM $T.$TB" "$((3 + BULK))" 20 || { echo "the continuous load didn't land: $ROWS_GOT"; exit 2; }
else update; [ "$(count)" = "$((3 + BULK))" ] || update; fi
VA=$(maxv); TID0=$(tid)
observe load "versions $(rows); $(count) rows; last read point A = version $VA"

# --- 2. after A: an update and a source delete, then the gap ---------------------------------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'b2' WHERE id = 2; DELETE FROM lfcddemo.$TB WHERE id = 3;
EOF
land "SELECT COUNT(*) FROM $T.$TB WHERE __END_AT IS NULL AND id < 1000" 2
GAPSQL="UPDATE lfcddemo.$TB SET v = 'a2' WHERE id = 1;"
[ "$BULK" != 0 ] && GAPSQL+=" UPDATE lfcddemo.$TB SET v = 'gap' WHERE id >= 1000 AND id % 10 = 0;"
echo "$GAPSQL" | src >/dev/null
land "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a2'" 1
case $DDL in
  add-column) DDLSQL="ALTER TABLE lfcddemo.$TB ADD c INT NULL;"; SET3="c = 1"; CHK="c = 1";;
  drop-column) DDLSQL="ALTER TABLE lfcddemo.$TB DROP COLUMN n;"; SET3=""; CHK="";;
  int-to-bigint) DDLSQL="ALTER TABLE lfcddemo.$TB ALTER COLUMN n BIGINT NULL;"; SET3="n = 3000000000"; CHK="n = 3000000000";;
  int-to-string) DDLSQL="ALTER TABLE lfcddemo.$TB ALTER COLUMN n NVARCHAR(20) NULL;"; SET3="n = 'text'"; CHK="";;
  *) DDLSQL=""; SET3=""; CHK="";;
esac
if [ -n "$DDLSQL" ]; then echo "$DDLSQL" | src >/dev/null || { echo "$DDL refused on the source"; exit 2; }; fi
echo "UPDATE lfcddemo.$TB SET v = 'a3'${SET3:+, $SET3} WHERE id = 1;" | src >/dev/null
if [ "$LANDS" = either ]; then   # one try: it lands, or the table stops
  if [ "$CONT" = 1 ]; then wait_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a3'${CHK:+ AND $CHK}" 1 10; else settle_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a3'${CHK:+ AND $CHK}" 1; fi
  if [ "$SETTLED" = 1 ]; then LANDS=1; observe ddl-outcome "$DDL applied: 1=a3${CHK:+ with $CHK} landed without a refresh"
  else LANDS=0; observe ddl-outcome "$DDL stopped the table: $(errs_for "$TB" 2>/dev/null)${ERRS:+ (update: $ERRS)}"; fi
elif [ "$LANDS" = 1 ]; then
  land "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a3'${CHK:+ AND $CHK}" 1 && { [ "$DDL" = none ] || claim ddl-landed PASS "$DDL in the gap; 1=a3${CHK:+ with $CHK} in the destination"; } \
    || claim ddl-landed FAIL "$DDL: 1=a3${CHK:+ with $CHK} not in the destination"
else   # the table stops: one try, to see the error, then on to the refresh
  if [ "$CONT" = 1 ]; then wait_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a3'" 1 5; else settle_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a3'" 1; fi
  [ "$SETTLED" = 1 ] && claim ddl-stops FAIL "$DDL: 1=a3 landed without a refresh (the DDL matrix says the table stops)" \
    || claim ddl-stops PASS "$DDL: 1=a3 not landed before the refresh; errors: $(errs_for "$TB" 2>/dev/null)${ERRS:+ (update: $ERRS)}"
fi
K1="1=a1(closed) 1=a2"; K2="2=b1(closed)"; K3="3=c1(closed)"; KN=""
if [ "$GAP" = long ]; then
  K1+="(closed) 1=a3(closed)"; K2+=" 2=b2(closed)"
  for r in 1 2 3 4; do
    sqlr="UPDATE lfcddemo.$TB SET v = 'a$((r + 3))' WHERE id = 1; UPDATE lfcddemo.$TB SET v = 'b$((r + 2))' WHERE id = 2;"
    sqlr+=" INSERT INTO lfcddemo.$TB (id, v) VALUES ($((10 + r)), 'n$r');"
    [ $r -gt 1 ] && sqlr+=" DELETE FROM lfcddemo.$TB WHERE id = $((9 + r));"
    [ $r = 3 ] && sqlr+=" INSERT INTO lfcddemo.$TB (id, v) VALUES (3, 'c2');"
    echo "$sqlr" | src >/dev/null
    land "SELECT COUNT(*) FROM $T.$TB WHERE v = 'a$((r + 3))'" 1
    [ $r -lt 4 ] && K1+=" 1=a$((r + 3))(closed)" && K2+=" 2=b$((r + 2))(closed)" && KN+=" $((10 + r))=n$r(closed)"
  done
  K1+=" 1=a7"; K2+=" 2=b6"; K3+=" 3=c2"; KN+=" 14=n4"
else
  [ "$LANDS" = 1 ] && K1+="(closed) 1=a3"; K2+=" 2=b2"
fi
WANT="$K1 $K2 $K3$KN"
BEFORE=$(rows); NBEFORE=$(count); VG=$(maxv)
[ "$BEFORE" = "$WANT" ] && claim before-refresh PASS "versions $BEFORE; $NBEFORE rows (version $VG)" \
  || claim before-refresh FAIL "versions $BEFORE (want $WANT): the gap isn't what the rest assumes"
PRE=$(feed "$((VA + 1))" "$VG")
observe feed-before-refresh "table_changes($((VA + 1)), $VG): $PRE"
case $PRE in *FAILED*) claim feed-before-refresh-reads FAIL "the gap's change feed doesn't read even before a refresh";; esac
echo "$PRE" | grep -Eq '(^| )delete([:= ]|$)' && claim no-delete-rows FAIL "the change feed has delete rows before any refresh: $PRE" \
  || claim no-delete-rows PASS "every source delete arrived as a close (update rows), no delete rows"

# --- 3. full refresh (and with REFRESHES=2, a change and a second one) -----------------------------------------------
refreshed_after "$VG"; check_refresh refresh-applied "$VG"; R1=$R
[ "$LANDS" = 1 ] || { [[ "$(rows)" == *"1=a3"* ]] && claim reload-has-a3 PASS "the reload brought 1=a3: $(rows)" || claim reload-has-a3 FAIL "after the refresh: $(rows)"; }
observe after-refresh "versions $(rows); $(count) rows (one per live key, the history gone)"
if [ "$REFRESHES" = 2 ]; then
  VM=$(maxv)   # the reload's last commit, as far as a reader can tell: the newest version before the next change
  echo "UPDATE lfcddemo.$TB SET v = 'x1' WHERE id = 2;" | src >/dev/null
  land "SELECT COUNT(*) FROM $T.$TB WHERE v = 'x1'" 1
  MID=$(feed "$((VM + 1))"); VB=$(maxv)
  observe feed-between "table_changes($((VM + 1)), $VB) before the second refresh: $MID"
  refreshed_after "$VB"; check_refresh second-refresh-applied "$VB"; R2=$R
fi
observe history "$(hist)"
TID1=$(tid); [ -n "$TID0" ] && [ "$TID0" != null ] && [ "$TID1" = "$TID0" ] && claim same-table-id PASS "$TID1" || claim same-table-id FAIL "before $TID0, after $TID1"

# --- 4. what can still be read ---------------------------------------------------------------------------------------
GOT=$(feed "$((VA + 1))" "$((R1 - 1))")
[ "$GOT" = "$PRE" ] && claim feed-before-R PASS "table_changes($((VA + 1)), $((R1 - 1))) after the refresh: the same rows as before it" \
  || claim feed-before-R FAIL "table_changes($((VA + 1)), $((R1 - 1))) after the refresh: $GOT (before it: $PRE)"
OLD=$(rows "VERSION AS OF $((R1 - 1))"); NOLD=$(count "VERSION AS OF $((R1 - 1))")
[ "$OLD" = "$BEFORE" ] && [ "$NOLD" = "$NBEFORE" ] && claim version-before-R PASS "VERSION AS OF $((R1 - 1)): $OLD; $NOLD rows" \
  || claim version-before-R FAIL "VERSION AS OF $((R1 - 1)): $OLD; $NOLD rows (before the refresh: $BEFORE; $NBEFORE rows)"
if [ "$REFRESHES" = 2 ]; then
  GOT2=$(feed "$((VM + 1))" "$((R2 - 1))")
  [ "$GOT2" = "$MID" ] && [[ "$MID" == *"=x1"* ]] && claim feed-between-refreshes PASS "table_changes($((VM + 1)), $((R2 - 1))) after the second refresh: $GOT2" \
    || claim feed-between-refreshes FAIL "table_changes($((VM + 1)), $((R2 - 1))) after the second refresh: $GOT2 (before it: $MID)"
  MIDV=$(rows "VERSION AS OF $((R2 - 1))")
  [[ "$MIDV" == *"2=x1"* ]] && claim version-before-R2 PASS "VERSION AS OF $((R2 - 1)): $MIDV" \
    || claim version-before-R2 FAIL "VERSION AS OF $((R2 - 1)): $MIDV (want 2=x1 among them)"
fi
observe feed-across-R "table_changes($((VA + 1))): $(feed "$((VA + 1))" | head -1)"
LV=$(maxv)
observe feed-reload "table_changes($R, $LV): $(feed "$R" "$LV" | head -1)"
echo "UPDATE lfcddemo.$TB SET v = 'z1' WHERE id = 2;" | src >/dev/null
land "SELECT COUNT(*) FROM $T.$TB WHERE v = 'z1'" 1
NEXT=$(feed "$((LV + 1))")
[[ "$NEXT" == *"=z1"* ]] && claim feed-after-reload PASS "table_changes($((LV + 1))): $NEXT" \
  || claim feed-after-reload FAIL "table_changes($((LV + 1))): $NEXT (want the z1 version)"
observe final "versions $(rows); history $(hist)"
result_md "sqlserver-scd2-refresh-probe MODE=$MODE GAP=$GAP DDL=$DDL REFRESHES=$REFRESHES BULK_ROWS=$BULK CONT=$CONT"
exit $FAILED
