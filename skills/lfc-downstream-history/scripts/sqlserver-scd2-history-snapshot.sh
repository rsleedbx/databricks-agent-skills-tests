#!/usr/bin/env bash
# sqlserver-scd2-history-snapshot.sh: keep an SCD type 2 destination table's versions across a full refresh
# (history-snapshot-scd2.py: AUTO CDC FROM SNAPSHOT keyed by (keys, the version's start)), through the lifecycle of
# sqlserver-history-snapshot.sh: normal sync, a gap, a full refresh of the destination with history runs while it is
# applied, the next sync, a run with nothing new. Same table and changes as that script; the destination is SCD type 2
# (the table on CDC, as SCD type 2 needs on SQL Server), which the full refresh reloads with one row per live key.
#
#   Needs: as lfc-schema-change-test lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER, or SOURCE_JSON: a source
#   contract, lfc-source-contract), and HISTORY_NB: the workspace path of history-snapshot-scd2.py imported as a
#   notebook (databricks workspace import <path> --file history-snapshot-scd2.py --format SOURCE --language PYTHON).
#   SRC_DB: an existing source database for the test table instead of a fresh one (the table is dropped at the end).
#   MODE=integrated for a source without a gateway route.
set -uo pipefail
. "$(dirname "$0")/../../lfc-schema-change-test/scripts/lib.sh"
: "${HISTORY_NB:?}"
export SCD=SCD_TYPE_2
lfc_init histscd2
TB=histscd2_$E; FP_TABLES=$TB
# a fresh database gets the utility objects (without them: INGESTION_GATEWAY_DDL_OBJECTS_MISSING); SRC_DB has them already
if [ -n "${SRC_DB:-}" ]; then DB=$SRC_DB; else src_db <<<"SELECT 1;"; src_util; fi
HS=${S}_history; HT="\`$CATALOG\`.$HS"; DP=""
cleanup_all() {   # the history pipeline, its schema and (on an existing source database) the source table go with the run
  EXTRA_PIPELINES="${DP:-}"; EXTRA_SCHEMAS="${HS:+$CATALOG.$HS}"
  [ -n "${SRC_DB:-}" ] && EXTRA_SQL="DROP TABLE IF EXISTS lfcddemo.$TB;"
  cleanup
}
trap cleanup_all EXIT

src >/dev/null <<EOF || { echo "source setup failed"; exit 2; }
CREATE TABLE lfcddemo.$TB (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
INSERT INTO lfcddemo.$TB (id, v) VALUES (1, 'a1'), (2, 'b1'), (3, 'c1'), (4, 'd1'), (5, 'e1');
EOF
# CDC, not change tracking: SCD type 2 needs CDC on SQL Server (the docs), and on a change-tracking table integrated
# CDC holds the changes until the next schema change (lfc-schema-change-test references/findings.md)
track cdc "$TB"
pipelines_create "$(obj "$TB")"
update; [ "$(q "SELECT COUNT(*) FROM $T.$TB" 2>&1)" = 5 ] || update

dbx schemas create "$HS" "$CATALOG" >/dev/null || exit 2
DP=$(dbx pipelines create -o json --json "{\"name\":\"$HS\",\"catalog\":\"$CATALOG\",\"schema\":\"$HS\",\"serverless\":true,
  \"configuration\":{\"src\":\"$CATALOG.$S.$TB\",\"keys\":\"id\"},\"libraries\":[{\"notebook\":{\"path\":\"$HISTORY_NB\"}}]}" | yq '.pipeline_id')
[ -n "$DP" ] && [ "$DP" != null ] || { echo "history pipeline create failed"; exit 2; }
echo "LINK history pipeline $(dbx auth describe -o json 2>/dev/null | yq -r '.details.host // ""')/pipelines/$DP"
# hupdate: one history update; HSTATE, HERRS, HUPD (its update ID). A platform rollout abort (nothing written) is run once more; no update
# ID (an expired login, an API error) stops the run.
HRETRY=0
hupdate() {
  local u n
  for n in 1 2; do
    u=$(dbx pipelines start-update "$DP" --json '{}' -o json | yq '.update_id')
    [ -n "$u" ] && [ "$u" != null ] || { echo "history start-update failed: stopping the run"; exit 2; }
    HUPD=$u
    for _ in $(seq 1 60); do HSTATE=$(dbx pipelines get-update "$DP" "$u" -o json | yq '.update.state')
      case $HSTATE in COMPLETED|FAILED|CANCELED) break;; esac; sleep 10; done
    dbx api get "/api/2.0/pipelines/$DP/events?max_results=250" -o json > "$W/hev.json"
    HERRS=$(U=$u yq -r '.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.error_class // ""' "$W/hev.json" | grep -v '^$' | sort -u | tr '\n' ' ')
    [ "$HSTATE" = COMPLETED ] && break
    # the error, for the log (the pipeline is deleted at cleanup): a Python exception's own line, else the first line
    U=$u yq -r '[.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.message // ""] | map(select(. != "")) | .[0] // ""' "$W/hev.json" > "$W/herr.txt"
    { grep -E '^[A-Za-z.]*(Error|Exception): ' "$W/herr.txt" | tail -1 || head -1 "$W/herr.txt"; } | cut -c1-300 | sed 's/^/  history error: /'
    dbx pipelines stop "$DP" >/dev/null 2>&1
    # run once more after a platform rollout abort (nothing written), or after a read of the table at a moment its
    # schema didn't resolve (UNRESOLVED_COLUMN, seen right after a stopped ingestion update): the next run catches up
    case $HERRS in *AUTO_MIGRATION_ENVIRONMENT_VERSION_ENABLEMENT_BLOCKED*|*UNRESOLVED_COLUMN*) ;; *) break;; esac
    observe "history-retry-$((++HRETRY))" "history update failed ($HERRS), running once more"
    echo "history update: $HERRS(try $n), running once more"
  done
  echo "history update $HSTATE ${HERRS}"
}
# the versions as the source had them, across the refresh (history-snapshot-scd2.py's versions table), in start order
scd2() { q "SELECT CONCAT_WS(' ', TRANSFORM(ARRAY_SORT(COLLECT_LIST(STRUCT(id, COALESCE(src_start_at, TIMESTAMP'1900-01-01') AS s, x))), e -> e.x)) FROM (SELECT id, src_start_at, CONCAT(id, '=', v, IF(src_end_at IS NULL, '', '(closed)')) x FROM $HT.versions)" 2>&1 | cut -c1-400; }
open_versions() { q "SELECT COUNT(*) FROM $HT.versions WHERE src_end_at IS NULL" 2>&1 | tr -d '[:space:]'; }
dest() { q "SELECT CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(CONCAT(id, '=', v, IF(__END_AT IS NULL, '', '(closed)'))))) FROM $T.$TB" 2>&1; }
hist() { q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.$TB) ORDER BY version)" 2>&1 | cut -c1-400; }
check() { [ "$HSTATE" = COMPLETED ] && [ "$(scd2)" = "$2" ] && claim "$1" PASS "history: $(scd2)" || claim "$1" FAIL "history: $(scd2) (want $2)"; }

hupdate; check load "1=a1 2=b1 3=c1 4=d1 5=e1"

# --- 1. normal sync: update 2, delete 3, insert 6 ------------------------------------------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'b2' WHERE id = 2; DELETE FROM lfcddemo.$TB WHERE id = 3; INSERT INTO lfcddemo.$TB (id, v) VALUES (6, 'f1');
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB WHERE __END_AT IS NULL), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'b2')" "$(printf '5\t1')"   # SCD type 2: open versions
hupdate; check sync-history "1=a1 2=b1(closed) 2=b2 3=c1(closed) 4=d1 5=e1 6=f1"
observe sync-destination "destination (SCD type 2): $(dest)"

# --- 2. the gap: ingested, but the history pipeline doesn't run: update 1, delete 5 --------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'a2' WHERE id = 1; DELETE FROM lfcddemo.$TB WHERE id = 5;
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB WHERE __END_AT IS NULL), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'a2')" "$(printf '4\t1')"
observe gap "destination: $(dest) (history pipeline not run)"
V0=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)

# --- 3. full refresh; history runs back to back while it is applied ------------------------------------------------
EARLY_STOP=0 update "$TB"   # integrated CDC: this one re-reads the source; the next update applies it
RUNS=0 GUARDED=0 BADRUN="" READ=0
if [ "$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)" = "$V0" ]; then
  u=$(dbx pipelines start-update "$AP" --json '{}' -o json | yq '.update_id')
  [ -n "$u" ] && [ "$u" != null ] || { echo "ingestion start-update failed: stopping the run"; exit 2; }
  while :; do
    st=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state')
    case $st in COMPLETED|FAILED|CANCELED) break;; esac
    [ $RUNS -ge 30 ] && break
    hupdate >/dev/null; RUNS=$((RUNS + 1)); OV=$(open_versions)
    # a run inside the reload's empty window must fail on the guard, not snapshot an empty table (all versions closed)
    GUARD=$(U=$HUPD yq -r '.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.message // ""' "$W/hev.json" 2>/dev/null | grep -c "being reloaded by a full refresh")
    if [ "$HSTATE" = COMPLETED ] && [ "${OV:-0}" = 0 ]; then BADRUN="${BADRUN:-}$RUNS "; fi
    [ "$HSTATE" = COMPLETED ] && [[ "${OV:-}" =~ ^[0-9]+$ ]] && [ "$OV" -gt 0 ] && READ=$((READ + 1))
    [ "$HSTATE" = FAILED ] && [ "${GUARD:-0}" -gt 0 ] && GUARDED=$((GUARDED + 1))
    observe "during-refresh-$RUNS" "ingestion $st; destination $(dest); history $HSTATE ${HERRS}; open versions $OV"
  done
  echo "refresh apply update $st after $RUNS history runs"
  # not done (failed, cancelled, or still running after 30 history runs): let lib.sh's update finish it; it follows
  # the pipeline's own retries, and joins an update that is still running
  [ "$st" = COMPLETED ] || { echo "refresh apply not completed ($st): finishing it with update"; EARLY_STOP=0 update; }
fi
if [ $((READ + GUARDED)) = 0 ]; then claim guard FAIL "no history run during the refresh completed or was stopped by the guard ($RUNS runs): nothing to judge"
elif [ -z "$BADRUN" ]; then claim guard PASS "$RUNS history runs during the refresh: none closed every version ($GUARDED stopped by the guard while the refresh was reloading the table)"
else claim guard FAIL "history runs $BADRUN completed with no open version: a snapshot of the emptied table"; fi
# the claims after this point only mean something if the refresh was applied: a DLT REFRESH after the gap's version
NREF=$(q "SELECT COUNT(*) FROM (DESCRIBE HISTORY $T.$TB) WHERE operation = 'DLT REFRESH' AND version > $V0" 2>&1)
[ "$NREF" -ge 1 ] 2>/dev/null && claim refresh-applied PASS "the destination was rebuilt (DLT REFRESH after version $V0)" \
  || { claim refresh-applied FAIL "no DLT REFRESH after version $V0 (${NREF}): the checks after this aren't about a refresh"; }
observe refresh "destination versions: $(hist); rows: $(dest)"
# what a snapshot read at each version from the refresh to the reload would see (the empty window, if any)
RV=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB) WHERE operation = 'DLT REFRESH'" 2>&1)
LV=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)
if [[ "$RV" =~ ^[0-9]+$ ]]; then
  W8=""; for v in $(seq "$RV" "$LV"); do W8+="v$v:$(q "SELECT COUNT(*) FROM $T.$TB VERSION AS OF $v" 2>&1 | cut -c1-40) "; done
  observe refresh-window "rows a snapshot would read at each version from the refresh: $W8"
fi

# --- 4. after the refresh: one more run, then the next sync, then a run with nothing new ---------------------------
WANT="1=a1(closed) 1=a2 2=b1(closed) 2=b2 3=c1(closed) 4=d1 5=e1(closed) 6=f1"
hupdate; observe history-after-refresh "history $HSTATE ${HERRS}; destination after the refresh (one row per live key): $(dest)"
check survives-refresh "$WANT"
echo "UPDATE lfcddemo.$TB SET v = 'd2' WHERE id = 4;" | src >/dev/null
settle_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'd2'" 1
hupdate; GOT=$(scd2)
[ "$HSTATE" = COMPLETED ] && [[ "$GOT" == *"4=d1(closed)"*"4=d2"* ]] && claim keeps-going PASS "history: $GOT" || claim keeps-going FAIL "history: $GOT"
hupdate; [ "$HSTATE" = COMPLETED ] && [ "$(scd2)" = "$GOT" ] && claim no-change-no-version PASS "a run with nothing new left the history as it was" \
  || claim no-change-no-version FAIL "changed on a rerun: $(scd2)"
result_md sqlserver-scd2-history-snapshot
exit $FAILED
