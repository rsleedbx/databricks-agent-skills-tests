#!/usr/bin/env bash
# sqlserver-history-snapshot.sh: SCD type 2 history of an SCD type 1 destination table from periodic snapshots
# (history-snapshot.py: AUTO CDC FROM SNAPSHOT), through the same lifecycle as the change-feed route
# (lfc-schema-change-test sqlserver-cdf-history-recovery.sh): normal sync, a gap, a full refresh of the destination,
# history runs while the refresh is applied, the next sync, a run with nothing new. Same table, changes and expected
# history as that script, so the two routes compare row for row.
#
#   Needs: as lfc-schema-change-test lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER), and HISTORY_NB: the
#   workspace path of history-snapshot.py imported as a notebook (databricks workspace import <path> --file
#   history-snapshot.py --format SOURCE --language PYTHON). SRC_DB: an existing source database for the test table
#   instead of a fresh one (the table is dropped at the end). MODE=integrated for a source without a gateway route.
#   Takes about 1.5 hours with integrated CDC: its full refresh is two updates, both run to the end.
set -uo pipefail
. "$(dirname "$0")/../../lfc-schema-change-test/scripts/lib.sh"
: "${HISTORY_NB:?}"
lfc_init histsnap
TB=histsnap_$E; FP_TABLES=$TB
if [ -n "${SRC_DB:-}" ]; then DB=$SRC_DB; else src_db <<<"SELECT 1;"; fi
HS=${S}_history; HT="\`$CATALOG\`.$HS"; DP=""
cleanup_all() {
  if [ "${KEEP:-0}" != 1 ]; then
    [ -n "$DP" ] && dbx pipelines delete "$DP"; sleep 5; dbx schemas delete "$CATALOG.$HS" --force 2>/dev/null
    [ -n "${SRC_DB:-}" ] && echo "DROP TABLE IF EXISTS lfcddemo.$TB;" | src >/dev/null && echo "source table lfcddemo.$TB dropped"
  fi
  cleanup
}
trap cleanup_all EXIT

src >/dev/null <<EOF || { echo "source setup failed"; exit 2; }
CREATE TABLE lfcddemo.$TB (id INT NOT NULL PRIMARY KEY, v VARCHAR(20));
INSERT INTO lfcddemo.$TB (id, v) VALUES (1, 'a1'), (2, 'b1'), (3, 'c1'), (4, 'd1'), (5, 'e1');
ALTER TABLE lfcddemo.$TB ENABLE CHANGE_TRACKING;
GRANT SELECT, VIEW CHANGE TRACKING ON lfcddemo.$TB TO $CONN_USER;
EOF
pipelines_create "$(obj "$TB")"
update; [ "$(q "SELECT COUNT(*) FROM $T.$TB" 2>&1)" = 5 ] || update

dbx schemas create "$HS" "$CATALOG" >/dev/null || exit 2
DP=$(dbx pipelines create -o json --json "{\"name\":\"$HS\",\"catalog\":\"$CATALOG\",\"schema\":\"$HS\",\"serverless\":true,
  \"configuration\":{\"src\":\"$CATALOG.$S.$TB\",\"keys\":\"id\"},\"libraries\":[{\"notebook\":{\"path\":\"$HISTORY_NB\"}}]}" | yq '.pipeline_id')
[ -n "$DP" ] && [ "$DP" != null ] || { echo "history pipeline create failed"; exit 2; }
echo "LINK history pipeline $(dbx auth describe -o json 2>/dev/null | yq -r '.details.host // ""')/pipelines/$DP"
# hupdate: one history update; HSTATE, HERRS. A platform rollout abort (nothing written) is run once more; no update
# ID (an expired login, an API error) stops the run.
hupdate() {
  local u n
  for n in 1 2; do
    u=$(dbx pipelines start-update "$DP" --json '{}' -o json | yq '.update_id')
    [ -n "$u" ] && [ "$u" != null ] || { echo "history start-update failed: stopping the run"; exit 2; }
    for _ in $(seq 1 60); do HSTATE=$(dbx pipelines get-update "$DP" "$u" -o json | yq '.update.state')
      case $HSTATE in COMPLETED|FAILED|CANCELED) break;; esac; sleep 10; done
    dbx api get "/api/2.0/pipelines/$DP/events?max_results=250" -o json > "$W/hev.json"
    HERRS=$(U=$u yq -r '.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.error_class // ""' "$W/hev.json" | grep -v '^$' | sort -u | tr '\n' ' ')
    [ "$HSTATE" = COMPLETED ] && break
    dbx pipelines stop "$DP" >/dev/null 2>&1
    [[ "$HERRS" == *AUTO_MIGRATION_ENVIRONMENT_VERSION_ENABLEMENT_BLOCKED* ]] || break
    echo "history update: platform rollout abort (try $n), running once more"
  done
  echo "history update $HSTATE ${HERRS}"
}
scd2() { q "SELECT CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(CONCAT(id, '=', v, IF(__END_AT IS NULL, '', '(closed)'))))) FROM $HT.history" 2>&1 | cut -c1-400; }
dest() { q "SELECT CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(CONCAT(id, '=', v)))) FROM $T.$TB" 2>&1; }
hist() { q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.$TB) ORDER BY version)" 2>&1 | cut -c1-400; }
check() { [ "$(scd2)" = "$2" ] && claim "$1" PASS "history: $(scd2)" || claim "$1" FAIL "history: $(scd2) (want $2)"; }

hupdate; check load "1=a1 2=b1 3=c1 4=d1 5=e1"

# --- 1. normal sync: update 2, delete 3, insert 6 ------------------------------------------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'b2' WHERE id = 2; DELETE FROM lfcddemo.$TB WHERE id = 3; INSERT INTO lfcddemo.$TB (id, v) VALUES (6, 'f1');
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'b2')" "$(printf '5\t1')"
hupdate; check sync-history "1=a1 2=b1(closed) 2=b2 3=c1(closed) 4=d1 5=e1 6=f1"

# --- 2. the gap: ingested, but the history pipeline doesn't run: update 1, delete 5 --------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'a2' WHERE id = 1; DELETE FROM lfcddemo.$TB WHERE id = 5;
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'a2')" "$(printf '4\t1')"
observe gap "destination: $(dest) (history pipeline not run)"
V0=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)

# --- 3. full refresh; history runs back to back while it is applied ------------------------------------------------
EARLY_STOP=0 update "$TB"   # integrated CDC: this one re-reads the source; the next update applies it
RUNS=0
if [ "$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)" = "$V0" ]; then
  u=$(dbx pipelines start-update "$AP" --json '{}' -o json | yq '.update_id')
  [ -n "$u" ] && [ "$u" != null ] || { echo "ingestion start-update failed: stopping the run"; exit 2; }
  while :; do
    st=$(dbx pipelines get-update "$AP" "$u" -o json | yq '.update.state')
    case $st in COMPLETED|FAILED|CANCELED) break;; esac
    [ $RUNS -ge 30 ] && break
    hupdate >/dev/null; RUNS=$((RUNS + 1))
    observe "during-refresh-$RUNS" "ingestion $st; destination $(dest); history $HSTATE ${HERRS}"
  done
  echo "refresh apply update $st after $RUNS history runs"
  # not done (failed, cancelled, or still running after 30 history runs): let lib.sh's update finish it; it follows
  # the pipeline's own retries, and joins an update that is still running
  [ "$st" = COMPLETED ] || { echo "refresh apply not completed ($st): finishing it with update"; EARLY_STOP=0 update; }
fi
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
hupdate; observe history-after-refresh "history $HSTATE ${HERRS}"
check survives-refresh "$WANT"
echo "UPDATE lfcddemo.$TB SET v = 'd2' WHERE id = 4;" | src >/dev/null
settle_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'd2'" 1
hupdate; GOT=$(scd2)
[[ "$GOT" == *"4=d1(closed)"*"4=d2"* ]] && claim keeps-going PASS "history: $GOT" || claim keeps-going FAIL "history: $GOT"
hupdate; [ "$(scd2)" = "$GOT" ] && claim no-change-no-version PASS "a run with nothing new left the history as it was" \
  || claim no-change-no-version FAIL "changed on a rerun: $(scd2)"
result_md sqlserver-history-snapshot
exit $FAILED
