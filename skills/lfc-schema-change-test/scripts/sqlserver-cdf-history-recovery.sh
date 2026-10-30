#!/usr/bin/env bash
# sqlserver-cdf-history-recovery.sh: a history kept downstream of an SCD type 1 destination table, from its change data
# feed, in a separate audit pipeline (audit-history.py: audit_log + AUTO CDC SCD type 2). Through normal syncs, a gap
# (changes ingested while the audit pipeline doesn't run), a full refresh of the destination table, and recovery: a new
# audit pipeline that copies the old log once, closes the rows deleted during the gap, and keeps reading the change feed.
# Answers whether such a history is a workable workaround for a connector without SCD type 2 (the SharePoint audit-trail
# question), so it tests the events that break it and the recovery, not only normal syncs.
#
#   Needs: as lib.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER), and AUDIT_NB: the workspace path of
#   audit-history.py imported as a notebook (databricks workspace import <path> --file audit-history.py --format SOURCE
#   --language PYTHON). SRC_DB: an existing source database for the test table instead of a fresh one (the table is
#   dropped at the end, the database kept). MODE=integrated for a source without a gateway route.
#   Takes about 1.5 hours: an integrated CDC full refresh is two updates, both run to the end (no early stop).
#
#   Two recovery pipelines: b backfills from the broken pipeline a, as a customer would (a's failed run after the
#   refresh may already have written some of the gap's events to its log); c backfills from a copy of a's log taken
#   before the refresh, so the gap's delete reaches c only through the reconcile step.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
: "${AUDIT_NB:?}"
lfc_init cdfrec
TB=cdfrec_$E; FP_TABLES=$TB
if [ -n "${SRC_DB:-}" ]; then DB=$SRC_DB; else src_db <<<"SELECT 1;"; fi
DPS=()
cleanup_all() {
  if [ "${KEEP:-0}" != 1 ]; then
    for p in ${DPS[@]+"${DPS[@]}"}; do dbx pipelines delete "$p"; done; sleep 5
    for s in a b c snap; do dbx schemas delete "$CATALOG.${S}_audit_$s" --force 2>/dev/null; done
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

# audit_create <x> [backfill_from schema]: an audit pipeline in schema ${S}_audit_<x>; sets DP and AT
audit_create() {
  local as=${S}_audit_$1 conf="{\"src\":\"$CATALOG.$S.$TB\"${2:+,\"backfill_from\":\"$CATALOG.$2\"}}"
  dbx schemas create "$as" "$CATALOG" >/dev/null || exit 2
  DP=$(dbx pipelines create -o json --json "{\"name\":\"$as\",\"catalog\":\"$CATALOG\",\"schema\":\"$as\",\"serverless\":true,
    \"configuration\":$conf,\"libraries\":[{\"notebook\":{\"path\":\"$AUDIT_NB\"}}]}" | yq '.pipeline_id')
  [ -n "$DP" ] && [ "$DP" != null ] || { echo "audit pipeline $1 create failed"; exit 2; }
  DPS+=("$DP"); AT="\`$CATALOG\`.$as"
  echo "LINK audit pipeline $1 $(dbx auth describe -o json 2>/dev/null | yq -r '.details.host // ""')/pipelines/$DP"
}
# dupdate: one audit pipeline update; DSTATE, DERRS. A failed one is stopped (no automatic retries), except a platform
# rollout abort (AUTO_MIGRATION_ENVIRONMENT_VERSION_ENABLEMENT_BLOCKED: nothing written, reverted on the next run),
# which is run once more.
dupdate() {
  local u n
  for n in 1 2; do
    u=$(dbx pipelines start-update "$DP" --json '{}' -o json | yq '.update_id')
    # no update started (an expired login, an API error): stop the run, a claim on no update would be read as a result
    [ -n "$u" ] && [ "$u" != null ] || { echo "audit pipeline start-update failed: stopping the run"; exit 2; }
    for _ in $(seq 1 60); do DSTATE=$(dbx pipelines get-update "$DP" "$u" -o json | yq '.update.state')
      case $DSTATE in COMPLETED|FAILED|CANCELED) break;; esac; sleep 15; done
    dbx api get "/api/2.0/pipelines/$DP/events?max_results=250" -o json > "$W/dev.json"
    DERRS=$(U=$u yq -r '.events[] | select(.origin.update_id == strenv(U)) | .error.exceptions[]?.error_class // ""' "$W/dev.json" | grep -v '^$' | sort -u | tr '\n' ' ')
    [ "$DSTATE" = COMPLETED ] && break
    dbx pipelines stop "$DP" >/dev/null 2>&1
    [[ "$DERRS" == *AUTO_MIGRATION_ENVIRONMENT_VERSION_ENABLEMENT_BLOCKED* ]] || break
    echo "audit update: platform rollout abort (try $n), running once more"
  done
  echo "audit update $DSTATE ${DERRS}"
}
scd2() { q "SELECT CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(CONCAT(id, '=', v, IF(__END_AT IS NULL, '', '(closed)'))))) FROM $AT.audit_scd2" 2>&1 | cut -c1-400; }
logc() { q "SELECT CONCAT_WS(', ', COLLECT_LIST(c)) FROM (SELECT CONCAT(_origin, ' ', _change_type, ':', COUNT(*)) c FROM $AT.audit_log GROUP BY _origin, _change_type ORDER BY 1)" 2>&1 | cut -c1-300; }
hist() { q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(version, ':', operation))) FROM (SELECT version, operation FROM (DESCRIBE HISTORY $T.$TB) ORDER BY version)" 2>&1 | cut -c1-400; }
dest() { q "SELECT CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(CONCAT(id, '=', v)))) FROM $T.$TB" 2>&1; }
state() { observe "$1" "audit $DSTATE ${DERRS}; audit_log {$(logc)}; audit_scd2 {$(scd2)}"; }

audit_create a; DP_A=$DP; AT_A=$AT; dupdate; state a-after-load

# --- 1. normal sync: update 2, delete 3, insert 6 ------------------------------------------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'b2' WHERE id = 2; DELETE FROM lfcddemo.$TB WHERE id = 3; INSERT INTO lfcddemo.$TB (id, v) VALUES (6, 'f1');
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'b2')" "$(printf '5\t1')"
dupdate; state a-after-sync
[ "$(scd2)" = "1=a1 2=b1(closed) 2=b2 3=c1(closed) 4=d1 5=e1 6=f1" ] && claim sync-history PASS "audit_scd2: $(scd2)" || claim sync-history FAIL "audit_scd2: $(scd2)"

# --- 2. the gap: ingested, but the audit pipeline doesn't run: update 1, delete 5 ----------------------------------
src >/dev/null <<EOF
UPDATE lfcddemo.$TB SET v = 'a2' WHERE id = 1; DELETE FROM lfcddemo.$TB WHERE id = 5;
EOF
settle_rows "SELECT (SELECT COUNT(*) FROM $T.$TB), (SELECT COUNT(*) FROM $T.$TB WHERE v = 'a2')" "$(printf '4\t1')"
observe gap "destination: $(dest) (audit pipeline a not run)"
dbx schemas create "${S}_audit_snap" "$CATALOG" >/dev/null && q "CREATE TABLE \`$CATALOG\`.${S}_audit_snap.audit_log AS SELECT * FROM $AT_A.audit_log" >/dev/null
V0=$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)

# --- 3. full refresh of the destination table, to the end (integrated CDC: re-read, then apply) --------------------
EARLY_STOP=0 update "$TB"
[ "$(q "SELECT MAX(version) FROM (DESCRIBE HISTORY $T.$TB)" 2>&1)" = "$V0" ] && EARLY_STOP=0 update
observe refresh "destination versions: $(hist); rows: $(dest)"
DP=$DP_A; AT=$AT_A; dupdate; state a-after-refresh
[ "$DSTATE" = FAILED ] && [ -n "${DERRS// /}" ] && claim old-audit-breaks PASS "audit pipeline a: $DSTATE $DERRS" \
  || claim old-audit-breaks FAIL "audit pipeline a after the refresh: ${DSTATE:-none} ${DERRS}"

# --- 4. recovery: c from the copy of a's log taken before the refresh, b from the broken pipeline a ----------------
WANT="1=a1(closed) 1=a2 2=b1(closed) 2=b2 3=c1(closed) 4=d1 5=e1(closed) 6=f1"
audit_create c "${S}_audit_snap"; dupdate; state c-after-recovery
GOT=$(scd2); RECON=$(q "SELECT CONCAT_WS(' ', COLLECT_LIST(CONCAT(id, '=', v))) FROM $AT.audit_log WHERE _origin = 'reconcile'" 2>&1)
[ "$GOT" = "$WANT" ] && [ "$RECON" = "5=e1" ] && claim reconcile-closes-gap-delete PASS "reconcile wrote a delete for $RECON; audit_scd2 c: $GOT" \
  || claim reconcile-closes-gap-delete FAIL "reconcile rows: ${RECON:-none}; audit_scd2 c: $GOT (want $WANT)"
audit_create b "${S}_audit_a"; dupdate; state b-after-recovery
GOT=$(scd2)
[ "$GOT" = "$WANT" ] && claim recovered-history PASS "audit_scd2 b: $GOT" || claim recovered-history FAIL "audit_scd2 b: $GOT (want $WANT)"

# --- 5. next sync after recovery: update 4; then a run with nothing new --------------------------------------------
echo "UPDATE lfcddemo.$TB SET v = 'd2' WHERE id = 4;" | src >/dev/null
settle_rows "SELECT COUNT(*) FROM $T.$TB WHERE v = 'd2'" 1
dupdate; state b-after-next-sync
GOT=$(scd2)
[[ "$GOT" == *"4=d1(closed)"*"4=d2"* ]] && claim keeps-going PASS "audit_scd2 b: $GOT" || claim keeps-going FAIL "audit_scd2 b: $GOT"
L=$(logc); dupdate
[ "$(scd2)" = "$GOT" ] && [ "$(logc)" = "$L" ] && claim once-flows-run-once PASS "a run with nothing new left audit_log and audit_scd2 as they were" \
  || claim once-flows-run-once FAIL "changed on a rerun: audit_log {$(logc)}; audit_scd2 {$(scd2)}"
result_md sqlserver-cdf-history-recovery
exit $FAILED
