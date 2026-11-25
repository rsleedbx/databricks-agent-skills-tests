#!/usr/bin/env bash
# test-engine.sh: one database end to end through the fan-in, the test behind each docs/database-connect page:
#   1. your side: zerobus-debezium-test (SOURCE, MODEL=tpch, PHASE=2, KEEP=1): the database, the snapshot as of T in a
#      Volume, Debezium Server with the Zerobus sink, the change log; silver equal to the source (its own check)
#   2. the fan-in: this skill's bundle (fanin_dbz_zbus) on that log and Volume, into its own schema, one run
#   3. every fan-in silver table against the skill's (compare.py), then that run's containers removed from the VM
#
#   PROFILE=<cli profile> CATALOG=<catalog> VM_IP=<vm> WAREHOUSE=<warehouse id> SOURCE=pg SOURCE_TYPE=POSTGRESQL \
#     bash skills/lfc-fanin/scripts/test-engine.sh
#
# SOURCE: zerobus-debezium-test's (mssql, pg, mysql, oracle, db2, informix, crdb, yb); SOURCE_TYPE: lfc-fanin's
# (SQLSERVER, POSTGRESQL, MYSQL, ORACLE, DB2, INFORMIX, COCKROACHDB, YUGABYTEDB). RESULTS (default
# ~/.lfc-test/results/fanin): <source>-skill.md/.log/.json, <source>-fanin.json, <source>-compare.json; fanin_page.py
# writes the docs page from them. VM_USER (azureuser), SSH_KEY (ssh's default): the VM's login, as for the skill.
# ARCHIVE: where the bundle installs lfc-change-source and lfc-fanin from (default: this repo's main).
set -u
: "${PROFILE:?}" "${CATALOG:?}" "${VM_IP:?}" "${WAREHOUSE:?}" "${SOURCE:?}" "${SOURCE_TYPE:?}"
HERE=$(cd "$(dirname "$0")" && pwd); SKILLS=$(cd "$HERE/../.." && pwd)
RES=${RESULTS:-$HOME/.lfc-test/results/fanin}; mkdir -p "$RES"
ARCHIVE=${ARCHIVE:-https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip}
SSH=(ssh -o ConnectTimeout=20 ${SSH_KEY:+-i "$SSH_KEY"} "${VM_USER:-azureuser}@$VM_IP")
C=$(mktemp -d "${TMPDIR:-/tmp}/fanin-$SOURCE.XXXXXX")   # a copy: editing the skills can't change this run
COPYFILE_DISABLE=1 tar -C "$SKILLS/.." --exclude __pycache__ -cf - "$(basename "$SKILLS")" | tar -xf - -C "$C"
t0=$SECONDS
(cd "$C/skills/zerobus-debezium-test" && SOURCE=$SOURCE MODEL=tpch NSRC=1 PHASE=2 KEEP=1 OUT="$RES/$SOURCE-skill.md" \
  bash scripts/debezium-zerobus.sh > "$RES/$SOURCE-skill.log" 2>&1); rc=$?
E=$(grep -o -m1 'run [0-9]*:' "$RES/$SOURCE-skill.log" | tr -dc 0-9)
R=$(grep -o -m1 "schema $CATALOG\.[a-z0-9_]*" "$RES/$SOURCE-skill.log" | sed "s/schema $CATALOG\.//")
printf '{"source":"%s","source_type":"%s","skill_exit":%s,"skill_pass":%s,"skill_fail":%s,"run":"%s","schema":"%s","skill_s":%s}\n' \
  "$SOURCE" "$SOURCE_TYPE" "$rc" "$(grep -c '^PASS' "$RES/$SOURCE-skill.log")" "$(grep -c '^FAIL' "$RES/$SOURCE-skill.log")" \
  "$E" "$R" $((SECONDS - t0)) > "$RES/$SOURCE-skill.json"
if [ "$rc" = 0 ] && [ -n "$R" ]; then
  t1=$SECONDS; FS=${FANIN_SCHEMA:-fanin_${SOURCE}_$E}
  (cd "$HERE/../bundle" && export BUNDLE_VAR_pipeline_name=fanin-$SOURCE-$E BUNDLE_VAR_connector_type=fanin_dbz_zbus \
    BUNDLE_VAR_dest_catalog=$CATALOG BUNDLE_VAR_dest_schema=$FS BUNDLE_VAR_change_log=$CATALOG.$R.cdc_log \
    BUNDLE_VAR_snapshot_volume=/Volumes/$CATALOG/$R/snapshot BUNDLE_VAR_source_type=$SOURCE_TYPE BUNDLE_VAR_tests_repo_archive=$ARCHIVE \
    && databricks bundle deploy -p "$PROFILE" > "$RES/$SOURCE-fanin.log" 2>&1 && databricks bundle run fanin_job -p "$PROFILE" >> "$RES/$SOURCE-fanin.log" 2>&1)
  # the CLI's wait can drop on a network blip: wait on the job's own latest run
  JID=$(databricks --profile "$PROFILE" jobs list -o json | jq -r --arg n "fanin-$SOURCE-$E" '.[] | select(.settings.name | endswith($n)) | .job_id' | head -1)
  st=; for _ in $(seq 1 120); do st=$(databricks --profile "$PROFILE" jobs list-runs --job-id "$JID" --limit 1 -o json 2>/dev/null \
    | jq -r '.[0].state | "\(.life_cycle_state) \(.result_state // "")"'); case "$st" in TERMINATED*|INTERNAL_ERROR*|SKIPPED*) break;; esac; sleep 30; done
  printf '{"fanin_job":"%s","fanin_schema":"%s","fanin_s":%s}\n' "$st" "$FS" $((SECONDS - t1)) > "$RES/$SOURCE-fanin.json"
  python3 -I "$HERE/compare.py" "$WAREHOUSE" "$PROFILE" "$CATALOG" "$R" "$FS" > "$RES/$SOURCE-compare.json"
else
  echo '{"fanin_job":"not run: the source side failed"}' > "$RES/$SOURCE-fanin.json"
  [ -n "$E" ] && "${SSH[@]}" "for c in \$(sudo podman ps -a --format '{{.Names}}' | grep -E -- '-$E(-|\$)'); do echo == \$c; sudo podman inspect --format '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' \$c; sudo podman logs --tail 40 \$c 2>&1; done" > "$RES/$SOURCE-containers.log" 2>&1
fi
[ -n "$E" ] && "${SSH[@]}" "sudo podman ps -a --format '{{.Names}}' | grep -E -- '-$E(-|\$)' | xargs -r sudo podman rm -f" > /dev/null 2>&1
rm -rf "${C:?}"
echo "$SOURCE: $(cat "$RES/$SOURCE-skill.json") $(cat "$RES/$SOURCE-fanin.json") $(jq -c '{tables, equal}' "$RES/$SOURCE-compare.json" 2>/dev/null)"
