#!/usr/bin/env bash
# fanin-round.sh: one fan-in round, run by zerobus-debezium-test's ROUND_HOOK after its round ROUND (test-engine.sh
# sets it): the bundle deployed on round 1, one run of its job, and every fan-in silver table against the skill's
# silver of that round. Appends a line to $RES/$SOURCE-rounds.jsonl. Needs (test-engine.sh exports them): PROFILE,
# CATALOG, SCHEMA (the run's), ROUND, WAREHOUSE, SOURCE, SOURCE_TYPE, RES, ARCHIVE, HERE (this folder).
set -u
E=${SCHEMA##*_}; FS=${FANIN_SCHEMA:-fanin_${SOURCE}_$E}; NAME=fanin-$SOURCE-$E; t0=$SECONDS
cd "$HERE/../bundle" || exit 2
export BUNDLE_VAR_pipeline_name=$NAME BUNDLE_VAR_connector_type=fanin_dbz_zbus BUNDLE_VAR_dest_catalog=$CATALOG \
  BUNDLE_VAR_dest_schema=$FS BUNDLE_VAR_change_log=$CATALOG.$SCHEMA.cdc_log BUNDLE_VAR_snapshot_volume=/Volumes/$CATALOG/$SCHEMA/snapshot \
  BUNDLE_VAR_source_type=$SOURCE_TYPE BUNDLE_VAR_tests_repo_archive=$ARCHIVE
[ "$ROUND" = 1 ] && { databricks bundle deploy -p "$PROFILE" >> "$RES/$SOURCE-fanin.log" 2>&1 || exit 1; }
databricks bundle run fanin_job -p "$PROFILE" >> "$RES/$SOURCE-fanin.log" 2>&1
# the CLI's wait can drop on a network blip: wait on the job's own latest run
JID=$(databricks --profile "$PROFILE" jobs list -o json | jq -r --arg n "$NAME" '.[] | select(.settings.name | endswith($n)) | .job_id' | head -1)
st=; for _ in $(seq 1 120); do st=$(databricks --profile "$PROFILE" jobs list-runs --job-id "$JID" --limit 1 -o json 2>/dev/null \
  | jq -r '.[0].state | "\(.life_cycle_state) \(.result_state // "")"'); case "$st" in TERMINATED*|INTERNAL_ERROR*|SKIPPED*) break;; esac; sleep 30; done
job_s=$((SECONDS - t0))
cmp=$(python3 -I "$HERE/compare.py" "$WAREHOUSE" "$PROFILE" "$CATALOG" "$SCHEMA" "$FS" | jq -c '{tables, equal}')
upd=$(python3 -I "$HERE/pipeline_times.py" "$PROFILE" "$NAME-silver" | jq -c '.[-1] // {}')
echo "{\"round\":$ROUND,\"fanin_job\":\"$st\",\"fanin_schema\":\"$FS\",\"job_s\":$job_s,\"compare\":$cmp,\"silver_update\":$upd}" >> "$RES/$SOURCE-rounds.jsonl"
[ "$(echo "$cmp" | jq -r '.tables == .equal and .tables > 0')" = true ] && [[ "$st" == "TERMINATED SUCCESS" ]]
