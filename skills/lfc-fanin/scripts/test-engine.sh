#!/usr/bin/env bash
# test-engine.sh: one database end to end through the fan-in, the test behind each docs/database-connect page:
#   1. your side: zerobus-debezium-test (SOURCE, MODEL=tpch, PHASE=2, KEEP=1): the database, the snapshot as of T in a
#      Volume, Debezium Server with the Zerobus sink, the change log; silver equal to the source (its own check)
#   2. the fan-in, after each of the skill's rounds (its ROUND_HOOK: fanin-round.sh): this skill's bundle
#      (fanin_dbz_zbus) on that log and Volume, into its own schema, deployed on round 1, run every round, and every
#      fan-in silver table against the skill's silver of that round (compare.py)
#   3. both silver pipelines' update times (pipeline_times.py), then that run's containers removed from the VM
#
#   PROFILE=<cli profile> CATALOG=<catalog> VM_IP=<vm> WAREHOUSE=<warehouse id> SOURCE=pg SOURCE_TYPE=POSTGRESQL \
#     bash skills/lfc-fanin/scripts/test-engine.sh
#
# SOURCE: zerobus-debezium-test's (mssql, pg, mysql, oracle, db2, informix, crdb, yb); SOURCE_TYPE: lfc-fanin's
# (SQLSERVER, POSTGRESQL, MYSQL, ORACLE, DB2, INFORMIX, COCKROACHDB, YUGABYTEDB). RESULTS (default
# ~/.lfc-test/results/fanin): <source>-skill.md/.log/.json, <source>-rounds.jsonl (a line a fan-in round),
# <source>-times.json (both silver pipelines' updates); fanin_page.py
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
export PROFILE CATALOG WAREHOUSE SOURCE SOURCE_TYPE ARCHIVE RES HERE
rm -f "$RES/$SOURCE-rounds.jsonl"; t0=$SECONDS
(cd "$C/skills/zerobus-debezium-test" && SOURCE=$SOURCE MODEL=tpch NSRC=1 PHASE=2 KEEP=1 OUT="$RES/$SOURCE-skill.md" \
  ROUND_HOOK="bash $HERE/fanin-round.sh" bash scripts/debezium-zerobus.sh > "$RES/$SOURCE-skill.log" 2>&1); rc=$?
E=$(grep -o -m1 'run [0-9]*:' "$RES/$SOURCE-skill.log" | tr -dc 0-9)
R=$(grep -o -m1 "schema $CATALOG\.[a-z0-9_]*" "$RES/$SOURCE-skill.log" | sed "s/schema $CATALOG\.//")
printf '{"source":"%s","source_type":"%s","skill_exit":%s,"skill_pass":%s,"skill_fail":%s,"run":"%s","schema":"%s","skill_s":%s}\n' \
  "$SOURCE" "$SOURCE_TYPE" "$rc" "$(grep -c '^PASS' "$RES/$SOURCE-skill.log")" "$(grep -c '^FAIL' "$RES/$SOURCE-skill.log")" \
  "$E" "$R" $((SECONDS - t0)) > "$RES/$SOURCE-skill.json"
SKP=$(grep -o -m1 "pipeline \"[A-Za-z0-9_.-]*-dbz-silver-$E\"" "$RES/$SOURCE-skill.log" | sed 's/^pipeline "//; s/"$//')
printf '{"skill":%s,"fanin":%s}\n' "$(python3 -I "$HERE/pipeline_times.py" "$PROFILE" "$SKP")" \
  "$(python3 -I "$HERE/pipeline_times.py" "$PROFILE" "fanin-$SOURCE-$E-silver")" > "$RES/$SOURCE-times.json"
if [ "$rc" != 0 ] && [ -n "$E" ]; then
  "${SSH[@]}" "for c in \$(sudo podman ps -a --format '{{.Names}}' | grep -E -- '-$E(-|\$)'); do echo == \$c; sudo podman inspect --format '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' \$c; sudo podman logs --tail 40 \$c 2>&1; done" > "$RES/$SOURCE-containers.log" 2>&1
fi
[ -n "$E" ] && "${SSH[@]}" "sudo podman ps -a --format '{{.Names}}' | grep -E -- '-$E(-|\$)' | xargs -r sudo podman rm -f" > /dev/null 2>&1
rm -rf "${C:?}"
echo "$SOURCE: $(cat "$RES/$SOURCE-skill.json") $(jq -c '{round, fanin_job, compare, silver_update}' "$RES/$SOURCE-rounds.jsonl" 2>/dev/null | paste -sd' ' -)"
