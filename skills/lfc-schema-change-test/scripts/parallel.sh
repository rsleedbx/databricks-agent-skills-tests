#!/usr/bin/env bash
# parallel.sh <slots file> <jobs file>: run test scripts in parallel, one job per source at a time.
#   slots: one line per source, "- json:<contract file>" (any source with a source contract, lfc-source-contract: its
#          connection and SRC_SQL come from the file; a database of yours on a source VM: the file lfc-source-cloud-vm
#          slot-source.sh writes) or "CONNECTION MSSQL_CONTAINER" (a SQL Server container on the test VM, through
#          vm-mssql.sh and the VM admin login). At most half as many as the VM's vCPUs.
#   jobs:  one line per run, "<name> <script> [VAR=value ...]", e.g. "ct200 sqlserver-ct-net-changes.sh TABLES_N=200";
#          a Python scenario by its name, without .sh (e.g. "addcol sqlserver-add-column MODE=integrated").
#   Each job runs from its own copy of the scripts in $RUNS/<name>/ (bash reads a script while it runs), with the slot's
#   CONNECTION, MSSQL_CONTAINER and SRC_SQL, OUT=$RUNS/<name>/result.md, SCENARIO, LFC_RECORD and LFC_RUNTIME set; the
#   rest of the environment (PROFILE, CATALOG, CONN_USER, VM_IP, LFC_PERF_DIR, ...) is passed through. Log: run.log.
#   RUNS (default ./runs-<epoch>). Blank lines and lines starting with # are skipped. Exits 1 if any job failed.
set -uo pipefail
: "${1:?slots file}" "${2:?jobs file}"
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../../.." && pwd)
RUNS=${RUNS:-$PWD/runs-$(date +%s)}; mkdir -p "$RUNS"
SLOTS=() JOBS=()   # bash 3.2: no mapfile
while IFS= read -r l; do SLOTS+=("$l"); done < <(grep -v -E '^\s*(#|$)' "$1")
while IFS= read -r l; do JOBS+=("$l"); done < <(grep -v -E '^\s*(#|$)' "$2")
[ ${#SLOTS[@]} -gt 0 ] || { echo "no slots in $1"; exit 2; }
declare -a PID NAMEOF; FAILED=0
reap() {   # free the slots whose job ended; report each
  local i rc
  for i in "${!SLOTS[@]}"; do
    [ -n "${PID[$i]:-}" ] || continue
    kill -0 "${PID[$i]}" 2>/dev/null && continue
    wait "${PID[$i]}"; rc=$?; [ $rc = 0 ] || FAILED=1
    echo "$(date +%T) DONE ${NAMEOF[$i]} (exit $rc): $(grep -c '| PASS |' "$RUNS/${NAMEOF[$i]}/result.md" 2>/dev/null) pass, $(grep -c '| FAIL |' "$RUNS/${NAMEOF[$i]}/result.md" 2>/dev/null) fail; $RUNS/${NAMEOF[$i]}/run.log"
    PID[$i]=""
  done
}
for job in ${JOBS[@]+"${JOBS[@]}"}; do
  read -r name script vars <<< "$job"
  # a Python scenario (lfc_schema_change/scenarios/<name with _>.py) by its name, without .sh: run through the package
  py=0; [ -f "$HERE/lfc_schema_change/scenarios/${script//-/_}.py" ] && py=1
  [ $py = 1 ] || [ -f "$HERE/$script" ] || { echo "$name: no script or scenario $script"; FAILED=1; continue; }
  while :; do
    reap; slot=""
    for i in "${!SLOTS[@]}"; do [ -z "${PID[$i]:-}" ] && { slot=$i; break; }; done
    [ -n "$slot" ] && break; sleep 15
  done
  read -r conn container <<< "${SLOTS[$slot]}"
  [ "$conn" = - ] && conn=""   # a contract's connection comes from the file
  if [[ $container == json:* ]]; then f=${container#json:}; case $f in /*) ;; *) f=$PWD/$f ;; esac; container=json:$f; fi   # the runs cd
  d=$RUNS/$name; mkdir -p "$d"; cp "$HERE"/lib.sh "$HERE"/vm-*.sh "$d/"; [ $py = 1 ] || cp "$HERE/$script" "$d/"
  if [ $py = 1 ]; then run=(python3 -I -u "$HERE/lfc_schema_change" "$script"); else run=(bash "./$script"); fi
  if [[ $container == json:* ]]; then src=(SOURCE_JSON="${container#json:}" LFC_SOURCE_ENV="$REPO/skills/lfc-source-contract/scripts/source-env.sh")
  else src=(MSSQL_CONTAINER="$container" SRC_SQL="$d/vm-mssql.sh" SRC_ENV="$d/vm-mssql.sh --env"); fi
  # shellcheck disable=SC2086
  ( cd "$d" && env CONNECTION="$conn" "${src[@]}" \
      OUT="$d/result.md" SCENARIO="${script%.sh}" LFC_RECORD="${LFC_RECORD:-$REPO/skills/lfc-test-results/scripts/record.py}" \
      LFC_RUNTIME="${LFC_RUNTIME:-$REPO/bin/lfc-runtime.sh}" $vars "${run[@]}" > "$d/run.log" 2>&1 ) &
  PID[$slot]=$!; NAMEOF[$slot]=$name
  echo "$(date +%T) START $name ($script ${vars:-}) on $conn / $container"
done
while :; do reap; busy=0; for i in "${!SLOTS[@]}"; do [ -n "${PID[$i]:-}" ] && busy=1; done; [ $busy = 0 ] && break; sleep 15; done
echo "all jobs done in $RUNS"
exit $FAILED
