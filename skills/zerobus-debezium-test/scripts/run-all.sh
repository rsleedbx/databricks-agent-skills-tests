#!/usr/bin/env bash
# run-all.sh: the repeatable test. Runs the committed skills, unchanged, and records which commit passed.
#
#   PROFILE=<cli profile> CATALOG=<catalog> VM_IP=<vm> bash skills/zerobus-debezium-test/scripts/run-all.sh
#   PROFILE=... CATALOG=... LIMA=<lima instance> SP_CLIENT_ID=<application id> SP_SECRET_FILE=<file> bash .../run-all.sh
#
# 1. Pre-flight: the skill folders it uses have no uncommitted changes (ALLOW_DIRTY=1 to run anyway, marked in the
#    summary). On a VM: SSH answers (a changed laptop IP blocks it at the VM's firewall, and every source query would
#    then come back empty), and lfc-source-podman's ensure-podman-socket.sh there. On the laptop: ensure-lima-podman.sh
#    (the VM running, then the same). Containers run rootful through the root Podman socket, with no sudo, in the one
#    store the databases use too.
#    SWEEP=1 first removes what earlier runs left behind (sweep.sh --apply; runs younger than 2 hours are kept).
# 2. zerobus-workspace-setup/scripts/check.sh: the Zerobus previews, and TCP to the endpoint from where the producer runs.
# 3. debezium-zerobus.sh for every source in SOURCES, in parallel, each on its own port (crdb 30026, yb 30031), each
#    cleaning up after itself. SOURCES defaults to "crdb yb" on a VM, "crdb" on the laptop (its schema dump, through
#    the volume, is CockroachDB only so far). MODEL (default shop) and SF go to every run: the source's schema, a
#    canonical schema.yaml (models/<MODEL>.yaml, or lfc-source-statschema's schemas/<MODEL>.yaml, e.g. tpcb).
#    NSRC, BRONZE (shared, fanout, per-table), BRONZE_SHARDS and ON_DRIFT go to every run too (debezium-zerobus.sh),
#    and for the many-tables test MIX, EMPTY, FKS, ROUNDS and PIPES.
# 4. The results in RESULTS (default ~/.lfc-test/results/zerobus-<UTC time>): check.txt, <source>.md (the run's result
#    table), <source>.log, summary.md. When everything passed and RECORD=1 (the default), one row per source goes into
#    docs/lakeflow-connect-zerobus.md, "Successful runs": commit that change with the run. Exit 0 only when all passed.
set -uo pipefail
export PATH=/opt/homebrew/bin:$PATH
: "${PROFILE:?}" "${CATALOG:?}"
[ -n "${VM_IP:-}" ] || [ -n "${LIMA:-}" ] || { echo "set VM_IP (a cloud VM) or LIMA (a local Lima instance)"; exit 2; }
SSH_KEY=${SSH_KEY:-$HOME/.ssh/lfc_test_rsa}; VM_USER=${VM_USER:-azureuser}
SOURCES=${SOURCES:-$([ -n "${LIMA:-}" ] && echo crdb || echo "crdb yb")}
SKILL=$(cd "$(dirname "$0")/.." && pwd); REPO=$(cd "$SKILL/../.." && pwd)
RESULTS=${RESULTS:-$HOME/.lfc-test/results/zerobus-$(date -u +%Y%m%dT%H%M%SZ)}; mkdir -p "$RESULTS"
cd "$REPO" || exit 2

# --- 1. pre-flight
COMMIT=$(git rev-parse --short HEAD)
DIRTY=$(git status --porcelain -- skills/zerobus-debezium-test skills/zerobus-workspace-setup skills/lfc-source-statschema skills/lfc-source-datagen skills/lfc-source-podman skills/lfc-source-cloud-vm/scripts/cloud-links.sh)
if [ -n "$DIRTY" ] && [ "${ALLOW_DIRTY:-0}" != 1 ]; then
  echo "uncommitted changes in the skills; commit them, or run from a clean checkout (git worktree add <dir> HEAD):"; echo "$DIRTY"; exit 2; fi
if [ -n "${LIMA:-}" ]; then
  bash skills/lfc-source-podman/scripts/ensure-lima-podman.sh "$LIMA" | tee "$RESULTS/lima.txt" || { echo "the Lima VM isn't ready"; exit 2; }
  FROM="limactl shell $LIMA --"; WHERE_HOST="Laptop, Lima \`$LIMA\`, no cloud storage"
else
  ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 "$VM_USER@$VM_IP" true 2>/dev/null \
    || { echo "no SSH to $VM_IP: is this laptop's IP ($(curl -s -m 5 https://api.ipify.org)) still allowed by the VM's firewall?"; exit 2; }
  FROM="ssh -i $SSH_KEY -o BatchMode=yes $VM_USER@$VM_IP"; WHERE_HOST="Cloud VM"
  # links to the VM and its network rules and disks, in whichever cloud has it (not fatal: the CLI may not be logged in)
  bash skills/lfc-source-cloud-vm/scripts/cloud-links.sh "$VM_IP" > "$RESULTS/cloud-links.txt" 2>&1; grep '^LINK' "$RESULTS/cloud-links.txt"
  bash skills/lfc-source-podman/scripts/ensure-podman-socket.sh --on "$FROM" | tee "$RESULTS/host.txt" || { echo "the VM isn't ready: rootful Podman through the socket"; exit 2; }
fi
[ "${SWEEP:-0}" = 1 ] && bash skills/zerobus-debezium-test/scripts/sweep.sh --apply | tee "$RESULTS/sweep.txt"
echo "commit $COMMIT${DIRTY:+ with uncommitted changes}; results in $RESULTS"

# --- 2. the workspace
bash skills/zerobus-workspace-setup/scripts/check.sh "$PROFILE" --interfaces "grpc$(case " $SOURCES " in (*" yb "*) echo ,kafka;; esac)" \
  --from "$FROM" > "$RESULTS/check.txt" 2>&1; CHECK=$?
grep -E '^(PASS|FAIL|WARN)' "$RESULTS/check.txt" | cut -c1-100

# --- 3. the sources, in parallel; each from a copy of the script with its own name (bash reads a script as it runs)
port() { case $1 in crdb) echo 30026;; yb) echo 30031;; *) echo 30040;; esac; }
for s in $SOURCES; do
  C=$RESULTS/run-$s.sh; cp skills/zerobus-debezium-test/scripts/debezium-zerobus.sh "$C"
  ( cd skills/zerobus-debezium-test && SOURCE=$s SRC_PORT=$(port $s) OUT="$RESULTS/$s.md" bash "$C" > "$RESULTS/$s.log" 2>&1
    echo "exit $?" >> "$RESULTS/$s.log" ) &
  sleep 5
done
wait

# --- 4. the summary, and the rows for the landing page
FAILED=$((CHECK != 0)); ROWS=
route() { case $1 in yb) echo "Kafka Connect → Zerobus Kafka endpoint";; *) echo "Debezium Server → Zerobus (gRPC)";; esac; }
version() { case $1 in crdb) echo "CockroachDB $(grep -m1 '^- Source:' "$RESULTS/$1.md" | grep -o -E 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)";;
  yb) echo "YugabyteDB $(grep -m1 '^- Source:' "$RESULTS/$1.md" | grep -o -E 'YB-[0-9.]+' | head -1 | cut -c4- | sed 's/\.$//')";; *) echo "$1";; esac; }
{ echo "# Zerobus CDC test, $(date -u +%F)"; echo
  echo "- Model: \`$([ -n "${MIX:-}" ] && echo "mix$MIX" || echo "${MODEL:-shop}")\`${SF:+ at SF $SF}$([ "${EMPTY:-0}" = 1 ] && echo ', empty tables')$([ "${FKS:-1}" = 0 ] && echo ', no foreign keys'); sources per host: ${NSRC:-1}; bronze: ${BRONZE:-shared}${BRONZE_SHARDS:+ ($BRONZE_SHARDS shards)}; drift: ${ON_DRIFT:-record}; rounds: ${ROUNDS:-5}; silver: ${SILVER:-pipeline} x ${PIPES:-1}$([ "${SPLIT:-0}" = 1 ] && echo ", a schema each")"
  echo "- Commit: \`$COMMIT\`${DIRTY:+ (with uncommitted changes)} of the tests repo; profile \`$PROFILE\`, catalog \`$CATALOG\`, ${LIMA:+Lima \`$LIMA\`}${VM_IP:+VM \`$VM_IP\`}"
  echo "- Workspace check: $([ $CHECK = 0 ] && echo PASS || echo FAIL) ($(grep -c '^PASS' "$RESULTS/check.txt") passed, $(grep -c '^FAIL' "$RESULTS/check.txt") failed)"; echo
  echo "| Source | Claims passed | Claims failed | Run | Exit |"; echo "|---|---|---|---|---|"
  for s in $SOURCES; do
    p=$(grep -c '^PASS' "$RESULTS/$s.log"); f=$(grep -c '^FAIL' "$RESULTS/$s.log"); x=$(grep -o '^exit [0-9]*' "$RESULTS/$s.log" | tail -1 | cut -d' ' -f2)
    r=$(grep -o '^run [0-9]*' "$RESULTS/$s.log" | head -1 | cut -d' ' -f2)
    [ "$f" = 0 ] && [ "$x" = 0 ] || FAILED=1
    echo "| $s | $p | $f | $r | $x |"
    host=$(grep -m1 '^- Host:' "$RESULTS/$s.md" 2>/dev/null | sed 's/^- Host: //'); dest=$(grep -m1 '^- Destination:' "$RESULTS/$s.md" 2>/dev/null | grep -o -E 'in [a-z]+ [a-z0-9-]+' | cut -c4-)
    ROWS="$ROWS| $(date -u +%F) | \`$COMMIT\` | $(version $s), model \`${MODEL:-shop}\` | $(route $s) | $WHERE_HOST ($host) → $dest serverless | $p of $((p + f)) | $r, [run-all.sh](../skills/zerobus-debezium-test/scripts/run-all.sh) |
"; done
  for s in $SOURCES; do f=$(grep '^FAIL' "$RESULTS/$s.log"); [ -n "$f" ] && { echo; echo "## $s failures"; echo; echo "$f" | cut -c1-300 | sed 's/^/- /'; }; done
  if grep -q '^LINK' "$RESULTS/cloud-links.txt" 2>/dev/null; then echo; echo "## Cloud resources"; echo; grep '^LINK' "$RESULTS/cloud-links.txt" | sed -E 's/^LINK ([^:]*): (.*)/- \1: <\2>/'; fi
  echo; echo "Each run's Databricks objects, with links, are in <source>.md (removed at the end of the run unless KEEP=1)."
} > "$RESULTS/summary.md"
printf '%s' "$ROWS" > "$RESULTS/rows.md"
PAGE=docs/lakeflow-connect-zerobus.md
if [ $FAILED = 0 ] && [ "${RECORD:-1}" = 1 ] && [ -z "$DIRTY" ] && [ -f "$PAGE" ]; then
  # newest first: the rows go right under the table's header in "## Successful runs"
  awk -v rows="$RESULTS/rows.md" '
    /^## Successful runs/ { insec = 1 }
    { print }
    insec && /^\|---/ { while ((getline l < rows) > 0) print l; insec = 0 }' "$PAGE" > "$RESULTS/page.md" && cp "$RESULTS/page.md" "$PAGE" \
    && echo "recorded in $PAGE: commit it with the run"
fi
cat "$RESULTS/summary.md"; exit $FAILED
