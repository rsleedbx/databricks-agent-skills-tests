#!/usr/bin/env bash
# Run several prompt files in parallel, each in its own scratch WORK_DIR.
#
# Usage:
#   source env.sh
#   bin/run_batch.sh prompts/lfc-matrix/qbc-*.txt
#
# Run tests that create pipelines on the same connection in separate batches, so two
# sessions don't pick the same default pipeline name at once.
#
# Results: $RESULTS_ROOT/<name>-results/<tag>.json (summarize with ./summarize.py), logs: $RESULTS_ROOT/<name>.log

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${RESULTS_ROOT:-/tmp}"
mkdir -p "$OUT"
[ $# -gt 0 ] || { echo "usage: $0 <prompts-file>..."; exit 1; }
# keep the target catalog and the foreign catalogs on the env's connections for another month (RemoveAfter tag)
conns=(); for v in $(compgen -v | grep -E '^LFC_.*_CONNECTION$'); do [ -n "${!v}" ] && conns+=(--connection "${!v}"); done
python3 -I "$ROOT/skills/lfc-workspace-setup/scripts/keep_catalogs.py" "${PIPELINE_WS:-DEFAULT}" "${conns[@]}" ${LFC_CATALOG:+"$LFC_CATALOG"}

for f in "$@"; do
  f="$(cd "$(dirname "$f")" && pwd)/$(basename "$f")"
  name=$(basename "$f" .txt)
  W=$OUT/$name-work
  rm -rf "${W:?}" "${OUT:?}/${name:?}-results"
  # Tools a deploy test needs. conn.json holds a generated password (Lakebase demo), so allow
  # deleting it by every path a session might use.
  ALLOWED="Read,Write,Edit,Glob,Grep,WebFetch,Bash(databricks:*),Bash(ls:*),Bash(mkdir:*),Bash(jq:*)"
  for p in conn.json "$W/conn.json" "/private$W/conn.json" scratch/conn.json "$W/scratch/conn.json" "/private$W/scratch/conn.json"; do
    ALLOWED="$ALLOWED,Bash(rm $p),Bash(rm -f $p)"
  done
  ( WORK_DIR=$W CLAUDE_ALLOWED_TOOLS="$ALLOWED" "$ROOT/run.sh" "$SKILLS_REPO" "$f" > "$OUT/$name.log" 2>&1 ) &
done
wait
for f in "$@"; do
  name=$(basename "$f" .txt)
  echo "== $name: $(grep -E 'passed|FAILED' "$OUT/$name.log" | tail -1)"
done
