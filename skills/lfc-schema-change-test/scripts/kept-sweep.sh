#!/usr/bin/env bash
# kept-sweep.sh [--dry-run] [--now <id>...]: delete the test runs lib.sh kept (stopped pipelines, schemas, source database)
# once their delete_after date has passed, or the named ones now (<id> is the run's destination schema, printed when the
# run was kept). Runs are listed in $LFC_KEPT (default ~/.local/state/lfc-tests/kept.jsonl), one JSON object a line;
# a deleted run's line is removed, a failed one stays for the next sweep. Each run is deleted with its own CLI profile
# and source command, through lib.sh's delete_run (the same steps as KEEP_DAYS=0 at exit). Daily from a scheduler:
#   kept-sweep.sh            # expired runs only
#   kept-sweep.sh --dry-run  # list what would be deleted
# shellcheck disable=SC2034  # S, STG, SLOT, E, AP, GW in the subshell are read by lib.sh's delete_run
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
KEPT=${LFC_KEPT:-$HOME/.local/state/lfc-tests/kept.jsonl}
DRY=0; NOW=()
while [ $# -gt 0 ]; do
  case $1 in --dry-run) DRY=1;; --now) shift; NOW+=("${1:?--now needs a run id}");; *) echo "usage: kept-sweep.sh [--dry-run] [--now <id>...]" >&2; exit 2;; esac
  shift
done
[ -s "$KEPT" ] || { echo "no kept runs ($KEPT)"; exit 0; }
today=$(date -u +%F); keep=$(mktemp); rc=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  id=$(jq -r .id <<<"$line"); after=$(jq -r .delete_after <<<"$line"); due=0
  [[ "$after" < "$today" || "$after" == "$today" ]] && due=1
  for n in "${NOW[@]:-}"; do [ "$n" = "$id" ] && due=1; done
  if [ $due = 0 ] || [ $DRY = 1 ]; then
    [ $DRY = 1 ] && [ $due = 1 ] && echo "would delete $id (delete_after $after): $(jq -c '{pipelines, schema, staging, db}' <<<"$line")"
    echo "$line" >> "$keep"; continue
  fi
  echo "deleting $id (kept $(jq -r .kept <<<"$line"), delete_after $after)"
  # lib.sh's delete_run with this run's variables, in a subshell (each run has its own profile and source)
  if (
    PROFILE=$(jq -r .profile <<<"$line"); CONNECTION=$(jq -r .connection <<<"$line"); CATALOG=$(jq -r .catalog <<<"$line")
    SRC_SQL=$(jq -r .src_sql <<<"$line"); CONN_USER=$(jq -r .conn_user <<<"$line"); ENGINE=$(jq -r .engine <<<"$line")
    export PROFILE CONNECTION CATALOG SRC_SQL CONN_USER ENGINE
    unset SOURCE_JSON
    . "$HERE/lib.sh"
    trap - EXIT
    W=$(mktemp -d); S=$(jq -r .schema <<<"$line"); STG=$(jq -r .staging <<<"$line"); DB=$(jq -r .db <<<"$line")
    SLOT=$(jq -r .slot <<<"$line"); E=${DB##*_}; [ -n "$DB" ] || E=none
    AP=$(jq -r '.pipelines[0] // ""' <<<"$line"); GW=$(jq -r '.pipelines[1] // ""' <<<"$line")
    delete_run; rm -rf "${W:?}"
    # done only when every pipeline is gone: an auth or network error must keep the record, not drop it
    for p in ${AP:-} ${GW:-}; do
      out=$(dbx pipelines get "$p" 2>&1) && { echo "  pipeline $p still exists"; exit 1; }
      grep -qiE 'does not exist|RESOURCE_DOES_NOT_EXIST|not found' <<<"$out" || { echo "  pipeline $p: $(head -1 <<<"$out" | cut -c1-160)"; exit 1; }
    done
    [ "${DB_LEFT:-0}" = 0 ] || [ -z "${DB:-}" ] || { echo "  source database $DB still there"; exit 1; }
  ); then :; else echo "  failed: kept for the next sweep"; echo "$line" >> "$keep"; rc=1; fi
done < "$KEPT"
[ $DRY = 1 ] || mv "$keep" "$KEPT"
rm -f "$keep"
exit $rc
