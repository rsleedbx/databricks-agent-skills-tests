#!/usr/bin/env bash
# kept-sweep.sh [--dry-run] [--now <id>...]: delete the test runs lib.sh kept (stopped pipelines, schemas, source database,
# the script's extras) once their delete_after date has passed, or the named ones now (<id> is the run's destination
# schema, printed when the run was kept). Runs are listed in $LFC_KEPT (default ~/.local/state/lfc-tests/kept.jsonl), one
# JSON object a line. Each run is deleted with its own CLI profile and source, through lib.sh's delete_run (the same
# steps as KEEP_DAYS=0 at exit). Its line is removed only when every pipeline and schema reads back as not found and
# the source database is gone; otherwise (an auth or network error) it stays for the next sweep and the exit status is 1.
# Daily from a scheduler:
#   kept-sweep.sh            # expired runs only
#   kept-sweep.sh --dry-run  # list what would be deleted; the file is left as it is
# shellcheck disable=SC2034  # S, STG, SLOT, E, AP, GW, EXTRA_*, KEEP_SOURCE in the subshell are read by lib.sh's delete_run
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
KEPT=${LFC_KEPT:-$HOME/.local/state/lfc-tests/kept.jsonl}
DRY=0; NOW=()
while [ $# -gt 0 ]; do
  case $1 in --dry-run) DRY=1;; --now) shift; NOW+=("${1:?--now needs a run id}");; *) echo "usage: kept-sweep.sh [--dry-run] [--now <id>...]" >&2; exit 2;; esac
  shift
done
[ -s "$KEPT" ] || { echo "no kept runs ($KEPT)"; exit 0; }
gone() {   # gone <kind> <name>: the object reads back as not found (anything else, an auth error too, is not gone)
  local out; out=$("${DATABRICKS_CLI:-databricks}" "$1" get "$2" --profile "$PROFILE" 2>&1) && return 1
  # the CLI's texts (2026-11-05): "The specified pipeline <id> was not found.", "Schema '<name>' does not exist."
  grep -qE 'RESOURCE_DOES_NOT_EXIST|does not exist\.|The specified pipeline [^ ]+ was not found' <<<"$out"
}
# read a snapshot: lines appended while the sweep runs are carried over at the end, not read twice
today=$(date -u +%F); keep=$(mktemp); snap=$(mktemp); cp "$KEPT" "$snap"; n_read=$(wc -l < "$snap"); rc=0
while IFS= read -r line <&3; do
  [ -n "$line" ] || continue
  id=$(jq -r .id <<<"$line"); after=$(jq -r .delete_after <<<"$line"); due=0
  [[ "$after" < "$today" || "$after" == "$today" ]] && due=1
  for n in "${NOW[@]:-}"; do [ "$n" = "$id" ] && due=1; done
  if [ $due = 0 ] || [ $DRY = 1 ]; then
    [ $DRY = 1 ] && [ $due = 1 ] && echo "would delete $id (delete_after $after): $(jq -c '{pipelines, schema, staging, db, extra_pipelines, extra_schemas}' <<<"$line")"
    echo "$line" >> "$keep"; continue
  fi
  echo "deleting $id (kept $(jq -r .kept <<<"$line"), delete_after $after)"
  # lib.sh's delete_run with this run's variables, in a subshell (each run has its own profile and source); its own
  # stdin, so a command in it can't read the rest of the list
  if (
    PROFILE=$(jq -r .profile <<<"$line"); CONNECTION=$(jq -r .connection <<<"$line"); CATALOG=$(jq -r .catalog <<<"$line")
    SRC_SQL=$(jq -r .src_sql <<<"$line"); CONN_USER=$(jq -r .conn_user <<<"$line"); ENGINE=$(jq -r .engine <<<"$line")
    SOURCE_JSON=$(jq -r '.source_json // ""' <<<"$line"); KEEP_SOURCE=$(jq -r '.keep_source // "0"' <<<"$line")
    export PROFILE CONNECTION CATALOG SRC_SQL CONN_USER ENGINE KEEP_SOURCE
    if [ -n "$SOURCE_JSON" ] && [ -r "$SOURCE_JSON" ]; then export SOURCE_JSON; else unset SOURCE_JSON; fi
    EXTRA_PIPELINES=$(jq -r '(.extra_pipelines // []) | join(" ")' <<<"$line"); EXTRA_SCHEMAS=$(jq -r '(.extra_schemas // []) | join(" ")' <<<"$line")
    EXTRA_SQL=$(jq -r '.extra_sql // ""' <<<"$line")
    . "$HERE/lib.sh"
    trap - EXIT
    W=$(mktemp -d); S=$(jq -r .schema <<<"$line"); STG=$(jq -r .staging <<<"$line"); DB=$(jq -r '.db // ""' <<<"$line")
    SLOT=$(jq -r .slot <<<"$line"); E=${DB##*_}; [ -n "$DB" ] || { E=none; DB=$(jq -r '.src_db // ""' <<<"$line"); }
    AP=$(jq -r '.pipelines[0] // ""' <<<"$line"); GW=$(jq -r '.pipelines[1] // ""' <<<"$line")
    delete_run; rm -rf "${W:?}"
    bad=0
    for p in ${AP:-} ${GW:-} $EXTRA_PIPELINES; do gone pipelines "$p" || { echo "  pipeline $p: not deleted"; bad=1; }; done
    for x in "$CATALOG.$S" ${STG:+"$CATALOG.$STG"} $EXTRA_SCHEMAS; do gone schemas "$x" || { echo "  schema $x: not deleted"; bad=1; }; done
    [ "$E" = none ] || [ "${DB_LEFT:-0}" = 0 ] || { echo "  source database $DB: not dropped"; bad=1; }
    [ "${SLOT_LEFT:-0}" = 0 ] || { echo "  replication slot $SLOT: not dropped"; bad=1; }
    exit $bad
  ) < /dev/null; then :; else echo "  failed: kept for the next sweep"; echo "$line" >> "$keep"; rc=1; fi
done 3< "$snap"
if [ $DRY = 0 ]; then
  # runs kept while this sweep ran were appended after the lines it read: carry them over, under lib.sh's writer lock
  # (the same mkdir lock as kept_lock there; a lock older than 10 minutes is taken over)
  n=0; until mkdir "$KEPT.lock" 2>/dev/null; do n=$((n + 1)); [ $n -ge 600 ] && { rm -rf "${KEPT:?}.lock"; n=0; }; sleep 1; done
  tail -n +"$((n_read + 1))" "$KEPT" >> "$keep"
  mv "$keep" "$KEPT"
  rm -rf "${KEPT:?}.lock"
fi
rm -f "$keep" "$snap"
exit $rc
