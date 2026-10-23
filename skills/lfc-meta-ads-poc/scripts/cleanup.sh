#!/usr/bin/env bash
# cleanup.sh <profile> [--pipeline <id>]... [--job <id>]... [--drop-schema <catalog>.<schema>] [--yes]
# Ends a POC: deletes the jobs, then the pipelines (a managed ingestion pipeline's streaming tables go with it), then
# optionally the destination schema with whatever is left in it. Prints what it will delete and asks first unless --yes.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,4p "$0"; exit 2; }
PROFILE=$1; shift
PIPES=() JOBS=() SCH="" YES=0
while [ $# -gt 0 ]; do
  case $1 in
    --pipeline) PIPES+=("$2"); shift ;; --job) JOBS+=("$2"); shift ;; --drop-schema) SCH=$2; shift ;; --yes) YES=1 ;;
    *) sed -n 2,4p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
for p in "${PIPES[@]}"; do uuid "$p"; echo "pipeline $p: $(dbx pipelines get "$p" -o json | jq -r .name)"; done
for j in "${JOBS[@]}"; do [[ $j =~ ^[0-9]+$ ]] || { echo "job id is a number: $j" >&2; exit 2; }; echo "job $j: $(dbx jobs get "$j" -o json | jq -r .settings.name)"; done
if [ -n "$SCH" ]; then
  [[ $SCH =~ ^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$ ]] || { echo "want <catalog>.<schema>: $SCH" >&2; exit 2; }
  [[ ${SCH#*.} != default && ${SCH#*.} != information_schema ]] || { echo "refusing to drop $SCH" >&2; exit 2; }
  echo "schema $SCH: $(dbx tables list "${SCH%%.*}" "${SCH#*.}" -o json | jq -r 'map(.name) | join(", ")')"
fi
if [ $YES = 0 ]; then read -r -p "delete all of the above? [y/N] " a; [ "$a" = y ] || exit 1; fi
for j in "${JOBS[@]}"; do dbx jobs delete "$j" && echo "deleted job $j"; done
for p in "${PIPES[@]}"; do dbx pipelines delete "$p" && echo "deleted pipeline $p"; done
[ -z "$SCH" ] || { dbx schemas delete "$SCH" --force && echo "dropped schema $SCH"; }
