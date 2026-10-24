#!/usr/bin/env bash
# run.sh <profile> <pipeline-id> [--full-refresh [t1,t2]] [--by HH:MM] [--tz <zone>] [--no-wait]
# Starts a pipeline update and waits for it, then prints the update's result (report.sh --update): state, start, end
# and duration, with --by whether it ended by that time of day (local to --tz, default UTC), each table's
# final status and rows written, and the error classes in its events (META_ADS_RATE_LIMIT_EXCEEDED,
# META_ADS_REQUESTED_DATA_LIMIT_EXCEEDED, ...).
# --full-refresh alone refreshes every table; with a list, only those. Exit 0 when the update COMPLETED, 1 otherwise.
# A pipeline that is already running is followed, not restarted. After a failed update the pipeline may start a retry of
# its own (cause RETRY_ON_FAILURE); the script says so.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,9p "$0"; exit 2; }
PROFILE=$1 PIPE=$2; shift 2
FR="" BY="" TZONE=UTC WAIT=1
while [ $# -gt 0 ]; do
  case $1 in
    --full-refresh) if [[ ${2:-} =~ ^[a-z0-9_,]+$ ]]; then FR=$2; shift; else FR=ALL; fi ;;
    --by) BY=$2; shift ;; --tz) TZONE=$2; shift ;; --no-wait) WAIT=0 ;;
    *) sed -n 2,9p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
uuid "$PIPE"
[[ -z $BY || $BY =~ ^[0-9]{2}:[0-9]{2}$ ]] || { echo "--by is HH:MM" >&2; exit 2; }
link pipeline "/pipelines/$PIPE"

running=$(dbx pipelines get "$PIPE" -o json | jq -r '[.latest_updates[]? | select(.state | IN("COMPLETED","FAILED","CANCELED") | not)][0].update_id // ""')
if [ -n "$running" ]; then
  UPD=$running; echo "following the running update $UPD" >&2
else
  case $FR in
    "") body='{}' ;;
    ALL) body='{"full_refresh": true}' ;;
    *) body=$(jq -n --arg t "$FR" '{full_refresh_selection: ($t | split(","))}') ;;
  esac
  UPD=$(dbx api post "/api/2.0/pipelines/$PIPE/updates" --json "$body" -o json | jq -r .update_id)
  echo "started update $UPD${FR:+ (full refresh: $FR)}" >&2
fi
link update "/pipelines/$PIPE/updates/$UPD"
[ $WAIT = 1 ] || exit 0

while :; do
  st=$(dbx pipelines get-update "$PIPE" "$UPD" -o json | jq -r .update.state)
  case $st in COMPLETED|FAILED|CANCELED) break ;; esac
  sleep 20
done
"$(dirname "$0")/report.sh" "$PROFILE" "$PIPE" --update "$UPD" ${BY:+--by "$BY"} --tz "$TZONE"
if [ "$st" = FAILED ]; then
  sleep 10
  retry=$(dbx api get "/api/2.0/pipelines/$PIPE/updates?max_results=5" -o json | jq -r --arg u "$UPD" \
    '[.updates[] | select(.update_id != $u and .cause == "RETRY_ON_FAILURE")][0] | select(.) | "\(.update_id) \(.state)"')
  [ -z "$retry" ] || echo "the pipeline started a retry of its own (cause RETRY_ON_FAILURE): $retry; stop it with: databricks pipelines stop $PIPE --profile $PROFILE" >&2
fi
[ "$st" = COMPLETED ]
