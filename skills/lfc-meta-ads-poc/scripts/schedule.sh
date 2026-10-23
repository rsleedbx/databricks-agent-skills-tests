#!/usr/bin/env bash
# schedule.sh <profile> <pipeline-id> [--cron "<quartz>"] [--tz <zone>] [--email a@x,b@y] [--name <job>] [--paused]
# Creates (or resets, when a job of that name exists) a Lakeflow Job that runs the pipeline on a schedule and emails on
# failure. Defaults: daily at 06:00 America/New_York ("0 0 6 * * ?"), two hours ahead of an 08:00 SLA; job name
# "<pipeline name> schedule". The docs recommend no more often than hourly during the Beta (Meta rate limits).
# --paused creates it with the schedule paused. Prints the job link and id.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,6p "$0"; exit 2; }
PROFILE=$1 PIPE=$2; shift 2
CRON="0 0 6 * * ?" TZONE=America/New_York EMAIL="" NAME="" PAUSE=UNPAUSED
while [ $# -gt 0 ]; do
  case $1 in
    --cron) CRON=$2; shift ;; --tz) TZONE=$2; shift ;; --email) EMAIL=$2; shift ;; --name) NAME=$2; shift ;;
    --paused) PAUSE=PAUSED ;;
    *) sed -n 2,6p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
uuid "$PIPE"
[ -n "$NAME" ] || NAME="$(dbx pipelines get "$PIPE" -o json | jq -r .name) schedule"
settings=$(jq -n --arg n "$NAME" --arg p "$PIPE" --arg c "$CRON" --arg z "$TZONE" --arg e "$EMAIL" --arg s "$PAUSE" '{
  name: $n,
  schedule: {quartz_cron_expression: $c, timezone_id: $z, pause_status: $s},
  max_concurrent_runs: 1,
  tasks: [{task_key: "refresh_pipeline", pipeline_task: {pipeline_id: $p}}]
} + (if $e != "" then {email_notifications: {on_failure: ($e | split(","))}} else {} end)')
id=$(dbx jobs list --name "$NAME" -o json | jq -r '.[0].job_id // ""')
if [ -n "$id" ]; then
  dbx jobs reset --json "$(jq -n --argjson i "$id" --argjson s "$settings" '{job_id: $i, new_settings: $s}')"
  echo "reset job $NAME" >&2
else
  id=$(dbx jobs create --json "$settings" -o json | jq -r .job_id)
  echo "created job $NAME: $CRON $TZONE ($PAUSE)" >&2
fi
link job "/jobs/$id"
echo "$id"
