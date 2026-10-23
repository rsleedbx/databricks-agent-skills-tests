#!/usr/bin/env bash
# probe.sh <profile> --connection <name> --account <act_id> --catalog <c> --schema <probe_schema>
#          [--days <n>] [--keep] <custom_report_options.json>...
# Tries custom report definitions before they go into the POC pipeline: one throwaway pipeline "<schema>_probe" with
# one custom_report per file (destination table cr_<file name>), over the last --days days (default 7), one update,
# then a line per report: COMPLETED (rows when the event has them), or FAILED with the error class and Meta's trace id.
# Meta rejects some breakdown combinations, and the pipeline then fails with META_ADS_SOURCE_API_ERROR; the probe finds
# which definition it was, since each report is its own table. The pipeline is deleted afterwards (its tables go with
# it) unless --keep. Use a schema of its own.
set -euo pipefail
[ $# -ge 1 ] || { sed -n 2,9p "$0"; exit 2; }
PROFILE=$1; shift
CONN="" ACCT="" CAT="" SCH="" DAYS=7 KEEP=0 FILES=()
while [ $# -gt 0 ]; do
  case $1 in
    --connection) CONN=$2; shift ;; --account) ACCT=$2; shift ;; --catalog) CAT=$2; shift ;; --schema) SCH=$2; shift ;;
    --days) DAYS=$2; shift ;; --keep) KEEP=1 ;;
    -*) sed -n 2,9p "$0"; exit 2 ;;
    *) FILES+=("$1") ;;
  esac
  shift
done
D=$(dirname "$0")
. "$D/lib.sh"
[ ${#FILES[@]} -gt 0 ] || { echo "no custom report files" >&2; exit 2; }
[[ $DAYS =~ ^[0-9]+$ ]] || { echo "--days is a number" >&2; exit 2; }
start=$(date -u -v-"${DAYS}"d +%F 2>/dev/null || date -u -d "-$DAYS days" +%F)
args=()
for f in "${FILES[@]}"; do
  n=$(basename "$f" .json | tr -c 'a-zA-Z0-9_\n' _ | tr A-Z a-z)
  args+=(--custom-report "cr_$n=$f")
done
NAME="${SCH}_probe"
PIPE=$("$D/pipeline.sh" "$PROFILE" --connection "$CONN" --account "$ACCT" --catalog "$CAT" --schema "$SCH" \
  --name "$NAME" --tables "" --start-date "$start" "${args[@]}" | tail -1)
uuid "$PIPE"
UPD=$("$D/run.sh" "$PROFILE" "$PIPE" 2>/dev/null | sed -n 's#^update: .*/updates/##p' || true)
uuid "$UPD"
echo "probe of ${#FILES[@]} custom reports, $start to today, update $UPD:"
dbx api get "/api/2.0/pipelines/$PIPE/events?max_results=250" -o json | jq -r --arg u "$UPD" '
  [.events[] | select(.origin.update_id == $u and .event_type == "flow_progress")]
  | group_by(.origin.flow_name) | .[] | max_by(.timestamp) as $l
  | ($l.origin.flow_name | split(".") | last) as $t
  | if $l.details.flow_progress.status == "FAILED" then
      ([$l.error.exceptions[]?.message // ""] | join(" ")) as $m
      | ([$m | scan("\\[([A-Z][A-Z0-9_]+(?:\\.[A-Z0-9_]+)*)\\]")[0]]) as $cs
      | ((([$cs[] | select(startswith("META_ADS"))] + $cs)[0]) // ($m | .[0:200])) as $c
      | ([$m | scan("trace id[^:]*: *([A-Za-z0-9_-]+)")[0]][0] // "") as $tr
      | "  \($t)  FAILED  \($c)\(if $tr != "" then "  Meta trace id \($tr)" else "" end)"
    else "  \($t)  \($l.details.flow_progress.status)  rows=\($l.details.flow_progress.metrics.num_output_rows // "-")" end'
# a failed update is followed by one the pipeline starts itself (cause RETRY_ON_FAILURE): stop it, it would fail again
dbx pipelines stop "$PIPE" >/dev/null 2>&1 || true
if [ $KEEP = 0 ]; then
  dbx pipelines delete "$PIPE" && echo "deleted probe pipeline $NAME" >&2
else
  link "kept probe pipeline" "/pipelines/$PIPE"
fi
