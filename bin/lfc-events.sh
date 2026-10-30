#!/usr/bin/env bash
# lfc-events.sh <profile> <pipeline-id> [--update <id>] [--since <hours>] [--source table|api|auto] [--warehouse <id>]
#               [--types <t1,t2>] [--summary]: a pipeline's events, from the pipeline events system table or the API.
#
# Two sources, both public:
#   table  system.lakeflow_pipeline_events_preview.pipeline_events (Beta; system.lakeflow.pipeline_events at GA): every
#          event of every pipeline in the account for 13 months, including METRICS-level events (flow_progress and
#          cluster_resources every minute while a pipeline runs) that the API never returns. Needs a SQL warehouse and
#          SELECT on the system schema. Rows arrive with a delay: seconds when measured, no stated bound.
#   api    /api/2.0/pipelines/<id>/events: INFO, WARN and ERROR events only, but current to the second. Use it while
#          debugging a pipeline that is running now.
#   auto   (default) the table, unless it can't be read or it is behind the API's newest event: then the API.
#
# Prints one line per event: time, type, level, update, flow or dataset, message. --types keeps only those event types.
# --summary prints instead: events by type and level; warnings (gc_pressure, abnormal_termination, disk_space, WARN
# and ERROR); outbound API calls (remote_api_usage, SaaS connectors); the newest cluster_resources (autoscaling, task
# slots); each update's start, full refresh and last state; rows and backlog per table (flow_progress); the latest
# snapshot or CDC operation per table and why (operation_progress); and streaming batch counts and durations per flow
# (stream_progress). The worked queries in the docs' pipeline events page and its observability guide cover the same
# ground in SQL. LFC_EVENTS_TABLE overrides the table path.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,20p "$0"; exit 2; }
PROFILE=$1 PIPE=$2; shift 2
UPD="" SINCE=24 SRC=auto WH="" TYPES="" SUMMARY=0
while [ $# -gt 0 ]; do
  case $1 in
    --update) UPD=$2; shift ;; --since) SINCE=$2; shift ;; --source) SRC=$2; shift ;;
    --warehouse) WH=$2; shift ;; --types) TYPES=$2; shift ;; --summary) SUMMARY=1 ;;
    *) sed -n 2,20p "$0"; exit 2 ;;
  esac
  shift
done
export PATH=/opt/homebrew/bin:$PATH
TABLE=${LFC_EVENTS_TABLE:-system.lakeflow_pipeline_events_preview.pipeline_events}
W=$(mktemp -d); trap 'rm -rf "${W:?}"' EXIT
note() { echo "$*" >&2; }
[[ $PIPE =~ ^[0-9a-f-]{36}$ ]] || { note "pipeline id should be a UUID: $PIPE"; exit 2; }
[[ -z $UPD || $UPD =~ ^[0-9a-f-]+$ ]] || { note "update id should be hex: $UPD"; exit 2; }
[[ $SINCE =~ ^[0-9]+$ ]] || { note "--since takes hours: $SINCE"; exit 2; }

# events -> $W/events.tsv: time, type, level, update, flow-or-dataset, message, details (JSON, base64: @tsv would
# escape the backslashes in JSON strings such as stream_progress.progress_json)
from_api() {
  echo '[]' > "$W/all.json"; local tok="" cutoff
  cutoff=$(jq -rn --argjson h "$SINCE" 'now - $h * 3600 | strftime("%Y-%m-%dT%H:%M:%S")')
  for _ in $(seq 1 40); do
    databricks api get "/api/2.0/pipelines/$PIPE/events?max_results=250${tok:+&page_token=$tok}" --profile "$PROFILE" -o json > "$W/page.json"
    yq -o json -i ". + load(\"$W/page.json\").events" "$W/all.json"
    tok=$(yq -r '.next_page_token // ""' "$W/page.json")
    [ -n "$tok" ] || break
    [[ "$(jq -r '.events[-1].timestamp // ""' "$W/page.json")" < "$cutoff" ]] && break   # newest first: past the window
  done
  jq -r --arg c "$cutoff" --arg u "$UPD" '.[] | select(.timestamp >= $c)
    | select($u == "" or ((.origin.update_id // "") | startswith($u)))
    | [.timestamp, .event_type, .level, (.origin.update_id // ""), (.origin.flow_name // .origin.dataset_name // ""),
       ((.message // "") | gsub("[\t\n]+"; " ")), (.details // {} | tojson | @base64)] | @tsv' "$W/all.json" | sort > "$W/events.tsv"
}

sql() {   # sql <statement>: rows as TSV; empty on failure, the error in $W/sql.err
  databricks api post /api/2.0/sql/statements --profile "$PROFILE" -o json --json "$(jq -n --arg w "$WH" --arg s "$1" \
    '{warehouse_id: $w, statement: $s, wait_timeout: "50s", disposition: "INLINE", format: "JSON_ARRAY"}')" > "$W/sql.json"
  jq -r '.status.error.message // ""' "$W/sql.json" > "$W/sql.err"
  [ "$(jq -r .status.state "$W/sql.json")" = SUCCEEDED ] || return 1
  jq -r '.result.data_array[]? | map(. // "") | @tsv' "$W/sql.json"
}

from_table() {
  [ -n "$WH" ] || WH=$(databricks warehouses list --profile "$PROFILE" -o json |
    yq -r '([.[] | select(.state == "RUNNING")] + .)[0].id // ""')
  [ -n "$WH" ] || { echo "no SQL warehouse in this workspace" > "$W/sql.err"; return 1; }
  sql "SELECT date_format(event_time, \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"), event_type, level, update_id,
         coalesce(origin.flow_name, origin.dataset_name, ''), regexp_replace(coalesce(message, ''), '[\\t\\n]+', ' '),
         regexp_replace(base64(to_json(details)), '[\\r\\n]', '')   -- Spark wraps base64 lines
       FROM $TABLE WHERE pipeline_id = '$PIPE' AND event_time > current_timestamp() - INTERVAL $SINCE HOURS
         ${UPD:+AND update_id LIKE '$UPD%'} ORDER BY event_time" > "$W/events.tsv"
}

newest_api() { databricks api get "/api/2.0/pipelines/$PIPE/events?max_results=1" --profile "$PROFILE" -o json | yq -r '.events[0].timestamp // ""'; }

case $SRC in
  api) from_api; used=api ;;
  table) from_table || { note "can't read $TABLE: $(tr "\n" " " < "$W/sql.err" | cut -c1-300)"; exit 1; }; used=table ;;
  auto)
    if from_table; then
      api_last=$(newest_api); tbl_last=$(sort "$W/events.tsv" | tail -1 | cut -f1)
      if [ -n "$api_last" ] && [[ "$tbl_last" < "$api_last" ]]; then
        note "the table ends at ${tbl_last:-nothing}, the API at $api_last: using the API (INFO, WARN, ERROR only)"
        from_api; used=api
      else used=table; fi
    else
      note "can't read $TABLE ($(tr "\n" " " < "$W/sql.err" | cut -c1-160)): using the API (INFO, WARN, ERROR only)"
      from_api; used=api
    fi ;;
  *) note "--source is table, api or auto"; exit 2 ;;
esac
if [ -n "$TYPES" ]; then
  awk -F'\t' -v t=",$TYPES," 'index(t, "," $2 ",")' "$W/events.tsv" > "$W/f.tsv" && mv "$W/f.tsv" "$W/events.tsv"
fi
n=$(wc -l < "$W/events.tsv" | tr -d ' ')
note "$n events from the $used, last $SINCE h${UPD:+, update $UPD}"

if [ "$SUMMARY" = 0 ]; then
  cut -f1-6 "$W/events.tsv"
  exit 0
fi
echo "Events by type and level ($used):"
cut -f2,3 "$W/events.tsv" | sort | uniq -c | sort -rn | awk '{ printf "  %6d  %-28s %s\n", $1, $2, $3 }'
echo "Warnings and errors:"
awk -F'\t' '$3 == "WARN" || $3 == "ERROR" || $2 ~ /^(gc_pressure|abnormal_termination|disk_space)$/ { print "  " $1 "  " $2 "  " $3 "  " substr($6, 1, 160) }' "$W/events.tsv" | tail -20
grep -q . <(awk -F'\t' '$3 == "WARN" || $3 == "ERROR" || $2 ~ /^(gc_pressure|abnormal_termination|disk_space)$/' "$W/events.tsv") || echo "  none"
echo "Outbound API calls (remote_api_usage):"
awk -F'\t' '$2 == "remote_api_usage" { print $7 }' "$W/events.tsv" > "$W/api.jsonl"
if [ -s "$W/api.jsonl" ]; then
  jq -Rr '@base64d' "$W/api.jsonl" > "$W/api.json" && mv "$W/api.json" "$W/api.jsonl"
  jq -rs 'map(.remote_api_usage // .) | group_by(.api_name // .endpoint // .source // "?")
    | map("  \(.[0].api_name // .[0].endpoint // .[0].source // "?"): \(length) events, \(map(.num_requests // .request_count // 1) | add) requests")[]' "$W/api.jsonl"
else
  echo "  none ($([ "$used" = api ] && echo "the API may not return them; try --source table" || echo "not a SaaS pipeline, or none in the window"))"
fi
echo "Newest cluster_resources:"
last=$(awk -F'\t' '$2 == "cluster_resources"' "$W/events.tsv" | tail -1)
if [ -n "$last" ]; then
  echo "  $(cut -f1 <<< "$last")  $(cut -f7 <<< "$last" | jq -Rr '@base64d' | jq -c '(.cluster_resources // .) | {autoscale: (.autoscale_info.state // null)} + (.task_slot_metrics // {}) | with_entries(select(.value != null))' | cut -c1-300)"
else
  echo "  none ($([ "$used" = api ] && echo "METRICS-level: only in the table" || echo "none in the window"))"
fi
echo "Updates (start, full refresh, last state):"
awk -F'\t' '$2 == "create_update" || $2 == "update_progress" { print $1 "\t" $2 "\t" $4 "\t" $7 }' "$W/events.tsv" |
  jq -Rr 'split("\t") | {t: .[0], type: .[1], u: .[2], d: (.[3] | @base64d | fromjson? // {})}' | jq -rs '
    group_by(.u) | map({u: .[0].u, start: (map(select(.type == "create_update")) | .[0].t // "?"),
      full_refresh: (map(select(.type == "create_update")) | .[0].d.create_update.full_refresh // false),
      state: (map(select(.type == "update_progress")) | sort_by(.t) | .[-1].d.update_progress.state // "?")})
    | sort_by(.start) | .[] | "  \(.start)  \(.u[0:8])  full_refresh=\(.full_refresh)  \(.state)"' | tail -10
echo "Rows per table (flow_progress: upserted, deleted, output; max backlog bytes):"
awk -F'\t' '$2 == "flow_progress" && $5 != "" { print $5 "\t" $7 }' "$W/events.tsv" |
  jq -Rr 'split("\t") | {f: .[0], m: ((.[1] | @base64d | fromjson? // {}) | .flow_progress.metrics // {})}' | jq -rs '
    group_by(.f) | map({f: .[0].f, up: (map(.m.num_upserted_rows // 0 | tonumber) | add),
      del: (map(.m.num_deleted_rows // 0 | tonumber) | add), out: (map(.m.num_output_rows // 0 | tonumber) | add),
      backlog: (map(.m.backlog_bytes // 0 | tonumber) | max)}) | map(select(.up + .del + .out + .backlog > 0))
    | if length == 0 then "  none with rows or backlog" else .[] | "  \(.f | split(".") | .[-1])  upserted=\(.up) deleted=\(.del) output=\(.out) max_backlog_bytes=\(.backlog)" end' | head -25
echo "Snapshots and CDC operations (operation_progress, latest per table):"
awk -F'\t' '$2 == "operation_progress" { print $1 "\t" $7 }' "$W/events.tsv" |
  jq -Rr 'split("\t") | {t: .[0], d: ((.[1] | @base64d | fromjson? // {}) | .operation_progress // {})}' | jq -rs '
    map(select(.d != {})) | map({t, s: .d.status, k: (.d | keys - ["status"] | .[0] // "?")} + {x: (.d | to_entries | map(select(.key != "status")) | .[0].value // {})})
    | group_by(.x.target_table_name // .k) | map(sort_by(.t) | .[-1])
    | if length == 0 then "  none" else .[] | "  \(.t)  \(.k) \(.x.target_table_name // "" | split(".") | .[-1])  \(.s)  \(.x.snapshot_reason // "")" end' | head -25
echo "Streaming batches (stream_progress: batches, avg and max duration ms):"
awk -F'\t' '$2 == "stream_progress" { print $7 }' "$W/events.tsv" |
  jq -Rr '@base64d | fromjson | .stream_progress.progress_json // empty | fromjson? | [.name // "?", (.batchDuration // 0)] | @tsv' |
  awk -F'\t' '{ n[$1]++; s[$1] += $2; if ($2 > m[$1]) m[$1] = $2 } END { if (!length(n)) print "  none"; for (k in n) { split(k, a, "."); printf "  %s  batches=%d avg_ms=%d max_ms=%d\n", a[length(a)], n[k], s[k] / n[k], m[k] } }' | sort | head -25
