#!/usr/bin/env bash
# report.sh <profile> <pipeline-id> [--update <id> | --days <n>] [--sla HH:MM] [--tz <zone>] [--tables]
# Freshness and reliability of a pipeline's updates (POC objectives 1 and 4), from the pipeline's own API:
#   one line per update: start and end (local to --tz, default America/New_York), duration, state, cause;
#   the error classes found in its ERROR and WARN events, counted ([META_ADS_RATE_LIMIT_EXCEEDED] and the like);
#   with --tables (on by default for --update): each table's last status and rows written in that update (the
#   flow_progress metric; "-" when the event has none, as the API's events often don't); --counts adds each table's
#   row count now, from a SQL warehouse (--warehouse, or the first running one).
# Then the SLA per day: met when an update COMPLETED that day at or before --sla (default 08:00), with the time of the
# last such update; MISSED otherwise (a day with updates but none completed by then). Then updates by state, and error
# classes with counts.
# --days defaults to 7. Events come from /api/2.0/pipelines/<id>/events (INFO, WARN, ERROR; the API keeps 30 days or so
# of history on a busy pipeline); updates from /api/2.0/pipelines/<id>/updates.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,13p "$0"; exit 2; }
PROFILE=$1 PIPE=$2; shift 2
UPD="" DAYS=7 SLA=08:00 TZONE=America/New_York TABLES=0 COUNTS=0 WH=""
while [ $# -gt 0 ]; do
  case $1 in
    --update) UPD=$2; TABLES=1; shift ;; --days) DAYS=$2; shift ;; --sla) SLA=$2; shift ;; --tz) TZONE=$2; shift ;;
    --tables) TABLES=1 ;; --counts) TABLES=1 COUNTS=1 ;; --warehouse) WH=$2; shift ;;
    *) sed -n 2,13p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
uuid "$PIPE"
[[ $DAYS =~ ^[0-9]+$ && $SLA =~ ^[0-9]{2}:[0-9]{2}$ ]] || { sed -n 2,13p "$0"; exit 2; }
W=$(mktemp -d); trap 'rm -rf "${W:?}"' EXIT

# updates, newest first, back to the window (or to the one asked for)
cutoff=$(date -u -v-"${DAYS}"d +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d "-$DAYS days" +%Y-%m-%dT%H:%M:%S)
echo '[]' > "$W/upd.json"; tok=""
for _ in $(seq 1 50); do
  dbx api get "/api/2.0/pipelines/$PIPE/updates?max_results=100${tok:+&page_token=$tok}" -o json > "$W/p.json"
  jq -s '.[0] + (.[1].updates // [])' "$W/upd.json" "$W/p.json" > "$W/u2.json" && mv "$W/u2.json" "$W/upd.json"
  tok=$(jq -r '.next_page_token // ""' "$W/p.json")
  [ -n "$tok" ] || break
  if [ -n "$UPD" ]; then jq -e --arg u "$UPD" 'any(.[]; .update_id == $u)' "$W/upd.json" >/dev/null && break
  else [ "$(jq -r '.[-1].creation_time // 0' "$W/upd.json")" -lt "$(( $(date -u -j -f %Y-%m-%dT%H:%M:%S "$cutoff" +%s 2>/dev/null || date -u -d "$cutoff" +%s) * 1000 ))" ] && break; fi
done
if [ -n "$UPD" ]; then
  jq --arg u "$UPD" '[.[] | select(.update_id == $u)]' "$W/upd.json" > "$W/sel.json"
  [ "$(jq length "$W/sel.json")" = 1 ] || { echo "no update $UPD on pipeline $PIPE" >&2; exit 1; }
  since_ms=$(jq '.[0].creation_time' "$W/sel.json")
else
  ms=$(( $(date -u -j -f %Y-%m-%dT%H:%M:%S "$cutoff" +%s 2>/dev/null || date -u -d "$cutoff" +%s) * 1000 ))
  jq --argjson m "$ms" '[.[] | select(.creation_time >= $m)]' "$W/upd.json" > "$W/sel.json"
  since_ms=$ms
fi
[ "$(jq length "$W/sel.json")" -gt 0 ] || { echo "no updates in the last $DAYS days" >&2; exit 0; }
since=$(jq -rn --argjson m "$since_ms" '$m / 1000 | todate')

# events, newest first, back to the first selected update's start
echo '[]' > "$W/ev.json"; tok=""
for _ in $(seq 1 200); do
  dbx api get "/api/2.0/pipelines/$PIPE/events?max_results=250${tok:+&page_token=$tok}" -o json > "$W/p.json"
  jq -s '.[0] + (.[1].events // [])' "$W/ev.json" "$W/p.json" > "$W/e2.json" && mv "$W/e2.json" "$W/ev.json"
  tok=$(jq -r '.next_page_token // ""' "$W/p.json")
  [ -n "$tok" ] || break
  [[ "$(jq -r '.events[-1].timestamp // ""' "$W/p.json")" < "$since" ]] && break
done

# per update: end = the terminal update_progress event; errors = [CLASS] tokens in ERROR/WARN messages and exceptions
jq --slurpfile ev "$W/ev.json" '
  def classes: [scan("\\[([A-Z][A-Z0-9_]+(?:\\.[A-Z0-9_]+)*)\\]")[0]];
  map(. as $u | ($ev[0] | map(select(.origin.update_id == $u.update_id))) as $e | $u + {
    end: ([$e[] | select(.event_type == "update_progress"
            and (.details.update_progress.state | IN("COMPLETED","FAILED","CANCELED"))) | .timestamp] | max),
    errors: ([$e[] | select(.level == "ERROR" or .level == "WARN")
              | ((.message // "") + " " + ([.error.exceptions[]?.message // ""] | join(" "))) | classes[]]
             | group_by(.) | map({(.[0]): length}) | add // {}),
    tables: ([$e[] | select(.event_type == "flow_progress") | {flow: .origin.flow_name, ts: .timestamp,
              status: .details.flow_progress.status,
              rows: (.details.flow_progress.metrics.num_output_rows // null)}]
             | group_by(.flow) | map({flow: .[0].flow, status: (max_by(.ts).status),
                 rows: ([.[].rows | select(. != null)] | if length == 0 then null else add end)}))
  })' "$W/sel.json" > "$W/rep.json"

loc() { [ -z "$1" ] || [ "$1" = null ] && { echo "-"; return; }
  TZ=$TZONE date -j -f %s "$(jq -rn --arg t "$1" '$t | sub("\\.[0-9]+Z$"; "Z") | fromdate')" "+%F %R" 2>/dev/null ||
  TZ=$TZONE date -d "$1" "+%F %R"; }
echo "pipeline $PIPE, times in $TZONE, SLA $SLA"
printf '%-10s %-16s %-16s %8s %-10s %-22s %s\n' update start end minutes state cause errors
: > "$W/days.tsv"
jq -r '.[] | [.update_id[0:8], (.creation_time / 1000 | todate), (.end // ""), .state, (.cause // ""),
             (.errors | to_entries | map("\(.key)x\(.value)") | join(","))] | @tsv' "$W/rep.json" |
while IFS=$'\t' read -r id st en state cause errs; do
  s=$(loc "$st") e=$(loc "$en") mins="-"
  if [ -n "$en" ]; then
    mins=$(( ($(jq -rn --arg t "$en" '$t | sub("\\.[0-9]+Z$"; "Z") | fromdate') - $(jq -rn --arg t "$st" '$t | fromdate')) / 60 ))
  fi
  printf '%-10s %-16s %-16s %8s %-10s %-22s %s\n' "$id" "$s" "$e" "$mins" "$state" "$cause" "$errs"
  ok=0; [ "$state" = COMPLETED ] && [ "$e" != - ] && [[ ! ${e:11:5} > $SLA ]] && ok=1
  printf '%s\t%s\t%s\n' "${s:0:10}" "$ok" "${e:11:5}" >> "$W/days.tsv"
done
echo; echo "SLA $SLA per day:"
sort "$W/days.tsv" | awk -F'\t' '{d[$1]=d[$1]; if ($2 == 1 && $3 > t[$1]) t[$1] = $3} END {for (k in d) print k}' |
  sort | while read -r d; do
    t=$(awk -F'\t' -v d="$d" '$1 == d && $2 == 1 {print $3}' "$W/days.tsv" | sort | tail -1)
    if [ -n "$t" ]; then echo "  $d  met (completed at $t)"; else echo "  $d  MISSED (no update completed by $SLA)"; fi
  done
if [ $TABLES = 1 ]; then
  echo; echo "tables (last status, rows written) per update:"
  jq -r '.[] | .update_id[0:8] as $u | .tables[] | [$u, .flow, .status, (.rows // "-")] | @tsv' "$W/rep.json" |
  while IFS=$'\t' read -r u f st r; do
    n=""; [ $COUNTS = 0 ] || n="  count=$(sql "SELECT count(*) FROM $(ident "$f")" 2>/dev/null || echo "?")"
    echo "  $u  $f  $st  rows=$r$n"
  done
fi
echo; echo "summary: $(jq -r 'group_by(.state) | map("\(.[0].state)=\(length)") | join(" ")' "$W/rep.json")"
echo "error classes: $(jq -r '[.[].errors | to_entries[]] | group_by(.key) | map("\(.[0].key)x\(map(.value) | add)") | join(" ") | if . == "" then "none" else . end' "$W/rep.json")"
