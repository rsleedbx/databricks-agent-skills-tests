#!/usr/bin/env bash
# pipeline.sh <profile> --connection <name> --account <act_id> --catalog <c> --schema <s> --name <pipeline>
#             [--tables t1,t2,...] [--scd2 t1,t2] [--custom-report <dest_table>=<options.json>]...
#             [--start-date YYYY-MM-DD] [--lookback <days>] [--dry-run]
# Creates (or updates, when a pipeline of that name exists) a Meta Ads ingestion pipeline and its destination schema.
#
#   --tables        source tables (default: the 8 documented objects). Objects, prebuilt reports and ad_insights mix freely.
#                   ad_accounts (every ad account the connection's Meta user can see) is read from source schema
#                   "default", not the ad account.
#   --scd2          tables to keep as SCD type 2 (history); the docs allow it for ads, ad_sets and campaigns only.
#   --custom-report one custom_report object: its destination table and a JSON file holding custom_report_options
#                   (level, breakdowns, action_breakdowns, time_increment, action_report_time,
#                   action_attribution_windows). Repeat for more reports. Examples: ../references/custom-report-*.json
#   --start-date    meta_ads_options.start_date for ad_insights, prebuilt and custom reports (default: connector's,
#                   36 months back; a shorter range makes the first run faster and lighter on Meta's rate limits)
#   --lookback      meta_ads_options.custom_insights_lookback_window in days (connector default 7)
#   --dry-run       print the pipeline JSON and stop
# Prints the pipeline id on stdout's last line and the pipeline and schema links.
set -euo pipefail
[ $# -ge 1 ] || { sed -n 2,18p "$0"; exit 2; }
PROFILE=$1; shift
CONN="" ACCT="" CAT="" SCH="" NAME="" START="" LOOK="" DRY=0 SCD2=""
TABLES="campaigns,ad_sets,ads,ad_images,ad_creatives,ad_videos,custom_audiences,custom_conversions"
CUSTOM=()
while [ $# -gt 0 ]; do
  case $1 in
    --connection) CONN=$2; shift ;; --account) ACCT=$2; shift ;; --catalog) CAT=$2; shift ;;
    --schema) SCH=$2; shift ;; --name) NAME=$2; shift ;; --tables) TABLES=$2; shift ;; --scd2) SCD2=$2; shift ;;
    --custom-report) CUSTOM+=("$2"); shift ;; --start-date) START=$2; shift ;; --lookback) LOOK=$2; shift ;;
    --dry-run) DRY=1 ;;
    *) sed -n 2,18p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
for v in CONN ACCT CAT SCH NAME; do [ -n "${!v}" ] || { echo "missing --$(tr A-Z a-z <<<"$v")" >&2; sed -n 2,4p "$0"; exit 2; }; done
[[ $ACCT =~ ^act_[0-9]+$ ]] || { echo "--account is the ad account id with its act_ prefix, e.g. act_1234567890" >&2; exit 2; }
[[ -z $START || $START =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "--start-date is YYYY-MM-DD" >&2; exit 2; }
[[ -z $LOOK || $LOOK =~ ^[0-9]+$ ]] || { echo "--lookback is a number of days" >&2; exit 2; }

# insights-style tables take meta_ads_options; object tables don't
is_insights() { [[ $1 == ad_insights || $1 == *_report ]]; }
opts() {   # meta_ads_options for an insights table, plus custom_report_options when $2 names a file
  jq -n --arg s "$START" --arg l "$LOOK" --argjson c "${2:-null}" '{}
    + (if $s != "" then {start_date: $s} else {} end)
    + (if $l != "" then {custom_insights_lookback_window: ($l | tonumber)} else {} end)
    + (if $c != null then {custom_report_options: $c} else {} end)'
}
objects='[]'
IFS=, read -ra TL <<<"$TABLES"
for t in "${TL[@]}"; do
  [[ $t =~ ^[a-z0-9_]+$ ]] || { echo "bad table name: $t" >&2; exit 2; }
  src=$ACCT; [ "$t" = ad_accounts ] && src=default   # the accounts the Meta user sees: source schema "default"
  o=$(jq -n --arg a "$src" --arg t "$t" --arg c "$CAT" --arg s "$SCH" \
    '{table: {source_schema: $a, source_table: $t, destination_catalog: $c, destination_schema: $s}}')
  if [[ ,$SCD2, == *,$t,* ]]; then o=$(jq '.table.table_configuration = {scd_type: "SCD_TYPE_2"}' <<<"$o"); fi
  if is_insights "$t"; then
    mo=$(opts "$t"); [ "$mo" = '{}' ] || o=$(jq --argjson m "$mo" '.table.connector_options = {meta_ads_options: $m}' <<<"$o")
  fi
  objects=$(jq --argjson o "$o" '. + [$o]' <<<"$objects")
done
for cr in "${CUSTOM[@]}"; do
  dest=${cr%%=*} file=${cr#*=}
  [[ $dest =~ ^[a-z0-9_]+$ && -f $file ]] || { echo "--custom-report wants <dest_table>=<file.json>: $cr" >&2; exit 2; }
  jq -e 'type == "object"' "$file" >/dev/null || { echo "$file: not a JSON object" >&2; exit 2; }
  bset=$(jq -r '(.breakdowns // []) | sort | if length == 0 then "-" else join(",") end' "$file")
  grep -qx -e "$bset	.*" "$(dirname "$0")/../references/meta-breakdowns.tsv" ||
    echo "warning: $dest: breakdowns $bset are not a combination Meta lists (references/meta-breakdowns.tsv);" \
      "Meta rejects unsupported ones and the table fails with META_ADS_SOURCE_API_ERROR. Try it with probe.sh first." >&2
  o=$(jq -n --arg a "$ACCT" --arg c "$CAT" --arg s "$SCH" --arg d "$dest" --argjson m "$(opts custom_report "$(cat "$file")")" \
    '{table: {source_schema: $a, source_table: "custom_report", destination_catalog: $c, destination_schema: $s,
              destination_table: $d, connector_options: {meta_ads_options: $m}}}')
  objects=$(jq --argjson o "$o" '. + [$o]' <<<"$objects")
done
n=$(jq length <<<"$objects")
[ "$n" -le 10 ] || echo "note: $n objects in one pipeline; for Meta Ads keep fewer (rate limits), split into more pipelines" >&2
spec=$(jq -n --arg n "$NAME" --arg c "$CAT" --arg s "$SCH" --arg conn "$CONN" --argjson o "$objects" \
  '{name: $n, catalog: $c, schema: $s, ingestion_definition: {connection_name: $conn, objects: $o}}')
if [ $DRY = 1 ]; then echo "$spec"; exit 0; fi

dbx schemas get "$CAT.$SCH" -o json >/dev/null 2>&1 || dbx schemas create "$SCH" "$CAT" -o json >/dev/null
link schema "/explore/data/$CAT/$SCH"
id=$(dbx pipelines list-pipelines --filter "name LIKE '$NAME'" -o json | jq -r --arg n "$NAME" '[.[] | select(.name == $n)][0].pipeline_id // ""')
if [ -n "$id" ]; then
  dbx pipelines update "$id" --json "$(jq --arg id "$id" '. + {id: $id}' <<<"$spec")" >/dev/null
  echo "updated pipeline $NAME ($n objects)" >&2
else
  id=$(dbx pipelines create --json "$spec" -o json | jq -r .pipeline_id)
  echo "created pipeline $NAME ($n objects)" >&2
fi
link pipeline "/pipelines/$id"
echo "$id"
