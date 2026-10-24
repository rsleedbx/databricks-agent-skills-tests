#!/usr/bin/env bash
# row-parity.sh <profile> --fivetran <cat.schema> --lfc <cat.schema> --from YYYY-MM-DD --to YYYY-MM-DD
#               [--report <ft_table>[:<date_col>]=<lfc_table>[:<date_col>]]... [--object <ft_table>=<lfc_table>]...
#               [--metrics spend,impressions] [--target 99] [--warehouse <id>]
# Rows and metrics per day on the Lakeflow Connect landing and an existing Fivetran landing, mapped table to table,
# from SQL on both (for a POC row-completeness target, --target).
#   --report  a report table pair, compared per day over [--from, --to]: rows, and the sum of each --metrics column
#             (default spend,impressions) on both sides, then LFC as a percentage of Fivetran. Date columns default
#             to date (Fivetran) and date_start (LFC). Pair a Fivetran custom report with the LFC custom_report or
#             prebuilt report built with the same level, breakdowns, action breakdowns and attribution windows;
#             otherwise the numbers differ by design.
#   --object  an object table pair (ad_history=ads, campaign_history=campaigns, ...): distinct ids on both sides.
#             Fivetran keeps one row per version, LFC one per object (SCD type 1) or per version (SCD type 2), so
#             distinct id is the comparable count.
# A pair is OK when every percentage is within --target (default 99) and 100 + (100 - target); rows for recent days can
# still move inside the attribution window (lookback, 7 days by default) on both sides: compare closed days.
# Exit 1 when any pair is under target.
set -euo pipefail
[ $# -ge 1 ] || { sed -n 2,17p "$0"; exit 2; }
PROFILE=$1; shift
FT="" LFC="" FROM="" TO="" METRICS=spend,impressions TARGET=99 WH="" REPORTS=() OBJECTS=()
while [ $# -gt 0 ]; do
  case $1 in
    --fivetran) FT=$2; shift ;; --lfc) LFC=$2; shift ;; --from) FROM=$2; shift ;; --to) TO=$2; shift ;;
    --report) REPORTS+=("$2"); shift ;; --object) OBJECTS+=("$2"); shift ;; --metrics) METRICS=$2; shift ;;
    --target) TARGET=$2; shift ;; --warehouse) WH=$2; shift ;;
    *) sed -n 2,17p "$0"; exit 2 ;;
  esac
  shift
done
. "$(dirname "$0")/lib.sh"
[[ $FROM =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && $TO =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "--from and --to are YYYY-MM-DD" >&2; exit 2; }
[[ $METRICS =~ ^[a-z0-9_,]+$ && $TARGET =~ ^[0-9]+(\.[0-9]+)?$ ]] || { sed -n 2,17p "$0"; exit 2; }
[ ${#REPORTS[@]} -gt 0 ] || [ ${#OBJECTS[@]} -gt 0 ] || { echo "nothing to compare: give --report or --object" >&2; exit 2; }
ftq=$(ident "$FT") lfq=$(ident "$LFC")
low=$TARGET high=$(jq -n "200 - $TARGET")
bad=0
pct() { jq -rn --arg a "$1" --arg b "$2" 'if ($a|tonumber? // 0) == 0 then (if ($b|tonumber? // 0) == 0 then "100" else "-" end)
  else (($b|tonumber) / ($a|tonumber) * 1000 | round / 10 | tostring) end'; }
ok() { [ "$1" = - ] && return 1; jq -en --argjson p "$1" --argjson l "$low" --argjson h "$high" '$p >= $l and $p <= $h' >/dev/null; }

for pr in "${REPORTS[@]}"; do
  l=${pr%%=*} r=${pr#*=}
  ft_t=${l%%:*} ft_d=date; [[ $l == *:* ]] && ft_d=${l#*:}
  lf_t=${r%%:*} lf_d=date_start; [[ $r == *:* ]] && lf_d=${r#*:}
  IFS=, read -ra M <<<"$METRICS"
  sums="" pick=""
  for i in "${!M[@]}"; do
    sums+=", sum(CAST($(ident "${M[$i]}") AS DOUBLE)) AS m$i"; pick+=", coalesce(f.m$i, 0), coalesce(l.m$i, 0)"
  done
  echo "report $ft_t ($ft_d) -> $lf_t ($lf_d), $FROM to $TO: day, rows ft/lfc %, then ${METRICS//,/, } ft/lfc %"
  q="WITH f AS (SELECT CAST($(ident "$ft_d") AS DATE) AS d, count(*) AS n $sums FROM $ftq.$(ident "$ft_t")
                WHERE CAST($(ident "$ft_d") AS DATE) BETWEEN DATE'$FROM' AND DATE'$TO' GROUP BY 1),
          l AS (SELECT CAST($(ident "$lf_d") AS DATE) AS d, count(*) AS n $sums FROM $lfq.$(ident "$lf_t")
                WHERE CAST($(ident "$lf_d") AS DATE) BETWEEN DATE'$FROM' AND DATE'$TO' GROUP BY 1)
     SELECT coalesce(f.d, l.d), coalesce(f.n, 0), coalesce(l.n, 0) $pick
     FROM f FULL OUTER JOIN l ON f.d = l.d ORDER BY 1"
  out=$(sql "$q") || { bad=1; continue; }
  [ -n "$out" ] || { echo "  no rows on either side in that range"; continue; }
  pair_ok=1
  while IFS=$'\t' read -r -a c; do
    line="  ${c[0]}  rows ${c[1]}/${c[2]} $(pct "${c[1]}" "${c[2]}")%"
    ok "$(pct "${c[1]}" "${c[2]}")" || pair_ok=0
    for ((i = 3; i < ${#c[@]}; i += 2)); do
      p=$(pct "${c[$i]}" "${c[$((i+1))]}"); line+="  ${c[$i]}/${c[$((i+1))]} $p%"; ok "$p" || pair_ok=0
    done
    echo "$line"
  done <<<"$out"
  [ $pair_ok = 1 ] && echo "  OK (all within $low-$high %)" || { echo "  UNDER TARGET"; bad=1; }
done

for pr in "${OBJECTS[@]}"; do
  ft_t=${pr%%=*} lf_t=${pr#*=}
  out=$(sql "SELECT (SELECT count(DISTINCT id) FROM $ftq.$(ident "$ft_t")), (SELECT count(DISTINCT id) FROM $lfq.$(ident "$lf_t"))") || { bad=1; continue; }
  IFS=$'\t' read -r a b <<<"$out"; p=$(pct "$a" "$b")
  if ok "$p"; then s=OK; else s="UNDER TARGET"; bad=1; fi
  echo "object $ft_t -> $lf_t: distinct ids $a/$b $p%  $s"
done
exit $bad
