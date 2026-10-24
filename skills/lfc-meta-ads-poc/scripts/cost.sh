#!/usr/bin/env bash
# cost.sh <profile> <pipeline-id>[,<pipeline-id>...] [--since YYYY-MM-DD] [--warehouse <id>]
# DBUs and list-price cost of the POC pipelines, from system.billing.usage joined to system.billing.list_prices
# (POC objective 3): per day and SKU, then totals split into the free-tier SKU (LAKEFLOW_CONNECT_FREE_USAGE, $0) and
# billed DBUs, and the average per day and 30 times it.
#   The free tier is a daily DBU allowance per workspace, shared by every managed connector
#   in it: the free share one pipeline gets depends on what else runs there. The last section shows the workspace's
#   free-tier DBUs per day and how many pipelines shared them; days that all end on the same round number are capped. The cost is list price; discounts and commitments are not applied.
#   Billing rows arrive hours after use: check the next day for a full day.
# --since defaults to 30 days ago. Needs SELECT on system.billing.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,10p "$0"; exit 2; }
PROFILE=$1 PIPES=$2; shift 2
SINCE="" WH=""
while [ $# -gt 0 ]; do
  case $1 in --since) SINCE=$2; shift ;; --warehouse) WH=$2; shift ;; *) sed -n 2,10p "$0"; exit 2 ;; esac
  shift
done
. "$(dirname "$0")/lib.sh"
IFS=, read -ra PL <<<"$PIPES"; for p in "${PL[@]}"; do uuid "$p"; done
[ -n "$SINCE" ] || SINCE=$(date -u -v-30d +%F 2>/dev/null || date -u -d '-30 days' +%F)
[[ $SINCE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "--since is YYYY-MM-DD" >&2; exit 2; }
ids="'$(IFS=,; echo "${PL[*]}" | sed "s/,/','/g")'"
base="WITH u AS (
  SELECT u.usage_date, u.usage_metadata.dlt_pipeline_id AS pipeline_id, u.sku_name, u.usage_quantity AS dbus,
         CASE WHEN u.sku_name LIKE '%FREE%' THEN 0 ELSE u.usage_quantity * coalesce(p.pricing.effective_list.default, p.pricing.default, 0) END AS usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.cloud = p.cloud AND u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
   AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_metadata.dlt_pipeline_id IN ($ids) AND u.usage_unit = 'DBU' AND u.usage_date >= DATE'$SINCE')"
echo "per day and SKU (DBU, list USD):"
printf '  %-10s  %-38s  %-45s  %10s  %9s\n' date pipeline sku dbus usd
sql "$base SELECT usage_date, pipeline_id, sku_name, round(sum(dbus), 3), round(sum(usd), 2) FROM u GROUP BY ALL ORDER BY 1, 2, 3" |
  while IFS=$'\t' read -r d p s q c; do printf '  %-10s  %-38s  %-45s  %10s  %9s\n' "$d" "$p" "$s" "$q" "$c"; done
echo
echo "totals since $SINCE:"
sql "$base SELECT count(DISTINCT usage_date), round(sum(dbus), 3), round(sum(CASE WHEN sku_name LIKE '%FREE%' THEN dbus ELSE 0 END), 3),
            round(sum(CASE WHEN sku_name LIKE '%FREE%' THEN 0 ELSE dbus END), 3), round(sum(usd), 2) FROM u" |
  while IFS=$'\t' read -r days all free billed usd; do
    [ "${days:-0}" -gt 0 ] || { echo "  no billing rows yet (they arrive hours after a run)"; break; }
    echo "  days=$days  dbus=$all  free_tier_dbus=$free  billed_dbus=$billed  list_usd=$usd"
    echo "  per day: dbus=$(jq -n "$all / $days * 1000 | round / 1000")  billed_usd=$(jq -n "$usd / $days * 100 | round / 100")  x30 billed_usd=$(jq -n "$usd / $days * 30 | round")"
  done
echo
WSID=$(dbx api get /api/2.1/unity-catalog/current-metastore-assignment -o json | jq -r .workspace_id)
[[ $WSID =~ ^[0-9]+$ ]] || { echo "can't read the workspace id"; exit 0; }
echo "workspace free tier use (all managed connectors, last 7 days): date, free DBUs, pipelines sharing them"
sql "SELECT usage_date, round(sum(usage_quantity), 2), count(DISTINCT usage_metadata.dlt_pipeline_id) FROM system.billing.usage
     WHERE sku_name LIKE '%LAKEFLOW_CONNECT_FREE%' AND usage_unit = 'DBU' AND workspace_id = '$WSID'
       AND usage_date >= current_date() - INTERVAL 7 DAYS GROUP BY 1 ORDER BY 1" | sed 's/^/  /;s/\t/  /g'
