#!/usr/bin/env bash
# check.sh <profile> [--table <catalog.schema.table>] [--sp <application id>] [--secret-file F] [--interfaces grpc,kafka,mqtt]
#          [--from "<command prefix>"]
#   Can a producer write to <table> through Zerobus in this workspace? Read-only. One line per requirement: PASS / FAIL /
#   WARN, and for each FAIL the command that fixes it (enable.sh, grant.sh, or the CLI). Exit code 1 if anything FAILs.
#   --sp           the service principal the producer authenticates as (its application ID)
#   --secret-file  a 0600 file with its OAuth secret: then a table-scoped token is minted, as the producer would
#   --interfaces   which Zerobus interfaces the producer uses (default grpc; grpc also covers the SDKs and REST)
#   --from         where the producer runs, for the network check: a prefix that runs a bash command there, for example
#                  "ssh azureuser@20.1.2.3" or "limactl shell <instance> --" (default: this machine)
set -uo pipefail
P=$1; shift; TABLE=; SP=; SECRET_FILE=; IFACES=grpc; FROM=
while [ $# -gt 0 ]; do case $1 in --table) TABLE=$2; shift 2;; --sp) SP=$2; shift 2;; --secret-file) SECRET_FILE=$2; shift 2;;
  --interfaces) IFACES=$2; shift 2;; --from) FROM=$2; shift 2;; *) echo "unknown $1"; exit 2;; esac; done
[ -n "$TABLE" ] || [ -z "$SP" ] || { echo "--sp needs --table <catalog.schema.table>"; exit 2; }
IFS=. read -r CAT SCH TBL <<< "$TABLE"
fails=0
row() { printf "%-5s %-52s %s\n" "$1" "$2" "$3"; [ "$1" = FAIL ] && fails=$((fails + 1)); [ -n "${4:-}" ] && printf "      fix: %s\n" "$4"; return 0; }
has() { case ",$IFACES," in *,$1,*) return 0;; esac; return 1; }
setting() { # the value of a workspace setting: set here (boolean_val) or inherited (effective_boolean_val)
  databricks workspace-settings-v2 get-public-workspace-setting "$1" --profile "$P" -o json 2>/dev/null \
    | jq -r 'if has("boolean_val") then .boolean_val.value else .effective_boolean_val.value end'; }
SCRIPTS=$(cd "$(dirname "$0")" && pwd)

echo "== Zerobus into ${TABLE:-(no table: previews and network only)} in $P (interfaces: $IFACES${SP:+, as $SP})"
# --- workspace: Unity Catalog, the endpoint, the previews
eval "$(databricks metastores current --profile "$P" -o json 2>/dev/null | yq -p json -o shell '{"WSID": .workspace_id, "MS": .metastore_id}')"
eval "$(databricks metastores summary --profile "$P" -o json 2>/dev/null | yq -p json -o shell '{"REGION": .region, "CLOUD": .cloud}')"
[ -n "${MS:-}" ] && row PASS "Unity Catalog metastore assigned" "$CLOUD $REGION" || { row FAIL "Unity Catalog metastore assigned" "none" "assign the workspace to a metastore (account admin)"; exit 1; }
case $CLOUD in aws) D=cloud.databricks.com;; azure) D=azuredatabricks.net;; gcp) D=gcp.databricks.com;; *) D=; esac
ZB=$WSID.zerobus.$REGION.$D; HOST=$(databricks auth describe --profile "$P" -o json 2>/dev/null | jq -r '.details.host // ""')
row PASS "Zerobus endpoint" "$ZB"
want="zerobus_ingest_core"; has kafka && want="$want zerobus_kafka_endpoint"; has mqtt && want="$want zerobus_mqtt_endpoint"
for s in $want; do v=$(setting "$s")
  [ "$v" = true ] && row PASS "preview $s" "on" || row FAIL "preview $s" "${v:-not found}" "bash $SCRIPTS/enable.sh $P $s --apply   (workspace admin)"; done
v=$(setting zerobus_rescue_column_json); [ "$v" = true ] && row PASS "preview zerobus_rescue_column_json" "on" \
  || row WARN "preview zerobus_rescue_column_json" "${v:-not found}: no rescue column for JSON records" "bash $SCRIPTS/enable.sh $P zerobus_rescue_column_json --apply, if records may not match the table"

# --- the table: a managed Delta table; on Default Storage it needs the default-storage preview
[ -n "$TABLE" ] && databricks tables get "$TABLE" --profile "$P" -o json > /tmp/zbcheck.$$ 2>/dev/null
if [ -z "$TABLE" ]; then :
elif [ -s /tmp/zbcheck.$$ ]; then
  eval "$(yq -p json -o shell '{"TTYPE": .table_type, "TFMT": (.data_source_format // ""), "TLOC": (.storage_location // ""), "TOWNER": .owner}' /tmp/zbcheck.$$)"
  { [ "$TTYPE" = MANAGED ] || [ "$TTYPE" = STREAMING_TABLE ]; } && [ "${TFMT:-DELTA}" = DELTA ] && row PASS "table is managed Delta" "$TTYPE, owner $TOWNER" \
    || row FAIL "table is managed Delta" "$TTYPE $TFMT" "create it as a managed table (no LOCATION), or use another"
  ds=$(databricks external-locations get __databricks_managed_storage_location --profile "$P" -o json 2>/dev/null | jq -r '.url // ""')
  if [ -n "$ds" ] && [ "${TLOC#"$ds"}" != "$TLOC" ]; then v=$(setting zerobus_default_storage_write)
    [ "$v" = true ] && row PASS "Default Storage table: zerobus_default_storage_write" "on" \
      || row FAIL "Default Storage table: zerobus_default_storage_write" "${v:-not found}" "bash $SCRIPTS/enable.sh $P zerobus_default_storage_write --apply"
  else row PASS "table storage" "the catalog's own managed storage"; fi
else row FAIL "table exists" "$TABLE not found or not visible" "create it first: Zerobus doesn't create tables"; fi
rm -f /tmp/zbcheck.$$

# --- the service principal: exists, granted directly on the table (inherited grants may not satisfy the token), token
if [ -n "$SP" ]; then
  n=$(databricks service-principals list --filter "applicationId eq \"$SP\"" --profile "$P" -o json 2>/dev/null | jq -r '.[0].displayName // ""')
  [ -n "$n" ] && row PASS "service principal in the workspace" "$n" || row FAIL "service principal in the workspace" "$SP not found" "add it to the workspace (an admin), or create one"
  direct() { databricks grants get "$1" "$2" --principal "$SP" --profile "$P" -o json 2>/dev/null | yq -p json -o tsv '[.privilege_assignments[]?.privileges[]?] | unique | join(" ")'; }
  for need in "catalog $CAT USE_CATALOG" "schema $CAT.$SCH USE_SCHEMA" "table $TABLE SELECT" "table $TABLE MODIFY"; do set -- $need
    case " $(direct "$1" "$2") " in *" $3 "*|*" ALL_PRIVILEGES "*) row PASS "$3 on $1 $2 (direct)" "";;
      *) row FAIL "$3 on $1 $2 (direct)" "no" "bash $SCRIPTS/grant.sh $P --table $TABLE --sp $SP --apply";; esac; done
  if [ -n "$SECRET_FILE" ]; then
    AD=$(printf '[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"%s"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"%s.%s"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"%s"}]' "$CAT" "$CAT" "$SCH" "$TABLE")
    r=$(curl -s -m 20 -X POST -K <(printf 'user = "%s:%s"\n' "$SP" "$(cat "$SECRET_FILE")") -d grant_type=client_credentials -d scope=all-apis \
        -d "resource=api://databricks/workspaces/$WSID/zerobusDirectWriteApi" --data-urlencode "authorization_details=$AD" "$HOST/oidc/v1/token")
    t=$(printf '%s' "$r" | jq -r '.access_token // ""' 2>/dev/null)
    [ -n "$t" ] && [ "$t" != null ] && row PASS "table-scoped token for $SP" "minted (expires in $(printf '%s' "$r" | jq -r '.expires_in')s)" \
      || row FAIL "table-scoped token for $SP" "$(printf '%s' "$r" | jq -r '(.error // "") + ": " + (.error_description // "")' 2>/dev/null | cut -c1-160)" "check the secret, then the grants above"
  else row WARN "table-scoped token" "no --secret-file: not minted"; fi
else row WARN "service principal" "no --sp: grants and token not checked"; fi

# --- network: TCP from where the producer runs, to the interfaces it uses (443 gRPC/REST, 9092 Kafka, 8883 MQTT)
ports=; has grpc && ports="$ports 443"; has kafka && ports="$ports 9092"; has mqtt && ports="$ports 8883"
for port in $ports; do
  # nc where there is one: macOS bash's /dev/tcp hung on this endpoint while nc connected (2026-10-21)
  cmd="if command -v nc >/dev/null; then nc -z -w 5 $ZB $port >/dev/null 2>&1; else timeout 8 bash -c 'exec 3<>/dev/tcp/$ZB/$port' 2>/dev/null; fi && echo open || echo closed"
  # ssh joins its arguments into one string for the remote shell, so the command goes quoted; limactl shell (and any
  # other prefix) passes them as they are
  st=$(case "$FROM" in ("") bash -c "$cmd";; (ssh\ *) $FROM bash -c "\"$cmd\"" 2>/dev/null;; (*) $FROM bash -c "$cmd" 2>/dev/null;; esac)
  [ "$st" = open ] && row PASS "TCP $port to the endpoint${FROM:+ from $FROM}" "open" \
    || row FAIL "TCP $port to the endpoint${FROM:+ from $FROM}" "${st:-no answer}" "open outbound $port to $ZB (firewall, proxy, VPN)"; done

echo; [ $fails -eq 0 ] && echo "ready: nothing failed" || echo "$fails requirement(s) failed"; exit $((fails > 0))
