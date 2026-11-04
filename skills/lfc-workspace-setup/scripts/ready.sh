#!/usr/bin/env bash
# ready.sh [<profile>] [--catalog C] [--principal P] [--egress-file FILE] [--no-enable]: is this workspace ready for a Lakeflow Connect demo?
#   Checks, in the order a demo needs them: the profile logs in; the workspace's cloud and region (where the sources
#   should be); serverless and classic compute; the connector previews; a target catalog that works for CDC; access for
#   you (and --principal, e.g. "account users") through check.sh; the serverless egress ranges the source firewall must
#   allow (with --egress-file, saved, and on a later run, what changed); the Lakeflow Connect connections already here; a SQL warehouse for remote_query. One line each, PASS / FAIL /
#   WARN / INFO / ASK, with the next command for anything to fix; ASK means stop and ask the user (the target
#   catalog isn't writable, or there is none). Exit 1 if anything FAILs or ASKs.
#   The one change it makes: a connector preview that's off is turned on (as a workspace admin), with a line saying so
#   first. --no-enable makes it read-only. Turn one off again with the same patch and "value": false.
#   Without <profile> or --catalog, the saved test settings give them (lfc-test-settings: DATABRICKS_PROFILE, LFC_CATALOG).
set -uo pipefail
D=$(cd "$(dirname "$0")" && pwd)
ST=$D/../../lfc-test-settings/scripts/settings.sh   # the answers asked once (lfc-test-settings)
P=; case ${1:-} in ''|--*) ;; *) P=$1; shift;; esac; CAT=; WHO=; EGRESS=
ENABLE=yes
while [ $# -gt 0 ]; do case $1 in --catalog) CAT=$2; shift 2;; --principal) WHO=$2; shift 2;; --egress-file) EGRESS=$2; shift 2;; --no-enable) ENABLE=no; shift;; *) echo "unknown $1"; exit 2;; esac; done
[ -n "$P" ] || P=$(bash "$ST" get DATABRICKS_PROFILE 2>/dev/null) || { echo "ready.sh: no <profile>, and no DATABRICKS_PROFILE saved: bash $ST setup"; exit 2; }
[ -n "$CAT" ] || CAT=$(bash "$ST" get LFC_CATALOG 2>/dev/null) || CAT=
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fails=0
row() { printf "%-5s %-40s %s\n" "$1" "$2" "$3" | tee -a $T/rows; case $1 in FAIL|ASK) fails=$((fails + 1));; esac; [ -n "${4:-}" ] && printf "      next: %s\n" "$4"; return 0; }
j() { databricks "$@" --profile $P -o json 2> $T/err; }

echo "== $P"
# 1. login
if j current-user me > $T/me.json; then ME=$(yq -p json -o tsv '.userName' $T/me.json); row PASS "1 profile logs in" "as $ME"
else row FAIL "1 profile logs in" "$(head -c 160 $T/err | tr '\n' ' ')" "databricks auth login --profile $P"; echo; echo "stopped: nothing else can be checked"; exit 1; fi

# 2. cloud and region: put the sources there (no cross-cloud or inter-region egress)
j metastores summary > $T/ms.json || true
CLOUD=$(yq -p json -o tsv '(.global_metastore_id // "") | split(":") | .[0] // ""' $T/ms.json); REGION=$(yq -p json -o tsv '.region // ""' $T/ms.json)
[ -n "$CLOUD" ] && row PASS "2 Unity Catalog metastore; cloud, region" "$CLOUD $REGION" || row FAIL "2 Unity Catalog metastore" "none assigned" "assign a metastore (account admin)"

# 3. compute: serverless (warehouses, pipelines) and classic (gateways on classic, serverless: false)
j warehouses list > $T/wh.json || echo '[]' > $T/wh.json
NSL=$(yq -p json -o tsv '[.[] | select(.enable_serverless_compute == true)] | length' $T/wh.json)
[ "${NSL:-0}" -gt 0 ] && row PASS "3 serverless SQL warehouses" "$NSL" || row WARN "3 serverless SQL warehouses" "none" "create one (databricks-execution-compute), or check serverless is enabled for the workspace"
# serverless compute: the serverless_compute setting. Classic compute: clusters list-zones lists zones where classic
# clusters can run; a serverless-only workspace answers "No such workerEnvironment 'serverless-<workspace id>'" (AWS) or
# "does not have any associated worker environment" (Azure), and an Azure workspace with classic compute "Azure worker
# environment does not support zones". Any other answer (seen on GCP) doesn't tell: say so. (list-node-types answers in
# all of them, so it can't tell.)
sv=$(databricks api get /api/2.1/settings/serverless_compute --profile $P -o json 2>/dev/null | yq -p json -o tsv 'with(select(has("boolean_val")); .v = .boolean_val.value) | with(select(has("boolean_val") | not); .v = .effective_boolean_val.value) | .v')
[ "$sv" = true ] && row PASS "3 serverless compute" "on (serverless_compute setting)" || row FAIL "3 serverless compute" "${sv:-not readable}" "a workspace admin turns serverless compute on (workspace settings), or ask the account team"
if databricks clusters list-zones --profile $P -o json > $T/z.json 2> $T/zerr; then
  CLASSIC=yes; row INFO "3 classic compute" "available (zones $(yq -p json -o tsv '.zones | join(" ")' $T/z.json)): classic gateways possible; their egress IP must be in the source firewall"
elif grep -q "does not support zones" $T/zerr; then              # Azure: a classic worker environment, without zones
  CLASSIC=yes; row INFO "3 classic compute" "available (an Azure worker environment): classic gateways possible; their egress IP must be in the source firewall"
elif grep -qE "workerEnvironment 'serverless-|does not have any associated worker environment" $T/zerr; then   # AWS; Azure
  CLASSIC=no; row INFO "3 classic compute" "none: a serverless-only workspace (gateways and pipelines run serverless)"
else CLASSIC=unknown; row INFO "3 classic compute" "can't tell here ($(head -c 90 $T/zerr | tr '\n' ' ')...)"; fi
# classic needs more than the workspace: creating a cluster (a classic gateway or pipeline creates one) takes the
# allow-cluster-create entitlement (workspace admins have every entitlement), or a cluster policy you can use
if [ "$CLASSIC" = yes ]; then
  databricks api get /api/2.0/preview/scim/v2/Me --profile $P -o json > $T/me2.json 2>/dev/null
  ents=" $(yq -p json -o tsv '[.entitlements[]?.value] | join(" ")' $T/me2.json) $(yq -p json -o tsv '.groups[]?.value' $T/me2.json | while read g; do
         databricks api get "/api/2.0/preview/scim/v2/Groups/$g?attributes=entitlements" --profile $P -o json 2>/dev/null | yq -p json -o tsv '[.entitlements[]?.value] | join(" ")'; done | tr '\n' ' ') "
  npol=$(databricks cluster-policies list --profile $P -o json 2>/dev/null | yq -p json -o tsv 'length')
  if yq -p json -e '.groups[]? | select(.display == "admins")' $T/me2.json > /dev/null 2>&1; then row PASS "3 classic: you can create clusters" "as a workspace admin"
  elif case "$ents" in *" allow-cluster-create "*) true;; *) false;; esac; then row PASS "3 classic: you can create clusters" "allow-cluster-create entitlement"
  elif [ "${npol:-0}" -gt 0 ]; then row WARN "3 classic: you can create clusters" "no allow-cluster-create; $npol cluster policies visible, so only within a policy" "set the classic gateway's cluster policy_id to one you can use, or use serverless: true"
  else CLASSIC=noperm; row WARN "3 classic: you can create clusters" "no allow-cluster-create and no cluster policy you can use" "use serverless: true, or ask a workspace admin for allow-cluster-create or a policy"; fi
fi
# what each mode runs on: a standard CDC gateway runs on classic compute by default (some sources' gateways can run
# serverless); integrated CDC and query-based pipelines run serverless by default (some sources can run them on classic)
# leave serverless unset in pipelines and gateways, read the choice back from the update (pipelines get-update:
# .update.config.serverless), and on a compute error try the opposite: what this workspace leads to expect
case $CLASSIC in
  no)     row INFO "3 modes on this workspace" "leave serverless unset; a gateway left unset is refused here (\"You must use serverless compute\"): set serverless: true for it";;
  yes)    row INFO "3 modes on this workspace" "leave serverless unset: query-based and integrated CDC ran serverless, a gateway classic (needs its NAT IP in the source firewall); read back with pipelines get-update";;
  noperm) row INFO "3 modes on this workspace" "leave serverless unset; a gateway defaulting to classic needs cluster creation you don't have: if it fails, set serverless: true";;
  *)      row INFO "3 modes on this workspace" "leave serverless unset and read back with pipelines get-update; if the create or update fails on compute, try the opposite";;
esac

# 4. connector previews (Settings API): integrated CDC per engine, column selection. One that's off is turned on
#    (unless --no-enable), as a workspace admin; a setting the public API won't write stays a WARN
ADMIN=$(databricks api get /api/2.0/preview/scim/v2/Me --profile $P -o json 2>/dev/null | yq -p json -o tsv '[.groups[]?.display | select(. == "admins")] | length')
val() { databricks api get /api/2.1/settings/$1 --profile $P -o json 2>/dev/null \
        | yq -p json -o tsv 'with(select(has("boolean_val")); .v = .boolean_val.value) | with(select(has("boolean_val") | not); .v = .effective_boolean_val.value) | .v'; }
for s in integrated_cdc_sql_server_connector integrated_cdc_mysql_connector integrated_cdc_oracle_connector integrated_cdc_postgres_connector conn_cdc_col_select; do
  v=$(val $s); fix="databricks workspace-settings-v2 patch-public-workspace-setting $s --json '{\"name\": \"$s\", \"boolean_val\": {\"value\": true}}' --profile $P"
  if [ "$v" = true ]; then row PASS "4 preview $s" "on"; continue; fi
  if [ "$s" = integrated_cdc_postgres_connector ] && { [ -z "$v" ] || [ "$v" = null ]; }; then row INFO "4 preview $s" "no setting on this workspace (rolled out in phases; ask the account team)"; continue; fi
  if [ $ENABLE = no ]; then row WARN "4 preview $s" "${v:-not readable}" "$fix   (workspace admin; or rerun without --no-enable)"; continue; fi
  if [ "${ADMIN:-0}" -eq 0 ]; then row WARN "4 preview $s" "${v:-not readable}; not a workspace admin, so not enabled" "$fix   (a workspace admin)"; continue; fi
  echo "      >>> turning on preview $s in $P (it was ${v:-not readable}; --no-enable to skip)"
  if databricks workspace-settings-v2 patch-public-workspace-setting $s --json "{\"name\": \"$s\", \"boolean_val\": {\"value\": true}}" --profile $P -o json > /dev/null 2> $T/err && [ "$(val $s)" = true ]
  then row PASS "4 preview $s" "turned on now (was ${v:-not readable})"
  else row WARN "4 preview $s" "couldn't turn it on: $(head -c 120 $T/err | tr '\n' ' ')" "the workspace's Previews page, or the account team"; fi
done
row INFO "4 standard CDC (gateway) per engine" "not a setting: a refused gateway needs the account team"

# 5. the target: the workspace's default catalog (where target tables go unless told otherwise), schema "default",
#    unless --catalog names another. Writable means CREATE TABLE in the schema, or CREATE SCHEMA in the catalog. If it
#    isn't writable, ASK: stop and ask the user where the target tables should go. CDC also needs the catalog's own
#    managed storage, not Default Storage (query-based ingestion works on either).
DS=$(j external-locations get __databricks_managed_storage_location | yq -p json -o tsv '.url // ""' 2>/dev/null)
DEF=$(j settings default-namespace get | yq -p json -o tsv '.namespace.value // ""' 2>/dev/null)
CAT=${CAT:-$DEF}; SCH=default
# owning a securable gives every privilege on it, and the effective-privileges API doesn't report it: the owner can be
# you, a group you're in, account users, or the workspace's admins group (_workspace_admins_<name>_<workspace id>,
# owner of the catalog the workspace was created with) when you're a workspace admin
WSID=$(j metastores current | yq -p json -o tsv '.workspace_id // ""' 2>/dev/null)
MYG="|$(databricks api get /api/2.0/preview/scim/v2/Me --profile $P -o json 2>/dev/null | yq -p json -o tsv '[.groups[]?.display] | join("|")')|account users|"
mine() { [ -n "$1" ] && { [ "$1" = "$ME" ] || case "$MYG" in *"|$1|"*) true;; *) case "$1" in _workspace_admins_*_"$WSID") case "$MYG" in *"|admins|"*) true;; *) false;; esac;; *) false;; esac;; esac; }; }
eff() { databricks grants get-effective "$1" "$2" --principal "$ME" --profile $P -o json 2>/dev/null | yq -p json -o tsv '[.privilege_assignments[]?.privileges[]?.privilege] | unique | join(" ")'; }
inp() { case " $1 " in *" $2 "*|*" ALL_PRIVILEGES "*) return 0;; esac; return 1; }
# the catalogs $ME can write to (USE CATALOG and CREATE SCHEMA, or owner), numbered, with whether CDC can use them
choices() { echo "      writable targets here (pick one, then rerun with --catalog <name>):"; k=0
  j catalogs list | yq -p json -o tsv '.[] | select(.catalog_type == "MANAGED_CATALOG" and .name != "system" and .name != "hive_metastore") | [.name, (.owner // ""), (.storage_root // "")] | join("|")' \
  | while IFS='|' read -r n o r; do p=$(eff catalog "$n"); mine "$o" && p="$p ALL_PRIVILEGES"
      if inp "$p" USE_CATALOG && inp "$p" CREATE_SCHEMA; then k=$((k + 1))
        c="CDC ok"; [ -n "$DS" ] && [ -n "$r" ] && [ "${r#"$DS"}" != "$r" ] && c="Default Storage: query-based only"; [ -z "$r" ] && c="metastore root storage"
        printf "        %2d. %-40s %s\n" $k "$n" "$c"; fi; done | tee $T/choices
  [ -s $T/choices ] || echo "        none: ask for a catalog and grants (SKILL.md step 2)"; }
if [ -z "$CAT" ]; then row ASK "5 target catalog" "no default catalog set" "ask the user which catalog and schema the target tables go to"; choices
elif [ "$CAT" = system ] || [ "$CAT" = hive_metastore ]; then row ASK "5 target catalog $CAT" "${DEF:+the default; }pipelines can't write to $CAT" "ask the user which catalog and schema the target tables go to"; choices
elif ! j catalogs get "$CAT" > $T/c.json; then row ASK "5 target catalog $CAT" "not found or not visible here" "ask the user which catalog to use (or bind this one to the workspace)"; choices
else
  own=$(yq -p json -o tsv '.owner // ""' $T/c.json); root=$(yq -p json -o tsv '.storage_root // ""' $T/c.json)
  ck=$(eff catalog "$CAT"); mine "$own" && ck="$ck ALL_PRIVILEGES"
  sk=$(j schemas get "$CAT.$SCH" > $T/s.json && { eff schema "$CAT.$SCH"; mine "$(yq -p json -o tsv '.owner // ""' $T/s.json)" && echo ALL_PRIVILEGES; } | tr '\n' ' ')
  where="$CAT.$SCH$([ "$CAT" = "$DEF" ] && echo ' (the workspace default)')"
  if inp "$ck" USE_CATALOG && { { inp "$sk" USE_SCHEMA && inp "$sk" CREATE_TABLE; } || inp "$ck" CREATE_SCHEMA; }; then
    if [ -n "$DS" ] && [ -n "$root" ] && [ "${root#"$DS"}" != "$root" ]; then row WARN "5 target $where" "writable, but on Default Storage: query-based only, not CDC" "if the demo uses CDC, ask the user to pick one marked CDC ok"; choices
    else row PASS "5 target $where" "writable$([ -z "$root" ] && echo "; on the metastore's root storage")"; fi
  else row ASK "5 target $where" "not writable by $ME (needs USE CATALOG and CREATE SCHEMA, or CREATE TABLE in $SCH)" "ask the user to pick a target below, or for the grants"; choices; fi
fi

# 6. access: check.sh for you, and for --principal (the catalog's grants only for a target that's settled)
grep -q "^ASK   5 " $T/rows 2>/dev/null && CAT=
M6=qbc,fcqbc,icdc; grep -q "^WARN  5 .*Default Storage" $T/rows 2>/dev/null && M6=qbc,fcqbc     # (step 5 already said CDC can't use it)

for w in "$ME" ${WHO:+"$WHO"}; do
  out=$(bash "$D/check.sh" "$P" "$w" ${CAT:+--catalog "$CAT"} --modes $M6 2>&1); r=$?
  bad=$(echo "$out" | grep -c '^FAIL'); warn=$(echo "$out" | grep -c '^WARN')
  [ $r -eq 0 ] && { row PASS "6 access: $w" "check.sh: 0 failed, $warn warnings${CAT:+, on $CAT}"; echo "$out" | grep -E '^WARN' | sed 's/^/        /'; } || { row FAIL "6 access: $w" "check.sh: $bad failed${CAT:+, on $CAT}" "bash $D/check.sh $P \"$w\" ${CAT:+--catalog $CAT} (for the fixes), then grant.sh"; echo "$out" | grep -E '^FAIL' | sed 's/^/        /'; }
done

# 7. serverless egress the source's firewall must allow (lfc-source-firewall)
if [ -n "$CLOUD" ]; then
  bash "$D/../../lfc-source-firewall/scripts/serverless-egress.sh" "$CLOUD" "$REGION" ${EGRESS:+--state "$EGRESS"} > $T/eg.txt 2> $T/eg.err; erc=$?
  n=$(grep -cv '^[#+-]' $T/eg.txt)
  case $erc in
    0) row PASS "7 serverless egress ranges ($CLOUD $REGION)" "$n IPv4 CIDRs in ip-ranges.json$([ -n "$EGRESS" ] && grep -q '^# unchanged' $T/eg.txt && echo ', unchanged since the saved list'): lfc-source-firewall";;
    3) row WARN "7 serverless egress ranges ($CLOUD $REGION)" "CHANGED since $EGRESS: $(grep -c '^+' $T/eg.txt) added, $(grep -c '^-' $T/eg.txt) removed" "sync the source firewall (lfc-source-firewall)";;
    *) row WARN "7 serverless egress ranges ($CLOUD $REGION)" "region not in ip-ranges.json (or not reachable)" "don't guess the IPs: check lfc-source-firewall";;
  esac
fi

# 8. Lakeflow Connect connections already here (lfc-connection-sync makes them)
j connections list > $T/conn.json || echo '[]' > $T/conn.json
c=$(yq -p json -o tsv '[.[] | select(.connection_type == "MYSQL" or .connection_type == "POSTGRESQL" or .connection_type == "SQLSERVER" or .connection_type == "ORACLE")] | group_by(.connection_type) | map(.[0].connection_type + " " + (length | tostring)) | join(", ")' $T/conn.json)
row INFO "8 database connections" "${c:-none} (lfc-connection-sync creates them; test each with remote_query)"

# the verdict per mode: CDC also needs the target off Default Storage
echo; if [ $fails -gt 0 ]; then echo "$P: NOT READY: $fails check(s) failed or need an answer (ASK)"
elif grep -q "^WARN  5 .*Default Storage" $T/rows; then echo "$P: ready for query-based; NOT ready for CDC: the target is on Default Storage (pick a catalog marked CDC ok, rerun with --catalog)"
else echo "$P: ready for query-based and CDC${CAT:+ into $CAT} (read any WARN lines)"; fi
exit $((fails > 0))
