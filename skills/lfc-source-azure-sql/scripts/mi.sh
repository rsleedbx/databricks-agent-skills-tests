#!/usr/bin/env bash
# mi.sh plan|create|wait|status|endpoint|stop|start|delete: an Azure SQL Managed Instance behind its own NSG.
#   Needs RG (resource group), VNET (an existing VNet in RG), MI_NAME. Identity: SUBSCRIPTION, required for every command
#   but plan/create (those take az's current one, pin it and print its IDENTITY line; see cli-setup cloud-identity.sh).
#   Optional: TENANT_ID (checked against the login),
#   LOCATION (the VNet's), SUBNET (<VNET>-mi-subnet), SUBNET_CIDR (10.10.200.0/27; /27 or larger, unused in the VNet),
#   VCORES (4), STORAGE_GB (32), PRICING (Freemium: the free offer, one per subscription; else Regular), OWNER ($USER).
#   create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the sqladmin password).
#   plan: read-only, says what create would make. create: the subnet's NSG (<SUBNET>-nsg) and route table
#   (<SUBNET>-rt, 0.0.0.0/0 to Internet), the subnet delegated to Microsoft.Sql/managedInstances, then the instance with
#   the public data endpoint (port 3342) on; returns at once. The NSG starts with no rule for 3342, so nothing outside
#   the VNet gets in until lfc-source-firewall syncs the laptop and Databricks egress ranges into <SUBNET>-nsg.
#   wait: polls until Succeeded (first instance in a subnet: 4-6 hours; later ones 30-60 min); exit 1 Failed, 2 timeout.
#   endpoint: prints <name>.public.<dns zone>.database.windows.net,3342. stop/start: pause and resume compute (General
#   Purpose). delete: the instance only (the subnet stays until Azure removes its virtual cluster, which takes hours),
#   and only when its CreatedBy tag is lfc-source-azure-sql (or, made before that tag, purpose=lfc-test with this
#   owner); not found exits 3, a failed lookup 1, another's 2. Tags as sqldb.sh (RemoveAfter: REMOVE_AFTER_DAYS).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../../secret-handling/scripts/pw.sh"   # pw/pw_check: a password file or keyring:<name>
CMD=${1:?plan|create|wait|status|endpoint|stop|start|delete}
: "${RG:?}" "${VNET:?}" "${MI_NAME:?}"
SUBNET=${SUBNET:-$VNET-mi-subnet}; SUBNET_CIDR=${SUBNET_CIDR:-10.10.200.0/27}
. "$(dirname "$0")/../../cli-setup/scripts/cloud-identity.sh"
case $CMD in   # plan/create take az's current subscription and pin it; the other commands need the one the instance was made in
plan|create) SUBSCRIPTION=${SUBSCRIPTION:-$(az account show --query id -o tsv)} ;;
*) : "${SUBSCRIPTION:?the subscription_id from the create IDENTITY line (the az default is not used)}" ;;
esac
az_require "$SUBSCRIPTION" "${TENANT_ID:-}" || exit 1
A=(--resource-group "$RG" --subscription "$SUBSCRIPTION")
# what create tags, and the only thing delete removes (lfc-test-settings: OWNER, REMOVE_AFTER_DAYS)
MINE=lfc-source-azure-sql; ST=$(cd "$(dirname "$0")" && pwd)/../../lfc-test-settings/scripts/settings.sh
TAGS=(owner="${OWNER:-$USER}" purpose=lfc-test created="$(date -u +%Y-%m-%d)" CreatedBy=$MINE RemoveAfter="$(bash "$ST" remove-after)")
ours() {   # ours "<az show command>": 0 when the resource's tags say this skill (or, before CreatedBy, this owner) made it
  local t; t=$($1 "${A[@]}" -n "$MI_NAME" --query tags -o json 2> "${TMPDIR:-/tmp}/ours.$$") || {
    grep -q ResourceNotFound "${TMPDIR:-/tmp}/ours.$$" && { echo "$MI_NAME not found: nothing to delete"; rm -f "${TMPDIR:-/tmp}/ours.$$"; return 3; }
    echo "couldn't read $MI_NAME's tags; nothing deleted:"; cat "${TMPDIR:-/tmp}/ours.$$"; rm -f "${TMPDIR:-/tmp}/ours.$$"; return 1; }
  rm -f "${TMPDIR:-/tmp}/ours.$$"
  printf '%s' "${t:-null}" | jq -e --arg m "$MINE" --arg o "${OWNER:-$USER}" '(.CreatedBy // "") == $m or ((.purpose // "") == "lfc-test" and (.owner // "") == $o)' > /dev/null \
    || { echo "$MI_NAME wasn't made by this skill for ${OWNER:-$USER} (tags: $(printf '%s' "${t:-null}" | jq -c .)): not deleted"; return 2; }
}
NSG=$SUBNET-nsg; RT=$SUBNET-rt
mi() { az sql mi show "${A[@]}" -n "$MI_NAME" -o json 2>/dev/null; }

case $CMD in
plan|create)
  V=$(az network vnet show "${A[@]}" -n "$VNET" -o json 2>/dev/null) || { echo "no VNet $VNET in $RG: ask before creating one"; exit 2; }
  LOCATION=${LOCATION:-$(echo "$V" | yq -p json '.location')}
  SN=$(echo "$V" | S="$SUBNET" yq -p json -o json '[.subnets[] | select(.name == strenv(S))] | .[0] // {}')
  have_sn=$(echo "$SN" | yq -p json '.name // ""')
  if [ -n "$have_sn" ]; then
    SUBNET_CIDR=$(echo "$SN" | yq -p json '.addressPrefix')
    other=$(echo "$SN" | yq -p json '[.delegations[]?.serviceName] | join(",")')
    [ -z "$other" ] || [ "$other" = Microsoft.Sql/managedInstances ] || { echo "subnet $SUBNET is delegated to $other"; exit 2; }
  else
    used=$(echo "$V" | C="$SUBNET_CIDR" yq -p json '[.subnets[] | select(.addressPrefix == strenv(C)) | .name] | join(",")')
    [ -z "$used" ] || { echo "$SUBNET_CIDR is already subnet $used: set SUBNET_CIDR"; exit 2; }
  fi
  echo "location   $LOCATION"
  echo "subnet     $SUBNET $SUBNET_CIDR $([ -n "$have_sn" ] && echo '(exists)' || echo '(new)'), delegated to Microsoft.Sql/managedInstances"
  echo "nsg        $NSG $(az network nsg show "${A[@]}" -n "$NSG" -o none 2>/dev/null && echo '(exists)' || echo '(new, no 3342 rule: lfc-source-firewall adds them)')"
  echo "route      $RT 0.0.0.0/0 -> Internet $(az network route-table show "${A[@]}" -n "$RT" -o none 2>/dev/null && echo '(exists)' || echo '(new)')"
  echo "instance   $MI_NAME GP_Gen5 ${VCORES:-4} vCores, ${STORAGE_GB:-32} GB, pricing ${PRICING:-Regular}, public data endpoint 3342 $(mi >/dev/null && echo '(exists)' || echo '(new)')"
  [ "$CMD" = plan ] && exit 0
  : "${ADMIN_PW_FILE:?}"; pw_check "$ADMIN_PW_FILE" || exit 2
  az network nsg show "${A[@]}" -n "$NSG" -o none 2>/dev/null || az network nsg create "${A[@]}" -n "$NSG" -l "$LOCATION" --tags "${TAGS[@]}" -o none
  if ! az network route-table show "${A[@]}" -n "$RT" -o none 2>/dev/null; then
    az network route-table create "${A[@]}" -n "$RT" -l "$LOCATION" --tags "${TAGS[@]}" -o none
    az network route-table route create "${A[@]}" --route-table-name "$RT" -n default-internet --address-prefix 0.0.0.0/0 --next-hop-type Internet -o none
  fi
  if [ -n "$have_sn" ]; then
    az network vnet subnet update "${A[@]}" --vnet-name "$VNET" -n "$SUBNET" --network-security-group "$NSG" --route-table "$RT" --delegations Microsoft.Sql/managedInstances -o none
  else
    az network vnet subnet create "${A[@]}" --vnet-name "$VNET" -n "$SUBNET" --address-prefixes "$SUBNET_CIDR" --network-security-group "$NSG" --route-table "$RT" --delegations Microsoft.Sql/managedInstances -o none
  fi
  echo "subnet ready"
  if mi >/dev/null; then echo "instance $MI_NAME exists: mi.sh wait"; exit 0; fi
  SID=$(az network vnet subnet show "${A[@]}" --vnet-name "$VNET" -n "$SUBNET" --query id -o tsv)
  # az takes the password only as a flag: it's in this machine's process list while the command runs
  az sql mi create "${A[@]}" -n "$MI_NAME" -l "$LOCATION" --subnet "$SID" --admin-user sqladmin --admin-password "$(pw "$ADMIN_PW_FILE")" \
    --edition GeneralPurpose --family Gen5 --capacity "${VCORES:-4}" --storage "${STORAGE_GB:-32}GB" --license-type LicenseIncluded \
    --public-data-endpoint-enabled true --minimal-tls-version 1.2 --backup-storage-redundancy Local \
    ${PRICING:+--pricing-model "$PRICING"} --tags "${TAGS[@]}" --no-wait -o none
  az_record "$SUBSCRIPTION"
  echo "creating $MI_NAME (first instance in a subnet: 4-6 hours). Next: mi.sh wait, in the background"
  echo "then: lfc-source-firewall scripts/nsg-copy.sh <shared-nsg> $NSG 3342 --apply" ;;
wait)
  for w in 60 120 300 600 900 1800 1800 1800 1800 1800 1800 1800 1800 1800; do   # about 6.5 hours
    sleep "$w"
    s=$(mi | yq -p json '.provisioningState // "missing"')
    pct=$(az sql mi op list "${A[@]}" --mi "$MI_NAME" -o json 2>/dev/null | yq -p json '.[0].percentComplete // "?"')
    echo "$(date +%T) $s $pct%"
    case $s in Succeeded) exit 0;; Failed|Canceled|missing) exit 1;; esac
  done
  echo "still provisioning: check the operation in the portal; don't delete and retry"; exit 2 ;;
status) mi | yq -p json '{"provisioningState": .provisioningState, "state": .state, "vCores": .vCores, "storageSizeInGb": .storageSizeInGb, "publicDataEndpointEnabled": .publicDataEndpointEnabled, "pricingModel": .pricingModel}' ;;
endpoint)
  f=$(mi | yq -p json '.fullyQualifiedDomainName // ""'); [ -n "$f" ] || { echo "no instance $MI_NAME"; exit 1; }
  echo "${f%%.*}.public.${f#*.},3342" ;;
stop|start) az sql mi "$CMD" "${A[@]}" --mi "$MI_NAME" -o none && echo "$CMD: $MI_NAME" ;;
delete) ours "az sql mi show" || exit $?
  az sql mi delete "${A[@]}" -n "$MI_NAME" --yes --no-wait && echo "deleting $MI_NAME (the subnet, NSG and route table stay)" ;;
*) echo "unknown $CMD"; exit 2 ;;
esac
