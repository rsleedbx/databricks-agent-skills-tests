#!/usr/bin/env bash
# flex.sh pg|mysql plan|create|status|stop|start|delete: an Azure Database for PostgreSQL or MySQL Flexible Server set
#   up for Lakeflow Connect query-based and CDC.
#   Needs RG, LOCATION, SERVER (globally unique), DB. Identity: SUBSCRIPTION, required for every command but plan/create
#   (those take az's current one, pin it and print its IDENTITY line). Optional: TENANT_ID (checked), SKU (Standard_B1ms),
#   TIER (Burstable), STORAGE_GB (32), VERSION (pg 16, mysql 8.0.21), OWNER ($USER).
#   create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the lfcadmin password).
#   create: the server with public access and no firewall rules (closed until lfc-source-firewall's server-fw.py adds
#   the same list as every other source), the database, and the parameters CDC needs, then a restart if one changed:
#     pg     wal_level=logical, max_replication_slots>=10, max_wal_senders>=10
#     mysql  binlog_row_image=FULL, binlog_expire_logs_seconds>=604800 (7 days), sql_generate_invisible_primary_key=OFF
#            (Azure's default ON gives keyless tables a hidden key)
#   TLS stays required (require_secure_transport); the UC connection uses TLS.
#   stop/start: a stopped server costs storage only; Azure starts it again by itself after 7 days.
#   Tags: owner, purpose=lfc-test, created, CreatedBy=lfc-source-azure-flexserver, RemoveAfter (lfc-test-settings
#   REMOVE_AFTER_DAYS, default 7). delete removes the server only when its CreatedBy tag is this skill's (or, for one
#   made before that tag, purpose=lfc-test with this owner); not found exits 3, a failed lookup 1, another's 2.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../../secret-handling/scripts/pw.sh"   # pw/pw_check: a password file or keyring:<name>
E=${1:?pg|mysql}; CMD=${2:?plan|create|status|stop|start|delete}
: "${RG:?}" "${SERVER:?}" "${DB:?}"
# public access with no firewall rules: postgres calls that Enabled (its None turns public access off); mysql None
case $E in pg) S=postgres; V=${VERSION:-16}; DBF=-n; PUBLIC=Enabled;; mysql) S=mysql; V=${VERSION:-8.0.21}; DBF=-d; PUBLIC=None;; *) echo "pg or mysql"; exit 2;; esac
. "$(dirname "$0")/../../cli-setup/scripts/cloud-identity.sh"
case $CMD in   # plan/create take az's current subscription and pin it; the other commands need the one the server was made in
plan|create) SUBSCRIPTION=${SUBSCRIPTION:-$(az account show --query id -o tsv)} ;;
*) : "${SUBSCRIPTION:?the subscription_id from the create IDENTITY line (the az default is not used)}" ;;
esac
az_require "$SUBSCRIPTION" "${TENANT_ID:-}" || exit 1
A=(--resource-group "$RG" --subscription "$SUBSCRIPTION")
# what create tags, and the only thing delete removes (lfc-test-settings: OWNER, REMOVE_AFTER_DAYS)
MINE=lfc-source-azure-flexserver; ST=$(cd "$(dirname "$0")" && pwd)/../../lfc-test-settings/scripts/settings.sh
TAGS=(owner="${OWNER:-$USER}" purpose=lfc-test created="$(date -u +%Y-%m-%d)" CreatedBy=$MINE RemoveAfter="$(bash "$ST" remove-after)")
ours() {   # ours "<az show command>": 0 when the resource's tags say this skill (or, before CreatedBy, this owner) made it
  local t; t=$($1 "${A[@]}" -n "$SERVER" --query tags -o json 2> "${TMPDIR:-/tmp}/ours.$$") || {
    grep -q ResourceNotFound "${TMPDIR:-/tmp}/ours.$$" && { echo "$SERVER not found: nothing to delete"; rm -f "${TMPDIR:-/tmp}/ours.$$"; return 3; }
    echo "couldn't read $SERVER's tags; nothing deleted:"; cat "${TMPDIR:-/tmp}/ours.$$"; rm -f "${TMPDIR:-/tmp}/ours.$$"; return 1; }
  rm -f "${TMPDIR:-/tmp}/ours.$$"
  printf '%s' "${t:-null}" | jq -e --arg m "$MINE" --arg o "${OWNER:-$USER}" '(.CreatedBy // "") == $m or ((.purpose // "") == "lfc-test" and (.owner // "") == $o)' > /dev/null \
    || { echo "$SERVER wasn't made by this skill for ${OWNER:-$USER} (tags: $(printf '%s' "${t:-null}" | jq -c .)): not deleted"; return 2; }
}
srv() { az $S flexible-server show "${A[@]}" -n "$SERVER" -o json 2>/dev/null; }
param() { az $S flexible-server parameter show "${A[@]}" --server-name "$SERVER" -n "$1" --query value -o tsv; }
setp() { # name value: set when different; prints the name if it needs a restart
  local cur; cur=$(param "$1"); [ "$(echo "$cur" | tr A-Z a-z)" = "$(echo "$2" | tr A-Z a-z)" ] && return 0
  az $S flexible-server parameter set "${A[@]}" --server-name "$SERVER" -n "$1" --value "$2" -o none; echo "$1"; }
atleast() { local cur; cur=$(param "$1"); [ "${cur:-0}" -ge "$2" ] 2>/dev/null || setp "$1" "$2"; }

case $CMD in
plan|create)
  : "${LOCATION:?}"
  echo "server   $SERVER $S $V ${TIER:-Burstable} ${SKU:-Standard_B1ms} ${STORAGE_GB:-32} GB ($LOCATION), public access, no firewall rules $(srv >/dev/null && echo '(exists)' || echo '(new)')"
  echo "database $DB"
  [ "$CMD" = plan ] && exit 0
  : "${ADMIN_PW_FILE:?}"; pw_check "$ADMIN_PW_FILE" || exit 2
  if ! srv >/dev/null; then
    # az takes the password only as a flag: it's in this machine's process list while the command runs
    az $S flexible-server create "${A[@]}" -n "$SERVER" -l "$LOCATION" --admin-user lfcadmin --admin-password "$(pw "$ADMIN_PW_FILE")" \
      --tier "${TIER:-Burstable}" --sku-name "${SKU:-Standard_B1ms}" --storage-size "${STORAGE_GB:-32}" --version "$V" \
      --public-access "$PUBLIC" --tags "${TAGS[@]}" --yes -o none
  fi
  az $S flexible-server db show "${A[@]}" -s "$SERVER" $DBF "$DB" -o none 2>/dev/null \
    || az $S flexible-server db create "${A[@]}" -s "$SERVER" $DBF "$DB" -o none
  if [ $E = pg ]; then
    changed=$(setp wal_level logical; atleast max_replication_slots 10; atleast max_wal_senders 10)
  else
    changed=$(setp binlog_row_image FULL; atleast binlog_expire_logs_seconds 604800; setp sql_generate_invisible_primary_key OFF)
  fi
  if [ -n "$changed" ]; then echo "set: $(echo $changed)"; az $S flexible-server restart "${A[@]}" -n "$SERVER" -o none; echo "restarted"; fi
  az_record "$SUBSCRIPTION"
  echo "ready: $(srv | yq -p json '.fullyQualifiedDomainName') database $DB. Next: lfc-source-firewall scripts/server-fw.py $E $SERVER" ;;
status) srv | yq -p json '{"state": .state, "fqdn": .fullyQualifiedDomainName, "version": .version, "sku": .sku.name}' ;;
stop|start) az $S flexible-server "$CMD" "${A[@]}" -n "$SERVER" -o none && echo "$CMD: $SERVER" ;;
delete) ours "az $S flexible-server show" || exit $?
  az $S flexible-server delete "${A[@]}" -n "$SERVER" --yes -o none && echo "deleted $SERVER" ;;
*) echo "unknown $CMD"; exit 2 ;;
esac
