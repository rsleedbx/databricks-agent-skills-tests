#!/usr/bin/env bash
# sqldb.sh plan|create|status|wake|delete: an Azure SQL Database (serverless, auto-pause) on its own logical server.
#   Needs RG, LOCATION, SERVER (globally unique), DB. Identity: SUBSCRIPTION, required for every command but plan/create.
#   plan/create take az's current subscription, pin it, and print its IDENTITY line: keep it with SUBSCRIPTION in your
#   env file. Optional: TENANT_ID (checked against the login), VCORES (1),
#   MIN_CAPACITY (0.5), AUTO_PAUSE (60 minutes idle, then paused; -1 never), FREE (1: the free offer, up to 10 databases
#   per subscription; when its monthly vCore seconds run out the database pauses until next month), OWNER ($USER).
#   create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the sqladmin password).
#   create: the server with public access and no firewall rules (closed until lfc-source-firewall's server-fw.py adds
#   the same list as every other source), then the database: GeneralPurpose serverless Gen5, local backup redundancy.
#   A paused database resumes on the next login; that first login fails with error 40613 ("Database ... is not
#   currently available") until it's online, about a minute. wake: logs in until it answers (needs sqlcmd, ADMIN_PW_FILE).
#   status: the database's state (Online, Paused, Resuming) and its pause settings. delete: the server and its databases.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../../lfc-source-setup/scripts/pw.sh"   # pw/pw_check: a password file or keyring:<name>
CMD=${1:?plan|create|status|wake|delete}
: "${RG:?}" "${SERVER:?}" "${DB:?}"
. "$(dirname "$0")/../../lfc-source-setup/scripts/cloud-identity.sh"
case $CMD in
plan|create) SUBSCRIPTION=${SUBSCRIPTION:-$(az account show --query id -o tsv)} ;;
*) : "${SUBSCRIPTION:?the subscription_id from the create IDENTITY line (the az default is not used)}" ;;
esac
az_require "$SUBSCRIPTION" "${TENANT_ID:-}" || exit 1
A=(--resource-group "$RG" --subscription "$SUBSCRIPTION")
HOST=$SERVER.database.windows.net
srv() { az sql server show "${A[@]}" -n "$SERVER" -o json 2>/dev/null; }
db() { az sql db show "${A[@]}" -s "$SERVER" -n "$DB" -o json 2>/dev/null; }

case $CMD in
plan|create)
  : "${LOCATION:?}"
  echo "server   $SERVER ($LOCATION) public access, no firewall rules $(srv >/dev/null && echo '(exists)' || echo '(new)')"
  echo "database $DB serverless GP Gen5 ${VCORES:-1} vCore, min ${MIN_CAPACITY:-0.5}, auto-pause ${AUTO_PAUSE:-60} min$([ "${FREE:-}" = 1 ] && echo ', free offer') $(db >/dev/null && echo '(exists)' || echo '(new)')"
  az_record "$SUBSCRIPTION"
  [ "$CMD" = plan ] && exit 0
  : "${ADMIN_PW_FILE:?}"; pw_check "$ADMIN_PW_FILE" || exit 2
  TAGS=(owner="${OWNER:-$USER}" purpose=lfc-test created="$(date -u +%Y-%m-%d)")
  # az takes the password only as a flag: it's in this machine's process list while the command runs
  srv >/dev/null || az sql server create "${A[@]}" -n "$SERVER" -l "$LOCATION" --admin-user sqladmin \
    --admin-password "$(pw "$ADMIN_PW_FILE")" --enable-public-network true --minimal-tls-version 1.2 -o none
  az resource tag "${A[@]}" --resource-type Microsoft.Sql/servers -n "$SERVER" --tags "${TAGS[@]}" -o none
  if ! db >/dev/null; then
    F=(); [ "${FREE:-}" = 1 ] && F=(--use-free-limit --free-limit-exhaustion-behavior AutoPause)
    az sql db create "${A[@]}" -s "$SERVER" -n "$DB" --edition GeneralPurpose --compute-model Serverless --family Gen5 \
      --capacity "${VCORES:-1}" --min-capacity "${MIN_CAPACITY:-0.5}" --auto-pause-delay "${AUTO_PAUSE:-60}" \
      --backup-storage-redundancy Local ${F[@]+"${F[@]}"} --tags "${TAGS[@]}" -o none
  fi
  az_record "$SUBSCRIPTION"
  echo "ready: $HOST,1433 database $DB. Next: lfc-source-firewall scripts/server-fw.py sql $SERVER" ;;
status) db | yq -p json '{"status": .status, "autoPauseDelay": .autoPauseDelay, "minCapacity": .minCapacity, "useFreeLimit": .useFreeLimit, "freeLimitExhaustionBehavior": .freeLimitExhaustionBehavior}' ;;
wake)
  : "${ADMIN_PW_FILE:?}"
  for _ in $(seq 1 20); do
    SQLCMDPASSWORD="$(pw "$ADMIN_PW_FILE")" sqlcmd -S "$HOST,1433" -U sqladmin -d "$DB" -C -l 30 -Q "SELECT 1" -h -1 >/dev/null 2>&1 \
      && { echo "online: $DB"; exit 0; }
    sleep 15
  done
  echo "still not online after 5 minutes: $(db | yq -p json '.status')"; exit 1 ;;
delete) az sql server delete "${A[@]}" -n "$SERVER" --yes && echo "deleted $SERVER and its databases" ;;
*) echo "unknown $CMD"; exit 2 ;;
esac
