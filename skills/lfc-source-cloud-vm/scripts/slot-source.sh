#!/usr/bin/env bash
# slot-source.sh [--connection <name>] [--facts <file>] [--out <file>] < up.json: the source contract (lfc-source-contract,
# contract 1) for a database from slot.sh up. up.json is its JSON line (the last line on stdin). sql_cmd is slot-sql.sh
# with this database and your slot key, so SRC_SQL reaches the database through the VM's forced command.
#   Needs SLOT_HOST (the VM's address for ssh). Optional SLOT_KEY (the private key; slot-sql.sh's default otherwise).
#   SLOT_OWNER + SLOT_ADMIN_KEY (+ SLOT_ADMIN_USER): another owner's database, reached through the VM admin login.
#   --connection: the password is left out (the UC connection holds it). Without it the contract has the password, so it
#   needs --out (a file, mode 600); nothing secret is printed. --facts: the facts JSON the contract points to (must exist).
#   Prints the file's path with --out, else the JSON (only when it has no password).
set -euo pipefail
if [ $# -eq 0 ] && [ -t 0 ]; then sed -n 2,11p "$0"; exit 2; fi
CONN="" FACTS="" OUT=""
while [ $# -gt 0 ]; do
  case $1 in
    --connection) CONN=${2:?--connection needs a name}; shift 2 ;;
    --facts) FACTS=${2:?--facts needs a file}; shift 2 ;;
    --out) OUT=${2:?--out needs a file}; shift 2 ;;
    *) echo "slot-source: unknown argument $1" >&2; exit 2 ;;
  esac
done
: "${SLOT_HOST:?set SLOT_HOST to the VM address for ssh}"
case $SLOT_HOST in *[!A-Za-z0-9.:-]*|"") echo "slot-source: SLOT_HOST is an address: $SLOT_HOST" >&2; exit 2 ;; esac
UP=$(tail -n 1)
[ -n "$UP" ] || { echo "slot-source: no slot.sh up JSON on stdin" >&2; exit 2; }
NAME=$(printf '%s' "$UP" | jq -r '.name // ""')
[[ $NAME =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { echo "slot-source: the up JSON has no database name" >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
SQL_CLIENT=$HERE/slot-sql.sh
[ -x "$SQL_CLIENT" ] || { echo "slot-source: no slot-sql.sh at $SQL_CLIENT" >&2; exit 2; }
SQL_CMD="env SLOT_HOST=$SLOT_HOST SLOT_NAME=$NAME"
if [ -n "${SLOT_KEY:-}" ]; then
  case $SLOT_KEY in *[[:space:]]*) echo "slot-source: SLOT_KEY has a space: slot-sql.sh is a word in the contract" >&2; exit 2 ;; esac
  SQL_CMD="$SQL_CMD SLOT_KEY=$SLOT_KEY"
fi
if [ -n "${SLOT_OWNER:-}" ]; then
  for v in "$SLOT_OWNER" "${SLOT_ADMIN_KEY:-}" "${SLOT_ADMIN_USER:-azureuser}"; do
    case $v in *[[:space:]]*|"") echo "slot-source: SLOT_OWNER needs SLOT_ADMIN_KEY, all without spaces" >&2; exit 2 ;; esac
  done
  SQL_CMD="$SQL_CMD SLOT_OWNER=$SLOT_OWNER SLOT_ADMIN_KEY=$SLOT_ADMIN_KEY SLOT_ADMIN_USER=${SLOT_ADMIN_USER:-azureuser}"
fi
SQL_CMD="$SQL_CMD $SQL_CLIENT"
if [ -n "$FACTS" ]; then
  [ -r "$FACTS" ] || { echo "slot-source: no facts file $FACTS" >&2; exit 2; }
  FACTS=$(cd "$(dirname "$FACTS")" && pwd)/$(basename "$FACTS")
fi
CONTRACT=$(printf '%s' "$UP" | jq -c --arg cmd "$SQL_CMD" --arg conn "$CONN" --arg facts "$FACTS" '
  {contract: 1, name: .name, engine: .engine, host: .host, port: .port, user: .user}
  + (if .database then {database: .database} else {} end)
  + (if .service then {service: .service} else {} end)
  + (if $conn != "" then {connection: $conn} elif .password then {password: .password} else {} end)
  + {sql_cmd: $cmd}
  + (if $facts != "" then {facts: $facts} else {} end)')
if [ -z "$OUT" ]; then
  [ -n "$CONN" ] || { echo "slot-source: without --connection the contract holds the password: give --out <file>" >&2; exit 2; }
  printf '%s\n' "$CONTRACT"
  exit 0
fi
umask 077
printf '%s\n' "$CONTRACT" > "$OUT"
chmod 600 "$OUT"
echo "$OUT"
