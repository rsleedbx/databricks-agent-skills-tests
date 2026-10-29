#!/usr/bin/env bash
# connection.sh <profile> <name> --bootstrap <host:port> --user <user> --password-file F [--apply]
#   A Unity Catalog connection of type KAFKA (SASL PLAIN, the options bootstrap_servers, sasl_mechanism, user, password)
#   for the Lakeflow Connect Kafka connector. For Event Hubs: --bootstrap <namespace>.servicebus.windows.net:9093
#   --user '$ConnectionString', and the connection string in the password file (eventhubs.sh up writes it). The password
#   goes from the file into a 0600 request body that's deleted after the call, never onto a command line. An existing
#   connection of that name is left as it is. Prints the plan; changes nothing without --apply.
set -uo pipefail
P=${1:-}; NAME=${2:-}
[ -n "$P" ] && [ -n "$NAME" ] || { sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
shift 2; BOOT=; USR=; PWF=; APPLY=no
while [ $# -gt 0 ]; do case $1 in --bootstrap) BOOT=$2; shift 2;; --user) USR=$2; shift 2;; --password-file) PWF=$2; shift 2;;
  --apply) APPLY=yes; shift;; *) echo "unknown $1"; exit 2;; esac; done
[ -n "$BOOT" ] && [ -n "$USR" ] && [ -n "$PWF" ] || { echo "--bootstrap, --user and --password-file are required"; exit 2; }
[ -s "$PWF" ] || { echo "$PWF is missing or empty"; exit 2; }
if have=$(databricks connections get "$NAME" --profile "$P" -o json 2>/dev/null); then
  echo "exists: $NAME ($(echo "$have" | jq -r '.connection_type'), bootstrap $(echo "$have" | jq -r '.options.bootstrap_servers // "?"')); left as it is"
  exit 0
fi
echo "plan: connection $NAME, KAFKA, bootstrap $BOOT, SASL PLAIN, user $USR, password from $PWF"
[ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
B=$(mktemp); chmod 600 "$B"; trap 'rm -f "$B"' EXIT
PW=$(cat "$PWF") jq -n --arg n "$NAME" --arg b "$BOOT" --arg u "$USR" \
  '{name: $n, connection_type: "KAFKA", comment: "Lakeflow Connect Kafka test", options: {bootstrap_servers: $b, sasl_mechanism: "PLAIN", user: $u, password: env.PW}}' > "$B"
databricks connections create --json @"$B" --profile "$P" -o json | jq -c '{name, connection_type, bootstrap: .options.bootstrap_servers}'
