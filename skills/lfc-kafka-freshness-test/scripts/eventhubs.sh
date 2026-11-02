#!/usr/bin/env bash
# eventhubs.sh up <resource group> <namespace> <region> --password-file F [--topic tag-observations] [--partitions 32]
#              [--tu 2] [--max-tu 20] [--tags "Owner=<you> RemoveAfter=<date>"] [--apply]
# eventhubs.sh down <resource group> <namespace> [--apply]
#   up: an Event Hubs namespace (Standard, Kafka endpoint on, auto-inflate from --tu to --max-tu) with one event hub as
#   the Kafka topic and a Listen+Send rule named kafka-test. The rule's connection string (the SASL password for user
#   $ConnectionString) goes into a 0600 file, never printed. Creates the resource group if it's missing.
#   down: deletes the namespace (its hubs and rules), not the resource group. Prints the plan; changes nothing without --apply.
set -uo pipefail
CMD=${1:-}; RG=${2:-}; NS=${3:-}
[ -n "$CMD" ] && [ -n "$RG" ] && [ -n "$NS" ] || { sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
shift 3
case $CMD in
up)
  LOC=${1:-}; [ -n "$LOC" ] || { echo "up needs <region>"; exit 2; }; shift
  PWF=; TOPIC=tag-observations; PARTS=32; TU=2; MAXTU=20; TAGS=; APPLY=no
  while [ $# -gt 0 ]; do case $1 in --password-file) PWF=$2; shift 2;; --topic) TOPIC=$2; shift 2;; --partitions) PARTS=$2; shift 2;;
    --tu) TU=$2; shift 2;; --max-tu) MAXTU=$2; shift 2;; --tags) TAGS=$2; shift 2;; --apply) APPLY=yes; shift;; *) echo "unknown $1"; exit 2;; esac; done
  [ -n "$PWF" ] || { echo "--password-file is required"; exit 2; }
  echo "plan: namespace $NS in $RG ($LOC), Standard, $TU-$MAXTU TU, Kafka on; hub $TOPIC ($PARTS partitions, 24 h); rule kafka-test (Listen, Send) -> $PWF"
  [ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
  # shellcheck disable=SC2086
  az group create -n "$RG" -l "$LOC" ${TAGS:+--tags $TAGS} -o none || exit 1
  # shellcheck disable=SC2086
  az eventhubs namespace create -g "$RG" -n "$NS" -l "$LOC" --sku Standard --capacity "$TU" --enable-auto-inflate true \
    --maximum-throughput-units "$MAXTU" --enable-kafka true ${TAGS:+--tags $TAGS} -o none || exit 1
  az eventhubs eventhub create -g "$RG" --namespace-name "$NS" -n "$TOPIC" --partition-count "$PARTS" \
    --retention-time-in-hours 24 --cleanup-policy Delete -o none || exit 1
  az eventhubs namespace authorization-rule create -g "$RG" --namespace-name "$NS" -n kafka-test --rights Listen Send -o none || exit 1
  (umask 077; az eventhubs namespace authorization-rule keys list -g "$RG" --namespace-name "$NS" \
    --authorization-rule-name kafka-test --query primaryConnectionString -o tsv > "$PWF") && [ -s "$PWF" ] || { echo "no connection string written"; exit 1; }
  echo "bootstrap $NS.servicebus.windows.net:9093  user \$ConnectionString  password file $PWF ($(wc -c < "$PWF" | tr -d ' ') bytes)";;
down)
  APPLY=no; [ "${1:-}" = --apply ] && APPLY=yes
  echo "plan: delete namespace $NS in $RG (its hubs and rules; the resource group stays)"
  [ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
  az eventhubs namespace delete -g "$RG" -n "$NS" && echo "deleted";;
*) echo "up or down, not $CMD"; exit 2;;
esac
