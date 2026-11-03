#!/usr/bin/env bash
# eventhubs.sh up <resource group> <namespace> <region> --password-file F [--topic tag-observations] [--partitions 32]
#              [--tu 2] [--max-tu 20] [--tags "Owner=<you> RemoveAfter=<date>"] [--source-json F] [--apply]
# eventhubs.sh down <resource group> <namespace> [--apply]
#   up: an Event Hubs namespace (Standard, Kafka endpoint on, auto-inflate from --tu to --max-tu) with one event hub as
#   the Kafka topic and a Listen+Send rule named kafka-test. The rule's connection string (the SASL password for user
#   $ConnectionString) goes into a 0600 file, never printed, and the source contract (lfc-source-contract, engine kafka:
#   bootstrap, user, topic, the password file's path; no secret) into --source-json (default <namespace>.source.json
#   beside the password file) for the tests to read as SOURCE_JSON. The resource group must exist (one you choose for
#   all your test resources): it's never created, re-tagged or deleted. The namespace is tagged CreatedBy=lfc-source-eventhubs;
#   one of that name without the tag is left alone (up stops). AZ_SUBSCRIPTION, if set, goes on every az call.
#   down: deletes the namespace (its hubs and rules) only when its CreatedBy tag says this script made it (also the tag
#   lfc-kafka-freshness-test, which it used before); never the
#   group. A failed lookup (login, subscription) exits 1, not 0. Prints the plan; changes nothing without --apply.
set -uo pipefail
CMD=${1:-}; RG=${2:-}; NS=${3:-}
[ -n "$CMD" ] && [ -n "$RG" ] && [ -n "$NS" ] || { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
shift 3
MINE=lfc-source-eventhubs   # the CreatedBy tag: what down may delete
mine() { [ "$1" = "$MINE" ] || [ "$1" = lfc-kafka-freshness-test ]; }   # and the tag this script wrote before it moved here
case $CMD in
up)
  LOC=${1:-}; [ -n "$LOC" ] || { echo "up needs <region>"; exit 2; }; shift
  PWF=; TOPIC=tag-observations; PARTS=32; TU=2; MAXTU=20; TAGS=; SJ=; APPLY=no
  while [ $# -gt 0 ]; do case $1 in --password-file) PWF=$2; shift 2;; --topic) TOPIC=$2; shift 2;; --partitions) PARTS=$2; shift 2;;
    --tu) TU=$2; shift 2;; --max-tu) MAXTU=$2; shift 2;; --tags) TAGS=$2; shift 2;;
    --source-json) SJ=$2; shift 2;; --apply) APPLY=yes; shift;; *) echo "unknown $1"; exit 2;; esac; done
  [ -n "$PWF" ] || { echo "--password-file is required"; exit 2; }
  SJ=${SJ:-$(dirname "$PWF")/$NS.source.json}
  echo "plan: namespace $NS in $RG ($LOC), Standard, $TU-$MAXTU TU, Kafka on; hub $TOPIC ($PARTS partitions, 24 h); rule kafka-test (Listen, Send) -> $PWF; contract -> $SJ"
  [ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
  az group show ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -n "$RG" -o none 2>/dev/null \
    || { echo "resource group $RG doesn't exist${AZ_SUBSCRIPTION:+ in subscription $AZ_SUBSCRIPTION}: pass an existing group; this script never creates one"; exit 2; }
  by=$(az eventhubs namespace show ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" -n "$NS" --query tags.CreatedBy -o tsv 2>/dev/null) && have=yes || have=no
  if [ $have = yes ] && ! mine "$by"; then echo "namespace $NS exists and wasn't created by this script (CreatedBy=${by:-none}): left alone; pick another name"; exit 2; fi
  if [ $have = no ]; then
    # shellcheck disable=SC2086
    az eventhubs namespace create ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" -n "$NS" -l "$LOC" --sku Standard --capacity "$TU" --enable-auto-inflate true \
      --maximum-throughput-units "$MAXTU" --enable-kafka true --tags CreatedBy=$MINE $TAGS -o none || exit 1
  else echo "namespace $NS: made by this script earlier, reused"; fi
  az eventhubs eventhub create ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" --namespace-name "$NS" -n "$TOPIC" --partition-count "$PARTS" \
    --retention-time-in-hours 24 --cleanup-policy Delete -o none || exit 1
  az eventhubs namespace authorization-rule create ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" --namespace-name "$NS" -n kafka-test --rights Listen Send -o none || exit 1
  (umask 077; az eventhubs namespace authorization-rule keys list ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" --namespace-name "$NS" \
    --authorization-rule-name kafka-test --query primaryConnectionString -o tsv > "$PWF") && [ -s "$PWF" ] || { echo "no connection string written"; exit 1; }
  PWA=$(cd "$(dirname "$PWF")" && pwd)/$(basename "$PWF")
  (umask 077; jq -n --arg ns "$NS" --arg t "$TOPIC" --arg pw "$PWA" '{contract: 1, engine: "kafka", name: $ns,
    host: ($ns + ".servicebus.windows.net"), port: 9093, user: "$ConnectionString", topic: $t, sasl_mechanism: "PLAIN",
    password_file: $pw}' > "$SJ") || { echo "no source contract written"; exit 1; }
  echo "bootstrap $NS.servicebus.windows.net:9093  user \$ConnectionString  password file $PWF ($(wc -c < "$PWF" | tr -d ' ') bytes)"
  echo "source contract $SJ: SOURCE_JSON=$SJ";;
down)
  APPLY=no; [ "${1:-}" = --apply ] && APPLY=yes
  echo "plan: delete namespace $NS in $RG if this script made it (CreatedBy=$MINE; its hubs and rules; the group stays)"
  [ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
  E=$(mktemp); trap 'rm -f "$E"' EXIT
  if ! by=$(az eventhubs namespace show ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" -n "$NS" --query tags.CreatedBy -o tsv 2>"$E"); then
    # only a namespace that isn't there is nothing to delete; an expired login or a wrong subscription is a failure
    grep -q '(ResourceNotFound)' "$E" && { echo "namespace $NS not found in $RG: nothing to delete"; exit 0; }
    grep -q 'ResourceGroupNotFound' "$E" && { echo "resource group $RG not found${AZ_SUBSCRIPTION:+ in subscription $AZ_SUBSCRIPTION}: check the subscription; nothing deleted"; exit 1; }
    echo "couldn't look up namespace $NS in $RG; nothing deleted:"; cat "$E"; exit 1
  fi
  mine "$by" || { echo "namespace $NS wasn't created by this script (CreatedBy=${by:-none}): not deleted"; exit 2; }
  az eventhubs namespace delete ${AZ_SUBSCRIPTION:+--subscription "$AZ_SUBSCRIPTION"} -g "$RG" -n "$NS" && echo "deleted";;
*) echo "up or down, not $CMD"; exit 2;;
esac
