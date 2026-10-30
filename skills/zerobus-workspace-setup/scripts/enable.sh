#!/usr/bin/env bash
# enable.sh <profile> <setting | interface>... [--apply]
#   Turn on Zerobus previews through the Settings API (workspace admin). Takes setting names, or the interfaces that
#   need them: core (zerobus_ingest_core), kafka (zerobus_kafka_endpoint), mqtt (zerobus_mqtt_endpoint),
#   default-storage (zerobus_default_storage_write), rescue (zerobus_rescue_column_json). Prints each setting's value
#   and the plan; changes nothing without --apply. With no setting, lists every Zerobus setting and its value.
set -uo pipefail
P=$1; shift; APPLY=no; WANT=()
for a in "$@"; do case $a in --apply) APPLY=yes;; core) WANT+=(zerobus_ingest_core);; kafka) WANT+=(zerobus_kafka_endpoint);;
  mqtt) WANT+=(zerobus_mqtt_endpoint);; default-storage) WANT+=(zerobus_default_storage_write);; rescue) WANT+=(zerobus_rescue_column_json);;
  *) WANT+=("$a");; esac; done
value() { databricks workspace-settings-v2 get-public-workspace-setting "$1" --profile "$P" -o json 2>/dev/null \
  | jq -r 'if has("boolean_val") then "\(.boolean_val.value) (set here)" else "\(.effective_boolean_val.value) (inherited)" end'; }
if [ ${#WANT[@]} -eq 0 ]; then
  databricks workspace-settings-v2 list-workspace-settings-metadata --profile "$P" -o json \
    | yq -p json -o tsv '.[] | select(((.name // "") + " " + (.display_name // "")) | downcase | test("zerobus")) | [.name, .display_name]' \
    | while IFS=$'\t' read -r n d; do printf '%-34s %-38s %s\n' "$n" "$d" "$(value "$n")"; done
  exit 0
fi
for s in ${WANT[@]+"${WANT[@]}"}; do
  v=$(value "$s"); printf '%-34s %s\n' "$s" "${v:-not found}"
  case $v in true*) continue;; "") echo "  not a setting on this workspace: check the name with enable.sh $P"; continue;; esac
  if [ $APPLY = yes ]; then
    databricks workspace-settings-v2 patch-public-workspace-setting "$s" --json "{\"name\": \"$s\", \"boolean_val\": {\"value\": true}}" --profile "$P" >/dev/null \
      && printf '  -> %s\n' "$(value "$s")" || echo "  refused: needs a workspace admin, or the setting isn't writable through the public API"
  else echo "  plan: turn on (run again with --apply)"; fi
done
