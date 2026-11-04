#!/usr/bin/env bash
# show-diagram.sh <ingestion pipeline id>: print the pipeline's text diagram to stderr right after it is created, so
# whoever ran the create sees which components exist, and save text + Mermaid to $DIAGRAM_MD when set. Never fails the
# caller. Profile: $PROFILE or $DATABRICKS_CONFIG_PROFILE. Flags from env: SRC_CLOUD, SRC_HOST, SRC_PRIMARY,
# SRC_SECONDARY, SRC_FIREWALL, SECRET_STORE, SECRET_KIND, CAPTURE (cdc|ct|cdc+ct), SRC_FACTS (a vm-facts.sh JSON file),
# SRC_VM (the VM as text), SRC_MODEL, SRC_MODEL_TABLES, SRC_MODEL_SCALE (the data model loaded into the source).
# Kafka: BROKER, PRODUCER, PRODUCER_HOST, PROBE (what the API can't see). LAYERS (space-separated NAME=CATALOG.SCHEMA[,...] after the landing layer), REVERSE_ETL (TARGET[:METHOD[:TABLES]]).
# DIAGRAM=0 skips it.
id=${1:?usage: show-diagram.sh <ingestion pipeline id>}
[ "${DIAGRAM:-1}" = 1 ] || exit 0
py="$(cd "$(dirname "$0")" && pwd)/pipeline-diagram.py"
fl=(--pipeline "$id")
p=${PROFILE:-${DATABRICKS_CONFIG_PROFILE:-}}; [ -n "$p" ] && fl+=(--profile "$p")
[ -n "${SRC_CLOUD:-}" ] && fl+=(--source-cloud "$SRC_CLOUD")
[ -n "${SRC_HOST:-}" ] && fl+=(--source-host "$SRC_HOST")
[ -n "${SRC_PRIMARY:-}" ] && fl+=(--primary "$SRC_PRIMARY")
[ -n "${SRC_SECONDARY:-}" ] && fl+=(--secondary "$SRC_SECONDARY")
[ -n "${SRC_FIREWALL:-}" ] && fl+=(--firewall "$SRC_FIREWALL")
[ -n "${SECRET_STORE:-}" ] && fl+=(--secret-store "$SECRET_STORE")
[ -n "${SECRET_KIND:-}" ] && fl+=(--secret-kind "$SECRET_KIND")
[ -n "${CAPTURE:-}" ] && fl+=(--capture "$CAPTURE")
[ -n "${SRC_FACTS:-}" ] && [ -f "$SRC_FACTS" ] && fl+=(--source-facts "$SRC_FACTS")   # from vm-facts.sh
[ -n "${SRC_VM:-}" ] && fl+=(--vm "$SRC_VM")
[ -n "${SRC_MODEL:-}" ] && fl+=(--model "$SRC_MODEL")
[ -n "${SRC_MODEL_TABLES:-}" ] && fl+=(--model-tables "$SRC_MODEL_TABLES")
[ -n "${SRC_MODEL_SCALE:-}" ] && fl+=(--model-scale "$SRC_MODEL_SCALE")
[ -n "${BROKER:-}" ] && fl+=(--broker "$BROKER")
[ -n "${PRODUCER:-}" ] && fl+=(--producer "$PRODUCER")
[ -n "${PRODUCER_HOST:-}" ] && fl+=(--producer-host "$PRODUCER_HOST")
[ -n "${PROBE:-}" ] && fl+=(--probe "$PROBE")
for l in ${LAYERS:-}; do fl+=(--layer "$l"); done
[ -n "${REVERSE_ETL:-}" ] && fl+=(--reverse-etl "$REVERSE_ETL")
{
  echo "Components created for pipeline $id:"
  python3 "$py" ${fl[@]+"${fl[@]}"} --format text 2>/dev/null || echo "(diagram skipped: pipeline-diagram.py failed)"
} >&2
[ -n "${DIAGRAM_MD:-}" ] && python3 "$py" ${fl[@]+"${fl[@]}"} -o "$DIAGRAM_MD" 2>/dev/null
exit 0
