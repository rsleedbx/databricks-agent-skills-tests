#!/usr/bin/env bash
# pipeline.sh <profile> --connection <name> --topic <topic> --table <catalog.schema.table> [--name <pipeline name>]
#             [--starting-offset latest|earliest] [--apply]
#   A serverless, continuous Lakeflow Connect Kafka pipeline into one table: the key as STRING, the value as VARIANT
#   (value_transformer JSON without json_options). The connector is Beta, so the pipeline runs on channel PREVIEW. Prints
#   the spec; with --apply creates it (a continuous pipeline starts on its own) and prints its id. It runs until stopped:
#   databricks pipelines stop <pipeline-id> --profile <profile>. The catalog and schema must exist.
set -uo pipefail
P=${1:-}; [ -n "$P" ] || { sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
shift; CONN=; TOPIC=; TABLE=; NAME=; OFF=latest; APPLY=no
while [ $# -gt 0 ]; do case $1 in --connection) CONN=$2; shift 2;; --topic) TOPIC=$2; shift 2;; --table) TABLE=$2; shift 2;;
  --name) NAME=$2; shift 2;; --starting-offset) OFF=$2; shift 2;; --apply) APPLY=yes; shift;; *) echo "unknown $1"; exit 2;; esac; done
[ -n "$CONN" ] && [ -n "$TOPIC" ] && [ -n "$TABLE" ] || { echo "--connection, --topic and --table are required"; exit 2; }
IFS=. read -r CAT SCH TBL <<< "$TABLE"
[ -n "$TBL" ] || { echo "--table must be catalog.schema.table"; exit 2; }
SPEC=$(jq -n --arg name "${NAME:-lfc-kafka-$TBL}" --arg conn "$CONN" --arg cat "$CAT" --arg sch "$SCH" --arg tbl "$TBL" --arg topic "$TOPIC" --arg off "$OFF" '{
  name: $name, serverless: true, continuous: true, channel: "PREVIEW", catalog: $cat, schema: $sch,
  ingestion_definition: {connection_name: $conn, objects: [{table: {source_table: "N/A",
    destination_catalog: $cat, destination_schema: $sch, destination_table: $tbl,
    connector_options: {kafka_options: {topics: [$topic], starting_offset: $off,
      key_transformer: {format: "STRING"}, value_transformer: {format: "JSON"}}}}}]}}')
echo "$SPEC"
[ $APPLY = yes ] || { echo "plan only: run again with --apply"; exit 0; }
databricks pipelines create --json "$SPEC" --profile "$P" -o json | jq -r '"pipeline " + .pipeline_id'
