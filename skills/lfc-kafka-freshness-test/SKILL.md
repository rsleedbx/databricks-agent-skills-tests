---
name: lfc-kafka-freshness-test
description: "Measure the Lakeflow Connect Kafka connector (Beta) from a Kafka topic into a Delta table: create an Azure Event Hubs namespace with its Kafka endpoint, a Unity Catalog KAFKA connection, and a serverless continuous ingestion pipeline (value as VARIANT), send simulated events (optionally the same events to Zerobus at the same moments), then report rows sent vs landed, the per-second rate and burst seen in the data, the table's commits, and freshness before, during and after a burst. Use to measure event-to-queryable time or burst behavior through Kafka, or to compare it with a direct Zerobus producer on identical events."
parent: zerobus-sdk-freshness-test
---

# Kafka (Event Hubs) -> Lakeflow Connect -> Delta: freshness and bursts

The producer, the freshness probe and the report come from **zerobus-sdk-freshness-test**: `producer.py` sends to a
Kafka topic with `--kafka-*` (and to Zerobus at the same moments unless `--no-zerobus`), and `freshness_probe.py` and
`report.py` read the event time from inside the VARIANT value. This skill adds the broker, the connection and the
pipeline:

| Script | Does | Changes anything |
|---|---|---|
| [`scripts/eventhubs.sh`](scripts/eventhubs.sh) | `up`: an Event Hubs namespace (Standard, Kafka on, auto-inflate), one event hub as the topic, a Listen+Send rule; its connection string into a 0600 file. `down`: deletes the namespace | Only with `--apply` |
| [`scripts/connection.sh`](scripts/connection.sh) | A UC connection of type `KAFKA` (`bootstrap_servers`, `sasl_mechanism`, `user`, `password`); the password from the file into a 0600 request body, deleted after | Only with `--apply` |
| [`scripts/pipeline.sh`](scripts/pipeline.sh) | A serverless, continuous ingestion pipeline on channel `PREVIEW` into one table: key `STRING`, value `VARIANT` | Only with `--apply` |

Run any script with no arguments for its usage. Any Kafka broker with SASL PLAIN works for `connection.sh` and
`pipeline.sh`; `eventhubs.sh` is the Azure one.

## Workflow

1. **Broker:** `eventhubs.sh up <resource group> <namespace> <region> --password-file <0600 file> --apply`. Size it for
   the burst: about 1 throughput unit per 1,000 events/s or 1 MB/s, and enough partitions (`--partitions`) to spread it.
   With Event Hubs the SASL user is the literal `$ConnectionString` and the password is the connection string.
2. **Connection:** `connection.sh <PROFILE> <name> --bootstrap <namespace>.servicebus.windows.net:9093 --user '$ConnectionString' --password-file <file> --apply`.
3. **Pipeline:** create the schema first, then
   `pipeline.sh <PROFILE> --connection <name> --topic tag-observations --table <catalog>.<schema>.<table> --apply`.
   A continuous pipeline starts on its own; wait for its update to be `RUNNING` before sending (`starting_offset`
   `latest` skips anything sent earlier).
4. **Probe, then producer** (from **zerobus-sdk-freshness-test**):

   ```bash
   Z=skills/zerobus-sdk-freshness-test/scripts; P=<PROFILE>; T=<catalog>.<schema>.<table>
   uv run $Z/freshness_probe.py $P --seconds 1200 --out probe.json \
     --path "lfc_kafka|<warehouse-id>|$T|timestamp_micros(value:producer_time::bigint)" \
     --path "zerobus|<warehouse-id>|<zerobus table>" &
   uv run $Z/producer.py --kafka-bootstrap <namespace>.servicebus.windows.net:9093 --kafka-topic tag-observations \
     --kafka-user '$ConnectionString' --kafka-password-file <file> <the Zerobus options, or --no-zerobus> \
     --minutes 15 --streams 10 --burst-at 5 --burst-minutes 2 --burst-x 10 --out run.json
   ```

5. **Report** each table with the same run file:

   ```bash
   uv run $Z/report.py $P --warehouse <warehouse-id> --table $T --run run.json --probe probe.json \
     --column 'timestamp_micros(value:producer_time::bigint)'
   ```

6. **Stop the pipeline** (it runs until stopped): `databricks pipelines stop <pipeline-id> --profile <PROFILE>`, and
   `eventhubs.sh down <resource group> <namespace> --apply` when done.
7. **Record** with **lfc-test-results** `record.py`: `test: load`, `setup.route: lfc-kafka`, `compute` as for any
   pipeline test, and the runtime from `bin/lfc-runtime.sh --summary <PROFILE> <pipeline-id>`.

## Reading the results

- **Freshness** is the warehouse's clock minus the newest `producer_time` in the table, as in
  **zerobus-sdk-freshness-test**. Through Kafka it also includes the broker and the pipeline's own commits.
- **Commits:** a pipeline writes `STREAMING UPDATE` commits, and `OPTIMIZE` commits of its own between them;
  `report.py` counts both and times the `STREAMING UPDATE` ones.
- **Same events on two paths:** with Zerobus and Kafka from one `producer.py`, both tables get each event with the same
  `event_id` and `producer_time`, so the counts and freshness compare directly. One process writing both reaches a
  lower burst rate than one writing only Zerobus; record the rate the data shows.

## Common Issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `JSON transformer options were specified but no parsing configuration was provided. Specify at least one of 'schema', 'schema_file_path', or 'schema_evolution_mode', or omit json_options entirely to parse the value as a Variant column.` | `value_transformer` has `json_options`; seen with `{"schema_evolution_mode": "rescue"}` and no `schema` | Omit `json_options`: the value lands as `VARIANT`, as `pipeline.sh` does. A typed value with a `schema` wasn't tried here |
| The table stays empty while the producer sends | The pipeline started after the producer, with `starting_offset` `latest` | Start the producer once the update is `RUNNING`, or use `earliest` |
| The pipeline's flow fails: `[STREAM_FAILED] ... terminated with exception: Failed to create new KafkaAdminClient SQLSTATE: XXKST` | The connection's bootstrap server can't be reached (seen with a namespace that doesn't exist) | Check `bootstrap_servers` and port 9093 from serverless: `databricks connections get <name>` |
| The producer prints `Kafka messages not delivered` and exits 1 | The broker refused or never answered: bootstrap, port 9093, user or password | Check the connection string file and the namespace's network rules |

## Tested

`recall.py --test load --all` and `results/2026-10-29-lfc-kafka-freshness.md`: one 15-minute run with a burst on Azure,
Zerobus and Kafka written by the same process. That run used an earlier version of the producer with the same event
shape and the pipeline spec `pipeline.sh` writes. As committed, `connection.sh` and `pipeline.sh` created a connection
and a pipeline that started on its own, against a bootstrap that doesn't exist. `eventhubs.sh` reproduces the commands
of that run and hasn't been run as committed: no Azure resource group was available for a new namespace on 2026-10-29.
