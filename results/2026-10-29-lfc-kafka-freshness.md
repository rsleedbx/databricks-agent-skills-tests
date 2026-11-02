# 2026-10-29: Kafka (Event Hubs) -> Lakeflow Connect -> Delta, with a 10x burst

One 15-minute run through the Lakeflow Connect Kafka connector (Beta), with the same events written to Zerobus by the
same process at the same moments, and a check of the skill's connection and pipeline scripts. Records:
`recall.py --run 2026-10-29-lfc-kafka-freshness --all`.

**Workspace:** Azure FEVM, centralus. **Broker:** Azure Event Hubs Standard, Kafka endpoint, 4 throughput units with
auto-inflate to 40, one event hub with 32 partitions, keyed by `asset_id`. **Connection:** UC `KAFKA`, SASL PLAIN.
**Pipeline:** serverless, continuous, channel `PREVIEW`, `starting_offset` `latest`, key `STRING`, value `VARIANT`;
DBR 19.x, dlt:19.12.6 build 20261023 (`bin/lfc-runtime.sh --summary`). **Producer:** a Databricks job on a single
8-core node in the same region, writing each event to Zerobus and to Kafka (confluent-kafka). **Workload:** 200 assets x
15 tags x 1 Hz, 0.1% duplicates, 10x for 2 minutes from minute 5. **Tooling:** earlier versions of
**zerobus-sdk-freshness-test**'s producer and probe; metrics recomputed with its `report.py --window --column
'timestamp_micros(value:<time field>::bigint)'`. **Freshness:** a serverless SQL warehouse (Small, 1 cluster) every 2 s.

## Results

| Measure | Value |
|---|---|
| Run (UTC) | 03:45:56 to 04:00:56; burst 03:50:56 to 03:52:56 |
| Sent = landed | 4,843,248 (the Zerobus table landed the same count) |
| Rate in the data | median 3,004/s; burst mean 20,828/s, peak 22,039/s |
| Commits | 225 `STREAMING UPDATE`, 2 to 6 s apart (p50 4 s); 11 `OPTIMIZE` |

Freshness (p50 / p90 / max):

| Phase | Kafka -> Lakeflow Connect | Zerobus, same events |
|---|---|---|
| Whole run | 3.4 / 5.0 / 6.5 s | 3.0 / 5.0 / 5.7 s |
| Before the burst | 3.9 / 4.3 / 6.3 s | 3.2 / 5.2 / 5.4 s |
| During | 4.3 / 6.2 / 6.5 s | 3.1 / 5.1 / 5.2 s |
| After | 3.2 / 5.0 / 5.4 s | 2.9 / 4.9 / 5.7 s |

The Zerobus column is run D of `2026-10-29-zerobus-sdk-burst` (pull request #61), sampled at the same moments.
With both outputs in one process, the burst reached about 21,000 events/s on average, short of the 30,000/s target.

## What broke

- **The first pipeline spec was refused at create.** It had `value_transformer: {format: JSON, json_options:
  {schema_evolution_mode: rescue}}`: `JSON transformer options were specified but no parsing configuration was provided.
  Specify at least one of 'schema', 'schema_file_path', or 'schema_evolution_mode', or omit json_options entirely to
  parse the value as a Variant column.` Without `json_options` it was accepted, and the value landed as `VARIANT`.
  `pipeline.sh` writes that spec.

## The scripts as committed

`connection.sh` and `pipeline.sh` were run against a bootstrap server that doesn't exist, because no Azure resource
group was available for a new Event Hubs namespace that day. The connection was created; the continuous pipeline
started on its own (`WAITING_FOR_RESOURCES` to `RUNNING` in 27 s) and its flow failed with `[STREAM_FAILED] ...
Failed to create new KafkaAdminClient SQLSTATE: XXKST`; it was then stopped. `eventhubs.sh` wasn't run as committed.
