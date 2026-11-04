# 2026-11-03: Kafka through the source contract, producer on a laptop

The first run with the broker as a source: **lfc-source-eventhubs** `eventhubs.sh up` made the namespace and wrote the
source contract (engine `kafka`), and **lfc-kafka-freshness-test**'s `connection.sh`, `pipeline.sh` and the producer took
the bootstrap, SASL user, password file and topic from it (`SOURCE_JSON`). The subscription, region, resource group,
workspace and catalog came from the saved settings (**lfc-test-settings**); nothing was asked. Records:
`recall.py --run 2026-11-03-lfc-kafka-contract --all`.

**Workspace:** Azure FEVM, centralus; its own login worked. **Broker:** Azure Event Hubs Standard, Kafka endpoint, 4
throughput units with auto-inflate to 40, 32 partitions, in an existing resource group (not created or changed).
**Pipeline:** serverless, continuous, channel `PREVIEW`, `starting_offset` `latest`, key `STRING`, value `VARIANT`.
**Producer:** `producer.py` on a macOS laptop, Kafka only, 10 streams. **Workload:** 200 assets x 15 tags x 1 Hz, 0.1%
duplicates, 4 minutes, 10x for 1 minute from minute 1.5. **Freshness:** a serverless SQL warehouse (Small) made for the
run, every 2 s.

## Results

| Measure | Value |
|---|---|
| Sent = landed | 2,326,703 rows; 0 errors |
| Distinct events | 2,324,347 sent (2,356 duplicates sent on purpose), 2,324,347 landed: 0 lost, 0 extra copies |
| Rate in the data | median 3,008/s; burst mean 29,789/s, peak 31,469/s |
| Commits | 61 `STREAMING UPDATE`, 2 to 7 s apart (p50 4 s); 3 `OPTIMIZE` |
| Freshness p50 / p90 / max | whole run 3.9 / 5.9 / 7.8 s; before 3.8 / 5.7 / 5.9 s; during 5.8 / 5.9 / 7.8 s; after 3.9 / 4.2 / 6.1 s |

With `dups_sent` in the run file, `report.py --id-column` tells lost events from extra copies: here both were 0. The
15-minute run before the move (`2026-11-03-lfc-kafka-laptop.md`) landed 330 rows more than it sent, which that run's
producer couldn't yet separate.

## Teardown

The pipeline, the schema, the connection, the warehouse, the namespace (by its `CreatedBy` tag), the password and the
contract file were removed; the resource group's tags were unchanged.
