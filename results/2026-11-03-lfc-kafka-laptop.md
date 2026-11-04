# 2026-11-03: Kafka (Event Hubs) -> Lakeflow Connect -> Delta, producer on a laptop

One 15-minute run of **lfc-kafka-freshness-test** as committed, end to end: `eventhubs.sh`, `connection.sh` and
`pipeline.sh` created the broker, the connection and the pipeline; **zerobus-sdk-freshness-test**'s producer sent to
Kafka only (`--no-zerobus`) from a laptop; teardown removed only what the run made. Records:
`recall.py --run 2026-11-03-lfc-kafka-laptop --all`.

**Workspace:** Azure FEVM, centralus. **Broker:** Azure Event Hubs Standard, Kafka endpoint, 4 throughput units with
auto-inflate to 40, one event hub with 32 partitions, keyed by `asset_id`, in an existing resource group (not created
or changed). **Connection:** UC `KAFKA`, SASL PLAIN. **Pipeline:** serverless, continuous, channel `PREVIEW`,
`starting_offset` `latest`, key `STRING`, value `VARIANT`; DBR 19.x, dlt:19.12.6 build 20261023
(`bin/lfc-runtime.sh --summary`). **Producer:** `producer.py` on a macOS laptop, 10 streams, over the internet to
centralus. **Workload:** 200 assets x 15 tags x 1 Hz, 0.1% duplicates, 10x for 2 minutes from minute 5.
**Freshness:** a serverless SQL warehouse (Small, 1 cluster) every 2 s; the workspace had none, so the run made one and
deleted it after.

The diagram (**lfc-pipeline-diagram**) was drawn from `pipeline.sh`'s spec before anything was created, and again by
`pipeline.sh` from the created pipeline; the two matched:

```text
model tag-simulator: 1 table, 200 assets x 15 tags x 1 Hz = 3,000/s; 10x for 2 min at minute 5
        | simulates
Laptop: producer.py, 10 streams, Kafka only (--no-zerobus)
        | Kafka produce, SASL PLAIN
Azure: Event Hubs (Kafka endpoint)
  Standard, 4-40 TU auto-inflate, 32 partitions, 24 h
  topic tag-observations
        |
Databricks (Azure)
  connection <connection> (KAFKA, SASL PLAIN)  <-- stored in the connection
        |
  ingestion pipeline <pipeline> (serverless, continuous, channel PREVIEW): Kafka consumer from offset latest
        |
  streaming table (append-only): <catalog>.<schema>.tag_observations: key STRING, value VARIANT (JSON)
        |
  freshness_probe.py: SQL warehouse, every 2 s
```

## Results

| Measure | Value |
|---|---|
| Sent | 5,934,943, 0 errors, 0 Kafka errors |
| Landed | 5,935,273 rows: 5,928,974 distinct event ids, 6,299 of them twice, none more |
| Rate in the data | median 3,005/s; burst mean 29,938/s, peak 31,195/s |
| Commits | 225 `STREAMING UPDATE`, 1 to 8 s apart (p50 4 s); 15 `OPTIMIZE` |

Freshness (p50 / p90 / max):

| Phase | This run (laptop, Kafka only) | 2026-10-29 (job in the region, Kafka and Zerobus) |
|---|---|---|
| Whole run | 3.9 / 5.3 / 29.7 s | 3.4 / 5.0 / 6.5 s |
| Before the burst | 4.6 / 5.1 / 29.7 s | 3.9 / 4.3 / 6.3 s |
| During | 5.1 / 7.2 / 11.1 s | 4.3 / 6.2 / 6.5 s |
| After | 3.7 / 5.0 / 6.1 s | 3.2 / 5.0 / 5.4 s |

- **The burst reached 30,000/s** with one process writing only Kafka; on 10-29 one process writing Kafka and Zerobus
  reached about 21,000/s.
- **330 more rows than sent.** The producer sends 0.1% of events twice on purpose (at-least-once upstream); 6,299 ids
  landed twice. Its run file didn't count those duplicates yet, so the extra copies (Kafka delivers at least once) and
  any lost events can't be told apart exactly: sent minus distinct landed is 5,969, the expected number of deliberate
  duplicates is about 5,930. `producer.py` now writes `dups_sent` and `report.py` reports lost events and extra copies.
- **The 29.7 s maximum is the first two minutes**, as the new pipeline's first commits came in; after that the worst
  outside the burst was 6 to 7 s.

## What broke

- **No SQL warehouse in the workspace** for the probe: one was created (Small, serverless, auto-stop 10 minutes) and
  deleted after. The skill now says so.
- **The workspace login had expired.** **account-token-fallback** (`ws-token.sh`) gave a token from the Azure CLI; no
  login was needed.
