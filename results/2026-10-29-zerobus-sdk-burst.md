# 2026-10-29: Zerobus SDK producer, 10x burst, with OPTIMIZE and with a second fan-out

Four 15-minute burst runs (3,000 events/s, 10x for 2 minutes from minute 5) over 10 Zerobus streams. One ran an
`OPTIMIZE` during the burst; three also wrote every event to Event Hubs from the same process. Records:
`recall.py --run 2026-10-29-zerobus-sdk-burst --all`.

**Workspace:** Azure FEVM, centralus. **Producer:** a Databricks job on a single 8-core node in the same region, the
Python SDK (`databricks-zerobus-ingest-sdk` >= 1.10.0), JSON records. **Workload:** 200 assets x 15 tags x 1 Hz, 0.1%
duplicates. **Tooling:** earlier versions of [zerobus-sdk-freshness-test](../skills/zerobus-sdk-freshness-test/SKILL.md);
metrics recomputed with `report.py --window`. **Freshness:** sampled on a serverless SQL warehouse (Small, 1 cluster)
every 2 s (the OPTIMIZE run: every 4 s, by the commit monitor).

| Run (UTC) | Also wrote to | Burst in the data | Sent = landed | Write commits | Freshness p50 / p90 / max |
|---|---|---|---|---|---|
| A, 00:43 to 00:58 | (none; OPTIMIZE during the burst) | mean 29,923/s, peak 30,689/s | 5,931,270 | 180, 4 to 6 s apart, + 1 OPTIMIZE | 2.9 / 5.1 / 5.5 s |
| B, 02:41 to 02:56 | Event Hubs | mean 21,761/s, peak 22,789/s | 4,951,972 | 180, 4 to 6 s apart | 4.9 / 8.9 / 10.0 s |
| C, 03:15 to 03:30 | Event Hubs | mean 22,248/s, peak 27,776/s | 5,009,547 | 180, 4 to 6 s apart | 6.6 / 8.6 / 10.0 s |
| D, 03:46 to 04:01 | Event Hubs | mean 20,828/s, peak 22,039/s | 4,843,248 | 180, 4 to 6 s apart | 3.0 / 5.0 / 5.7 s |

Freshness p50 by phase (before / during / after the burst):

| Run | Before | During | After |
|---|---|---|---|
| A | 3.0 s | 3.2 s | 2.7 s |
| B | 4.3 s | 3.2 s | 6.0 s |
| C | 7.5 s | 6.2 s | 6.1 s |
| D | 3.2 s | 3.1 s | 2.9 s |

- **Run A:** a manual `OPTIMIZE` on the table started 13 s after the burst began and finished 11 s later.
- **Runs B to D:** with the second fan-out, the producer reached 21,000 to 22,000 events/s on average during the burst,
  short of the 30,000/s target. Freshness in B and C was higher than in A and D, with the same commit intervals; the
  cause wasn't investigated.
- **Run D:** the Event Hubs copy fed a Lakeflow Connect Kafka pipeline, which is recorded separately.
