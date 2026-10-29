# 2026-10-13: Zerobus SDK producer, steady 3,000 events/s on one stream

A direct SDK producer writing simulated tag observations to one managed Delta table for 30 minutes, on one Zerobus
stream. Records: `recall.py --run 2026-10-13-zerobus-sdk-steady --all`.

**Workspace:** Azure FEVM, centralus, classic and serverless compute. **Producer:** a Databricks job on a single 4-core
node in the same region, the Python SDK (`databricks-zerobus-ingest-sdk` >= 1.10.0 from PyPI), JSON records,
`ingest_records_nowait`. Every event was also written to Event Hubs by the same process. **Workload:** 200 assets x 15 tags
x 1 Hz, 0.1% duplicates. **Tooling:** an earlier version of [zerobus-sdk-freshness-test](../skills/zerobus-sdk-freshness-test/SKILL.md)
(a notebook producer with the same event shape); landed rows, the rate and the commits were read again from the table
with `report.py --window` on 2026-10-29.

| Measure | Value |
|---|---|
| Rows landed | 5,403,122 over 30 min (15:36:15 to 16:06:14 UTC) |
| Rate in the data | median 3,003/s, peak 3,010/s |
| Write commits | 360, 4 to 6 s apart (p50 5 s); 1 OPTIMIZE in the window |
| Freshness | p50 3.4 s, p90 5.3 s |

**Partial:** the producer's sent count and the raw freshness samples weren't kept. Sent vs landed can't be checked,
and freshness has only the p50 and p90 noted at the time.
