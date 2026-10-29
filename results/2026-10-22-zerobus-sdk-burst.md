# 2026-10-22: Zerobus SDK producer, 10x burst over 10 streams

Two 15-minute runs: 3,000 events/s, then 10x for 2 minutes from minute 5, then 3,000 events/s again, over 10 Zerobus
streams. Records: `recall.py --run 2026-10-22-zerobus-sdk-burst --all`.

**Workspace:** Azure FEVM, centralus. **Producer:** a Databricks job on a single 8-core node in the same region, the
Python SDK (`databricks-zerobus-ingest-sdk` >= 1.10.0), JSON records, Zerobus only. **Workload:** 200 assets x 15 tags x 1 Hz,
0.1% duplicates. **Tooling:** earlier versions of [zerobus-sdk-freshness-test](../skills/zerobus-sdk-freshness-test/SKILL.md);
metrics recomputed with `report.py --window` from the table and the saved samples. **Freshness:** sampled on a
serverless SQL warehouse (Small, 1 cluster): `current_timestamp()` minus the newest event time.

| Run | Burst in the data | Sent | Landed | Write commits | Freshness p50 / p90 / max |
|---|---|---|---|---|---|
| 1 (19:51 to 20:06 UTC) | 19:56:11 to 19:58:11, mean 29,856/s, peak 30,393/s | 5,928,316 | 5,928,316 | 180, 4 to 6 s apart | not sampled (see below) |
| 2 (20:51 to 21:06 UTC) | 20:56:39 to 20:58:39, mean 29,850/s, peak 30,196/s | 5,922,250 | 5,922,250 | 180, 4 to 6 s apart | 2.8 / 4.9 / 5.5 s |

Run 2's freshness by phase (p50 / p90 / max): before the burst 2.8 / 4.9 / 5.4 s (42 samples), during 3.0 / 4.9 / 5.5 s
(18), after 2.7 / 4.8 / 5.5 s (71). Sampled by a commit monitor every 5 s.

**Run 1 is partial.** Its freshness probe ran from a laptop that lost its network during the run, so it has no usable
samples. Logs that arrived late also made the run look as if it had stopped early; the run's own start and end times and
the rows in the table show it ran its full 15 minutes. `report.py` takes the window from the run, not from logs, for
that reason.

An earlier run that day (19:35 to 19:43 UTC, one stream, 1,513,757 rows landed) was cancelled by hand and isn't recorded.
