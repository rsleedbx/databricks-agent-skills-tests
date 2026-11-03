# 2026-10-29: zerobus-sdk-freshness-test scripts, end to end

The scripts as committed in [zerobus-sdk-freshness-test](../skills/zerobus-sdk-freshness-test/SKILL.md), run end to end: `table.sql`, `producer.py`, `freshness_probe.py` and `report.py` from a laptop, and `producer.py` as a job. Records:
`recall.py --run 2026-10-29-zerobus-sdk-scripts --all`.

**Workspace:** Azure FEVM, centralus; a new table from `table.sql`, the service principal granted directly on it.
**Producer:** a laptop (macOS) over the internet to the region, `databricks-zerobus-ingest-sdk` 1.10.0.
**Workload:** 50 assets x 15 tags x 1 Hz over 2 streams for 2 minutes, 5x for 30 s from minute 1.

| Measure | Value |
|---|---|
| Sent = landed | 178,082 |
| Rate in the data | median 751/s, burst mean 3,758/s, peak 3,762/s |
| Write commits | 24, 5 s apart |
| Freshness p50 / p90 / max | run 3.3 / 5.2 / 5.3 s; before the burst 3.3 s, during 3.2 s, after 3.3 s p50 |

The first version recorded the run's end after the producer's last progress line, 10 s after its last send, so samples
from that idle tail entered the "after" phase (max 10.3 s). `producer.py` now records the time of its last send.

## As a job

`producer.py` as a one-time job, as in [run-as-job.md](../skills/zerobus-sdk-freshness-test/references/run-as-job.md):
a single 8-core node, DBR 17.3 LTS, the secret from a secret scope through `spark_env_vars`, `run.json` written to a
Volume. 200 assets x 15 tags x 1 Hz over 2 streams for 2 minutes, no probe.

| Run | Sent = landed | Write commits | Task |
|---|---|---|---|
| 1 | 360,629 | 25, 5 s apart | failed: the script ended with `sys.exit(0)`, which a Python task counts as a failure |
| 2, after the fix | 357,623 | 24, 5 s apart | SUCCESS |


## Kafka fan-out

`producer.py --no-zerobus --kafka-bootstrap ...` against a bootstrap address with nothing listening, 5 assets for 12 s:
901 events queued, none delivered. The first version counted them as sent with 0 errors, because the 60 s flush timed
out silently; now the messages still queued after the flush count as Kafka errors (901) and the run exits 1. A
Zerobus-only run after the change: 8,707 sent = landed. Not yet run against a live broker.
