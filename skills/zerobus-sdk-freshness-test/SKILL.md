---
name: zerobus-sdk-freshness-test
description: "Measure Zerobus Ingest from a direct SDK producer: write simulated tag observations (assets x tags x Hz, optionally with a burst, over one or more streams) to a managed Delta table with the Python SDK, sample freshness (the warehouse's clock minus the newest visible event) on one or more paths at the same moments, then report rows sent vs landed, the per-second rate and burst seen in the data, the table's commit intervals, and freshness before, during and after the burst. Use to measure event-to-queryable time, throughput or burst behavior for a non-CDC producer, or to compare read paths on the same table."
parent: zerobus-workspace-setup
---

# Zerobus SDK producer: throughput, bursts and freshness

The other Zerobus tests here capture database changes (**zerobus-debezium-test**). This one is for a producer that
pushes events itself, such as an IoT gateway: no source database, just a stream of narrow events
and the question of how soon each one can be queried in Delta.

| Script | Does |
|---|---|
| [`scripts/table.sql`](scripts/table.sql) | The target table: a managed Delta table with the producer's columns |
| [`scripts/producer.py`](scripts/producer.py) | Sends `assets x tags x hz` events/s through N Zerobus streams for M minutes, with an optional burst (`--burst-at`, `--burst-minutes`, `--burst-x`) and duplicates (`--dup-rate`). With `--kafka-*`, the same events at the same moments to a Kafka topic too (`--no-zerobus`: Kafka only), for comparing a Kafka path on identical events. Writes `run.json`: rows sent, errors, run and burst windows |
| [`scripts/freshness_probe.py`](scripts/freshness_probe.py) | Every `--interval` s on each `--path` (warehouse + table + time column), in parallel: `current_timestamp()` minus the newest event time from the last 2 minutes, in ms. Writes `probe.json` |
| [`scripts/report.py`](scripts/report.py) | For one run: sent vs landed, per-second rate (median, peak, burst mean) from the data, the commits in `DESCRIBE HISTORY` (operations, write interval), and freshness per path for the run and before, during and after the burst. `--json` gives the metrics for a record |

Run any script with no arguments for its usage.

## Workflow

1. **Prepare the workspace** with **zerobus-workspace-setup**: `check.sh` for the previews, the table and the network
   path; `sp.sh` if you need a service principal and its secret file; `grant.sh` on the table.
2. **Create the table** from `table.sql` (replace `<catalog>.<schema>.<table>`), then grant the service principal on it.
3. **Start the probe first,** for longer than the run, so it sees the start and the end:

   ```bash
   S=skills/zerobus-sdk-freshness-test/scripts; P=<PROFILE>; T=<catalog>.<schema>.<table>
   uv run $S/freshness_probe.py $P --path "serverless|<warehouse-id>|$T" --seconds 1200 --out probe.json &
   ```

   A second `--path` on another warehouse samples the same table through it at the same moments.
4. **Run the producer** where the real producer would run. From a laptop, the network to the region is part of what
   you measure. In the workspace's region, run it as a job ([references/run-as-job.md](references/run-as-job.md)).

   ```bash
   uv run $S/producer.py --endpoint <workspace-id>.zerobus.<region>.<cloud domain> --workspace-url https://<workspace host> \
     --table $T --client-id <application id> --secret-file <0600 file> \
     --assets 200 --tags 15 --hz 1 --minutes 15 --streams 10 --burst-at 5 --burst-minutes 2 --burst-x 10 --out run.json
   ```

5. **Report:**

   ```bash
   uv run $S/report.py $P --warehouse <warehouse-id> --table $T --run run.json --probe probe.json
   uv run $S/report.py $P --warehouse <warehouse-id> --table $T --run run.json --probe probe.json --json   # metrics for a record
   ```

6. **Record** the run with **lfc-test-results** `record.py` (`test: load`, `setup.route: zerobus-sdk`; the fields are
   below), and write `results/<run>.md`.

## Sizing a run

- **Rate:** `assets x tags x hz` events/s, plus one fault event per asset and tick while that asset has a fault. A burst
  multiplies the sampling rate, so the same assets send `--burst-x` times as often.
- **Streams:** the assets are split evenly between `--streams` threads, one Zerobus stream each. Check the per-stream
  limit on the [Zerobus limits page](https://docs.databricks.com/ingestion/zerobus-quotas) and use enough streams
  that the burst rate per stream stays under it.
- **Producer host:** one Python process builds and sends every stream. Its progress lines show the rate it reached; if
  that's short of the target, check the host's CPU (building events) and whether `ingest_records_nowait` calls wait
  (the SDK's in-flight limit) before reading the run as a test of the target rate.

## Reading the results

- **Freshness** is measured on the warehouse's clock against `producer_time`, which the producer stamps when it hands
  the event to the SDK. So it includes the SDK, the network, the commit to Delta and the warehouse seeing that commit.
  Clock skew between the producer host and the workspace adds to it or subtracts from it; run the producer in-region.
- **The first seconds and the tail:** `report.py` leaves out the first 15 s of the run (`--warmup`) and everything after
  the producer's last send. Samples after the end grow by a second per second, because nothing new arrives.
- **Commit intervals** come from `DESCRIBE HISTORY`, whose timestamps are whole seconds, so each interval is +/- 1 s.
  Other operations in the window (`OPTIMIZE`, from Predictive Optimization or by hand) are counted separately.
- **Landed** counts every row in the run window, duplicates included, as the producer counts what it sent.
  `count(DISTINCT event_id)` gives the events without the duplicates.
- **Repeated benchmark queries** (not this probe) can be answered from the serverless result cache. Add a unique
  comment or literal to each statement when you time queries.
- **The run window** comes from `run.json`, or for a job, from the run's own time and the data
  (`report.py --window`). A window taken from logs that arrived late can make a normal end look like a stall.

## Recording a run

One record per producer run (`test: load`):

| Field | Content |
|---|---|
| `subject` | `{"engine": "zerobus", "interface": "python-sdk", "sdk": "databricks-zerobus-ingest-sdk 1.10.0"}` |
| `host` | where the producer ran: `databricks-job` (same region) or `laptop` |
| `setup.route`, `setup.producer` | `zerobus-sdk`, `zerobus-python-sdk` |
| `setup.producer_host` | the machine: `Databricks job, single node 8 cores, same region` |
| `setup.workload` | `{"name": "tag-simulator", "assets": 200, "tags": 15, "hz": 1, "dup_rate": 0.001, "streams": 10, "minutes": 15, "burst": {"at_min": 5, "minutes": 2, "x": 10}}` |
| `setup.probe` | the warehouse (type, size, clusters) and the interval |
| `setup.fanout` | `Zerobus only`, or the other target the same process wrote to |
| `metrics` | from `report.py --json`: rows sent and landed, rate, commits, freshness by phase |

## Common Issues

| Symptom | Likely cause | Fix |
|---|---|---|
| Error 4024 or a permission error when the stream opens | The grants are inherited, not on the table | `zerobus-workspace-setup` `grant.sh --apply` |
| The producer's rate is below the target during a burst | The host's CPU, or the SDK waiting on in-flight records | A larger host, more processes or streams, or a lower `--burst-x`; record the rate reached |
| `Kafka messages not delivered` and exit 1 | The broker refused or never answered: bootstrap, port 9093, SASL user or password | Check them; messages still queued after the 60 s flush count as errors |
| Every probe sample is null | No event in the last 2 minutes: the producer hasn't started, has stopped, or writes another table | Check `--path`'s table and column |

## Tested

Runs and their write-ups: `recall.py --test load --all`, and `results/2026-10-13-zerobus-sdk-steady.md`,
`results/2026-10-22-zerobus-sdk-burst.md` and `results/2026-10-29-zerobus-sdk-burst.md`. Those runs used earlier
versions of these scripts (a notebook producer with the same event shape, and a commit monitor). The scripts as
committed here: `results/2026-10-29-zerobus-sdk-scripts.md` (a 2-minute laptop run with a burst, and the job path; the Kafka fan-out only against a broker that doesn't exist).
