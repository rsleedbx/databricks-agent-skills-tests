# Calibrate statement size and sessions

Each engine and version has its own best statement size (rows per `INSERT ... SELECT`) and its own point where more sessions stop helping, and bulk inserts can flood the disk. **Before a big load (about 100M rows or more), measure both on the actual server**, then use the winners for the whole load. For smaller loads, start from the measured defaults below.

## Measured (4 vCPUs, Podman VM, local NVMe, 12 columns, `order=random`, `data=scramble`)

| Engine | Best rows per statement | Best sessions | Throughput | Bottleneck |
|---|---|---|---|---|
| PostgreSQL 16 | **1M–2M** (≈112k rows/s in one session; 4M dips slightly) | **4** (242,542 rows/s); 8 is 28% slower | CPU: run queue ≈ CPUs, disk util ≈ 20% | One core per INSERT |
| MySQL 8.4 | **100k** (33,383 rows/s; 25k: 14,529; 50k: 24,181; 250k: 27,353; 500k into a 4M-row table: 11,651) | **1–2**: 1, 2, and 4 sessions all ran about 30k rows/s (30,180; 29,849; 30,720); 8 dropped to 21,014 | ≈30k rows/s with 100k-row statements | Disk: from 1 to 8 sessions iowait rose 8% → 42% and util 23% → 67%, with no gain up to 4 and a 32% loss at 8; larger statements raise iowait too |

**MySQL slows down as the table grows**: 250k-row statements ran at 27,353 rows/s while the table stayed under 1M rows, and at 16,055 rows/s up to 4M rows (iowait 17% → 23%), once the table and its random-order key index outgrew the buffer pool. Calibrate with a table near the size the load will reach, not an empty one.

## What to watch while it runs

| Signal | Where | Healthy | Too much |
|---|---|---|---|
| Run queue `r` | `vmstat 1` (first column) | About the number of CPUs | Well above the CPUs: more sessions only queue |
| iowait `wa` | `vmstat 1` | Under about 20% | Rising with each added session: the disk is the limit |
| Disk await | `/proc/diskstats`: Δ(read ms + write ms) ÷ Δ(reads + writes) | Steady, low single-digit ms | Climbing: the device queue is full |
| Disk util | `/proc/diskstats`: Δ(io ms) ÷ 10 per second | Under about 80% | Near 100%: saturated |

Fedora CoreOS (the `podman machine` VM) has no `iostat`, so the script reads `/proc/diskstats` directly. Stop adding sessions at the first count that doesn't raise rows/s, and drop back one if `r` or await jumped there.

## Run it

Inside the database's VM (`podman machine ssh`, the Lima VM, or the cloud VM), with the engine's cast and insert planners from [postgres.md](postgres.md) or [mysql.md](mysql.md) in `/tmp` as `pg-cast.sql` / `pg-plan-insert.sql` or `my-cast.sql` / `my-plan-insert.sql`, a `cal` schema (database) of 8 tables made with `make-dataset`, and a seed table at least as large as the biggest statement size. Containers are named `dg-pg` and `dg-my`; change `SQL=` for others.

Copy [`scripts/calib.py`](../scripts/calib.py) and the engine's planner directory to the VM, then (with `DIR` set to it):

```bash
sudo DIR=/tmp/postgres python3 /tmp/calib.py pg size                                     # Postgres: 250k ... 4M rows per statement
sudo DIR=/tmp/mysql SIZES="25000 50000 100000 250000" TOTAL=1000000 python3 /tmp/calib.py my size   # MySQL: start small
sudo DIR=/tmp/postgres B=1000000 PER=2000000 python3 /tmp/calib.py pg sessions           # then sessions at the winning size
sudo DIR=/tmp/mysql B=100000 PER=1000000 python3 /tmp/calib.py my sessions
```

`DEV` is the data disk's name in `/proc/diskstats` (`vda` in `podman machine`; `sdc` or `nvme1n1` on a cloud VM: check `lsblk`).

**Run the sweeps in the background and check them early** (**watch-progress**): look at the first statement within a minute, and stop a sweep as soon as its points have decided the answer. If each larger size is slower than the last, the larger sizes won't win; sweep smaller ones instead.
