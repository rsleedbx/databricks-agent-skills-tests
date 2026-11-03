---
name: zerobus-debezium-test
description: "Test database change data capture through Debezium Server's Zerobus sink: CockroachDB in Podman on a cloud VM, a snapshot as of T into a Volume, Debezium Server 3.7 streaming the changes from T through Zerobus into one append-only bronze change log, and silver AUTO CDC tables built from the source schema read through a UC connection. Checks that silver equals the source after inserts, updates and deletes, records the versions, and cleans up. Silver is kept both ways, current rows (SCD type 1) and every version (SCD type 2), checked against the source's own changefeed. Then gold, a materialized view, and three harder rounds: Debezium killed and restarted, a column added, and a column dropped and another retyped. Phases 1 to 3 of the Debezium -> Zerobus -> bronze -> silver -> gold test."
parent: lfc-source-cloud-vm
---

# Debezium -> Zerobus -> bronze -> silver -> gold

[scripts/debezium-zerobus.sh](scripts/debezium-zerobus.sh) runs the whole test from your laptop. It drives a cloud VM over SSH, and the VM runs the source database and Debezium Server in Podman. Debezium pushes each change to Zerobus, which appends it to one change-log Delta table for all source tables. Debezium needs only outbound 443 to the Zerobus endpoint. Phase 2 adds a snapshot as of T, taken with `EXPORT INTO PARQUET` and copied into a Volume, and a silver pipeline. The pipeline is generated from the source's schema, read through a UC connection, so serverless reaches CockroachDB on `CRDB_PORT`. Phase 3 adds gold and rounds 3 and 4. `PHASE=1` stops after bronze, `PHASE=2` after silver.

This isn't Lakeflow Connect. It's for sources Lakeflow Connect doesn't cover, or for teams that want the change log itself as an append-only bronze table.

## What it creates, and removes at the end

The Databricks objects and VM containers a run creates, with their names, and what removes each one: [references/resources.md](references/resources.md).

The OAuth secret is never printed. It goes from the CLI's response into Debezium's `application.properties` in 0600 temp files, on the laptop and on the VM, which are deleted when the run ends. `KEEP=1` keeps everything for a look afterwards.

## Models

One canonical `schema.yaml` per model (statschema's format, **lfc-source-statschema**) drives the whole run, and no script names a table or a column. `MODEL=<name>` reads [models/](models/)`<name>.yaml`, or **lfc-source-statschema**'s `schemas/<name>.yaml` (`tpcb`, `tpcc`, `tpch`, `tpcds`, `tpce`, `tpcdi`). `shop`, the default, is [models/shop.yaml](models/shop.yaml). `SF` (default 0.001) scales the TPC models' `row_count_per_sf`.

| Step | Script |
|---|---|
| Size it, and tag each column for the data generator | **lfc-source-statschema** `shape.py --sf $SF` |
| The DDL | **lfc-source-statschema** `ddl.py --dialect cockroachdb` or `postgres` |
| Fill it, before T | **lfc-source-datagen** `fill.py pg`, in a container on the run's Podman network |
| The changes of rounds 1 to 3, the schema changes of rounds 4 and 5, and the queries every check compares | [scripts/model-sql.py](scripts/model-sql.py) |
| Silver | [scripts/silver-from-schema.py](scripts/silver-from-schema.py), from the canonical schema read back from the source |
| Gold | `models/<name>-gold.sql`, when there is one: `-- name:`, `-- cluster_by:`, `-- comment:` header lines and a query on the model's tables. No file, no gold claims |

The workload each round runs, what rounds 1 to 3 and 4 and 5 change, and how both sides are rendered for comparison: [references/rounds.md](references/rounds.md).

## What a production pipeline meets

Behaviors the tests found in the sources, Debezium, and the bronze and silver layers, each with the fix the harness uses and the combination it applies to: [references/production-findings.md](references/production-findings.md).

## Many sources, bronze layouts, schema drift

Many sources (`NSRC`): the producers, the bronze layouts (`shared`, `fanout`, `per-table`), moving a table, and schema drift with its evolution: [references/many-sources.md](references/many-sources.md).

## Many tables: silver by pipeline or by MERGE

`MIX=N` builds N tables from copies of the TPC models (`scripts/mix-models.py`: `tpcb001_branch` ... `tpcdi085_DimMessage`, 95 tables a copy). The settings for this test:

| Setting | What it does |
|---|---|
| `EMPTY=1` | Creates the tables with no rows (no fill.py, and the datagen tags are left out of the DDL) |
| `FKS=0` | Leaves the foreign keys out: CockroachDB takes much longer to add a foreign key than to create a table |
| `ROUNDS=0` | No changes. The run stops after the snapshot and silver's first update, then checks every silver table at once (columns and row counts against the source), times a second update with nothing new, and checks that each producer is streaming |
| `PIPES=K` | K silver pipelines (or K MERGE jobs), each with the tables of `crc32(name) % K` |
| `SPLIT=1` | With `SILVER=merge` and `PIPES=K`: job k writes its silver tables to their own schema, `<S>_s<k>` (bronze and the volume stay in `<S>`) |
| `SILVER=merge` | Silver without a pipeline: `scripts/merge-silver.py` as a serverless job, with MERGE instead of AUTO CDC (`BRONZE=shared` only so far) |
| `DBZ_SHARDS=K` | K Debezium Servers per source, each with the tables of `crc32(name) % K`, started one at a time (each waits up to `DBZ_START_WAIT` s for the one before) |

`ROUNDS=0 PHASE=1` runs the producers only.

`SILVER=merge`, the table counts measured on the Azure VM, the producer limit, and the composite placement file: [references/many-tables.md](references/many-tables.md).

## MySQL and an application's schema (`SOURCE=mysql APP=mautic`)

Foreign key actions that MySQL through 9.5 and MariaDB don't log, how silver applies them, and the Mautic test: [references/mysql-mautic.md](references/mysql-mautic.md).

## PostgreSQL, SQL Server, Oracle, Db2, Informix and MySQL with the TPC models (`SOURCE=pg`, `mssql`, `oracle`, `db2`, `informix`, `mysql`)

Each engine's change capture, T and sequence, snapshot, rounds, and what the tests found in its clients and types: [references/engines.md](references/engines.md).

### Many sources and models at once: `matrix.sh`

[scripts/matrix.sh](scripts/matrix.sh) runs every combination of `SOURCES` × `MODELS` × `NSRCS` (how many source databases merge into one silver), at most `PARALLEL` at once:

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> SOURCES="pg mssql oracle" MODELS="tpch tpcb tpcc" NSRCS="1 2 4" PARALLEL=2 \
  bash skills/zerobus-debezium-test/scripts/matrix.sh
```

Isolation, output, coverage records, choosing `PARALLEL`, and the laptop-sleep caveat: [references/matrix.md](references/matrix.md).

## Trade-offs

What silver gives up, and where, so a result can be read for what it is: [references/trade-offs.md](references/trade-offs.md).

## Before running

1. **A VM with Podman** from **lfc-source-cloud-vm**, reachable over SSH, with outbound 443.
2. **A Databricks CLI profile** for a workspace in a Zerobus region (see the **databricks-zerobus-ingest** skill), where you can create service principals.
3. **A catalog with its own storage**, which you can create schemas in.
4. **Phase 2:** `CRDB_PORT` (default 30026) free on the VM and open to the workspace's serverless egress. The shared NSG from **lfc-source-cloud-vm** opens 30000-30063. Only one run at a time can publish the port, so a `KEEP=1` run holds it until you remove its containers. You also need permission to create connections and pipelines.

## Run

```bash
cd skills/zerobus-debezium-test
export PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip>   # SOURCE=yb for YugabyteDB (see below)
C=/tmp/dbz-$(date +%s).sh; cp scripts/debezium-zerobus.sh $C && OUT=/tmp/dbz-result.md bash $C
```

Run a copy with its own name, as above, so that editing the script, or starting another run, can't change a run in progress (bash reads a script as it runs). It exits 0 when every claim passed, 1 when one failed and 2 when a prerequisite is missing. The images are pinned by default: `CRDB_IMAGE` (CockroachDB v25.4.13) and `DBZ_IMAGE` (Debezium Server 3.7.0.Final).

Phase 2 (silver: the flows, SCD types 1 and 2, the history check, the DECIMAL export) and phase 3 (gold), and the traceability files: [references/silver.md](references/silver.md). Rounds 3 to 5, with what each one changed and found: [references/rounds.md](references/rounds.md).

The Debezium settings that matter, the event size each setting saved, and the commit time the CockroachDB connector drops: [references/debezium.md](references/debezium.md).

## Links to inspect

After silver, the run prints "Components created:" and a text diagram of the model, the source and its capture, the producer, Zerobus and bronze (**lfc-pipeline-diagram**), and saves it with the Mermaid as `diagram.md` in its work folder; `DIAGRAM=0` skips it.

Every Databricks object a run creates gets a clickable link, printed as `LINK <what>: <url>` when it's created: the schema, the change log, the service principal, the snapshot volume, the UC connection, the pipeline and its source folder, and the silver and gold tables. The result file lists them under "Created in Databricks". The run removes them when it ends, so to inspect them, run with `KEEP=1`; the links are printed again at the end, and `sweep.sh` removes the run later. On a VM, `run-all.sh` also writes **lfc-source-cloud-vm**'s `cloud-links.sh` links (the VM, its network rules, its disks) into `summary.md`.

## The repeatable test

[scripts/run-all.sh](scripts/run-all.sh) runs the committed skills unchanged and records which commit passed. Use it after any change, and before relying on these skills:

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> bash skills/zerobus-debezium-test/scripts/run-all.sh   # from the repo root
```

On the laptop, with no cloud VM or storage: `PROFILE=... CATALOG=... LIMA=<instance> SP_CLIENT_ID=<application id> SP_SECRET_FILE=<file> bash skills/zerobus-debezium-test/scripts/run-all.sh` (CockroachDB only so far).

1. **Pre-flight.** It refuses to run with uncommitted changes in `zerobus-debezium-test`, `zerobus-workspace-setup`, `lfc-source-statschema`, `lfc-source-datagen` or `lfc-source-podman`: commit them, or run from a clean checkout (`git worktree add <dir> HEAD`). `ALLOW_DIRTY=1` runs anyway and marks it in the summary. It also checks SSH to the VM first. When the laptop's public IP changes, the VM's firewall drops SSH, every source query comes back empty, and every claim fails. It runs **lfc-source-podman**'s `ensure-podman-socket.sh` on the VM (on the laptop, `ensure-lima-podman.sh`, which runs it inside Lima). So every container runs rootful through the root Podman socket (`$PODMAN`, default `podman --remote --url unix:///run/podman/podman.sock`), in the same store as the database sources, and with no `sudo` anywhere in the test. Files that containers write are removed from a container. `PODMAN=podman` runs rootless, on a host with no rootful store. `SWEEP=1` first removes what earlier runs left behind ([`sweep.sh`](scripts/sweep.sh) below).
2. **The workspace.** **zerobus-workspace-setup** `check.sh`: the Zerobus previews, and TCP 443 and 9092 from the VM to the endpoint.
3. **Every source**, `SOURCES="crdb yb"` by default, in parallel on their own ports (CockroachDB 30026, YugabyteDB 30031). Each run cleans up after itself.
4. **The results**, in `~/.lfc-test/results/zerobus-<UTC time>/`: `check.txt`, `<source>.md` (the result table, with images and digests), `<source>.log`, and `summary.md` (the commit, claims passed and failed per source). The exit code is 0 only when everything passed. When everything passed, it also writes one row per source into the "Successful runs" table of [docs/lakeflow-connect-zerobus.md](../../docs/lakeflow-connect-zerobus.md), newest first (`RECORD=0` to skip): commit that change with the run.

Leftovers from killed or kept runs, and `sweep.sh`: [references/leftovers.md](references/leftovers.md).

## Without cloud storage or service principal permissions: the laptop

The pieces of the laptop route (Lima, the managed Volume, the service principal, `SCHEMA_VIA=volume`) and its tested run: [references/laptop.md](references/laptop.md).

```bash
cd skills/zerobus-debezium-test
export PROFILE=<cli-profile> CATALOG=<a catalog you can create a schema in> LIMA=<lima instance with podman>
export SP_CLIENT_ID=<application id> SP_SECRET_FILE=<file with its OAuth secret>
C=/tmp/dbz-$(date +%s).sh; cp scripts/debezium-zerobus.sh $C && OUT=/tmp/dbz-result.md bash $C
```

## YugabyteDB (`SOURCE=yb`): Kafka Connect into the Zerobus Kafka endpoint

The YugabyteDB route (`SOURCE=yb`): the logical slot and snapshot, Kafka Connect into the Zerobus Kafka endpoint, and what differs from the Debezium Server route: [references/yugabytedb.md](references/yugabytedb.md).

## CockroachDB licensing

`start-single-node` is enough; `cockroach demo` isn't needed. Tested on v25.4.13 with no license key (2026-10-20): a sinkless changefeed (what the connector uses), a changefeed into `nodelocal://self` with `initial_scan = 'only'`, and `EXPORT INTO PARQUET ... AS OF SYSTEM TIME` all work. Current CockroachDB has no Core edition. A cluster without a license key has a 7-day grace period, after which it's throttled to 5 concurrent open transactions, and Free and Trial licenses need telemetry. For anything that runs longer than a week, set a free Enterprise Trial key with `SET CLUSTER SETTING enterprise.license = '<key>'`.

## After running

Add the result table and the environment lines (images with digests, the CockroachDB version, the host) to the run record. Compare the per-op counts with the workload: the claims `changes-1` to `changes-3` list what each round's manifest expected, per table.
