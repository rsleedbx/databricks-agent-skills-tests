---
name: lfc-schema-change-test
description: "Re-run a Lakeflow Connect schema-change test end to end: make a fresh source database, run the DDL, check what the pipeline does (inline, or which full-refresh error), recover, and delete everything. Use to re-validate an FAQ answer about schema evolution, or after a product release. First test: does ADD COLUMN on SQL Server need a full refresh."
parent: lfc-source-setup
---

# Schema-change tests

Each script answers one schema-evolution question by running it: it creates a **fresh database** on a source you control, standard CDC pipelines on serverless, makes the source change, reads back the pipeline's state, error class and destination rows, and prints one PASS or FAIL row per claim. At the end, also on failure, it stops the pipelines and keeps them, the schemas and the source database for 7 days (`KEEP_DAYS`, listed in `~/.local/state/lfc-tests/kept.jsonl`), so a result can still be read back; [scripts/kept-sweep.sh](scripts/kept-sweep.sh) deletes them once that date passes, or earlier with `--now <schema>`. `KEEP_DAYS=0` deletes at the end; `KEEP=1` leaves everything running.

**Test it as a black box.** Treat Lakeflow Connect as a process whose behavior isn't known: change the input (rows and DDL on the source, with known values), read the output (the destination rows; the gateway's staging files in between), and compare. Expectations from the docs or the code are what a run checks, not what it assumes. **State each case as a one-line example of input and output**, not as a description: "`id = 1` set to `i0`, `i1` … `i59`, 2 s apart → SCD type 2 kept all 60 versions; SCD type 1: the gateway staged all 60, the destination shows `i59` (one ingestion update applied them all)". A script prints these as `EXAMPLE` observations. Row-by-row tables (`example` in [scripts/lib.sh](scripts/lib.sh), the result's *Input and output* section) are backing evidence, shown only when the line isn't enough (a gap, an order, a timing).

Never point these at a shared source: the DDL forces a full refresh on everyone's pipelines that read the table.

| Script | Question | Time |
|---|---|---|
| [scripts/sqlserver-add-column.sh](scripts/sqlserver-add-column.sh) | When does adding a column to a SQL Server table need a full refresh? Covers tables with and without a primary key, change tracking and CDC, columns with and without defaults, identity and computed columns, and `auto_full_refresh_policy`. `MODE=integrated` runs it on integrated CDC (one pipeline) instead of standard CDC | About 40 minutes standard, 1–2 hours integrated |
| [scripts/sqlserver-add-column-staged.sh](scripts/sqlserver-add-column-staged.sh) | Can a column with a default be added without a full refresh by staging it: ADD the column nullable, fill it with `UPDATE`s, then ADD the default as a constraint? On CDC and change tracking, with `ADD ... NOT NULL DEFAULT` as the control and `ALTER COLUMN ... NOT NULL` afterwards observed. `GATEWAY_COMPUTE=classic` puts the gateway on a classic cluster | About 50 minutes |
| [scripts/integrated-scd2-ct.sh](scripts/integrated-scd2-ct.sh) | What does integrated CDC do with `SCD_TYPE_2` on a change-tracking table? Plain inserts, more inserts, then an added column, against SCD type 1 and CDC controls. `SERVER_STATE=1` grants the login `VIEW SERVER STATE` for the run, to show its effect | About 30 minutes |
| [scripts/sqlserver-table-lifecycle.sh](scripts/sqlserver-table-lifecycle.sh) | A table's lifecycle on change tracking and CDC: change data feed on the destination, DELETE every row + INSERT, adding a table to the pipeline, TRUNCATE, DROP and recreate, and the recovery of each | About 1 hour standard, 40 minutes integrated |
| [scripts/sqlserver-cdf.sh](scripts/sqlserver-cdf.sh) | Change data feed on the destination tables: is it on, can `table_changes()` read it (not from version 0), and what an insert, update and delete look like on SCD type 1 and 2 | About 15 minutes |
| [scripts/sqlserver-cdf-history-recovery.sh](scripts/sqlserver-cdf-history-recovery.sh) | A history kept downstream from the destination's change data feed ([scripts/audit-history.py](scripts/audit-history.py), `AUDIT_NB`): normal syncs, a gap, a full refresh of the destination (breaks the reader), and recovery in a new pipeline that copies the old log once and closes the rows deleted during the gap | About 1.5 hours integrated |
| [scripts/external-reads.sh](scripts/external-reads.sh) | Who else can read a destination streaming table a test kept (`TABLE=`, after `KEEP=1`): dbt models (table, incremental, a view over `table_changes()`), UniForm properties, credential vending, the Iceberg REST catalog, a Delta Sharing share | About 5 minutes |
| [scripts/sqlserver-ddl-afr.sh](scripts/sqlserver-ddl-afr.sh) | Which DDL needs a full refresh (widen, narrow, rename, drop column, primary key change), on CDC and change tracking, and does `auto_full_refresh_policy` do it on its own | About 40 minutes |
| [scripts/sqlserver-permissions.sh](scripts/sqlserver-permissions.sh) | What the pipeline reports when the login can't read a table or its CDC change table, or the table isn't tracked; do the other tables keep loading; does fixing the source and a full refresh recover | About 30 minutes |
| [scripts/sqlserver-cii-deny.sh](scripts/sqlserver-cii-deny.sh) | On a CDC table, does a column added after the first load need the login's UPDATE on the utility objects' `lakeflowCaptureInstanceInfo`? A row, then ADD COLUMN and a row, with the info table's row and the capture instances printed; `CII_DENY=0` is the control | About 30 minutes |
| [scripts/sqlserver-ct-net-changes.sh](scripts/sqlserver-ct-net-changes.sh) | With a gateway that runs all the time, does a change-tracking table get every update of a row, and is every table read on every poll? `TABLES_N` change-tracking SCD type 2 tables and a CDC control, one row updated every `BURST_GAP` seconds for `BURST_N` rounds on `BUSY_PCT`% of the tables, then 5 updates back to back; the kept versions and the gateway's staging files show how often each table was read | About 25 minutes for 22 tables, longer for 200 |
| [scripts/sqlserver-same-name.sh](scripts/sqlserver-same-name.sh) | Two source tables with the same name in different source schemas (`lfcddemo.orders`, `lfcdb.orders`) in one pipeline: `CASE` maps them to two destination schemas, schema objects, one schema renamed, the legacy publishing mode, one destination table, or two names that are equal once `.` becomes `_`; `TRACK=ct\|cdc`. Each destination must keep only its own source's rows and changes | About 10 minutes a case |
| [scripts/sqlserver-scale.sh](scripts/sqlserver-scale.sh) | Many tables in one pipeline: `COPIES` copies of a model, one schema each, kept and reused between runs; the model, scale, DDL and fill come from `WORKLOAD_JSON` (a workload file, lfc-workload-contract; lfc-source-datagen's `workload.sh` writes one); `MODE`, `OBJECTS=schema\|table`. With [scripts/vm-throttle.sh](scripts/vm-throttle.sh) `<MiB/s> --cold` before (and `off` after) and `DISK_MBPS`, the same run on a slow source disk | 10 minutes to over an hour, by table count |
| [scripts/postgres-ddl.sh](scripts/postgres-ddl.sh) | PostgreSQL DDL (`ENGINE=postgres`): nullable and default ADD COLUMN, TOAST-able columns and replica identity, DROP, type changes, rename, primary key, TRUNCATE, parse failures, with and without `auto_full_refresh_policy` | About 40 minutes |

## Before running

1. **A SQL Server source you can run DDL on**, reachable from serverless. The tested one is the SQL Server 2022 Podman container on the cloud VM from **lfc-source-cloud-vm**: start the VM and the container, and open the firewall with **lfc-source-firewall** if serverless can't reach it. Stop the VM afterwards.
2. **A UC `SQLSERVER` connection** to that source (**lfc-connection-sync** or **lfc-source-setup**), and the login it uses on the source.
3. **A catalog with its own storage.** CDC refuses Default Storage, and a catalog whose storage credential is broken fails at create ("Azure storage credential was not found"). The script checks for Default Storage before it creates anything.
4. **A command that runs T-SQL on the source as an admin**, reading stdin, first argument the database. For the VM container, [scripts/vm-mssql.sh](scripts/vm-mssql.sh) does it over SSH and keeps the `sa` password on the VM. Or set `SOURCE_JSON=<contract file>` in place of `SRC_SQL`, `CONN_USER` and `CONNECTION`: the source contract, [lfc-source-contract](../lfc-source-contract/SKILL.md).

## Run

```bash
cd skills/lfc-schema-change-test
export PROFILE=<cli-profile> CONNECTION=<uc-connection> CATALOG=<catalog> CONN_USER=<connection-login>
export SRC_SQL="$PWD/scripts/vm-mssql.sh" SRC_ENV="$PWD/scripts/vm-mssql.sh --env" VM_IP=<vm-ip> MSSQL_CONTAINER=<container-name>
OUT=/tmp/addcol-result.md scripts/sqlserver-add-column.sh
MODE=integrated OUT=/tmp/addcol-result-integrated.md scripts/sqlserver-add-column.sh   # the same on integrated CDC
```

### Several runs at once

Most of these tests are low volume, so run them side by side, each on its own source: one SQL Server container per two vCPUs of the VM (check `nproc`, `free -g`, `podman stats --no-stream` and the other sessions' containers first; `vm-mssql.sh --env` gives the disk's IOPS and MB/s). A separate source per run keeps one run's reads from slowing another's, which matters for tests that time the gateway. Make another container and its connection with [lfc-source-cloud-vm `add-mssql-container.sh`](../lfc-source-cloud-vm/scripts/add-mssql-container.sh), then queue the runs:

```bash
cat > slots <<'EOF2'
<connection-1> <container-1>
<connection-2> <container-2>
EOF2
cat > jobs <<'EOF2'
ct22   sqlserver-ct-net-changes.sh
ct200  sqlserver-ct-net-changes.sh TABLES_N=200 BURST_N=30
ct20pc sqlserver-ct-net-changes.sh TABLES_N=200 BURST_N=30 BUSY_PCT=20
EOF2
RUNS=/tmp/ct-runs scripts/parallel.sh slots jobs     # PROFILE, CATALOG, CONN_USER, VM_IP exported as above
```

[scripts/parallel.sh](scripts/parallel.sh) starts each job on the next free source, from its own copy of the scripts, with that source's `CONNECTION`, `MSSQL_CONTAINER` and `SRC_SQL`, and prints a line as each starts and ends. It needs bash 4 or later. A slot line `- json:<contract file>` takes the source from a source contract ([lfc-source-contract](../lfc-source-contract/SKILL.md)).

It exits 0 when every claim passed, 1 when one failed, 2 when a prerequisite is missing (nothing is left behind in either case).

## Tables, and the path each covers

| Table | Shape | Storage | Covers |
|---|---|---|---|
| `intpk` | primary key, change tracking | SCD type 1 | the keyed change-tracking path |
| `cdcpk` | primary key, CDC | SCD type 1 | the keyed CDC path |
| `dtix` | no key, CDC, non-unique index on a column whose values repeat | SCD type 1 | the keyless path (keyed on every column) |
| `afrpk` | primary key, change tracking, `auto_full_refresh_policy` on | SCD type 1 | automatic full refresh |
| `scd2pk` | primary key, CDC | SCD type 2 | the SCD type 2 path (`SCD_TYPE_2` needs CDC on SQL Server; see below) |

`APPEND_ONLY` isn't a CDC path: pipeline create refuses it ("APPEND_ONLY can only be used for incremental ingestion of query based connector"). It's covered by the query-based tests.

## What the runs taught

Findings from the runs (SQL Server and Postgres behaviors the tests found), the rules for writing a new script, and how each result is recorded: [references/findings.md](references/findings.md).

## With an agent

Give Isaac (or another coding agent with this repository and a Databricks CLI login) this prompt:

> Re-validate the SQL Server ADD COLUMN FAQ: follow `skills/lfc-schema-change-test/SKILL.md` in rsleedbx/databricks-agent-skills-tests. Use CLI profile `<profile>`, connection `<connection>`, catalog `<catalog>`. Start the test VM and its SQL Server container first and stop the VM afterwards. Report the claim table and anything that differs from the last run.

## After running

Each result also records the Lakeflow runtime every pipeline ran on (DBR version, channel and the image build, from the update's `runtime_details` event). Workspaces on different releases can behave differently, and settings appear, disappear and change between releases. [bin/lfc-runtime.sh](../../bin/lfc-runtime.sh) saves the full list of settings each update ran with on this machine and prints what changed since the last run on that workspace; the run log shows it. Run it yourself on any pipeline: `bin/lfc-runtime.sh <profile> <pipeline-id> [update-id]`.

`SRC_ENV` adds the host OS, the Podman version and the container image with its digest to the result, so a reader can run the same image. Copy them into the run file and the FAQ entry's test section.

Add the claim table as a new run file next to the FAQ entry's `validation.yaml`, update the entry's `## Validation` line and its `last_validated`, and say what changed since the last run.

## DDL matrix

The same schema-change cases run on each engine, mechanism and setup, so the results compare. The case catalog is [scripts/ddl-cases.yaml](scripts/ddl-cases.yaml); the file format and the marks are in [references/ddl-matrix.md](references/ddl-matrix.md); the table, generated from [results/ddl-matrix](../../results/ddl-matrix/), is [references/ddl-matrix-results.md](references/ddl-matrix-results.md).

For one engine, mechanism and setup:

1. `scripts/ddl-matrix.py sql <engine> <mechanism> --setup <none|utility-1.7|utility-1.7-cleanup>` prints the tables, the source's setup, and each case's DDL and insert. Run it on the source with your client.
2. `scripts/ddl-matrix.py bundle --connection <uc-connection> --catalog <c> --schema <s> --source-catalog <db> --engine <engine> --mechanism <mechanism>` writes a `databricks.yml` with one table per case. Deploy it and start the pipeline.
3. Wait for the initial snapshot. Then run each case's DDL and insert in the `sql` order, noting the UTC time before the first DDL.
4. When the updates have finished, and before the pipeline is destroyed (`collect` reads the runtime from its events), `scripts/ddl-matrix.py collect --pipeline <id> --profile <p> --warehouse <wh> --dest <c>.<s> --env <env.yaml> --since <utc> --out results/ddl-matrix/<engine>-<mechanism>-<setup>-<date>.yaml`.
5. `scripts/ddl-matrix-validate.py results/ddl-matrix/*.yaml`, then `scripts/ddl-matrix.py render > references/ddl-matrix-results.md`.

The rules that make the comparison valid:

- Run each mechanism with and without the setup script (`none` and `utility-1.7`), on the same cases. The two columns are the comparison; a difference between them is a finding.
- A case passes only when its destination row is verified. "Pipeline created" and "update COMPLETED" are not passes, and a batch that ran every DDL before one update can't verify a row of a table that had no error of its own. Run those cases alone.
- Every run records the runtime (DBR version, channel, dp build), the setup script version, and, when a result rests on a code change, which release builds have it (the change's commit and the builds checked) and the flag's observed rollout value if one decides it. Record the method (`batch` or `one-case-at-a-time`) and anything that didn't run in the file's `caveats`.

## Trade-offs

- Kept runs (7 days by default): a result can be read back after the run, at the cost of a week of stopped pipelines, schemas and a source database. A kept PostgreSQL slot holds WAL on the source for that week; on a busy shared server use `KEEP_DAYS=0` or set `max_slot_wal_keep_size`.
