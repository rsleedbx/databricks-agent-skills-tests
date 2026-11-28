---
name: lfc-sqlserver-secondary
description: Run a Lakeflow Connect SQL Server CDC reader (a gateway, or an integrated CDC pipeline) against an Always On readable secondary to keep the snapshot and change reads off the primary, and move it to the primary only for the steps that write to the source (a schema change, a full refresh), then back. Use when a customer wants ingestion to read from a secondary replica, when a table stopped updating after a schema change on a secondary, or to script the switch, the "reset done" check and the cluster log delivery to a UC volume without opening the UI.
---

# SQL Server CDC from an Always On readable secondary

The Lakeflow Connect docs support the primary only ([SQL Server limits](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits)). This skill is for a customer who reads from a secondary anyway: what runs there, what needs the primary, and `scripts/secondary.py`, which moves the gateway between the two.

## What runs where

CDC is enabled on the primary; the capture and cleanup jobs run there. The availability group copies the change tables and the utility objects' tracking table to the secondaries, so a gateway connected to a secondary reads them.

| Gateway step | On a read-only secondary |
|---|---|
| Snapshot, CDC change reads, DDL history, startup checks, adding a table | Work: reads only |
| Change tracking tables | Not possible: SQL Server rejects change tracking reads on a secondary (`INGESTION_GATEWAY_SQL_SERVER_CT_NOT_SUPPORTED_ON_REPLICA`) |
| ADD COLUMN: the utility trigger creates the new capture instance on the primary, then the gateway merges the old one into it and disables the old one | The merge fails with SQL Server error 3906 (database is read-only) and is retried with backoff. The table stops updating; no pipeline warning or error. Other tables keep flowing, later |
| Full refresh: the gateway disables and re-creates the capture instance, then snapshots | The reset fails the same way |
| Periodic committed-cursor update in the tracking table | Fails with 3906 and is retried every cycle, which slows the extractor: CDC lag on the secondary is higher than on the primary |

Not reachable this way: the readable replica of an Azure SQL Managed Instance Business Critical (or any listener that routes by read intent). The gateway connects with host, port and database only and doesn't send `ApplicationIntent=ReadOnly`, even when the UC connection sets `applicationIntent` (from the gateway code; not tested on a Managed Instance). A node or listener that accepts plain connections works (an AG secondary with `ALLOW_CONNECTIONS = ALL`, a failover group's read-only listener).

## The fix: a short stay on the primary

Point the UC connection at the primary, restart the gateway, let it do its writes, check the secondary has caught up, point it back, restart. Tested behavior:

- The gateway's cursor survives a host change between AG nodes: no full refresh, no gap, no duplicate.
- After an unplanned ADD COLUMN, the switch alone recovers the table: the stuck merge finishes on the primary and the new column and the waiting rows land. No full refresh.
- A full refresh's reset runs as its own step before the snapshot. If the gateway stops before the reset finishes, the next start runs the reset again (so go back to the primary). Once it finished, restarting on the secondary doesn't repeat it, and the snapshot reads from the secondary.

- A full refresh of an SCD type 2 table drops its history: the table is reloaded with one version per key, and deleted keys are gone. With SCD type 2, prefer the switch (no full refresh) and keep a history table downstream (`lfc-downstream-history`) for the refreshes you can't avoid.
- Stopping the gateway is safe while CDC cleanup retention still holds its position: every version arrives at the next start. Past retention the table fails with `INGESTION_GATEWAY_CDC_LOG_TRUNCATED` and needs a full refresh; the other tables keep going.

The trade-off: each switch restarts the gateway twice, and changes wait until it runs again. Changes made on the primary meanwhile arrive once the gateway runs, none lost.

## Integrated CDC (one serverless pipeline)

The same steps read and write the same way when the reader is an integrated CDC pipeline: reads work on the secondary, the ADD COLUMN merge, the reset and the cursor update fail with 3906, and the switch to the primary and back recovers without a full refresh. Set `PIPELINE_ID` and leave `GATEWAY_ID` unset; `secondary.py` then stops and starts the pipeline itself. What differs:

- A continuous pipeline refuses a selected-table full refresh (`Refresh selection is not supported for the continuous mode`): set the pipeline to triggered for the refresh, or refresh the whole pipeline.
- The snapshot lands in staging in the refresh update, but the destination table is reset by the next update that starts after the snapshot finished (events `Snapshot completed ... dataset will be full refreshed in this update`, then `STREAMING_TABLE ... has been successfully reset`). `refresh` waits for that event and restarts the pipeline once if needed.
- No driver log: serverless takes no cluster settings (`You cannot provide cluster settings when using serverless compute`), so `log-delivery` and `wait-reset` are gateway-only. Use the tracking table (`wait-ci`).
- A triggered pipeline on the secondary can take a long time to stop on its own: it waits for the lag to settle, and the failing cursor update keeps the lag moving.

## How to know the reset is done (no UI)

| Signal | How | Note |
|---|---|---|
| Source tracking table | the table's row in `dbo.lakeflowCaptureInstanceInfo_<version>` has `oldCaptureInstance` NULL and `cdc.change_tables` holds one capture instance for the table (`secondary.py ci` / `wait-ci`) | Fastest; needs a SQL login on the primary |
| Gateway driver log | `Reinitialization completed successfully for <replication id>` in `log4j-active.log`, with cluster log delivery to a UC volume (`secondary.py log-delivery`, then `wait-reset`) | Needs only the workspace login; the cluster copies its logs every few minutes, so it shows up after the fact |
| Pipeline event | `Reinitializing. Tables added: ..., refreshed: ...` | Marks the start only, not the end |

The gateway's staging volume has no reset marker. Cluster log delivery is a setting on the gateway pipeline (`clusters[].cluster_log_conf.volumes.destination`); saving it starts a gateway update.

## The script

`scripts/secondary.py` (prints its usage with no arguments; `python3 scripts/secondary.py <command>`). Needs Python 3 and a workspace login (the lfc-rest skill: the SDK signs in, the rest is REST), and `sqlcmd` (or `PRIMARY_SQL_CMD` / `SECONDARY_SQL_CMD`, any command that reads T-SQL on stdin and takes sqlcmd's flags).

```bash
export GATEWAY_ID=<gateway pipeline id> PIPELINE_ID=<ingestion pipeline id> CONNECTION=<uc connection>
export DATABASE=<source database> PRIMARY=<host>,<port> SECONDARY=<host>,<port>
export SQL_USER=<connection login> SQL_PASSWORD=<its password>   # a connection update must repeat them
export LOG_VOLUME=/Volumes/<catalog>/<schema>/<volume>           # optional, for the driver log check
scripts/secondary.py log-delivery             # once
scripts/secondary.py where                    # connection, gateway state, gateway sessions per node
scripts/secondary.py ddl dbo.orders add_column.sql   # planned schema change: primary, DDL, wait, back
scripts/secondary.py recover dbo.orders       # the table stopped after an unplanned schema change
scripts/secondary.py refresh dbo.orders orders       # full refresh, snapshot from the secondary
```

`ddl` records the current capture instance, runs the file on the primary and waits until a new one is the only one left. `refresh` starts the full refresh on the primary, waits for the reset (tracking table, and the driver log when `LOG_VOLUME` is set), stops the gateway, waits for the secondary, then starts it on the secondary. Every step logs a UTC time, and the building blocks (`switch`, `wait-ci`, `wait-reset`, `caught-up`, `full-refresh`, `stop`, `start`) run on their own. The password goes into the connection update only; the script never prints it.

## Checks for the customer

- Where the gateway reads: `sys.dm_exec_sessions WHERE program_name LIKE 'lakeflow-connect-%'` on each node (`secondary.py where`).
- A table that stopped updating on the secondary: `secondary.py ci <table>` shows an old capture instance still set and two capture instances.
- First-time setup on SQL Server and Managed Instance: a sysadmin runs `sys.sp_cdc_enable_db` (a `db_owner` alone gets Msg 22902), then a `db_owner` runs the utility setup. The ingestion user needs no `db_owner`: it runs the three capture-instance procedures, which run as their owner; revoking its `EXECUTE` on them stalls schema changes without an error.
- Logins: the AG doesn't copy logins or server-level grants in `master`. Create the connection's login on every node with the same SID, and repeat the utility script's server-level grants on each node.

## Test it yourself

The source comes from the lfc-source-podman skill: its `ag-up.py <utility_script.sql>` builds a two-node availability group in containers with CDC and a readable secondary, `ag-sql.py` runs SQL on a node (and `--xe` shows the gateway's statements and errors per node), `ag-down.py` removes it. The cases to run and what each should show: [references/ag-test.md](references/ag-test.md).

Related: `lfc-source-log-state` (where CDC stands on the source), `lfc-source-setup` (utility objects and CDC setup).
