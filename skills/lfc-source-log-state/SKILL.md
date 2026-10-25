---
name: lfc-source-log-state
description: Read where a SQL Server source's change tracking and CDC stand right now, in one batch and one JSON document (current change version, per-table min valid and last change versions, CDC max/min/last LSNs and their times, capture job settings, the last log scan), to compare with the position a CDC reader such as a Lakeflow Connect gateway reports. Use when a table looks behind, when checking whether a reader's position is still valid (not cleaned up), or before and after a test to record the source's log state.
---

# Source log state: SQL Server change tracking and CDC

`scripts/sqlserver-log-state.sh <database>` runs `scripts/sqlserver-log-state.sql` in one round trip and prints one JSON line. Pipe it to `jq`.

```bash
SQLCMDSERVER=<host>,<port> SQLCMDUSER=<login> SQLCMDPASSWORD=<pw> scripts/sqlserver-log-state.sh <database> | jq .
# a container on this machine:
SQL_CMD='podman exec -i <container> /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PW"' scripts/sqlserver-log-state.sh <database>
```

`SQL_CMD` is any command that reads T-SQL on stdin and takes sqlcmd's `-d`, `-v`, `-y` and `-b` flags (it runs through `bash -c`, so it can hold quotes). `--no-per-table` skips the per-table reads, for a source with many large tables.

## Output

| Path | What |
|---|---|
| `database` | `name`, `server_time_utc`, `recovery_model`, `log_reuse_wait` (`NOTHING`, `LOG_BACKUP`, `REPLICATION`, ...), `log_used_pct`, `change_tracking`, `cdc` |
| `change_tracking.current_version` | `CHANGE_TRACKING_CURRENT_VERSION()`: the newest committed version in the database |
| `change_tracking.retention_period`, `retention_units`, `auto_cleanup` | how long versions are kept |
| `change_tracking.tables[]` | per tracked table: `min_valid_version` (`CHANGE_TRACKING_MIN_VALID_VERSION`), `cleanup_version`, `last_change_version` and `changed_rows` (`CHANGETABLE(CHANGES ..., min_valid_version)`), `error` |
| `cdc.max_lsn`, `max_lsn_time` | `sys.fn_cdc_get_max_lsn()`: the newest LSN in any change table, and its commit time |
| `cdc.jobs[]` | capture: `polling_interval_s`, `maxtrans`, `maxscans`, `continuous`; cleanup: `retention_min`, `threshold` (`sys.sp_cdc_help_jobs`); `jobs_error` when the login can't read them |
| `cdc.last_scan`, `cdc.last_capture` | the latest log scan session, and the latest one that captured transactions: times, `latency_s`, `end_lsn`, `tran_count` (`sys.dm_cdc_log_scan_sessions`) |
| `cdc.capture_instances[]` | per capture instance: `min_lsn` (oldest kept, after cleanup), `start_lsn`, `last_lsn` (newest in its change table) with times, `change_rows`, `error` |

LSNs are 20-digit hex strings (`0x0000002B00000DB00003`), so two compare correctly as strings.

## Comparing with a reader's position

- **Change tracking:** a reader at version `v` for a table has changes to read when `v < last_change_version`. It has lost changes when `v < min_valid_version`: the table needs a full reload. `current_version` minus `v` is how far behind the database it is, even when that table had no changes.
- **CDC:** a reader at LSN `l` for a capture instance has changes to read when `l < last_lsn`, and has lost changes when `l < min_lsn` (cleanup removed them). `max_lsn_time` minus the time of `l` is its lag in time.
- **Capture job:** `last_capture.latency_s` is how far the capture job itself is behind the log; a reader can't be closer than that. A `log_reuse_wait` of `REPLICATION` with CDC on means the log waits for the capture job.
- Take the source's state and the reader's within a few seconds of each other, and compare `server_time_utc` with the time of the reader's position: both keep moving.

## Rights

`VIEW DATABASE STATE`; `VIEW CHANGE TRACKING` and `SELECT` on the tracked tables; `SELECT` on the `cdc` schema. The job settings need `db_owner` (`sys.sp_cdc_help_jobs`). A part the login can't read is null with an `error` next to it; the rest still runs.

## Cost

One round trip. The per-table parts read each table's change-tracking side table (`CHANGETABLE` since the min valid version) and each CDC change table (`MAX` and `COUNT` over it); both grow with the changes kept, so on a large source use `--no-per-table` or run it at a quiet moment.
