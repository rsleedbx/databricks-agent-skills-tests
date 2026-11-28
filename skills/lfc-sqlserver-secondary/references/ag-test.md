# Repeat the availability group test

Two SQL Server 2022 containers form a read-scale availability group (`CLUSTER_TYPE = NONE`, synchronous commit, secondary `ALLOW_CONNECTIONS = ALL`). A CDC gateway reads from the secondary; `secondary.py` moves it to the primary and back.

## Build the source

```bash
# on the podman host (a source VM whose firewall lets the workspace reach the two ports), from lfc-source-podman's scripts
AG_NAME=lfc-ag PORT_P=<port> PORT_S=<port> ag-up.py <utility_script.sql>
AG_NAME=lfc-ag ag-sql.py s lfcag < check.sql          # T-SQL on a node as sa
AG_NAME=lfc-ag ag-sql.py s --xe <since> writes       # the gateway's writes and errors on a node
AG_NAME=lfc-ag ag-down.py                            # remove (add --keep-data to keep the files)
```

`ag-up.py` creates both nodes, the endpoint certificate, the availability group with automatic seeding, a database with keyed tables, the connection's login with the same SID on both nodes, the utility objects and CDC on the primary, the server-scoped grants in `master` on both nodes (the availability group doesn't copy `master`), and an Extended Events session `lfc_watch` on each node that records every statement and error from a session whose application name starts with `lakeflow-connect`. The passwords are in `$AG_DIR/ag.env` (mode 600).

Then a UC connection (`SQLSERVER`, `trustServerCertificate=true`, host and port of the secondary), a gateway and an ingestion pipeline for the tables, as in `lfc-source-setup`.

## Cases

Each case: the input, then where the gateway's sessions ran (`secondary.py where`), its writes and errors per node (`ag-sql.py <node> --xe <since> writes`), pipeline events, and destination versus source (row count and a column sum per table).

| Case | Input | Expected |
|---|---|---|
| Baseline | gateway on the secondary; INSERT / UPDATE / DELETE on the primary | snapshot and changes read on the secondary only; destination equals source |
| Cursor update | leave it running past the committed-cursor refresh interval | error 3906 on the secondary for the tracking-table UPDATE, no pipeline event, data keeps flowing |
| Unplanned ADD COLUMN | `ALTER TABLE ... ADD` on the primary while on the secondary | merge procedure fails 3906 and is retried; the table stops; no pipeline warning or error |
| Recover | `secondary.py recover <table>` | merge and old capture instance cleanup on the primary; the new column and the waiting rows land; no full refresh |
| Planned ADD COLUMN | `secondary.py ddl <table> <file>` | handled on the primary; changes made during each switch arrive once |
| Full refresh, stopped mid-reset | full refresh on the primary, stop the gateway before `wait-ci` passes | the next start runs the reset again |
| Full refresh, snapshot on the secondary | `secondary.py refresh <table> <destination table>` | no reset writes on the secondary after the switch back; snapshot sessions on the secondary only |
| Host change | any switch | cursor kept: no full refresh, no gap, no duplicate |
| Integrated: continuous full refresh | `secondary.py full-refresh <table>` on a continuous pipeline | refused (`Refresh selection is not supported for the continuous mode`) |
| Integrated: refresh, snapshot on the secondary | `secondary.py refresh <table> <destination table>`, `GATEWAY_ID` unset, triggered pipeline | reset on the primary only; snapshot read on the secondary; the destination reset in the next update (`... has been successfully reset`) |
| SCD type 2: stop window | stop the reader, several UPDATEs per key and deletes on the primary, start | every version arrives once; no key open twice |
| SCD type 2: full refresh | `refresh` on an SCD type 2 table | the table holds the current snapshot only: older versions and deleted keys are gone |

A lock on the table blocks the reset (it needs a schema-modification lock), so to stop a gateway mid-reset, stop it right after the `Reinitializing` event rather than holding locks.
