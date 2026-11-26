# Engines: PostgreSQL, SQL Server, Oracle, Db2, Informix and MySQL

Each runs the TPC models through Debezium Server's own connector and the Zerobus sink. The same model, rounds and checks apply to every source; only these parts differ:

| Source | Change capture | T, and silver's sequence | Snapshot as of T | Rounds |
|---|---|---|---|---|
| PostgreSQL 16 | `wal_level=logical`, a publication and a `pgoutput` slot per database | the slot's LSN; `source.lsn` (`cdc.seq=lsn`) | `COPY ... CSV` | 1 to 5 |
| SQL Server 2022 Developer | CDC on the database and every table, SQL Server Agent running the capture job | the log's last LSN; `source.change_lsn` as one number (`cdc.seq=change_lsn`) | `export.py` | 1 to 3 (`ROUNDS=3`) |
| Oracle AI Database 26ai Free (23.26) | ARCHIVELOG, supplemental logging (all columns), LogMiner with the online catalog | the database's SCN; `source.scn` (`cdc.seq=scn`) | `export.py` | 1 to 3 (`ROUNDS=3`) |
| Db2 11.5 Community | archive logging; ASN capture with Debezium's management scripts (debezium-examples, pinned): `ADDTABLE` per table, the capture agent started and reinitialized | the capture's synch point; `source.change_lsn`'s low 8 bytes (`cdc.seq=db2_lsn`) | `export.py` | 1 to 3 (`ROUNDS=3`) |
| Informix 14.10 Developer (14.10.FC7W1DE) | `syscdcv1` (the image's script), a logged database per source; Debezium Server with the Informix JDBC driver and change-stream client added | 0: Debezium starts after the snapshot; `source.change_lsn` (`cdc.seq=ifx_lsn`) | `export.py` | 1 to 3 (`ROUNDS=3`) |
| MySQL 8.4 | binlog in row format with GTIDs (as for Mautic) | the snapshot's binlog position, taken after the producers start; `cdc.seq=binlog` | `INTO OUTFILE` | 1 to 5 |

- **T is taken while nothing writes.** PostgreSQL can't read as of an earlier LSN, and neither SQL Server nor Oracle reads as of a log position the way the snapshot needs. So the harness writes nothing between the fill and the snapshot: the snapshot is the state at T. The rows at T for the history check are kept from that moment (`snapT` files).
- **Clients.** SQL Server and Oracle use their own command-line clients in their containers (`sources/mssql-sq.sh`, `sources/oracle-sq.sh`). Db2's CLP prints space-padded columns and Informix's `dbaccess` no columns at all, so those two use lfc-source-datagen's `scripts/python/sq.py`: SQL on stdin, tab-separated rows out, through the same drivers as `fill.py`, in a container kept for the run.
- **The snapshot exporter.** lfc-source-datagen's `scripts/python/export.py` reads each table through the same driver as `fill.py` and writes CSV with a header: NULL as `\N`, a quote doubled inside quotes. It runs in a container on the run's network.
- **The history check's truth.** Each source records its own changes: audit triggers write every row change, in commit order, as JSON, to a table its change capture leaves out (`sources/mssql-cdc.sql`, `sources/oracle-cdc.sql`, `sources/audit-triggers.py` for MySQL, Db2 and Informix; YugabyteDB's and PostgreSQL's `cdc_sql`). Informix has no `JSON_OBJECT`, so its triggers build the JSON as text.
- **Names.** Oracle keeps unquoted names in upper case, and its change events carry them that way (`ORACLE1.TPCH1.ORDERS`). `model-sql.py --dialect oracle` folds every name it writes, and silver's schema for Oracle is the model in upper case.
- **Types.**
  - SQL Server's `DATETIME2` and `TIME` default to 7 digits of fraction, which Debezium sends as nanoseconds: silver reads 7 to 9 digits as nanoseconds.
  - Oracle's `DATE` holds a time and comes as epoch milliseconds: silver's schema gives it as a timestamp to the second.
- **Not yet: rounds 4 and 5 on SQL Server and Oracle.** A column added to a CDC table needs a new capture instance in SQL Server. In Oracle, the online catalog doesn't follow DDL; the redo-log catalog would.

Found while building the test (what a pipeline itself meets is in [What a production pipeline meets](../SKILL.md#what-a-production-pipeline-meets)):
- **The workload's updates always set a new value**, since SQL Server logs no update that changes nothing (`model-sql.py`'s values are base 36 of the seed, so a 1-character column gets a different one each time).
- **`mssql-python` on Linux needs OS libraries**: `libltdl7`, `libkrb5-3` and `libgssapi-krb5-2`, none of them in `python:3.12-slim`. The harness builds an image with them once on the VM (`localhost/lfc-datagen-mssql:3.12-2`).
- **Containers that run as their own user** (SQL Server's `mssql`, Oracle's `oracle`) can't read a mounted file the run wrote at mode 0600, so the client scripts (no secrets in them) are mounted readable.
- **`AUDIT` is a reserved word in Oracle**: the audit table there is `LFC_AUDIT`, which the test's table list and capture leave out with every `LFC_` table.
- **Db2 on the VM's 4096-byte-sector disk**: the image's own `CREATE DATABASE` fails (`SQL0293N`) while its log still says setup completed. The harness starts Db2 without `DBNAME`, sets `DB2_4K_DEVICE_SUPPORT=ON`, restarts the instance, creates `LFCDB` with archive logging (`LOGARCHMETH1`) and stops if it can't connect (lfc-source-podman's engines.md, Db2).
- **Db2 names an unnamed result column `1`, `2`, ...**: a query's header row then reads like data; the harness names every column it parses.
- **Informix's developer image needs its volumes given to its user**: `/opt/ibm/data` and `/opt/ibm/config` mounted with `:U` (lfc-source-podman's engines.md), without `--privileged`; otherwise its start script's `sudo` fails and the server goes offline. The tested image, 14.10.FC7W1DE, is the harness's default.
- **The checks trim strings on both sides for Informix**, which ignores trailing blanks.
- **Informix's `VARCHAR` stops at 255 bytes, and a row's casts share a size limit** (error -361 on TPC-DS's `catalog_returns`): the checks cast a string column to `LVARCHAR` of its own length, and other types to `LVARCHAR(40)`.
- **Informix pads a `CASE` that mixes a literal with an `LVARCHAR` cast** to 2048 characters: the audit trigger's JSON for TPC-DS's `date_dim` (16 integers) passed the audit column's 32000 and was cut off. Each value is trimmed.
- **At a small scale a composite key can need more child rows than its parents allow.** TPC-E at SF 0.001 has 7 securities but gives each watch list 15 items, so `watch_item.wi_s_symb` pointed at securities that don't exist; TPC-DI's `FactWatches` hit the same problem. statschema's `shape.py` now caps the rows per first-key value at the second key column's values, and the table's rows at the pairs both parents allow.
- **The laptop's route to the VM drops for half an hour and more** (sleep, VPN reconnects): an answer lost then reads as an empty source and fails a check. The VM client retries for `VM_RETRY_MIN` minutes (default 120).
