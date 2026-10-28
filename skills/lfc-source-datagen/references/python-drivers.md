# Run the planners from Python (no database CLI)

The SQL planners (`scripts/postgres`, `mysql`, `sqlserver`, `oracle`) run with each engine's own client (`psql`, `mysql`, `sqlcmd`, `sqlplus`), and `fill.sh` sends their output back through the same client. That works where we run the database ourselves (a container in Lima or on a cloud VM). It doesn't work where there's no client to run:

- **Managed services:** RDS, Aurora, Azure SQL Database, Azure Database for PostgreSQL and MySQL, and Cloud SQL give no shell on the database host. A client can still run elsewhere, but not on the host, and not always on the machine the skill runs on.
- **Databricks serverless:** a notebook or job has Python and pip, not `psql`, `sqlcmd`, `sqlplus`, `db2` or `dbaccess`.

[`scripts/python/fill.py`](../scripts/python/fill.py) is the same fill in Python, through a driver, for **PostgreSQL, MySQL, SQL Server, Oracle, Db2 and Informix**. The SQL planners and `fill.sh` stay as they are.

## Usage

```bash
P=scripts/python
uv run --with psycopg2-binary $P/fill.py pg tpch --host db.example.com --user postgres --password-file pg.pw            # Postgres: schema tpch in database postgres
uv run --with pymysql         $P/fill.py my tpch --host ... --user root --password-file my.pw --sessions 2              # MySQL: database tpch
uv run --with mssql-python    $P/fill.py ms dbo --database tpch --host ... --user sa --password-file ms.pw              # SQL Server: schema dbo in database tpch
uv run --with oracledb        $P/fill.py ora tpch --database FREEPDB1 --host ... --user system --password-file ora.pw   # Oracle: user TPCH, service FREEPDB1
uv run --with ibm-db          $P/fill.py db2 tpch --database LFCDB --host ... --user db2inst1 --password-file db2.pw    # Db2: schema TPCH in database LFCDB
uv run --with informix-driver $P/fill.py ifx tpch --host ... --user informix --password-file ifx.pw                     # Informix: database tpch
```

- `fill` (the default) tops every table up to its target (`lfc:rows`), one level at a time, `--sessions` statements at once, each on its own connection. It stops if a round adds no rows.
- `plan` prints the statements; `status` prints `table|min|max|rows|target`, as `fill.sh` does.
- The options are `fill.sh`'s: `--rows` (per statement), `--mode fill | append`, `--order`, `--data`, `--plain-every`, `--block`, `--lob-len`. `--seed` changes the scramble's constants.
- The password file's first line is the password. Use `--trust-server-certificate` for a SQL Server with a self-signed certificate.
- The schemas come from **lfc-source-statschema**'s `ddl.py`, which also writes `--dialect db2` and `--dialect informix`.

Only the fill planner is ported: insert, status, and the report of skipped tables and columns. Delete, audit, boundary and `make-dataset` are still SQL-only (Postgres and MySQL).

## How it's built

- **The rows are still generated inside the database.** Python sends one `INSERT ... SELECT` per table; only SQL text crosses the network.
- **Each statement still checks its own keys.** It reads the table's current max key, and the parent's key range, when it runs. So a rerun never duplicates rows, and Python holds no state about the database.
- **The plan is made once per run.** `fill.sh` re-plans every round. The scramble's constants come from a hash of table and column, so every plan of a schema writes the same rows.
- [`planner.py`](../scripts/python/planner.py) has the rules every engine shares: levels, `lfc:e` / `lfc:v` / `lfc:n` / `lfc:by`, foreign keys, scramble and plain blocks.
- [`engines/<engine>.py`](../scripts/python/engines/) has what's specific to one engine:
  - the catalog query;
  - a cast per type;
  - n from a key;
  - the key probes;
  - the row generator;
  - quoting.

  Each is 126 to 151 lines. A new engine is a new file there.
- [`drivers.py`](../scripts/python/drivers.py) holds one `connect()` per engine. A JDBC path would be another function there.

| Engine | Row generator | Rows per statement (default) |
|---|---|---|
| PostgreSQL | `generate_series` | 1,000,000 |
| MySQL | the seed table `lfc_seed.seed` (2^20 rows, made if missing) | 100,000 |
| SQL Server | `GENERATE_SERIES` (compatibility level 160) | 1,000,000 |
| Oracle | `CONNECT BY LEVEL` on `DUAL` | 1,000,000 |
| Db2 | a recursive common table expression (1M rows in 0.5 s) | 100,000 |
| Informix | `CONNECT BY LEVEL` on `sysmaster:sysdual` | 50,000 (two sessions of 100,000 wide rows each filled the developer image's logical log: -458, long transaction aborted) |

## Tested

All tests ran on the laptop (Lima VZ VM, 4 vCPU, 10 GiB), with the six engines in Podman (Db2, Informix and SQL Server are x86_64 under Rosetta) and Python on the Mac through forwarded ports.

- **Same SQL as the SQL planners.** On Postgres, MySQL, SQL Server and Oracle, `fill.py plan` wrote the same statements as `plan-insert.sql` for all six TPC schemas, byte for byte apart from the scramble's constants.
- **Every table at its target.** All six TPC workloads (B, C, H, DS, E, DI) at SF 0.1 reached every target on all six engines: 13.0M rows in 95 tables per engine.
- **The driver path is as fast as the CLI.** Timings for TPC-H SF 1 (8,660,030 rows):

| Engine | `fill.py` | `fill.sh` (laptop baseline) |
|---|---|---|
| PostgreSQL 16 (4 sessions) | 118 s | 119 s |
| MySQL 8.4 (2 sessions) | 151 s | 174 s |
| SQL Server 2022 (4 sessions) | 208 s | 207 s |
| Oracle 23ai Free (2 sessions) | 372 s | 307 s |
| Db2 11.5 (2 sessions) | 583 s | — |
| Informix 14.10 (2 sessions) | 1,150 s | — |

Db2 and Informix are slow on TPC-E: at SF 0.1, 433 s and 1,650 s, against 45 s on Postgres.

## Engine notes (found in the tests)

| Engine | Gotcha | What to do |
|---|---|---|
| All | Oracle and MySQL take one statement per `execute()`; SQL Server takes a batch | Each INSERT is one statement; SQL Server's `SET IDENTITY_INSERT` rides in the same batch |
| SQL Server | `mssql-python` refuses a self-signed certificate; a just-started server refuses the handshake for some seconds | `--trust-server-certificate`; `drivers.py` retries the connection |
| SQL Server on Linux | `mssql-python` imports, then fails every connection with `Failed to load the driver`: its ODBC driver (in the separate `mssql-python-odbc` wheel from 1.16) needs `libltdl.so.7`, which isn't in the wheel or in Ubuntu 24.04's cloud image. On macOS the wheel has it | Install the OS packages once: `sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2` (RHEL: `libtool-ltdl krb5-libs`); `python:3.12-slim` lacks all three (`libkrb5.so.3` and `libgssapi_krb5.so.2` are also needed). Not pip-only on Linux. Check `ldd` on the driver's `libmsodbcsql-*.so` for anything `not found` |
| Oracle | Positional binds (`:1`) count each use, not each name | Named binds |
| Db2 | `ibm_db_dbi`'s `.autocommit = True` does nothing (nothing commits, even DDL) | `set_autocommit(True)` |
| Db2 | A long INSERT (19 KB for TPC-H's `part`) fails with SQL0973N: the package cache was too small, and self-tuning memory shrank an `AUTOMATIC` setting back to 320 pages | `CALL SYSPROC.ADMIN_CMD('UPDATE DB CFG FOR <db> USING PCKCACHESZ 16384')`, fixed, not `AUTOMATIC` |
| Db2 | SQL0964C, transaction log full: the default log is 38 files of 4 MB, which two sessions inserting 100,000 wide rows each overran. Building a 2^20-row seed table by doubling also overran it | `UPDATE DB CFG ... USING LOGSECOND 200`; Db2 needs no seed table anyway |
| Db2 | `RAND()` isn't allowed in `ROW_NUMBER() OVER (ORDER BY ...)` (SQL0583N); `HEX` of a BIGINT is little-endian; there's no `HASH_MD5` or `PI()` | Random key order sorts by a hash of n; `TO_HEX`, `HASH(x, 0)` (MD5), `ACOS(-1)` |
| Informix | `autocommit` set after connecting leaves a transaction open (`CREATE DATABASE`: -377) | `informix_db.connect(..., autocommit=True)` |
| Informix | No `%`, `GREATEST`, `LEAST`, MD5, random function, recursive CTE, or `ORDER BY` in a subquery | `MOD`, `CASE`; strings repeat n instead of MD5; keys go in ascending order; `--data random` isn't available; probes use `MAX` / `MIN` |
| Informix | `INT8 * INT` is a DECIMAL, and `MOD` of a DECIMAL returns INTEGER: -1215 above 2^31. n from a DECIMAL key formula made `MOD` return NULL (-391 on a NOT NULL column) | `ddl.py` casts `MOD`'s dividend to BIGINT; the planner casts n to INT8 |
| Informix | `NUMTODSINTERVAL(n, 'DAY')` gives NULL for a few thousand days and more; an `HOUR TO SECOND` value plus a `DAY TO SECOND` interval is -1266 | Days by `MDY(1, 1, 1970) + n`; times are `EXTEND` of the full timestamp |
| Informix | The developer image's root dbspace (1.6 GB) filled with the six SF 0.1 databases | Add a dbspace (`onspaces -c -d datadbs ...`) and `CREATE DATABASE <db> IN datadbs WITH LOG` |
| Db2, Informix | A 150-value list as a simple `CASE` over an inner `CASE` re-evaluates the inner one for every `WHEN` (Informix: 2.5 ms a row) | `ddl.py` folds it into one searched `CASE`, as for SQL Server |
| CockroachDB | The Postgres planners create their functions in `pg_temp`, which CockroachDB refuses (`cannot create user-defined functions under a temporary schema`) | `fill.py postgres` (no SQL functions); `tpc-master.sh` does this |
| CockroachDB | A leading-zero string casts to an integer as octal: `'0008'::bigint` fails, `CAST('0320' AS BIGINT)` is 208. TPC-E's `lfc:n={CAST(tx_id AS BIGINT)}` tags make the probes wrong, and a second round re-inserts keys (`taxrate_pkey`) | The planner's text-key cast strips leading zeros (fixed in `engines/postgres.py`); the tag itself is `ddl.py`'s CockroachDB output and isn't fixed yet |
| CockroachDB | One 600,000-row INSERT with its foreign-key checks ran 12 minutes without committing a row | `--rows 50000` (TPC-H SF 1 at about 2.0M rows a minute on the shared VM, 2026-10-28) |
| Informix | The developer image refuses a 30 GB dbspace (`Demo Version limits exceeded`); 1 GB is accepted. The chunk file must exist, owner `informix`, mode 660 | Size the dbspace to the demo's limit: the SF 0.1 master holds all six workloads (13.0M rows) in 1 GB; SF 1 (about 110M rows) doesn't fit |
| Db2 | `CREATE DATABASE` fails with `SQL0293N` on a disk with 4096-byte logical sectors (Azure Premium SSD v2's default): Db2's 512-byte direct-I/O writes get `EINVAL` | `db2set DB2_4K_DEVICE_SUPPORT=ON`, restart the instance, then create the database (lfc-source-cloud-vm `references/disk-setup.md`, "Logical sector size"); `slot.sh` and `tpc-master.sh` do it |

`ddl.py`'s own sqlglot gotchas for the two new dialects are listed in **lfc-source-statschema**.

## Pip-installable drivers

Every engine has a driver that installs with `pip` alone, with no JVM, and with no OS packages except `libltdl7` for SQL Server on Linux. These are the versions tested here, the same ones [forgedb2](https://github.com/rsleedbx/forgedb2) uses (its `src/forgedb/sqlcli.py` has the connect calls, and `src/forgedb/dbkit/` a small connection layer modeled on SQLAlchemy):

| Engine | Package | What's inside | Connect |
|---|---|---|---|
| PostgreSQL, and Postgres-protocol engines (CockroachDB, YugabyteDB) | `psycopg2-binary` 2.9.12 | libpq, built into the wheel | `psycopg2.connect(host=, port=, user=, password=, dbname=)` |
| MySQL, MariaDB | `pymysql` 1.2.0 | Pure Python | `pymysql.connect(host=, port=, user=, password=, database=, charset="utf8mb4")` |
| SQL Server, Azure SQL | `mssql-python` 1.11.0 (macOS), 1.16.0 (Linux) | Microsoft's ODBC driver and driver manager, inside the wheel (from 1.16, in the `mssql-python-odbc` wheel it pulls in). **On Linux it also needs the OS library `libltdl.so.7`** (`apt-get install libltdl7`) | `mssql_python.connect("Server=host,port;Database=;UID=;PWD={...};Encrypt=yes", autocommit=True)` |
| Oracle | `oracledb` 4.0.2 | Thin mode is pure Python (no Instant Client; Oracle 12.1 and later) | `oracledb.connect(user=, password=, dsn="host:port/service")` |
| Db2 | `ibm-db` 3.2.9 | IBM's C client (`clidriver`), downloaded by `pip install`; installs on Apple Silicon | `ibm_db_dbi.connect("DATABASE=;HOSTNAME=;PORT=;PROTOCOL=TCPIP;UID=;PWD=;", "", "")` |
| Informix | `informix-driver` 2026.5.8.1 | Pure Python: speaks Informix's SQLI protocol over a socket, no client SDK | `informix_db.connect(host=, port=, user=, password=, database=, server=, autocommit=True)`; `server` is the `onsoctcp` name in `sqlhosts` |

## JDBC through JPype (not built yet)

The same JDBC drivers Lakeflow Connect itself uses, called from Python: JPype starts a JVM inside the Python process with the driver jars on its classpath, and `jpype.dbapi2` wraps a JDBC connection as a PEP 249 one. Use it when a pip driver lacks something, or to connect exactly the way the connector does (TLS settings such as `trustServerCertificate`, authentication options). It would be one more function in `drivers.py`, returning the same kind of connection.

```python
import jpype, jpype.dbapi2
jpype.startJVM(classpath=[str(p) for p in jars])      # once per process, with every jar it will need
conn = jpype.dbapi2.connect("jdbc:postgresql://<host>:5432/<db>",
                            driver_args={"user": user, "password": password})
conn.autocommit = True
```

| Engine | Jar (Maven Central `group:artifact`) | URL |
|---|---|---|
| PostgreSQL | `org.postgresql:postgresql` | `jdbc:postgresql://<host>:<port>/<db>` |
| MySQL | `com.mysql:mysql-connector-j` | `jdbc:mysql://<host>:<port>/<db>?sslMode=PREFERRED&allowPublicKeyRetrieval=true` |
| MariaDB | `org.mariadb.jdbc:mariadb-java-client` | `jdbc:mariadb://<host>:<port>/<db>` |
| SQL Server | `com.microsoft.sqlserver:mssql-jdbc` | `jdbc:sqlserver://<host>:<port>;database=<db>` |
| Oracle | `com.oracle.database.jdbc:ojdbc8` (or `ojdbc11`) | `jdbc:oracle:thin:@//<host>:<port>/<service>` |
| Db2 | `com.ibm.db2:jcc` | `jdbc:db2://<host>:<port>/<db>` |
| Informix | `com.ibm.informix:jdbc` | `jdbc:informix-sqli://<host>:<port>/<db>:INFORMIXSERVER=<server>` |

Gotchas:

- **One JVM per process, and it can't be restarted.** JPype can't start a second JVM after `shutdownJVM()`, so start it once with every jar the run needs.
- **Pass the password in `driver_args`, never in the URL**, which ends up in logs and in `getMetaData().getURL()`.
- **Oracle and a long OS user name.** The Oracle driver sends the JVM's `user.name` as `v$session.osuser`, and fails when it's longer than 30 characters (it is on Spark, where it's the application name). Set it first: `jpype.JClass("java.lang.System").setProperty("user.name", "lfc-datagen")`.
- **Oracle takes the service from the URL**, not from `driver_args`; the other drivers take `user` and `password` there, and any JDBC property the connection needs.
- **It needs a Java runtime** (`JAVA_HOME`, or `jpype.getDefaultJVMPath()` finding one) and the jars on disk: download them once per machine (e.g. into a UC volume on Databricks).

## Which to use

| Where the skill runs | Database | Use |
|---|---|---|
| Laptop or VM, database in a container we run | Postgres, MySQL, SQL Server, Oracle | `fill.sh` or `fill.py` |
| Laptop or VM | Db2, Informix | `fill.py` |
| Laptop or VM | Managed service (RDS, Aurora, Azure SQL, Cloud SQL) | `fill.py` |
| Databricks serverless | Any reachable from serverless | `fill.py`, with the driver in the job's environment (`dependencies`) |

On Databricks serverless, the database must accept connections from serverless egress, as a Lakeflow Connect connection does (**lfc-source-firewall**). `ibm-db` downloads its C client from IBM during `pip install`, so that install also needs outbound access to IBM.

**Not tested yet:**
- on Databricks serverless: the drivers on Linux (in particular `mssql-python`'s bundled ODBC and `ibm-db`), and whether a Java runtime is available to JPype there;
- against managed services;
- the JDBC path.
