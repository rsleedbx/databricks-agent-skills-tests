---
name: lfc-source-contract
description: "The source contract for Lakeflow Connect tests: one small versioned JSON file that says where a test's source database is (engine, host, port, login, database, and optionally a password and an admin SQL command), so a test takes its database only through it and works with a customer's own database, a Podman container, or a slot VM; also a Kafka broker (engine kafka: bootstrap, topic, SASL user and password file). Use when pointing a test at a source (SOURCE_JSON), writing a file for a database, or changing what a test reads from a source."
---

# Source contract (contract 1)

A Lakeflow Connect test takes its source database only through a contract file: `SOURCE_JSON=<file>` on the script. Set it in place of `SRC_SQL`, `CONN_USER` and `CONNECTION`; the environment variables still work when it isn't set.

Scripts that accept it: **lfc-schema-change-test** (`lib.sh` and the scripts that use it, its Python scenarios, `integrated-scd2-ct.sh`, `parallel.sh` through a `- json:<file>` slot line), and **lfc-downstream-history**'s `sqlserver-history-snapshot.sh`; for a broker, **lfc-kafka-freshness-test**'s `connection.sh` and `pipeline.sh` (`--source-json`). The zerobus scripts build their own source containers, so they have no database for a contract to name.

## The file

```json
{"contract": 1, "name": "pg1", "engine": "postgres", "host": "203.0.113.10", "port": 30000,
 "user": "lfc_user", "database": "postgres", "connection": "me_pg1",
 "sql_cmd": "env SLOT_HOST=203.0.113.10 SLOT_NAME=pg1 python3 -I /abs/path/slot.py sql"}
```

| Field | Required | Meaning | Variable |
|---|---|---|---|
| `contract` | yes | `1` | |
| `engine` | yes | `sqlserver`, `postgres`, `mysql`, `oracle`, `mariadb`, `db2`, `informix`, `cockroachdb`, `yugabytedb`, or `kafka` (a broker: below) | `SRC_ENGINE` |
| `host` | yes | the address the test connects to | `SRC_HOST` |
| `port` | yes | a whole number, 1 to 65535 | `SRC_PORT` |
| `user` | yes | the Lakeflow Connect login | `CONN_USER` |
| `database` | for `db2`, `informix`, `cockroachdb`, `yugabytedb`; optional otherwise | the database a test uses | `SRC_DB` |
| `service` | for `oracle` | the service (the PDB) the login lives in | `SRC_DB` |
| `password` | no | omit when the UC connection holds it; the default client needs it | (never exported) |
| `sql_cmd` | no | the admin SQL command (below); without it, the default client | `SRC_SQL` |
| `connection` | no | the UC connection's name | `CONNECTION` |
| `facts` | no | a facts JSON; a relative path is relative to the file | `SRC_FACTS` |
| `name` | no | a label | `SRC_NAME` |
| `topic` | for `kafka` | the topic the test reads | `SRC_TOPIC` |
| `sasl_mechanism` | no | `kafka`: default `PLAIN` | `SRC_SASL` |
| `password_file` | no | `kafka`: a 0600 file holding the SASL password; a relative path is relative to the file | `SRC_PASSWORD_FILE` |

A Kafka broker: `host` and `port` are the bootstrap server, `user` the SASL user. It has no database and no SQL client, so `SRC_SQL` isn't set. The password stays in `password_file`, so the contract itself holds no secret:

```json
{"contract": 1, "engine": "kafka", "name": "ns1", "host": "ns1.servicebus.windows.net", "port": 9093,
 "user": "$ConnectionString", "topic": "tag-observations", "password_file": "ns1.pw"}
```

Other fields are ignored. Adding an optional field keeps the version at 1; a change that a reader can't ignore makes contract 2.

## What a producer guarantees

- The file is one JSON object with the fields above, typed as the table says.
- The database and the login exist and accept connections when the file is written.
- `sql_cmd`, when present, runs SQL from stdin as an administrator of that database.
- With `connection`, there is no password: the UC connection holds it. A file with a password is a secret: mode 600, and never printed.

## What a consumer may assume

- The fields above hold, once `source_env.py` has accepted the file.
- With `SOURCE_JSON`, the contract sets the variables it gives. Variables it doesn't give stay as they were in the environment.
- A consumer doesn't write the contract or change the database beyond what its test does.

## Version rule

Consumers read contract 1 only. `source_env.py` exits 2 with one line on any other `contract` value, and on a missing or wrong field, so nothing runs against a source it can't read. A producer that writes a new version updates its consumers first.

## The admin command (`sql_cmd`)

A command line: words separated by spaces, with no quoting inside (consumers expand it unquoted, as they always have). A consumer calls it as `$SRC_SQL <database>`, with the SQL on stdin. It prints rows tab-separated without headers, and its exit status is the SQL's. Server-level work passes `master` (SQL Server) or `postgres` (PostgreSQL) as the database. Oracle and Db2 ignore the database argument.

Without `sql_cmd`, `sql_client.py` is the default client. It connects as `user` with `password`, and the password reaches the client only through its environment (or the SQL, for sqlplus). It is an administrator only when `user` is one, so a test that creates databases needs `sql_cmd`.

| Engine | Default client | Password through |
|---|---|---|
| sqlserver | `sqlcmd -C` | `SQLCMDPASSWORD` |
| postgres, cockroachdb, yugabytedb | `psql` | `PGPASSWORD` |
| mysql, mariadb | `mysql`, `mariadb` | `MYSQL_PWD` |
| oracle | `sqlplus` | `CONNECT` on stdin |
| db2, informix | none: `sql_cmd` needed | |

## Scripts

- [scripts/lfc_source_contract/source_env.py](scripts/lfc_source_contract/source_env.py) `<source.json>`: validates the file and prints the exports. Use `eval "$(python3 -I scripts/lfc_source_contract/source_env.py source.json)"`.
- [scripts/lfc_source_contract/sql_client.py](scripts/lfc_source_contract/sql_client.py) `[database]`: the default client; `source_env.py` sets `SRC_SQL` to it. It reads `SRC_CONTRACT`.
- [scripts/test_source_env.py](scripts/test_source_env.py): offline checks on [scripts/fixtures/](scripts/fixtures/) (each engine, the refusals, the default clients on fake clients).
- **lfc-source-eventhubs** `eventhubs.sh up --source-json <file>`: writes the contract for an Event Hubs namespace's Kafka endpoint.
- **lfc-source-cloud-vm**'s slot databases: [scripts/lfc_source_contract/slot.py](scripts/lfc_source_contract/slot.py) `source` writes the contract for one from `lfc-slot up`'s JSON (`slot-source.sh` runs it). Its `sql_cmd` is `slot.py sql` for that database and key (older contracts name `slot-sql.sh`, which runs the same); `Slot` is the database from Python (up, sql, info, a shell in its container); that skill's `test-slot-source.sh` checks it end to end through a fake ssh.
- [scripts/lfc_source_contract/lakebase_sql.py](scripts/lfc_source_contract/lakebase_sql.py) `[database]`: the `sql_cmd` for a Lakebase project (below).
- [scripts/lfc_source_contract/jdbc.py](scripts/lfc_source_contract/jdbc.py): SQL on a source through a TYPE JDBC connection from a notebook or job, no password in the code: `JdbcSql(spark, connection).execute()` (writes, as `sessionInitStatement`) and `.query()`, `guarded()` for a statement safe to rerun, `connection_body()` with the Maven driver.
- [scripts/lfc_source_contract/cdc.py](scripts/lfc_source_contract/cdc.py): a PostgreSQL source's change capture on any DB-API cursor, safe to rerun: `publication(cur, name, schemas)` (FOR TABLE, created, tables added, or kept) and `slot(cur, name, drop=False)`. In a notebook the cursor is `jdbc.dbapi(spark, <JDBC connection>, read_connection=<the POSTGRESQL one>, database=...)`: writes through the JDBC connection, reads through `remote_query`.
- [scripts/pg-cdc-slot.py](scripts/pg-cdc-slot.py) and [scripts/pg-cdc-slot-drop.py](scripts/pg-cdc-slot-drop.py): notebooks that create, then drop, a pipeline's replication slot `lfc_<uuid>` (named by `cdc.names()`) on an existing publication through UC JDBC, with `cdc.pipeline_slot()`; the first prints the pipeline's `slot_config` ([PostgreSQL pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql-pipeline)). Not the gateway's own form, `dbx_<database OID>_<pipeline ID>`: a custom name in that form is refused (`POSTGRES_SLOT_NAME_CONFLICTS_WITH_CONNECTOR_FORMAT`).
- [scripts/lakebase-source.py](scripts/lakebase-source.py): a Lakebase project as the source in one step: password logins, the login, the UC connection, the contract (below); its code is the package [scripts/lfc_source_contract](scripts/lfc_source_contract), which a workspace job installs.

## A pipeline's PostgreSQL slot from a notebook

Every PostgreSQL pipeline needs its own replication slot (its `slot_config`), whatever the source: a VM, a cloud database or Lakebase.

1. **Get the notebook:** download [scripts/pg-cdc-slot.py](scripts/pg-cdc-slot.py) from its GitHub page, then Workspace > Import > File.
2. **Have:** a TYPE JDBC connection to the database whose login has `REPLICATION`, and a publication on the tables (or a login that owns them: the notebook makes one).
3. **Run:** set the widgets `uc_jdbc_connection`, `publication_name` (empty: make `lfc_pub_<uuid>`) and `schemas` (comma list), then Run all (serverless). Each run makes a new slot `lfc_<uuid>`.
4. **Use:** paste the printed `slot_config` into the pipeline's `source_configurations[].catalog`, next to `source_catalog`.
5. **Clean up:** after the pipeline is deleted, run [scripts/pg-cdc-slot-drop.py](scripts/pg-cdc-slot-drop.py) with the same `uc_jdbc_connection` and `publication_name`, and that `slot_name` (an active slot is refused).

## Lakebase as the source

A Lakebase project stands in for a customer's PostgreSQL when the point is to test Lakeflow Connect's PostgreSQL connector: same workspace, no VM, no firewall. (To get Lakebase data into Unity Catalog for its own sake, Lakebase's Lakehouse Sync needs no connector.) The contract is the same as for any PostgreSQL, so a test written against it runs unchanged on a customer's own database: change the file, not the test.

A project `lakebase-source.py --create-project` makes suspends its compute after 5 idle minutes (`--suspend-seconds`, `0` = never): Lakebase's own default is 24 hours, so a test project would otherwise run all day. A running CDC gateway keeps it awake; stop the pipeline and it scales to zero.

```json
{"contract": 1, "name": "lakebase", "engine": "postgres", "host": "<endpoint host>", "port": 5432,
 "user": "<replication role>", "database": "databricks_postgres", "connection": "<UC connection>",
 "sql_cmd": "env LB_PROJECT=<project> LB_PROFILE=<profile> /abs/path/lakebase_sql.py"}
```

`lakebase_sql.py` runs as the project owner through psql, with the owner's OAuth database credential from `lakebase.owner_login` (no password on the command line, a new credential per call, renewed from the account login when the workspace's has expired); `postgres` as the database means `databricks_postgres`. Before a run, [`scripts/lakebase-source.py`](scripts/lakebase-source.py) `--project P --role R --connection C --profile X --contract source.json` does the setup in one step: it turns on the project's password logins (`enable_pg_native_login`), creates or rotates the connection's login (`LOGIN REPLICATION`, a random password), creates or updates the UC `POSTGRESQL` connection from it, and writes this contract. The password goes only into the login and the connection. `--create-project` makes a missing project. From a notebook: `from lfc_source_contract.lakebase import setup`, then `setup(project, role=..., connection=...)`; with `database=`, `writer=` and `jdbc_connection=` it also makes a writer login and the TYPE JDBC connection holding it, for writes through `JdbcSql` (tested 2026-11-15: database, both logins, both connections). The same code runs as a serverless job, without `--profile` or `--contract`: a `python_wheel_task` with `package_name` `lfc-source-contract`, `entry_point` `lfc-lakebase-source`, and the package from the repo's archive URL in the environment's dependencies (`https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts`), which needs no Git credentials (a Git-source task does). Tested 2026-11-15 on an existing project and a new one: the connection read through the login, and still read after a rerun rotated the password. Tested on 2026-11-14 (PostgreSQL 17.12): as the owner, `CREATE DATABASE` and `CREATE PUBLICATION ... FOR TABLES IN SCHEMA` work, which **lfc-schema-change-test**'s `lib.sh` uses.

## Use

```bash
cat > source.json <<'EOF'
{"contract": 1, "engine": "sqlserver", "host": "203.0.113.10", "port": 1433, "user": "lfc_user",
 "sql_cmd": "env SLOT_HOST=203.0.113.10 SLOT_NAME=ss1 python3 -I /abs/path/slot.py sql"}
EOF
SOURCE_JSON="$PWD/source.json" PROFILE=<cli-profile> CATALOG=<catalog> <a scenario of lfc-schema-change-test>   # its SKILL.md: Run
```

Check a file before a run, and what it exports, without printing a password:

```bash
eval "$(python3 -I skills/lfc-source-contract/scripts/lfc_source_contract/source_env.py source.json)" && echo "$SRC_SQL" && echo 'SELECT 1' | $SRC_SQL master
```

## Errors

| Message | Cause | Fix |
|---|---|---|
| `missing <field>` | the file lacks a required field | add it (table above) |
| `contract <n> ... reads contract 1 only` | another version | use a `source_env.py` that reads `<n>`, or a producer that writes 1 |
| `no default SQL client for <engine>` | db2 or informix without `sql_cmd` | add `sql_cmd` |
| `no sql_cmd and no password` | the default client needs the password | add `password` or `sql_cmd` |
| `slot-source: ... holds the password: give --out` | `slot.py source` (`slot-source.sh`) without `--connection` or `--out` | pass one of them |
