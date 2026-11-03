---
name: lfc-source-contract
description: "The source contract for Lakeflow Connect tests: one small versioned JSON file that says where a test's source database is (engine, host, port, login, database, and optionally a password and an admin SQL command), so a test takes its database only through it and works with a customer's own database, a Podman container, or a slot VM; also a Kafka broker (engine kafka: bootstrap, topic, SASL user and password file). Use when pointing a test at a source (SOURCE_JSON), writing a file for a database, or changing what a test reads from a source."
---

# Source contract (contract 1)

A Lakeflow Connect test takes its source database only through a contract file: `SOURCE_JSON=<file>` on the script. Set it in place of `SRC_SQL`, `CONN_USER` and `CONNECTION`; the environment variables still work when it isn't set.

Scripts that accept it: **lfc-schema-change-test** (`lib.sh` and the scripts that use it, `sqlserver-add-column.sh`, `integrated-scd2-ct.sh`, `parallel.sh` through a `- json:<file>` slot line), and **lfc-downstream-history**'s `sqlserver-history-snapshot.sh`; for a broker, **lfc-kafka-freshness-test**'s `connection.sh` and `pipeline.sh` (`--source-json`). The zerobus scripts build their own source containers, so they have no database for a contract to name.

## The file

```json
{"contract": 1, "name": "pg1", "engine": "postgres", "host": "203.0.113.10", "port": 30000,
 "user": "lfc_user", "database": "postgres", "connection": "me_pg1",
 "sql_cmd": "env SLOT_HOST=203.0.113.10 SLOT_NAME=pg1 /abs/path/slot-sql.sh"}
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

- The fields above hold, once `source-env.sh` has accepted the file.
- With `SOURCE_JSON`, the contract sets the variables it gives. Variables it doesn't give stay as they were in the environment.
- A consumer doesn't write the contract or change the database beyond what its test does.

## Version rule

Consumers read contract 1 only. `source-env.sh` exits 2 with one line on any other `contract` value, and on a missing or wrong field, so nothing runs against a source it can't read. A producer that writes a new version updates its consumers first.

## The admin command (`sql_cmd`)

A command line: words separated by spaces, with no quoting inside (consumers expand it unquoted, as they always have). A consumer calls it as `$SRC_SQL <database>`, with the SQL on stdin. It prints rows tab-separated without headers, and its exit status is the SQL's. Server-level work passes `master` (SQL Server) or `postgres` (PostgreSQL) as the database. Oracle and Db2 ignore the database argument.

Without `sql_cmd`, `sql-client.sh` is the default client. It connects as `user` with `password`, and the password reaches the client only through its environment (or the SQL, for sqlplus). It is an administrator only when `user` is one, so a test that creates databases needs `sql_cmd`.

| Engine | Default client | Password through |
|---|---|---|
| sqlserver | `sqlcmd -C` | `SQLCMDPASSWORD` |
| postgres, cockroachdb, yugabytedb | `psql` | `PGPASSWORD` |
| mysql, mariadb | `mysql`, `mariadb` | `MYSQL_PWD` |
| oracle | `sqlplus` | `CONNECT` on stdin |
| db2, informix | none: `sql_cmd` needed | |

## Scripts

- [scripts/source-env.sh](scripts/source-env.sh) `<source.json>`: validates the file and prints the exports. Use `eval "$(scripts/source-env.sh source.json)"`.
- [scripts/sql-client.sh](scripts/sql-client.sh) `[database]`: the default client; `source-env.sh` sets `SRC_SQL` to it. It reads `SRC_CONTRACT`.
- [scripts/test-source-env.sh](scripts/test-source-env.sh): offline checks on [scripts/fixtures/](scripts/fixtures/) (each engine, the refusals, the default clients on fake clients).
- **lfc-source-eventhubs** `eventhubs.sh up --source-json <file>`: writes the contract for an Event Hubs namespace's Kafka endpoint.
- **lfc-source-cloud-vm** `scripts/slot-source.sh`: writes the contract for a slot database from `slot.sh up`'s JSON. Its `sql_cmd` runs `slot-sql.sh` for that database and key; that skill's `test-slot-source.sh` checks it end to end through a fake ssh.

## Use

```bash
cat > source.json <<'EOF'
{"contract": 1, "engine": "sqlserver", "host": "203.0.113.10", "port": 1433, "user": "lfc_user",
 "sql_cmd": "env SLOT_HOST=203.0.113.10 SLOT_NAME=ss1 /abs/path/slot-sql.sh"}
EOF
SOURCE_JSON="$PWD/source.json" PROFILE=<cli-profile> CATALOG=<catalog> bash sqlserver-add-column.sh   # a test script of lfc-schema-change-test
```

Check a file before a run, and what it exports, without printing a password:

```bash
eval "$(skills/lfc-source-contract/scripts/source-env.sh source.json)" && echo "$SRC_SQL" && echo 'SELECT 1' | $SRC_SQL master
```

## Errors

| Message | Cause | Fix |
|---|---|---|
| `missing <field>` | the file lacks a required field | add it (table above) |
| `contract <n> ... reads contract 1 only` | another version | use a `source-env.sh` that reads `<n>`, or a producer that writes 1 |
| `no default SQL client for <engine>` | db2 or informix without `sql_cmd` | add `sql_cmd` |
| `no sql_cmd and no password` | the default client needs the password | add `password` or `sql_cmd` |
| `slot-source: ... holds the password: give --out` | `slot-source.sh` without `--connection` or `--out` | pass one of them |
