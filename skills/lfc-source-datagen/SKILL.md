---
name: lfc-source-datagen
description: "Create and fill Lakeflow Connect source tables with large amounts of test data generated inside the database (INSERT ... SELECT from generate_series or a seed table, with a tested per-type cast of an integer), so no row data crosses the network. Handles requests like 'create 100 tables with up to 1 million rows each and a variety of data types', resumes after an interruption, deletes from either end of the key range, and reports fast min/max/row counts across 10,000+ tables. Also the quick reference for each engine's limits (max columns, row size, rows per VALUES list) and every data type's range and cast."
parent: lfc-source-setup
---

# Generate test data inside the source database

Loading 100 GB from a laptop or a notebook means pushing 100 GB over the network. This skill sends **only SQL text**: the database generates every row itself, from an integer.

**Tools:** the engine's SQL client (psql or mysql), and uv for the TPC scripts; see **lfc-source-setup** "Tools".

**Without a database CLI** (RDS, Aurora, Azure SQL and other managed services, or Databricks serverless), and for **Db2 and Informix**, use [`scripts/python/fill.py`](scripts/python/fill.py): the same fill through a pip-installable driver, for Postgres, MySQL, SQL Server, Oracle, Db2 and Informix. It writes the same SQL as the SQL planners and is as fast; see [references/python-drivers.md](references/python-drivers.md).

Two more scripts use the same drivers (`drivers.py`), for engines whose CLI can't do it:
- [`scripts/python/export.py`](scripts/python/export.py) `<engine> <schema> --tables a,b --out <dir>`: each table as CSV with a header, NULL as `\N`, a quote doubled inside quotes. On Informix it reads a `DATETIME HOUR TO ...` column as text: the pure-Python driver fails decoding one (`hour must be in 0..23`).
- [`scripts/python/sq.py`](scripts/python/sq.py) `<engine>`: SQL on stdin, tab-separated rows out, for Db2 and Informix, whose CLIs pad columns (Db2's CLP) or print no column layout at all (Informix's `dbaccess`). A line `@` ends a statement that can't end at `;` (a trigger body).
- **Informix:** fill with `--sessions 1`. Each connection sets `SET LOCK MODE TO WAIT 30`, since the default lock mode fails a concurrent writer at once (`-425` / ISAM `-107`), and several sessions still collided.

**A few rows from Databricks** (a test's inserts from a notebook or job, serverless included): Spark `mode("append")` through a TYPE JDBC Unity Catalog connection; a federation connection (`POSTGRESQL`, `MYSQL`, `SQLSERVER`) refuses Spark writes. [references/append-through-connection.md](references/append-through-connection.md). **Changes at a fixed rate** (say 10 rows a minute) from several uncoordinated notebooks, jobs and retries: statements that cap themselves, a lock for overlapping runs, and Postgres slot and publication setup, all through `sessionInitStatement`: [references/capped-dml-through-connection.md](references/capped-dml-through-connection.md).

How the planner works (every row from one integer, resuming, keys only at the ends, scrambled and random columns, the audit, foreign keys and the tags), and why generated values instead of realistic fake data: [references/how-it-works.md](references/how-it-works.md).

## Make a dataset

A typical request: *"Create 100 tables, rows uniformly distributed up to 1 million, with a variety of data types."*

**If the user doesn't know what schema they want, offer a standard one**: TPC-B (4 tables, the banking schema pgbench uses, including a table with no primary key) or TPC-H (8 tables, composite keys and foreign keys, 8.66M rows at scale factor 1), with the spec's sizes, keys, foreign keys, and constraints. See [references/tpc.md](references/tpc.md); `tpc-schema.sh` writes all six (TPC-C, TPC-DS, TPC-E, and TPC-DI shaped from **lfc-source-statschema**'s schemas). **If they have a database of their own but can't share its data**, build a copy with its shape from its statistics: **lfc-source-statschema**. Otherwise:

| Choice | Default | Options |
|---|---|---|
| Tables | as asked | Names `t001` … `t100` in the schema (or database) the user names |
| Rows per table | `even`: table *k* of *N* gets ⌈*k* × max ÷ *N*⌉ rows (10,000, 20,000, … 1,000,000) | `random`: a uniform pseudo-random count in 1 … max, the same on every run |
| Columns | 12 per table, plus a `bigint` key | Each table starts at a different place in the engine's type menu, so every type is covered |
| Type menu | About 20 common types per engine: integers, decimals, floats, text, dates and times, boolean, binary, uuid, JSON, a spatial type | Any type in the engine's cast table |
| Dimensions and facts | none | `dims=D, fks=K`: the first D tables (the smallest, with `even`) are dimensions; every other table gets K `NOT NULL` foreign-key columns pointing at them in turn |
| Key order | `random`: shuffled within each INSERT (about 55% slower and a 40% larger key index on Postgres than ascending, as with real random keys) | `asc`, `desc` |
| Long values | `varchar(L)`, `char(L)`, `binary(L)` are filled to their full length L; unbounded types (`text`, `bytea`, blobs) to `lob_len` = 100 | Any `lob_len` up to the type's maximum, to test how long values survive the pipeline |
| Column data | `scramble`, with every 4th block of 1,000 keys plain (`plain_every=4`, `block=1000`): no visible sequence, reproducible, and a quarter of the rows auditable | `seq`: every row plain and auditable; `random`: per-row random, different on every run; any `plain_every` (0 = no plain blocks) |

1. **Estimate the size and check the disk first.** Plan on about **400 bytes per row** for 12 columns of the default menu (measured: 50.5M rows took 20 GB on Postgres; 300 to 570 bytes per row on MySQL), plus the same again for WAL or binlog if logs are kept for CDC. 100 tables up to 1M rows (`even`) is 50.5M rows, about 20 GB of table data. **Stop and say so if that's more than about 70% of the free disk** — a full disk crashed MySQL in testing.
2. **Show the plan and wait for a yes:** tables, total rows, size estimate, and where it will be created.
3. **Create the tables** with the engine's `make-dataset` query.
4. **Build the seed table** once (any table above 10,000 rows needs it on MySQL; on Postgres it's optional).
5. **Fill:** plan with `mode=fill` (and the key order and column data asked for), then **run the levels in order**, each split across the calibrated number of sessions (4 on Postgres, 2 on MySQL, for 4 vCPUs), and **rerun until status shows every table at its target.** Measured on 4 vCPUs: 50.5M rows in 2 min 48 s on Postgres.
6. **Report** the status: each table's min, max, rows, and the total. Run the **audit** on the source too: it should show 0 bad rows.
7. **After the pipeline has run** (snapshot and CDC), audit the target the same way: decode each plain-block column from the key. The checks are in each engine's `cast` file (`lfc_check` / `chk`); the target's Databricks SQL forms depend on how Lakeflow Connect maps each type and aren't written yet.

### Portable: build many copies where the database is

To build many copies of a model (100 copies of TPC-C, each in its own schema `<model>_NNNN`), run the generator next to the database rather than over a network that can drop. [`scripts/bundle.sh`](scripts/bundle.sh) packs [`scripts/copies.sh`](scripts/copies.sh), `fill.sh` and every engine's planner files into one file. It needs only bash and the engine's SQL client on the target; no Python, nothing to install. The model's DDL comes from **lfc-source-statschema** (`shape.py`, `ddl.py`) on any machine with uv.

```bash
uv run ../lfc-source-statschema/scripts/shape.py ../lfc-source-statschema/schemas/tpcc.yaml --sf 0.01 > tpcc.yaml
uv run ../lfc-source-statschema/scripts/ddl.py tpcc.yaml --dialect tsql > model.sql
bash scripts/bundle.sh /tmp/datagen.tgz
scp /tmp/datagen.tgz model.sql sq vm:run/     # sq: SQL on stdin, connected to the target database, sqlcmd -y 0
ssh vm 'cd run && tar xzf datagen.tgz && MSSQL=$PWD/sq nohup bash copies.sh ms model.sql tpcc 1 100 4 > build.log 2>&1 &'
ssh vm 'tail -1 run/build.log'                # "all done", or "finished with failures"
```

`copies.sh` prints `done <schema> <seconds>s` per copy, reuses a schema that already has its tables and resumes a fill, so a rerun continues. SQL Server only for now. lfc-schema-change-test `sqlserver-scale.sh` uses it.

### Fill level by level

When columns have formulas that point at a parent (`lfc:v`), the parent must be complete before its children start. `fill.sh` fills one level at a time and moves on only when every table of the level is at its target; it re-plans every round, so a rerun resumes. It reads the planner files from `DIR` (`scripts/postgres`, `scripts/mysql`, `scripts/sqlserver`, or `scripts/oracle`), and stops if a round adds no rows.

[`scripts/fill.sh`](scripts/fill.sh):

```bash
DIR=scripts/postgres bash scripts/fill.sh pg tpch                                    # Postgres: psql, connection from PGHOST, PGUSER, ...
DIR=scripts/mysql MYSQL="mysql -h db.example.com -u lfc" bash scripts/fill.sh my tpch   # MySQL: password from MYSQL_PWD
MSSQL=./sq DIR=scripts/sqlserver bash scripts/fill.sh ms dbo                         # SQL Server: a client script (references/sqlserver.md)
ORACLE=./sq DIR=scripts/oracle bash scripts/fill.sh ora tpch                         # Oracle: a client script (references/oracle.md)
```

## Random values per row

A random function in an `INSERT ... SELECT` isn't always evaluated once per row, and the planner needs **one** random value per row: a cast can use its input twice (a range needs `v` and `v + 1`, a string pads and hashes `n`), and two different random values there would break it. So `data=random` draws the value once in a subquery the optimizer can't merge away (`OFFSET 0` on Postgres, `NO_MERGE` on MySQL), and every column scrambles that value.

| Engine | Per-row random | Notes |
|---|---|---|
| PostgreSQL | `random()` | Per row ✓ |
| MySQL | `RAND()` | Per row ✓. `RAND(<constant>)` repeats the same sequence |
| SQL Server | `ABS(CHECKSUM(NEWID()))` (or `RAND(CHECKSUM(NEWID()))` for a float in 0 … 1) | `RAND()` is evaluated **once per statement**, so every row gets the same value (from the docs, not tested yet) |
| Oracle | `DBMS_RANDOM.VALUE` | Per row (from the docs, not tested yet) |

## Schema changes during a load

Pipelines must survive DDL, so the skill can apply random DDL while it loads, and the load **recovers by re-planning**: nothing in the SQL files is saved state, so after any schema change the next plan reads the new catalog and continues. Each engine's `plan-ddl` writes N random operations, weighted like real schema changes:

| Weight | Operation | What the planner picks | Effect on the load |
|---|---|---|---|
| 25% | Add column | a random table and type; nullable, tagged `lfc:added` in its column comment | New rows get values; older rows are NULL, which the audit accepts for tagged columns |
| 20% | Modify column | widen a `varchar(L ≥ 38)` to 2L, `int` to `bigint`, or drop `NOT NULL` | Values written before stay valid under the new type (strings from 38 characters are audited by both ends, not length) |
| 15% | Create index | a column without one, of an indexable type | None |
| 15% | Truncate table | a table no foreign key points at | The next fill refills it, keys starting again at 1 |
| 10% | Create table | a new table with 6 columns and a random `lfc:rows=` target | The next fill picks it up |
| 10% | Drop column | not the key, not a foreign key, and the table keeps at least 2 columns | An INSERT already planned with the column fails and rolls back; the next plan leaves it out |
| 5% | Drop table | a table no foreign key points at | Its planned INSERTs fail; the next plan no longer has it |

Operations planned together can clash (one drops a column another modifies), and the database may reject some: run the DDL file **without stopping on errors**, then keep filling.

**Fill until done:** plan with `mode=fill`, run the levels, read the status (it shows each table's target), and repeat until no table is below its target. A failed INSERT rolls back, so every round can simply be planned again. Tested with 4 rounds of 8 random operations running during the fill: on Postgres one INSERT failed (a column dropped under it) and the next round recovered; on MySQL the DDL waited for running INSERTs (metadata locks), and none failed. On both, every table ended at its target, including the tables created mid-load, with no key gaps, every foreign key valid, and 0 bad rows in the audit.

### When a statement fails

Read the error and the table, then fix the cause **in the planner's input** (the cast file, or the table itself), not in the generated SQL, so the fix survives the next plan. This is why the skill is markdown: no fixed program could anticipate every failure.

Errors and fixes: [references/errors.md](references/errors.md).

## Quick limits

Limits per engine (columns, row size, rows per statement, identifiers, row generators, string and decimal sizes). Answer from that file; don't look the docs up: [references/limits.md](references/limits.md).

## Rows per statement

| Rows | Source | Notes |
|---|---|---|
| Up to about 10,000 | `generate_series` (Postgres), recursive CTE (MySQL) | Nothing to set up |
| 100,000 and more | `INSERT ... SELECT` from a seed table of keys `1..N` (`lfc_seed.seed`), built once per database | SQL Server and Oracle can run this in parallel |
| More than the seed holds | Rerun: each run continues from the new max | `fill` mode does this by itself |

**On Postgres and MySQL one INSERT runs on one core** whichever source you use (1M rows: about 3 s on Postgres, 5.5 s on MySQL, either way). Speed comes from **loading several tables at once**, up to a limit: past it, bulk inserts only flood the CPU or the disk.

**Calibrate before a big load** (about 100M rows or more): sweep the statement size, then the session count, on the actual server, watching the run queue and disk await, and use the winners for the whole load. The best size differs a lot by engine: **1M–2M rows per statement and 4 sessions on Postgres 16, 100k rows and 1–2 sessions on MySQL 8.4 (more sessions only raised iowait)** (4 vCPUs). See [references/calibrate.md](references/calibrate.md) for the script and the results.

## Rules

- **Tables need a one-column primary key** that maps back to `n` (an integer, an integer-valued `numeric`, a text key of at least 19 characters, or a `uuid` on Postgres), **or an `lfc:n` tag** for a composite key or no key at all. The planner skips other tables; the engine's "skipped tables" query lists them. Keyless tables stay keyless; on Postgres, CDC needs `REPLICA IDENTITY FULL` for them.
- **Only plain values can be audited.** Use `seq`, or keep `plain_every` blocks, if the target's values must be checked after CDC.
- **Run the levels in order.** A fact's INSERT probes its dimensions' ranges; if a dimension is still empty, a `NOT NULL` foreign key fails. Run delete files in one session, in order: each dimension's re-point UPDATEs come before its DELETE.
- **Foreign keys that aren't one column referencing the parent's primary key** (composite foreign keys, or ones referencing a non-key unique column) get the column's ordinary cast, and the database may reject the rows. Postgres needs an index on each foreign-key column for the re-point UPDATEs to be fast (`make-dataset` creates them; InnoDB always does).
- **Never delete from the middle** of the key range, and **never change a key** in an update: either breaks rows = max − min + 1 and the resume point.
- **Columns the database numbers are left out:** non-key identity, `serial`, and `AUTO_INCREMENT` columns. Generated (computed) columns too.
- **Unsupported column types get `NULL`.** A `NOT NULL` column of such a type makes that table's INSERT fail: add a cast for it, or change the table.
- **Show the plan and the size estimate before running it** on a shared database.

## Engines

| Engine | Reference | Status |
|---|---|---|
| PostgreSQL | [references/postgres.md](references/postgres.md) | Tested on 16: every type family, 10,000 tables, 50.5M-row dataset |
| MySQL | [references/mysql.md](references/mysql.md) | Tested on 8.4: every type, 10,000 tables, interrupt and resume. MariaDB 11.4: the same planners, all six TPC masters at SF 1 (**lfc-source-cloud-vm** `references/engine-masters.md`) |
| SQL Server | [references/sqlserver.md](references/sqlserver.md) | Fill planners tested on SQL Server 2022 (`GENERATE_SERIES`): TPC-H SF 1 in 179 s; delete, DDL, and audit planners not ported |
| Oracle | [references/oracle.md](references/oracle.md) | Fill planners tested on 23ai Free (`CONNECT BY LEVEL`): TPC-H SF 1 in 585 s; delete, DDL, and audit planners not ported |
| Db2 | [references/python-drivers.md](references/python-drivers.md) | Python fill only (`fill.py`, recursive CTE), tested on 11.5: all six TPC workloads at SF 0.1; needs a larger package cache and log (the reference) |
| Informix | [references/python-drivers.md](references/python-drivers.md) | Python fill only (`fill.py`, `CONNECT BY LEVEL`), tested on 14.10: all six TPC workloads at SF 0.1; no random order or `data=random`, no MD5 filler |
| CockroachDB | [references/python-drivers.md](references/python-drivers.md) | `fill.py postgres` (the rows from `generate_series`; the psql planner's `pg_temp` functions are refused), tested on v25.4: all six TPC workloads at SF 1 |
| YugabyteDB | [references/postgres.md](references/postgres.md) | The Postgres planners through psql, `SRC=series`, tested on 2026.1 at SF 1 (TPC-B to TPC-DS; TPC-E in progress when this was written) |

## Gotchas

- **Long loads:** run them in the background and check for progress early, then less often (**watch-progress**): a 1M-row MySQL statement shows no new rows until it commits, so watch its age in `processlist` and the table's size too.
- **Disk:** check free space against the estimate before a big load. Dropping 10,000 tables in one statement fails on Postgres (`out of shared memory`, lock limit): drop them in batches of 500.
- **CDC sees every generated row.** A large load writes a matching amount of WAL or binlog. Load before the pipeline starts if you only need a snapshot, and make sure log retention and disk can hold it.
- **Sequences and counters:** MySQL moves `AUTO_INCREMENT` past the inserted keys. Postgres `serial` and identity sequences don't move: run `setval` if anything else inserts into the table.
- **`int` keys wrap** at 2,147,483,647; use `bigint` keys for bigger tables.
- **A large database to start from:** generate it once, stop it, and keep its data directory as a master to clone from (see **lfc-source-podman** for local, or **lfc-source-cloud-vm** `references/master-snapshots.md` for cloud VMs).
