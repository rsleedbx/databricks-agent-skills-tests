---
name: lfc-source-statschema
description: "Build a Lakeflow Connect test source shaped like a customer's database, or like a TPC benchmark, from statschema's canonical schema.yaml and stats.yaml: collect a customer's schema, statistics, and top queries from the catalog (no table scans; data values only if they allow it), scale it down, write DDL for any engine (Postgres, MySQL, SQL Server, Oracle, Databricks, ...), fill it inside the database with lfc-source-datagen, check the copy's statistics against the source's, and replay the source's queries on the copy to compare plans. Ships statschema's TPC-B, TPC-C, TPC-H, TPC-DS, TPC-E, and TPC-DI schemas."
parent: lfc-source-setup
---

# A test source shaped like the customer's database

Customers often say: *"I can't give you my data, but you can look at my schema and statistics."* That's enough to build a database that **behaves** like theirs for Lakeflow Connect testing (the same tables and keys, row counts at a chosen scale, the same nulls, cardinalities, skew, and value ranges, and the same query plans) without a single real row leaving their site. When there is no customer, the same pipeline builds a TPC benchmark schema.

The format is [statschema](https://github.com/rsleedbx/statschema)'s: **`schema.yaml`** (tables, columns in canonical types, keys, foreign keys, generation rules) and **`stats.yaml`** (column, index, and foreign-key statistics), plus `queries.yaml` for the workload. Every step reads and writes these files, so a schema collected from Postgres can be built on MySQL, SQL Server, or Databricks. The parts of statschema this skill needs (its model and file I/O) are in `scripts/statschema/`; the rest of the pipeline was rewritten with newer techniques (below).

**FIRST**: use **lfc-source-datagen** for the planners and `fill.sh`; this skill writes the tables and the formulas the planners fill them with.

## Pipeline

| Step | Where | Script | Output |
|---|---|---|---|
| 1. Collect schema and statistics | the customer's database | [`collect-postgres.sql`](scripts/collect-postgres.sql), [`collect-mysql.sql`](scripts/collect-mysql.sql) + [`mysql-normalize.jq`](scripts/mysql-normalize.jq) | `raw.json` |
| 1a. Collect each table's inserts, updates and deletes | the customer's database, after real traffic | [`collect-activity-postgres.sql`](scripts/collect-activity-postgres.sql), [`collect-activity-mysql.sql`](scripts/collect-activity-mysql.sql) | `activity.json` (for **cdc-table-plan**) |
| 1b. When each table is busy: changes per table per hour | the customer's database, from its change journal | [`collect-activity-hourly-sqlserver.sql`](scripts/collect-activity-hourly-sqlserver.sql) (SQL Server, tables with change tracking or CDC); others below | `activity-hourly.psv` |
| 2. To the canonical format | anywhere | [`to-canonical.py`](scripts/to-canonical.py) | `schema.yaml`, `stats.yaml` |
| 2a. Or import existing DDL | anywhere | [`ddl-to-schema.py`](scripts/ddl-to-schema.py) `--dialect <source>` (any sqlglot dialect; keeps lfc tags in comments, and checks) | `schema.yaml` |
| 3. Collect the top queries and their plans | the customer's database | [`collect-queries-postgres.sql`](scripts/collect-queries-postgres.sql), [`collect-queries-mysql.sql`](scripts/collect-queries-mysql.sql) | `queries.json` |
| 4. Shape: scale, and a formula per column | test side | [`shape.py`](scripts/shape.py) | `shaped.yaml` (still statschema's format), `expected.json` |
| 5. DDL for the target engine | test side | [`ddl.py`](scripts/ddl.py) | `schema.sql` |
| 6. Fill | the test database | **lfc-source-datagen** `scripts/fill.sh` | rows |
| 7. Check the statistics | test side | the step 1 collector on the copy, then [`compare.sql`](scripts/compare.sql) (DuckDB) | what's off |
| 8. Replay the queries | the test database | [`replay.py`](scripts/replay.py) `explain`, then `compare` | plan differences |

## When each table is busy

Step 1a gives totals; it doesn't say *when*. The busy tables change with the time of day: an OLTP database has its order and payment tables hot by day, and a nightly batch (aggregation, loads, purges) hits a different set. A change-capture test or sizing that assumes one busy set misses the other, so read the hours from the database's own change journal, over the window it still holds:

| Source | Journal | How |
|---|---|---|
| SQL Server | change tracking, CDC | [`collect-activity-hourly-sqlserver.sql`](scripts/collect-activity-hourly-sqlserver.sql): change tracking per changed key (net: a key changed twice in an hour counts once) with `sys.dm_tran_commit_table` times; CDC per change with `cdc.lsn_time_mapping`. Needs `VIEW SERVER STATE`. Tested 2026-10-23 on SQL Server 2022 |
| SQL Server, no CDC/CT | none kept | sample `sys.dm_db_index_usage_stats.user_updates` per table every hour and take the differences (cumulative since the last restart) |
| MySQL | binary log | `mysqlbinlog --base64-output=DECODE-ROWS -v <binlog file>`: each event's `#yymmdd hh:mm:ss` line and its `Table_map: \`db\`.\`table\`` give changes per table per hour (not run yet) |
| PostgreSQL | WAL | no per-table history is kept beyond the WAL; sample `pg_stat_user_tables` (`n_tup_ins`, `n_tup_upd`, `n_tup_del`) every hour with step 1a's collector and take the differences (not run yet) |
| Oracle | redo | LogMiner `V$LOGMNR_CONTENTS` grouped by `SEG_NAME` and the hour of `TIMESTAMP` (not run yet) |

Without the customer's database, the TPC workloads give the usual shapes: which tables each transaction writes, and which a batch maintains ([lfc-source-sizing references/workload-patterns.md](../lfc-source-sizing/references/workload-patterns.md)). Use both phases in a test: a few hot tables, then a different hot set (lfc-schema-change-test `sqlserver-ct-net-changes.sh` `BUSY_PCT`).

[`validate.py`](scripts/validate.py) loads any of the YAML files with statschema's own model, to confirm a file is still its format. For a TPC benchmark, skip steps 1–3 and start at step 4 with a schema from [`schemas/`](schemas/).

Python steps run with `uv run scripts/<script>.py`: each script declares its packages (PyYAML, sqlglot) in its first lines, so uv builds the environment itself; the checks need DuckDB and the MySQL collector jq. **Tools:** see **lfc-source-setup** "Tools" (`check-tools.sh base`).

## Run

The customer, on their database ([references/collect.md](references/collect.md) for privileges and fields):

```bash
psql -X -At -v schema=sales -v values=none -f collect-postgres.sql > raw.json                 # Postgres: catalog only
psql -X -At -v top=50 -v search_path=sales -f collect-queries-postgres.sql > queries.json     # plans only, nothing runs
# MySQL: the DBA builds histograms first (ANALYZE TABLE t UPDATE HISTOGRAM ON ...: samples each table once)
mysql -N -B -r -e "SET @db='sales', @vals='none'; SOURCE collect-mysql.sql" sales | jq -f mysql-normalize.jq > raw.json
mysql -N -B -r -e "SET @db='sales', @top=50; SOURCE collect-queries-mysql.sql" > queries.json
```

**Ask the customer which `values` level they accept** before they run it:

| `values` | The file has | The copy gets |
|---|---|---|
| `none` (default) | No data values: names, types, keys, counts, null fractions, distinct counts, frequencies, widths | The shape, with stand-in values |
| `ranges` | Also min, max, histograms, and most common values of numeric and date columns | Also real numeric and date ranges, peaks, and distributions |
| `all` | Also the most common values of text columns, and MySQL's statement samples | The same as `ranges`: text values are never copied |

Table and column names are in the files at every level; rename them consistently in the files if they are sensitive. The query texts have no literals (Postgres and MySQL replace them with `$1` / `?`).

Then, on the test side ([references/shape.md](references/shape.md) for how each column is chosen, [references/replay.md](references/replay.md) for replay):

```bash
PY="uv run"; S=skills/lfc-source-statschema/scripts
$PY $S/to-canonical.py raw.json schema.yaml stats.yaml
$PY $S/shape.py schema.yaml --stats stats.yaml --scale 0.01 > shaped.yaml                   # also writes expected.json
$PY $S/ddl.py shaped.yaml --dialect postgres --source-dialect postgres > schema.sql          # or mysql, tsql, oracle, db2, informix, databricks
psql -v ON_ERROR_STOP=1 -c "CREATE SCHEMA sales_s" -c "SET search_path = sales_s" -f schema.sql
DIR=skills/lfc-source-datagen/scripts/postgres bash skills/lfc-source-datagen/scripts/fill.sh pg sales_s
psql -c ANALYZE && psql -X -At -v schema=sales_s -v values=ranges -f $S/collect-postgres.sql > got.json
duckdb -c "SET VARIABLE expected = 'expected.json'; SET VARIABLE actual = 'got.json';" -c ".read $S/compare.sql"
$PY $S/replay.py explain queries.json --dialect postgres > explain.sql
psql -X -At -c "SET search_path = sales_s" -f explain.sql > plans.json
$PY $S/replay.py compare queries.json plans.json --dialect postgres --scale 0.01
```

A TPC benchmark: `$PY $S/shape.py $S/../schemas/tpcc.yaml --sf 10 > shaped.yaml`, then `ddl.py` and `fill.sh` as above. Show the plan before creating anything: the tables, total rows (`shape.py` prints them and every decision it made), and where.

## What the copy reproduces

| From | How |
|---|---|
| Row counts | `row_count_per_sf` × `--sf`, or a collected `row_count` × `--scale`; tables of `--keep-small` rows or fewer (default 1,000: lookup tables) keep their size |
| Generation rules (`schema.yaml`) | `sequential`, `cyclic`, `block`, `block_cyclic`, `constant`, `uniform`, `zipf` (inverse power-law CDF), `normal` (Box–Muller), value lists with weights, `format_pattern` (names, phones, addresses, emails, URLs, ids, words, sentences, TPC-C last names), `unique` |
| Null fraction, most common values and their frequencies | Exactly: each takes its share of a permutation of the rows, `(n − 1) × p mod rows` with a large prime `p` per column |
| Distinct count | At most the rows and the value range; one at least 10% of the rows grows with the table, a smaller one stays (categories) |
| Histograms | The values that aren't most common spread through the equi-height bounds, so the median and quartiles follow the source |
| String widths | Postgres `avg_width`, as an `lfc:len` tag |
| Keys | Rules that form a mixed radix (TPC-C's warehouse, district, customer) give `n` back from the key; other keys go through a codec per column (code lists, zero-padded digits, base 36 for short text, days or seconds for dates) that the children's foreign keys share |
| Foreign keys | Every value is a parent key: the parent's codec at a parent row drawn uniformly, by zipf, or by the source's share of parents with children and its hot parents |
| Indexes, unique keys, check constraints | Indexes and unique keys as in the source; checks left out (a cross-column check can fail: statistics don't relate columns) |
| Tables without a primary key | Stay without one (Postgres: `REPLICA IDENTITY FULL`); a timestamp (one second per row) or an integer column numbers the rows |
| Dates in order (`temporal_ordering_constraints`, `"a <= b"`, `<`, `>=`, `>`) | An `offset` rule (`distribution: offset`, `distribution_params: {from: a, min: 1, max: 30}`, days or seconds) makes `b` the formula of `a` plus each offset equally often, so the order holds in every engine. A constraint without one gets `GREATEST(b, a)`, logged: the order holds, but many rows sit at `a` |

**Not reproduced:** other relationships between columns (totals of lines, dates across tables), real text values, the physical order of rows. For TPC-H with the spec's dates across tables (ship = order + 1..121, commit = order + 30..90), prices and lines per order, which its `tpch-verify.sql` checks, use lfc-source-datagen's `scripts/tpc/tpc-schema.sh tpch`.

## Measured (4 vCPUs, Podman VM, Postgres 16 and MySQL 8.4)

statschema's TPC schemas, built by this pipeline (TPC-C at 2 warehouses, the others at SF 0.1), every table at its target and every foreign key enforced:

| Schema | Tables | Rows | Postgres | MySQL |
|---|---|---|---|---|
| TPC-B | 4 | 20,002 | 3 s | 11 s |
| TPC-C | 9 | 1,098,022 | 20 s | 48 s |
| TPC-H | 8 | 866,030 | 16 s | 35 s |
| TPC-DI | 18 | 102,567 | 4 s | 20 s |
| TPC-DS | 24 | 3,834,794 | 35 s | 102 s |
| TPC-E | 32 | 8,062,150 | not timed | 180 s |

A customer-shaped copy: TPC-H at SF 1 on Postgres as the "customer" (`values=ranges`), copied at 1% (86,630 rows): every one of the 61 columns' null fraction, distinct count, and top frequency, all 11 histogram medians, and every table's rows match within tolerance. Replaying its 10 top queries: all 10 plan on the Postgres copy, 8 with the source's plan shape (the other 2 pick a nested loop or an index where 100× the data had a hash join or a scan); all 10 plan on a MySQL copy, 5 with the same shape (the engines differ).

## What changed from statschema

| statschema | Here |
|---|---|
| Collects statistics with full scans on most engines (`COUNT(DISTINCT)`, `GROUP BY`, `NTILE`); MySQL's sampling uses `TABLESAMPLE`, which MySQL doesn't have, and falls back to a full scan | Catalog only: `pg_stats`, MySQL histograms and index cardinalities; data values only by `values` |
| Six hand-written DDL emitters that drop foreign keys, `CHAR`, and table comments | One emitter building sqlglot syntax trees: any sqlglot dialect, with foreign keys, comments, indexes |
| Rows generated in Python, pandas, or Spark and loaded | Generated inside the database from one integer per row (lfc-source-datagen) |
| Values drawn with `x % 100 < frequency` (1% steps) | Exact shares of a permutation of the rows |
| Table names in queries by regex; replay prints EXPLAIN text, and fails on `$1` / `?` | Tables by parsing; Postgres generic plans, MySQL placeholders set from the copy; a plan-shape comparison |
| Bugs in its TPC schemas: TPC-C's district ids don't wrap at 10 per warehouse (fixed in `schemas/tpcc.yaml`); TPC-E's `holding` has more rows than `trade` though each points at its own trade (`shape.py` caps it) | |
| `pip install` from git fails: the `.cursor` submodule points at a commit GitHub doesn't have | Not needed: the model lives in `scripts/statschema/` |

## Gotchas

- **Statistics are estimates.** Postgres samples 30,000 rows per table (`default_statistics_target` 100); MySQL's row counts are InnoDB's estimates and its histograms are sampled. The copy reproduces what the statistics say. A flagged high-cardinality column on a MySQL copy is often MySQL's estimate: check it with `COUNT(DISTINCT)` on the copy (it's test data).
- **MySQL caches row counts** in `information_schema` for up to a day: the collector sets `information_schema_stats_expiry = 0`; run `ANALYZE TABLE` on the copy before collecting from it.
- **MySQL comments hold 1,024 characters**: longer formulas go to `lfc_seed.tags`, which `ddl.py` fills and the MySQL planners read.
- **SQL Server and Oracle keep every tag in a side table** (`lfc_seed.tags` per schema; `LFC_TAGS` in the Oracle schema), which `ddl.py --dialect tsql` / `oracle` fills and their planners read: SQL Server has no comments, Oracle's hold 4,000 bytes. `ddl.py` also rewrites what sqlglot leaves MySQL-shaped (lfc-source-datagen's `references/sqlserver.md` and `oracle.md` list each): integer division, `LPAD`, `LEFT`, `CONCAT`, `ROUND`, `POWER`, `PI`, and on T-SQL a value list's nested CASE, which SQL Server can't compile at 150 values.
- **Db2 and Informix** (`--dialect db2` / `informix`), which sqlglot doesn't know, are rendered as Postgres and rewritten. Every tag goes to a side table (`LFC_TAGS` in the Db2 schema, `lfc_tags` in the Informix database), which lfc-source-datagen's `scripts/python/fill.py` reads. Names are unquoted, so Db2 folds them to upper case and Informix to lower. The rewrites, each found by a failing fill:
  - Informix types: `LVARCHAR` above 255 characters, `DATETIME YEAR TO FRACTION(5)` and `HOUR TO SECOND`, `SERIAL`, `DEFAULT` before `NOT NULL`, `ADD CONSTRAINT FOREIGN KEY`, no second index on the same columns.
  - Db2 types: `CLOB` and `BLOB` for unbounded columns, `DECIMAL` capped at 31 digits.
  - Arithmetic: `MOD(CAST(a AS BIGINT), b)` for `%` on Informix (`INT8 * INT` is a DECIMAL there, and `MOD` of one returns INTEGER); `TRUNC` for `DIV` on Informix; `POWER` of a `DOUBLE PRECISION`; `ACOS(-1)` for `PI()`.
  - Precedence: parentheses around arithmetic inside `||`, which binds like `+` there.
  - `%` in MySQL formulas: sqlglot's MySQL reader binds it at the level of `+` (`1 + a % 25` read as `(1 + a) % 25`), so `ddl.py` moves it to `*`'s level before any rewrite. Before this, the Informix `MOD` and the Oracle tags of TPC-B's `history.delta` and TPC-H's `nation.n_regionkey` (a foreign key that pointed at -1 … 3) were wrong; Postgres and MySQL output didn't change.
  - Casts: `DECIMAL(31, 0)` for a bare `DECIMAL` cast (Db2's is (5,0)); `ROUND` written as is (sqlglot's Postgres `ROUND` adds that bare cast).
  - Days and seconds since 1970: `DAYS()` and `MIDNIGHT_SECONDS` on Db2; `MDY()` and `TO_CHAR(x, '%H')` on Informix, kept out of sqlglot, which would rewrite the format.
  - The value list's nested `CASE` folded, as for T-SQL.
  - Oracle has no `TIME`: `TIMESTAMP(0)`.
- **MySQL binary logs grow with every load** (25 GB filled a 50 GB test VM during this skill's tests): on a test container without a CDC reader, purge them (`PURGE BINARY LOGS BEFORE NOW()`) or set `binlog_expire_logs_seconds`; with CDC, size the disk for them.
- **Keys are renumbered** (a source's nation keys 0 … 24 become what the rules or codecs say), and every foreign key follows, so joins work the same.
- **Text-key order:** a key's `n` must rise with its sort order (the resume probe reads the highest key); code lists are sorted, and digits and base 36 sort as numbers in byte order. A collation that sorts digits after letters breaks this.
