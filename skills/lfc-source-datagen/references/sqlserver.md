# SQL Server

**Status: the fill planners are tested** (SQL Server 2022 CU, compatibility level 160, on an Azure VM in Podman): [`scripts/sqlserver/`](../scripts/sqlserver/) `cast.sql`, `plan-insert.sql`, `plan-status.sql`, `skipped.sql`, run by `fill.py ms` (client mode). TPC-B filled in 6 s and TPC-H SF 1 (8,660,030 rows, every foreign key enforced) in 179 s; on a MacBook in Lima under Rosetta (**lfc-source-podman**), 202 s, 4 sessions, 1M rows per statement, every table at its target. Tested types: the integer, `numeric`, `varchar`, `char`, `date`, `datetime2` casts the TPC schemas use; the other casts below are in `cast.sql` but not run yet, nor are the delete, DDL, and audit planners (not ported).

## Fill (tested)

```bash
# the client: a script that reads SQL on stdin and hands it to sqlcmd as a file (see Gotchas)
cat > sq <<'Q'
#!/bin/bash
f=$(mktemp); cat > $f; sqlcmd -C -S <host> -U <user> -d <database> -y 0 -i $f; r=$?; rm -f $f; exit $r
Q
chmod 700 sq      # password from SQLCMDPASSWORD in the environment, never on the command line
MSSQL=./sq python3 scripts/python/fill.py ms dbo
```

- Rows come from `INSERT ... WITH (TABLOCK) SELECT ... FROM ... CROSS APPLY GENERATE_SERIES(CAST(1 AS bigint), CAST(need AS bigint), CAST(1 AS bigint))`. Give the step: with start > stop and no step, `GENERATE_SERIES` counts down.
- The lfc tags live in `lfc_seed.tags (sch, tbl, col, tag)`, which statschema's `ddl.py --dialect tsql` fills: SQL Server has no table or column comments.
- `order=random` adds no `ORDER BY`: SQL Server sorts the rows by the clustered key before inserting them anyway.

## Gotchas (tested)

| Symptom | Cause | Fix |
|---|---|---|
| A long statement or tag has newlines in the middle of a word or string | `sqlcmd` splits **stdin** lines every 4,096 bytes, even inside a string literal | Give sqlcmd a file: `-i <file>` reads long lines whole |
| `The -h and the -y 0 options are mutually exclusive` | `-y 0` (no width limit for `max` types) can't drop the header | Use `-y 0` and filter the header lines (blank and dashes); `fill.py` does |
| `Msg 8632 ... expression services limit has been reached` | `CASE (CASE WHEN ... 150 branches END) WHEN 0 THEN ...`: a simple CASE is expanded into one comparison per WHEN, each with a copy of the inner CASE | `ddl.py` folds it into one searched CASE for T-SQL |
| `The round function requires 2 to 3 arguments` | MySQL's `ROUND(x)` | `ddl.py` writes `ROUND(x, 0)` |
| A zipf column all 0 or 1 | `POWER` returns its base's type: `POWER(150001, -0.2)` on an int is 0 | `ddl.py` casts the base to `FLOAT` |
| `Arithmetic overflow` in a key formula | `int` column `* 2147482693` overflows `int` | `ddl.py` casts integer columns to `bigint` in formulas |
| `'' + 5` is 5, not '5' | `+` adds when either side is a number; sqlglot writes MySQL's `CONCAT` as `+` | `ddl.py` keeps `CONCAT(...)`, and writes `LPAD` (which T-SQL lacks) as `RIGHT(REPLICATE(...) + s, n)` |


## Plan

The same design as the tested engines: a catalog query (`sys.columns`, `sys.types`, `sys.indexes`, `sys.index_columns`) writes one `INSERT ... SELECT` per table; each INSERT reads its own min and max key; targets live in an extended property on the table (`lfc_rows`), since SQL Server tables have no comment.

| Piece | SQL Server form |
|---|---|
| Row source up to 10,000 | `GENERATE_SERIES(1, n)` (SQL Server 2022+ and Azure SQL, database compatibility level 160); otherwise the seed table |
| Seed table | `INSERT ... VALUES` takes at most **1,000 rows**: insert 1..1000 with one `VALUES` list, then double with `INSERT INTO lfc_seed.seed SELECT i + (SELECT MAX(i) FROM lfc_seed.seed) FROM lfc_seed.seed` |
| Max key | `SELECT TOP (1) <key> FROM t ORDER BY <key> DESC` (one probe of the clustered key) |
| Delete from one end | `WITH c AS (SELECT TOP (k) * FROM t ORDER BY <key> ASC) DELETE FROM c` (`DESC` for the tail) |
| Identity keys | `SET IDENTITY_INSERT t ON` before the INSERT, `OFF` after; only one table per session at a time |
| Parallel insert | `INSERT INTO t WITH (TABLOCK) SELECT ...` can run in parallel into a heap or a clustered columnstore table (2016+, compatibility level 130+); into a clustered rowstore index the insert itself is serial (unverified) |
| Not insertable | `rowversion` / `timestamp` and computed columns: leave them out of the column list |

## Types

`n` is the row's `bigint`. Draft casts; none tested yet.

| Category | Types | Range | Draft value from `n` | Key |
|---|---|---|---|---|
| Integer | `bit`, `tinyint` (**unsigned 0 … 255**), `smallint`, `int`, `bigint` | 0/1; 0 … 255; ±32,767; ±2,147,483,647; ±9.2 × 10¹⁸ | `CAST(n % 2 AS bit)`, `CAST(n % 256 AS tinyint)`, `CAST(n % 32767 AS smallint)`, `CAST(n % 2147483647 AS int)`, `n` | integers |
| Exact numeric | `decimal(p,s)` / `numeric(p,s)` (p ≤ 38), `money`, `smallmoney` | below 10^(p−s); ±922,337,203,685,477.5807; ±214,748.3647 | `CAST((n % 10^min(p,18)) * 0.0…01 AS decimal(p,s))`; `CAST((n % 100000000000) / 100.0 AS money)`; `CAST((n % 20000000) / 100.0 AS smallmoney)` | `decimal(p,0)` |
| Float | `float(n)` (1 … 53), `real` | | `CAST(n AS float)`, `CAST(n AS real)` | — |
| Date / time | `date`, `datetime2(p)`, `datetime`, `smalldatetime`, `datetimeoffset(p)`, `time(p)` | `date`/`datetime2` 0001 … 9999; `datetime` 1753 … 9999 (3.33 ms steps); `smalldatetime` 1900-01-01 … 2079-06-06 (1 min) | `DATEADD(day, CAST(n % 2932897 AS int), '1970-01-01')`; `DATEADD(second, CAST(n % 86400 AS int), DATEADD(day, CAST((n / 86400) % 2932897 AS int), CAST('1970-01-01' AS datetime2)))` (`DATEADD` takes an `int`, so days and seconds are split); `DATEADD(minute, CAST(n % 94370000 AS int), '1900-01-01')`; `TODATETIMEOFFSET(<datetime2>, '+00:00')`; `CAST(DATEADD(second, CAST(n % 86400 AS int), CAST('00:00' AS time)) AS time)` | — |
| String | `char(L)`, `varchar(L)` (L ≤ 8,000), `varchar(max)`, `nchar(L)`, `nvarchar(L)` (L ≤ 4,000), `nvarchar(max)`, `text`, `ntext` | `max`: 2 GB | `LEFT(RIGHT(REPLICATE('0', 19) + CAST(n AS varchar(20)), 19) + REPLICATE(CONVERT(varchar(32), HASHBYTES('MD5', CAST(n AS varchar(20))), 2), k), L)` (100 for `max`, `text`); `N`-prefixed for the national types | L ≥ 19 |
| Binary | `binary(L)`, `varbinary(L)` (L ≤ 8,000), `varbinary(max)`, `image` | | `CAST(CAST(n AS binary(8)) + HASHBYTES('MD5', CAST(n AS varchar(20))) AS varbinary(L))` | — |
| UUID | `uniqueidentifier` | | `CAST('00000000-0000-0000-' + LEFT(h, 4) + '-' + RIGHT(h, 12) AS uniqueidentifier)` with `h = CONVERT(varchar(16), CAST(n AS binary(8)), 2)`. SQL Server sorts a `uniqueidentifier` by its **last 6 bytes first**, so `n`'s low 48 bits go there and keys stay in `n` order below 2⁴⁸ | `CONVERT(bigint, CONVERT(binary(6), RIGHT(CAST(key AS char(36)), 12), 2))` |
| XML, JSON | `xml`, `json` (SQL Server 2025, Azure SQL) | 2 GB | `CAST('<n>' + CAST(n AS varchar(20)) + '</n>' AS xml)`; `JSON_OBJECT('n': n)` | — |
| Spatial | `geometry`, `geography` | `geography` latitude ±90 | `geometry::Point(n % 1000, n % 997, 0)`; `geography::Point(n % 90, n % 180, 4326)` (latitude, longitude) | — |
| Other | `sql_variant`, `hierarchyid` | | `CAST(n AS sql_variant)`; `hierarchyid::Parse('/' + CAST(n % 2147483647 AS varchar(20)) + '/')` | — |
| Not insertable / not supported | `rowversion` (`timestamp`), computed columns; `vector` (2025) until tested | | | |

## Limits (from the docs, not yet tested)

| Limit | Value | Source |
|---|---|---|
| Columns per table | 1,024; 30,000 in a wide table with sparse columns | [maximum capacity specifications](https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server) |
| Bytes per row | 8,060 in-row; variable-length columns can move to row-overflow pages; `varchar(max)` / `varbinary(max)` up to 2 GB | same |
| Rows in `INSERT ... VALUES` | 1,000 (error 10738 above that); a `VALUES` derived table in `FROM` has no such limit | [table value constructor](https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql) |
| Parameters per call | 2,100 | maximum capacity specifications |
| Batch size | 65,536 × network packet size (4 KB default) | same |
| Recursive CTE depth | `MAXRECURSION` default 100; `OPTION (MAXRECURSION 0)` for unlimited, or up to 32,767 | [WITH common table expression](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) |
| `GENERATE_SERIES` | SQL Server 2022+ and Azure SQL; compatibility level 160 | [GENERATE_SERIES](https://learn.microsoft.com/en-us/sql/t-sql/functions/generate-series-transact-sql) |
| Identifier length | 128 characters | maximum capacity specifications |
| Index key | 900 bytes clustered, 1,700 nonclustered; 32 key columns; 999 nonclustered indexes | same |

## More gotchas (from the docs)

- **`tinyint` is unsigned** (0 … 255), unlike MySQL's.
- **`DATEADD` takes an `int`**: adding more than 2,147,483,647 seconds fails, so split into days and seconds.
- **`datetime` rounds** to .000, .003, or .007 seconds; use `datetime2` for exact values.
- **Logging:** a large load fills the transaction log in the `FULL` recovery model; CDC also needs the log. Size the log, or load before enabling CDC.
