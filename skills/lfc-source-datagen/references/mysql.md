# MySQL

Tested on MySQL 8.4 (Podman, arm64) in its default strict mode (`STRICT_TRANS_TABLES`). Uses `information_schema` and recursive CTEs (MySQL 8.0+). `ORDER BY key DESC LIMIT 1` is a backward index scan, and `DELETE ... ORDER BY key LIMIT k` deletes from one end through the index.

## Connect

```bash
MY="mysql -h <host> -u <user>"                                  # password via MYSQL_PWD from a umask 077 file
# Local Podman: MY="podman exec -i <container> mysql -uroot"
SQL="$MY -N -B -r <database>"
```

`-N -B -r` prints bare values with no escaping, so the planners' output is runnable SQL. **Connect with a default database** (any one that exists, normally the target): `cast.sql` creates session-only temporary tables (`lfc_cols`, `lfc_keys`) in it.

## Files

**`cast.sql`: the cast table.** Builds the session table `lfc_cols`: for every column of every table in `@db` with a one-column key, the expression that turns `x.n` into a valid value, and for the key, the reverse. It disappears when the session ends.

[`scripts/mysql/cast.sql`](../scripts/mysql/cast.sql)

**`plan-insert.sql`: inserts** (`@mode='append'` adds `@rows` per table; `@mode='fill'` tops each table up to its `lfc:rows=` comment; `@order` and `@data` choose shuffled keys and scrambled or random values):

[`scripts/mysql/plan-insert.sql`](../scripts/mysql/plan-insert.sql)

**`plan-audit.sql`: audit plain rows** (decode every checkable column back to `n` and compare it with the key; with `@plain_every` it checks only the plain blocks):

[`scripts/mysql/plan-audit.sql`](../scripts/mysql/plan-audit.sql)

**`plan-delete.sql`: deletes from one end** (`@end='head'` or `'tail'`), dimension first: it re-points foreign keys that reference the keys going away, then deletes. It never empties a table:

[`scripts/mysql/plan-delete.sql`](../scripts/mysql/plan-delete.sql)

**`plan-status.sql`: min, max, and rows** for every table, one query per 500 tables:

[`scripts/mysql/plan-status.sql`](../scripts/mysql/plan-status.sql)

**`make-dataset.sql`: create N tables** with a variety of types and a target row count each:

[`scripts/mysql/make-dataset.sql`](../scripts/mysql/make-dataset.sql)

**`plan-ddl.sql`: random DDL** (see "Schema changes during a load" in the skill):

[`scripts/mysql/plan-ddl.sql`](../scripts/mysql/plan-ddl.sql)

**`skipped.sql`: tables the planner leaves out:**

[`scripts/mysql/skipped.sql`](../scripts/mysql/skipped.sql)

**`boundary.sql`: check the casts for a schema** with real strict-mode INSERTs into the tables of `@db` (any `ERROR` is a bad cast). It inserts test rows, so run it on a scratch database.

[`scripts/mysql/boundary.sql`](../scripts/mysql/boundary.sql)

## Run

The planners read user variables, so set them in the same session:

```bash
P=skills/lfc-source-datagen/scripts/mysql                       # the planner files (adjust to where the skill is)
D=lfc                                                                          # database (schema)
# A dataset: 100 tables, even spread up to 1M rows, 12 columns each; the 10 smallest are dimensions, the rest have 2 FKs each
{ echo "SET @db='$D', @tables=100, @max_rows=1000000, @cols=12, @dist='even', @dims=10, @fks=2;"; cat $P/make-dataset.sql; } | $SQL | $SQL
{ echo "SET @db='$D';"; cat $P/skipped.sql; } | $SQL                              # should print nothing
# Seed table: one 1,000-row VALUES list, then each INSERT ... SELECT doubles it (about 7 KB of SQL for 1,024,000 keys)
want=1000000
{ echo "CREATE DATABASE IF NOT EXISTS lfc_seed; DROP TABLE IF EXISTS lfc_seed.seed; CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY);"
  echo "INSERT INTO lfc_seed.seed (i) VALUES $(printf '(%s),' $(seq 1 1000) | sed 's/,$//');"
  n=1000; while [ $n -lt $want ]; do
    echo "INSERT INTO lfc_seed.seed (i) SELECT s.i + b.m FROM lfc_seed.seed s, (SELECT MAX(i) AS m FROM lfc_seed.seed) b;"; n=$((n * 2))
  done
  # r: a random permutation of i, for shuffled keys (InnoDB stores rows in i order, so a scan returns r shuffled)
  echo "CREATE TABLE lfc_seed.seed_r (i bigint PRIMARY KEY, r bigint NOT NULL);"
  echo "INSERT INTO lfc_seed.seed_r SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed;"
  echo "DROP TABLE lfc_seed.seed; RENAME TABLE lfc_seed.seed_r TO lfc_seed.seed;"; } | $SQL
MYSQL="$MY" DIR=$P SESSIONS=2 python3 $P/../python/fill.py my $D          # fill until done, level by level (see the skill's "Fill level by level")
{ echo "SET @db='$D';"; cat $P/plan-status.sql; } | $SQL > status.sql && $SQL < status.sql > status.out   # table, min, max, rows, target
awk '{ t += $4 } END { print NR " tables, " t " rows" }' status.out
{ echo "SET @db='$D', @plain_every=4, @block=1000, @lob_len=100;"; cat $P/cast.sql $P/plan-audit.sql; } | $SQL > audit.sql && $SQL < audit.sql   # same @lob_len; bad must be 0
```

Other operations:

```bash
{ echo "SET @db='$D', @rows=10000, @src='series', @mode='append', @order='random', @data='random';"; cat $P/cast.sql $P/plan-insert.sql; } | $SQL > more.sql && $SQL < more.sql   # random data: not auditable
{ echo "SET @db='$D', @rows=1000, @end='tail';"; cat $P/cast.sql $P/plan-delete.sql; } | $SQL > deletes.sql && $SQL < deletes.sql   # one session
{ echo "SET @db='$D', @ops=8;"; cat $P/plan-ddl.sql; } | $SQL > ddl.sql && $SQL --force < ddl.sql 2>&1 | grep ERROR || true   # 8 random DDL; then run the fill loop again
{ echo "SET @db='<scratch-db>';"; cat $P/cast.sql $P/boundary.sql; } | $SQL > bnd.sql && $SQL --force < bnd.sql 2>&1 | grep ERROR || echo "no bad casts"
```

## Types

`n` is the row's `BIGINT`. All casts are in `cast.sql`; this table summarizes them. "Key" types can be the primary key.

| Category | Types | Range | Value from `n` | Key |
|---|---|---|---|---|
| Integer | `tinyint`, `smallint`, `mediumint`, `int`, `bigint`, signed or `unsigned`; `boolean` (= `tinyint(1)`) | signed ±127 / ±32,767 / ±8,388,607 / ±2,147,483,647 / ±9.2 × 10¹⁸; unsigned 255 / 65,535 / 16,777,215 / 4,294,967,295 / 1.8 × 10¹⁹ | `n % <type max>` (`boolean` gets 0 … 126) | yes |
| Exact numeric | `decimal(p,s)` (p ≤ 65, s ≤ 30) | below 10^(p−s) | `(n % 10^min(p,18)) * 0.0…01` (exact; dividing would round to 4 decimals and overflow `decimal(12,10)`) | — |
| Float | `float`, `double` | 7 / 15 significant digits | `CAST(n AS FLOAT)`, `CAST(n AS DOUBLE)` | — |
| Bit | `bit(M)` (M ≤ 64) | 0 … 2^M − 1 | `n % 2^M` | — |
| Date / time | `date`, `datetime(fsp)`, `timestamp(fsp)`, `time(fsp)`, `year` | `date` 1000 … 9999; `timestamp` 1970-01-01 00:00:01 … 2038-01-19 UTC; `time` ±838 h; `year` 1901 … 2155 | 1970-01-01 plus `n % 2,932,896` days or `n % 253,402,300,800` s; `FROM_UNIXTIME(1 + n % 2147483646)`; `SEC_TO_TIME(n % 86400)`; `1901 + n % 255` | — |
| String | `char(L)` (L ≤ 255), `varchar(L)`, `tinytext`, `text`, `mediumtext`, `longtext` | `varchar` up to 65,535 bytes (16,383 utf8mb4 characters), less the rest of the row | exactly L characters (`@lob_len` for text types, capped at the type's max): zero-padded 19-digit `n`, MD5 filler, and from L = 38 the zero-padded `n` again at the end; L < 19: the last L digits | `char`/`varchar` with L ≥ 19 |
| Binary | `binary(L)`, `varbinary(L)`, `tinyblob` … `longblob` | | exactly L bytes (`@lob_len` for blobs): 8-byte big-endian `n`, MD5 bytes, and from 16 bytes the 8-byte `n` again at the end | — |
| JSON | `json` | `max_allowed_packet` | `JSON_OBJECT('n', n)` | — |
| Enum, set | `enum(...)` (≤ 65,535 values), `set(...)` (≤ 64 members) | | value number `1 + n % <count>`; bit mask `n % 2^<members>` | — |
| Spatial | `geometry`, `point`, `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`, `geometrycollection`, in any `SRID` | | WKT shapes with coordinates 0 … 89 (valid latitude and longitude), in the column's SRID | — |
| Not supported (`NULL`) | `vector` (MySQL 9) and anything else | | | |

Tested: every type above at `n` = 1, 2, 1,000,003, 1,000,000,000,039, 2⁶², and 2⁶³ − 2 with strict-mode INSERTs, no errors (58 columns, 348 rows).

**Audit (`chk` in `lfc_cols`)** decodes these back to `n` modulo their wrap (strings and binaries also at the end, and their exact length, so truncation shows up): integers, `decimal`, `double`, `bit`, `date`, `datetime`, `timestamp`, `time`, `year`, all string types (non-digits where `n` should be count as bad), `binary`/`varbinary` of 8 bytes or more and the blob types, `json`, `enum`, and `set`. `float` and the spatial types aren't checked. Tested: 0 bad rows on plain blocks; a changed date, varchar, JSON value, or binary value each shows up as one bad row, and so does a 5,000-character `mediumtext` cut to 4,096, a 5,000-byte `mediumblob` cut to 4,000, or a `varchar(100)` / `varbinary(50)` one short. A change in the middle filler isn't detected.

## Limits

| Limit | Value | Source |
|---|---|---|
| Columns per table | 1,017 with InnoDB (tested: 1,018 fails "Too many columns"); the server's 4,096 is unreachable with InnoDB | [column count limit](https://dev.mysql.com/doc/refman/8.4/en/column-count-limit.html) |
| Row size | 65,535 bytes declared, BLOB/TEXT counting 9–12 bytes (tested: 1,017 `varchar(100)` fails); about 8,126 bytes in a 16 KB page (tested: a full row of 1,017 `bigint` fails "Row size too large (> 8126)") | [InnoDB limits](https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html) |
| Rows in one `INSERT ... VALUES` | Bounded by `max_allowed_packet`: 64 MB default in 8.0 and 8.4 (tested), 1 GB max. Tested: 1,000,000 rows (8.9 MB) | [system variables](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html) |
| Placeholders per prepared statement | 65,535 | |
| Recursive CTE depth | `cte_max_recursion_depth`, default 1,000 (the insert planner raises it per statement) | [WITH](https://dev.mysql.com/doc/refman/8.4/en/with.html) |
| `group_concat_max_len` | default 1,024 bytes (the planners raise it) | [system variables](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html) |
| Identifier length | 64 characters | [identifier length](https://dev.mysql.com/doc/refman/8.4/en/identifier-length.html) |
| Index key length | 3,072 bytes (DYNAMIC rows); 64 secondary indexes; 16 columns per index | [InnoDB limits](https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html) |
| Partitions | 8,192 | [partitioning limits](https://dev.mysql.com/doc/refman/8.4/en/partitioning-limitations.html) |

## Measured (Podman machine, 4 vCPU, XFS)

| Step | Time |
|---|---|
| 100-table dataset, even spread up to 200,000 rows (10.1M rows), 4 sessions | 21 s to finish after an interrupt at 4.7M rows; a third run added nothing |
| Create 10,000 tables | 40 s |
| Plan inserts for 10,000 tables | 1.0 s |
| Insert 100 rows into each of 10,000 tables (1M rows) | 18 s |
| Status of 10,000 tables | 5.1 s (plan 0.5 s) |
| 1M rows into one table, recursive CTE / seed table | 5.5 s / 5.7 s |
| Seed table of 1,024,000 keys (VALUES + 10 doublings) | 2.1 s |

**Foreign keys, tested** with the same star schema as Postgres (a `varchar`-keyed dimension, a fact referencing a fact, a self-referencing table): fill in level order, head and tail deletes with re-pointing and no violations, contiguous key ranges, a refill onto new dimension keys, and a 12-table `@dims=3, @fks=2` dataset filled to its targets with 0 bad rows in the audit.

## Gotchas

- **Disk:** a killed INSERT rolls back, which needs disk too. A full disk crashed MySQL 8.4 during a rollback in testing: check free space first.
- `AUTO_INCREMENT` keys get explicit values; MySQL moves the counter past them. Generated columns are left out; columns with a `DEFAULT CURRENT_TIMESTAMP` get generated values.
- **`@data='random'`** draws one `RAND()` per row in a derived table kept unmerged with `/*+ NO_MERGE(x) */`, so a cast that uses its input twice gets the same value both times.
- `boolean` is `tinyint(1)`, so it gets `n % 127`, not just 0 and 1.
- `timestamp` values are converted from the session time zone: set `time_zone = '+00:00'` for the same values everywhere.
- An `enum` or `set` value list that contains commas inside a value is miscounted.
