# PostgreSQL

Tested on Postgres 16 (Podman, arm64). The planners use `pg_catalog` only, so no extensions are needed. `ORDER BY key DESC LIMIT 1` finds the max through the primary-key index for any key type; Postgres 16 has no `max(uuid)`.

## Connect

Every command pipes SQL into `$SQL`, a `psql` session to the source database. Only SQL text goes over it.

```bash
PG="psql -h <host> -U <user> -d <database>"                    # password via PGPASSWORD from a umask 077 file
# Local Podman: PG="podman exec -i <container> psql -U postgres -d <database>"
SQL="$PG -At -q -v ON_ERROR_STOP=1"
```

## Files

They are in [`scripts/postgres/`](../scripts/postgres/); the commands below read them from `$D`.

**`cast.sql`: the cast table.** Session-only (`pg_temp`) functions that turn `x.n` into a valid value of any column type (`lfc_cast`), a primary key back into `n` (`lfc_to_int`), and a data column into a check against its key (`lfc_check`, for the audit). They exist only while the planner runs; the SQL they write doesn't need them. Domains use their base type, enums pick a label by position, arrays wrap the element cast.

[`scripts/postgres/cast.sql`](../scripts/postgres/cast.sql)

**`plan-insert.sql`: inserts** (`mode=append` adds `rows` per table; `mode=fill` tops each table up to its `lfc:rows=` comment; `order` and `data` choose shuffled keys and scrambled or random values):

[`scripts/postgres/plan-insert.sql`](../scripts/postgres/plan-insert.sql)

**`plan-audit.sql`: audit plain rows** (decode every checkable column back to `n` and compare it with the key; with `plain_every` it checks only the plain blocks):

[`scripts/postgres/plan-audit.sql`](../scripts/postgres/plan-audit.sql)

**`plan-delete.sql`: deletes from one end** (`end=head` or `tail`), dimension first: it re-points foreign keys that reference the keys going away, then deletes. It never empties a table:

[`scripts/postgres/plan-delete.sql`](../scripts/postgres/plan-delete.sql)

**`plan-status.sql`: min, max, and rows** for every table, one query per 500 tables:

[`scripts/postgres/plan-status.sql`](../scripts/postgres/plan-status.sql)

**`make-dataset.sql`: create N tables** with a variety of types and a target row count each:

[`scripts/postgres/make-dataset.sql`](../scripts/postgres/make-dataset.sql)

**`plan-ddl.sql`: random DDL** (see "Schema changes during a load" in the skill):

[`scripts/postgres/plan-ddl.sql`](../scripts/postgres/plan-ddl.sql)

**`skipped.sql`: tables the planner leaves out:**

[`scripts/postgres/skipped.sql`](../scripts/postgres/skipped.sql)

**`boundary.sql`: check the casts for a table** before loading it (any `ERROR` in the output is a bad cast):

[`scripts/postgres/boundary.sql`](../scripts/postgres/boundary.sql)

## Run

```bash
D=skills/lfc-source-datagen/scripts/postgres                       # the planner files (adjust to where the skill is)
S=lfc                                                                          # schema
# A dataset: 100 tables, even spread up to 1M rows, 12 columns each; the 10 smallest are dimensions, the rest have 2 FKs each
$SQL -v schema=$S -v tables=100 -v max_rows=1000000 -v cols=12 -v dist=even -v dims=10 -v fks=2 < $D/make-dataset.sql > /dev/null
$SQL -v schema=$S < $D/skipped.sql                                                # should print nothing
echo "CREATE SCHEMA IF NOT EXISTS lfc_seed; DROP TABLE IF EXISTS lfc_seed.seed;
CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY, r bigint NOT NULL);           -- r: a random permutation of i, for shuffled keys
INSERT INTO lfc_seed.seed SELECT i, r FROM (SELECT i, row_number() OVER (ORDER BY random()) AS r FROM generate_series(1, 1000000) i) q ORDER BY i;" | $SQL
PSQL="$PG" DIR=$D SESSIONS=4 python3 $D/../python/fill.py pg $S          # fill until done, level by level (see the skill's "Fill level by level")
$SQL -v schema=$S < $D/plan-status.sql > status.sql && $SQL < status.sql > status.out    # table|min|max|rows|target
awk -F'|' '{ t += $4 } END { print NR " tables, " t " rows" }' status.out
cat $D/cast.sql $D/plan-audit.sql | $SQL -v schema=$S -v plain_every=4 -v block=1000 -v lob_len=100 > audit.sql && $SQL < audit.sql   # same lob_len; bad must be 0
```

Other operations:

```bash
cat $D/cast.sql $D/plan-insert.sql | $SQL -v schema=$S -v rows=10000 -v src=series -v order=random -v data=random > more.sql && $SQL < more.sql   # append 10,000 per table (data=random: these rows can't be audited)
cat $D/cast.sql $D/plan-delete.sql | $SQL -v schema=$S -v rows=1000 -v end=head > deletes.sql && $SQL < deletes.sql   # 1,000 from each head, one session
cat $D/cast.sql $D/plan-ddl.sql | $SQL -v schema=$S -v ops=8 > ddl.sql && $SQL -v ON_ERROR_STOP=0 < ddl.sql 2>&1 | grep ERROR || true   # 8 random DDL; then run the fill loop again
cat $D/cast.sql $D/boundary.sql | $SQL -v table=$S.t001 > bnd.sql && $SQL -v ON_ERROR_STOP=0 < bnd.sql 2>&1 | grep ERROR || echo "no bad casts"
```

## Types

`n` is the row's `bigint`. All casts are in `cast.sql`; this table summarizes them. "Key" types can be the primary key (they map back to `n`).

| Category | Types | Range | Value from `n` | Key |
|---|---|---|---|---|
| Integer | `smallint`, `integer`, `bigint` (and `smallserial`, `serial`, `bigserial`), `oid` | ±32,767; ±2,147,483,647; ±9.2 × 10¹⁸; `oid` 0 … 4,294,967,295 | `n % 32767`, `n % 2147483647`, `n`, `n % 2^32` | yes |
| Exact numeric | `numeric(p,s)` (p ≤ 1,000), `numeric`, `money` | below 10^(p−s); `money` ±9.2 × 10¹⁶ | `(n % 10^min(p,18)) / 10^s`; `n`; `(n % 10^11) / 100` | `numeric` with scale 0 |
| Float | `real`, `double precision` | 6 / 15 significant digits | `n::real`, `n::float8` | — |
| String | `varchar(L)` (L ≤ 10,485,760), `char(L)`, `text`, `name` (63 bytes) | 1 GB per value | exactly L characters (`lob_len` for `text` and plain `varchar`): zero-padded 19-digit `n`, MD5 filler, and from L = 38 the zero-padded `n` again at the end; L < 19: the last L digits | L ≥ 19 |
| Binary | `bytea` | 1 GB | exactly `lob_len` bytes: 8-byte `n` (`int8send`), MD5 bytes, 8-byte `n` | — |
| Date / time | `date`, `timestamp(p)`, `timestamptz`, `time`, `timetz`, `interval` | dates 4713 BC … 5,874,897 AD; timestamps … 294,276 AD | 1970-01-01 plus `n % 2,932,896` days or `n % 253,402,300,800` s (≤ 9999-12-31); `time`: `n % 86400` s; `interval`: `n % 10^12` s | — |
| Boolean, UUID | `boolean`, `uuid` | | `n % 2 = 1`; `lpad(to_hex(n), 32, '0')::uuid` | `uuid` |
| JSON, XML | `json`, `jsonb`, `xml` | 1 GB | `{"n": n}`; `<n>n</n>` | — |
| Network | `inet`, `cidr`, `macaddr`, `macaddr8` | IPv4 / IPv6; 6 / 8 bytes | `0.0.0.0 + n % 2^32` (as `/32` for `cidr`); `n` as 12 / 16 hex digits | — |
| Bit string | `bit(L)`, `bit varying(L)` | | the low L bits of `n` (zero-padded above 64) | — |
| Geometric | `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle` | float8 coordinates | shapes from `n % 1000` (always non-degenerate) | — |
| Range | `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange`, and the `*multirange` types | | `[v, v + 1)` with `v` from `n` | — |
| Text search | `tsvector`, `tsquery` | | `to_tsvector('simple', 'w' \|\| n)`; `'w' \|\| n` | — |
| Other | `pg_lsn`; arrays of any type above; enums; domains | | `n` as `hi/lo`; `ARRAY[element cast]`; label `1 + n % count`; base type's cast | — |
| Not supported (`NULL`) | composite types, `regclass` and other `reg*` types, `"char"`, extension types (PostGIS etc.) | | | |

Tested: every type above at `n` = 1, 2, 1,000,003, 1,000,000,000,039, 2⁶², and 2⁶³ − 2, with no errors (62 columns, 372 values).

**Audit (`lfc_check`)** decodes these back to `n` modulo their wrap (strings and `bytea` also at the end, and their exact length, so truncation shows up): integers, `oid`, `numeric`, `money`, `double precision`, all string types (a value that isn't digits where `n` should be counts as bad, not as an error), `bytea`, `date`, `timestamp`, `timestamptz`, `time`, `interval`, `boolean`, `uuid`, `json`, `jsonb`, `inet`, `int8range`, and enums. `real` (exact only below 2²⁴) and the geometric, bit, and text-search types aren't checked. Tested: 0 bad rows on plain blocks; a changed date, varchar, uuid, JSON value, or inet address each shows up as one bad row, and so does a 5,000-character `text` cut to 4,096, a 5,000-byte `bytea` cut to 4,000, or a `varchar(100)` one character short. A change in the middle filler isn't detected.

## Limits

| Limit | Value | Source |
|---|---|---|
| Columns per table | 1,600, including dropped columns (tested: 1,601 fails) | [limits](https://www.postgresql.org/docs/current/limits.html) |
| Row size | Must fit an 8 KB page: 8,160 bytes in-row (tested: 1,600 `bigint` columns create, but a full row fails with "row is too big: size 12824, maximum size 8160"). Long variable-length values move out of row (TOAST) | [limits](https://www.postgresql.org/docs/current/limits.html) |
| Field size | 1 GB | [limits](https://www.postgresql.org/docs/current/limits.html) |
| Table size | 32 TB (8 KB blocks) | [limits](https://www.postgresql.org/docs/current/limits.html) |
| Rows in one `VALUES` list | No fixed limit (tested: 1,000,000 rows, 8.9 MB of SQL) | [INSERT](https://www.postgresql.org/docs/current/sql-insert.html) |
| Bind parameters per statement | 65,535 | [limits](https://www.postgresql.org/docs/current/limits.html) |
| Identifier length | 63 bytes (longer names are truncated silently) | [lexical structure](https://www.postgresql.org/docs/current/sql-syntax-lexical.html) |
| Columns per index | 32 | [limits](https://www.postgresql.org/docs/current/limits.html) |
| `numeric` | up to 131,072 digits before the point and 16,383 after; 1,000 declared digits | [numeric types](https://www.postgresql.org/docs/current/datatype-numeric.html) |

## Measured (Podman machine, 4 vCPU, XFS)

| Step | Time |
|---|---|
| 100-table dataset, even spread up to 1M rows (50.5M rows, 20 GB), 4 sessions | 2 min 48 s |
| Create 10,000 tables | 9.3 s |
| Plan inserts for 10,000 tables | 0.4 s |
| Insert 100 rows into each of 10,000 tables (1M rows) | 8.6 s |
| Status of 10,000 tables (20,000 index probes) | 6.2 s (plan 0.4 s) |
| 1M rows into one table, `generate_series` / seed table | 3.1 s / 3.0 s |
| 8 tables × 500,000 rows, 1 session / 4 sessions | 11.3 s / 7.0 s |
| 1M rows into one table: keys in order / shuffled / in order with `data=random` | 2.8 s / 4.3 s / 3.1 s (key index 21 MB / 30 MB) |
| Key order per 8,000-key batch: `random` / `asc` / `desc` | 3,982 down and 4,017 up / all up / all down |

**Foreign keys, tested** (two dimensions, one with a `varchar` key; a fact referencing both; a table referencing the fact; a self-referencing table): fill in level order with every foreign key enforced, facts spread over all 1,000 and 200 dimension keys; 150 keys deleted from every head and then every tail with 4 re-point UPDATEs and no violations, every key range still contiguous and the facts pointing only into the dimensions' remaining ranges; a refill continues above the max and the new facts reference the new dimension keys; a table with fewer rows than the delete count is left alone. A 12-table dataset with `dims=3, fks=2` filled level by level to exactly its targets and audited with 0 bad rows.

## Gotchas

- **Identity keys** (`GENERATED ALWAYS AS IDENTITY`) are inserted with `OVERRIDING SYSTEM VALUE`. Identity and `serial` sequences don't move: `SELECT setval(pg_get_serial_sequence('<schema>.<table>', '<key>'), <max>)` before anything else inserts.
- **Dropping thousands of tables** in one statement fails with `out of shared memory` (lock limit): drop them in batches of 500.
- **The seed table must be stored in `i` order** (`... ORDER BY i` when filling it): `row_number() OVER (ORDER BY random())` returns its rows sorted by the random value, which would make `r` ascending in a table scan and the "shuffled" keys sequential again.
- **`data=random`** draws one `random()` per row in a subquery that `OFFSET 0` keeps from being merged; without it, a cast that uses its input twice (ranges, strings) could get two different values.
- **Domains keep their `CHECK`s**: a domain on `int` with `CHECK (VALUE < 100)` rejects most values of `n % 2147483647`.
- `numeric` with a negative scale (Postgres 15+) isn't handled.
