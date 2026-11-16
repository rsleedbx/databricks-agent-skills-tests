# TPC schemas

Most people don't know what schema to test with. The TPC benchmarks define well-known ones, with exact row counts per scale factor, keys, foreign keys, and check constraints. This reference builds them with the planners in [postgres.md](postgres.md) and [mysql.md](mysql.md): the **sizes, keys, foreign keys, and constraints follow the spec**, and so do the spec's key formulas and value ranges, but the data comes from the planners' casts, not from TPC's `dbgen`, so text columns (names, comments, flags) don't use the spec's word lists. The benchmark queries run, but their answers won't match TPC's. This is "a TPC-H schema filled by lfc-source-datagen", never a TPC result (TPC's fair-use policy).

| Workload | Spec | Tables | Status |
|---|---|---|---|
| TPC-B (also TPC-A: the same 4 tables) | 2.0.0 | branch, teller, account, history | ✓ Postgres 16 and MySQL 8.4 |
| TPC-H (also TPC-D 2.1.0 and TPC-R 2.1.0: the same 8 tables) | 3.0.1 | region, nation, supplier, part, partsupp, customer, orders, lineitem | ✓ Postgres 16 and MySQL 8.4 |
| TPC-C | 5.11.0 | 9 tables, composite keys per warehouse | ✓ Postgres 16 and MySQL 8.4 (`tpc-schema.sh tpcc`, below) |
| TPC-DS | 4.0.0 | 7 fact and 17 dimension tables | ✓ Postgres 16 and MySQL 8.4 (`tpc-schema.sh tpcds`, below) |
| TPC-E | 1.14.0 | 32 tables in statschema's schema | ✓ Postgres 16 and MySQL 8.4 (`tpc-schema.sh tpce`, below) |
| TPC-DI | 1.1.0 | warehouse Dim and Fact tables | ✓ Postgres 16 and MySQL 8.4 (`tpc-schema.sh tpcdi`, below) |
| TPC-W / TPC-APP | 2.0.0 / 1.3.0 | 8-table online bookstore | Planned |
| TPC-VMS, TPCx-* | | No schema of their own (VMS runs C, E, H, DS; HS is a sort, IoT key-value, AI files; BB is DS plus logs; V is based on E) | Not applicable |

**All six (TPC-B, C, H, DS, E, DI) are also in statschema's canonical format** in **lfc-source-statschema** (`schemas/`), with each column's generation rule: build any of them at any scale with its `shape.py --sf N`, `ddl.py` (Postgres, MySQL, SQL Server, Oracle, Databricks), and `fill.sh`; that skill lists the measured fills. The TPC-B and TPC-H here follow the spec's own key formulas more closely (TPC-H's sparse order keys, the `partsupp` supplier formula, the date and price relationships in `lineitem`), so `tpch-verify.sql` checks them.

The specs are free PDFs at `https://www.tpc.org/TPC_Documents_Current_Versions/pdf/<name>.pdf` (for example `TPC-H_v3.0.1.pdf`); only the tools need TPC's download form, and these schemas don't use them.

## How the spec maps onto the planners

The planners need each row to come from one integer `n`. Three comment tags let a schema say how, so no planner code is specific to a workload:

| Tag | On | Meaning | Example |
|---|---|---|---|
| `lfc:v={formula}` | a column | The column's value is its usual cast of this integer formula in `x.n`, instead of `n` or scrambled `n`. Never scrambled; the audit checks it against the formula | `o_orderkey`: `((x.n - 1) DIV 8) * 32 + (x.n - 1) % 8 + 1` (the spec's sparse keys); `l_discount`: `h(n) % 11` (0.00 … 0.10) |
| `lfc:n={formula}` | a table | `n` back from a row, for a composite key, a key from a formula, or a table without a primary key | `lineitem`: `(((l_orderkey - 1) DIV 32) * 8 + (l_orderkey - 1) % 32) * 4 + l_linenumber` |
| `lfc:by={column}` | a table | The column `n` rises with, for the min and max probes (default: the first key column); the probes read the rows with the highest (lowest) value of it and take the max (min) of `lfc:n` | `history` (no key): `h_time` |

`n` must rise with the `lfc:by` column (rows sharing a value can come in any order: a `partsupp` part's 4 suppliers). Formulas use only integer arithmetic, `DIV`, `%`, and `CASE`, so they mean the same on every engine; `h(e, a)` in `tpc-schema.sh` is the non-linear scramble `(e mod p)((e mod p + a) mod p) mod p`, `p = 2³¹ − 1`, for the spec's "random value" columns, the same on every run.

Rules that follow from the spec:

- **Tables the spec gives no primary key keep none** (TPC-B's `history`; TPC-C's `history` too). On Postgres they get `REPLICA IDENTITY FULL`, which CDC needs for such tables, as customers' keyless tables do. Their min and max probes scan the table unless the `lfc:by` column has an index; add a plain index for large ones (an index, not a key).
- **Parents first, completely.** A formula like `l_partkey = 1 + h(n) % 200000` assumes `part` is full, so `fill.sh` (in the skill's "Fill level by level") fills one level at a time and moves on only when every table of the level is at its target.
- **Not yet:** deletes. The delete planners skip tables with `lfc:n` and parents whose children's foreign key is part of the child's key or has a formula (re-pointing those would change a key). TPC-H's refresh functions (RF1, RF2) would delete an order with its lines; that's the next step.

Where it differs from the spec:

| Spec | Here | Why |
|---|---|---|
| TPC-H `lineitem`: 1 to 7 lines per order, random (6,001,215 at SF 1) | Exactly 4 (6,000,000 at SF 1) | `n` maps to (order, line) only with a fixed count |
| TPC-H `o_totalprice`, `o_orderstatus`, `l_returnflag`, `l_linestatus`: derived from other rows and dates | Ordinary casts (non-negative, the right type and length) | Derived from other rows; not needed for size or constraints |
| Text columns (`p_name`, `c_phone`, comments, …): word lists and formats | Zero-padded `n` and MD5 filler, at the declared length | The planners' auditable strings |
| TPC-B `history` size: storage for the measurement interval | `HISTORY` rows (default: as many as accounts) | The spec ties it to the benchmark run |
| TPC-B `h_delta`: the transaction's amount | `n` / 100 | Gives `history`, which has no key, a column that maps back to `n` |
| TPC-DS and TPC-E word-list text columns (11 and 7 columns) whose formula would pass MySQL's 1,024-character comment | No tag: the planner's default text cast | A comment holds 1,024 characters; the DDL has no side table |

## Files

- [`scripts/tpc/tpc-schema.sh`](../scripts/tpc/tpc-schema.sh): The six workloads as MySQL DDL with the tags; `bash tpc-schema.sh tpch 10` for scale factor 10, `bash tpc-schema.sh tpcds 1` for TPC-DS.

- [`scripts/lfc_source_datagen/tpc_schema.py`](../scripts/lfc_source_datagen/tpc_schema.py): TPC-B and TPC-H from the spec, the code `tpc-schema.sh` runs for those two. From a notebook: `%pip install "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-datagen/scripts"`, then `from lfc_source_datagen.tpc_schema import tpc_schema` and `tpc_schema("tpch", 0.1)`; statschema's `ddl_to_schema()` and `ddl()` (package `lfc-statschema`) turn it into any engine's DDL.


- [`scripts/tpc/tpcb-verify.sql`](../scripts/tpc/tpcb-verify.sql): TPC-B checks.

- [`scripts/tpc/tpch-verify.sql`](../scripts/tpc/tpch-verify.sql): TPC-H checks.

- [`scripts/tpc/tpch-sources.sh`](../scripts/tpc/tpch-sources.sh), [`tpch-labels.sql`](../scripts/tpc/tpch-labels.sql), [`tpch-lead-times.sql`](../scripts/tpc/tpch-lead-times.sql): TPC-H as several business sources (Postgres): [tpch-sources.md](tpch-sources.md).

## Run

`tpc-schema.sh` writes a workload's tables as MySQL DDL, with the tags in the column and table comments: all six workloads, one entry point (`tpc-schema.sh <tpcb|tpcc|tpch|tpcds|tpce|tpcdi> [SF]`). TPC-C, TPC-DS, TPC-E and TPC-DI are shaped from statschema's schemas (`shape.py --sf`, then `ddl.py --dialect mysql`). **lfc-source-statschema**'s `ddl-to-schema.py` turns that into its canonical `schema.yaml` (keeping the tags and the checks), and its `ddl.py` writes the DDL for any engine, transpiling the formulas with [sqlglot](https://github.com/tobymao/sqlglot) (`DIV` stays integer division). Then `fill.sh` (in the skill's "Fill level by level") fills it with the engine's planners, one level at a time, and the verify file checks the spec's rules on the data.

```bash
SS=../lfc-source-statschema/scripts                                      # from this skill's directory
bash scripts/tpc/tpc-schema.sh tpch 1 | uv run $SS/ddl-to-schema.py --dialect mysql > tpch.yaml   # scale factor 1: 8.66M rows
# Postgres: schema tpch
{ echo "CREATE SCHEMA tpch; SET search_path = tpch;"; uv run $SS/ddl.py tpch.yaml --dialect postgres; } | psql -v ON_ERROR_STOP=1
DIR=scripts/postgres bash scripts/fill.sh pg tpch
{ echo "SET search_path = tpch;"; sed -E 's/DATEDIFF\(([^,]+), ([^)]+)\)/(\1 - \2)/g' scripts/tpc/tpch-verify.sql; } | psql -At
# MySQL: database tpch
{ echo "CREATE DATABASE tpch; USE tpch;"; uv run $SS/ddl.py tpch.yaml --dialect mysql; } | mysql
# Any other engine's DDL: uv run $SS/ddl.py tpch.yaml --dialect tsql (or oracle, databricks)
DIR=scripts/mysql bash scripts/fill.sh my tpch
mysql -N tpch < scripts/tpc/tpch-verify.sql
```

Then the engine's audit (`plan-audit` with `plain_every=4`) checks every column with a formula and every plain block: 0 bad rows. The planner files come from [postgres.md](postgres.md) or [mysql.md](mysql.md), created in the `pg` or `my` directory, and both need the seed table `lfc_seed.seed` with its random column `r`.

## Measured (4 vCPUs, Podman VM, Postgres 16 and MySQL 8.4, one engine at a time)

| Workload | Rows | Postgres fill | MySQL fill | Checks |
|---|---|---|---|---|
| TPC-B, scale 10 | 2,000,110 | 23 s | 1 min 25 s | 10 tellers and 100,000 accounts per branch; every history row's branch is its teller's; `history` has no primary key (Postgres: replica identity FULL); audit 0 bad on both |
| TPC-H, SF 1 | 8,660,030 | 3 min 39 s | 38 min 30 s | Exact row counts; region and nation keys 0 … 4 and 0 … 24 with the spec's nation-to-region map; sparse order keys; `o_custkey % 3 <> 0`; 4 distinct suppliers per part; every line's (part, supplier) in `partsupp`; `l_extendedprice = l_quantity × p_retailprice`; the spec's retail price formula; quantity, discount, and tax ranges; ship, commit, and receipt dates within the spec's days of the order date; audit 0 bad, on both engines |

Postgres ran 1M-row statements and MySQL 100k (see [calibrate.md](calibrate.md)). Lineitem dominates: one table, so one session, with five foreign keys and a composite key checked per row. MySQL took 10 times as long, waiting on the disk; InnoDB stores rows in key order, so `order=random` (the default) splits pages across lineitem's whole clustered key. Try `ORDER=asc` for a large MySQL load (not measured yet).
