# What's unique, and how it compares

Five things here aren't product automation. Each section says what the repo does, where other tools have something like it, and what's different. Back to the [README](../README.md).

How this was checked (2026-10): the closest tools' own pages for Database Lab Engine, Neon and SDV; the rest from what was known about them before then, not rechecked. Tools we don't know about may do more.

## A fresh copy of any database, kept in object storage

**What it does.** A master database is populated once, then kept as a stopped data directory in a JuiceFS volume on object storage (tested on Azure Blob; JuiceFS also supports S3 and GCS), with Redis for metadata. A test mounts the volume on a VM running Podman, and `juicefs clone` makes a copy-on-write branch of the master in 0.05–0.3 s. The database then starts on the branch. One master can back many branches at once, each mounted into its own container on the same VM. The VM's disk holds only a cache and the blocks the test changes. Every engine that keeps its data in files works the same way. Tested with Postgres 16, SQL Server 2022, Oracle 23ai Free, Db2 11.5, CockroachDB 25 and YugabyteDB 2025. Setup and measurements: [juicefs-branches.md](../skills/lfc-source-cloud-vm/references/juicefs-branches.md); the local-disk alternative: [master-snapshots.md](../skills/lfc-source-cloud-vm/references/master-snapshots.md).

| | Engines | Where the data lives | Copy | Model |
|---|---|---|---|---|
| [Neon](https://neon.com/docs/introduction/branching) / Lakebase branching | Postgres | the service's storage | copy-on-write branch, instant | managed service |
| [Database Lab Engine](https://github.com/postgres-ai/database-lab-engine) | Postgres 10–18 | local disk, ZFS or LVM | thin clone, about 10 s for 1 TiB | open source (Apache 2.0) |
| Delphix | many | its own storage layer | virtual copies | commercial platform |
| Managed-database snapshots (RDS, Azure, Cloud SQL) | per service | the service | a restore: minutes to hours, full size | per service |
| **This repo** | **any engine that keeps data in files** | **your own bucket** | **copy-on-write branch, 0.05–0.3 s** | **open-source parts on your own VM** |

**What's different here:** one approach for every engine, with the master in your own bucket and only a cache on the VM, built from open-source parts.

**Its limits, measured:**
- **Reads:** a full read of a 30M-row Postgres table (3.8 GB) took 17–20 s from object storage. Local XFS on a 32 GB Premium SSD (P4) took 38–46 s; that disk was running on burst I/O, not a provisioned-performance disk.
- **Writes:** 2–4 times slower than local XFS, so XFS reflink clones of a master are the choice for high change rates.
- **One VM per volume, in this setup.** Redis, which holds the volume's metadata, runs on the VM itself and listens only locally. That's this repo's choice, not a JuiceFS limit: with a metadata engine every VM can reach (a shared Redis, or a SQL database), several VMs can mount the same volume. That hasn't been tested here. Today, to give another VM a copy, copy the master out and use rclone. Never mount one bucket with two copies of its metadata.

## Data generated inside the database, from any schema, with zero bytes of data over the network

**What it does.** Takes the schema (tables, keys, foreign keys, types) in a canonical format, `schema.yaml` and `stats.yaml` from [statschema](https://github.com/rsleedbx/statschema). The format is collected from one engine (MySQL or Postgres today) and written as another's DDL (Postgres, MySQL, SQL Server, Oracle, Db2, Informix or Databricks), so a MySQL schema and its statistics can be built on Oracle. It then plans each table's fill as engine-specific `INSERT … SELECT` from the engine's own row generator (`generate_series`, `GENERATE_SERIES`, a recursive CTE, `CONNECT BY LEVEL`) or a seed table. That SQL runs inside the database: only the SQL crosses the network, never the rows.
- **Foreign keys stay valid.** Tables fill by level: parents (dimensions) before children (facts), and each child key lands in its parent's key range.
- **Deletes keep them valid.** Before a delete trims a parent, the children that point at the trimmed keys are re-pointed.
- **Fills resume.** Each round re-plans from what's there, and a rerun adds only what's missing until every table reaches its target.
- **Realistic shapes.** Keys in random, ascending or descending order; values sequential, scrambled or random; part of every table kept auditable.

SQL planners for Postgres, MySQL, SQL Server and Oracle, and a Python planner (through pip drivers, for managed services and for Db2 and Informix) that writes the same SQL for those six engines. Measured: 110M rows, all six TPC workloads at scale factor 1, inside SQL Server 2022 in 1,125 s. [lfc-source-datagen](../skills/lfc-source-datagen/SKILL.md).

| | Your schema? | Foreign keys | Where rows are made | Engines |
|---|---|---|---|---|
| `generate_series` scripts | written by hand per table | by hand | inside the database | one at a time |
| Benchmark kits (HammerDB, BenchBase, TPC dbgen / dsdgen) | no: the benchmark's own schema | yes, for that schema | inside, or files loaded in | several |
| Schema-driven generators (Tonic, Synth, Mockaroo, Faker-based tools) | yes | yes | outside: rows are sent over the network | many |
| **This repo** | **yes, any schema, in a canonical format: collected on one engine, built on another** | **yes, parents first, kept valid through deletes** | **inside the database** | **six: Postgres, MySQL, SQL Server, Oracle, Db2, Informix** |

**What's different here:** any schema, collected on one engine and built on another, generated inside the database with valid parent/child keys and zero bytes of data over the network.

## A copy shaped like the customer's database, from statistics only

**The problem.** A DBA won't hand over production data without an NDA, and often not with one. But most will share the schema and the catalog's statistics, which can be collected without a single data value.

**What it does.** Collects the schema, statistics and top queries from the catalog (MySQL or Postgres today), with no table scans. By default that's names, types, keys, row counts, null fractions, distinct counts, frequencies and widths, and no data values. Numeric and date ranges, histograms and most common values are added only if the customer allows it. Text values are never copied. Then it scales the shape to the size you want, writes DDL for Postgres, MySQL, SQL Server, Oracle, Db2, Informix or Databricks, fills the copy inside the database (the section above), and checks it. The copy's statistics are compared with the source's, and the source's queries are replayed on the copy to compare plans. A schema collected from Postgres can be built on MySQL, SQL Server or Databricks. The same pipeline builds TPC-B, -C, -H, -DS, -E and -DI. [lfc-source-statschema](../skills/lfc-source-statschema/SKILL.md), format from [statschema](https://github.com/rsleedbx/statschema).

| | Needs real rows? | Keeps parent/child keys | Where it runs | Checks the copy |
|---|---|---|---|---|
| [SDV](https://docs.sdv.dev/sdv/multi-table-data/modeling/synthesizers) (HMA, HSA, Independent) | yes, it learns from the rows. Its DayZ synthesizer works from metadata alone, without statistics | yes | Python, outside the database | quality reports on the data |
| Gretel, MOSTLY AI, Tonic | yes, models or rules trained on production data | yes | their service or workers | privacy and fidelity reports |
| **This repo** | **no: catalog statistics only** | **yes** | **inside the target database, on six engines** | **statistics and query plans against the source** |

**What's different here:** the copy behaves like the customer's database without a single real row leaving their site. What's shared is the schema, its statistics and the queries: no production data, so no data to put under an NDA. The schema and queries may still be confidential, so ask.

## Answers tested as claims, tied to the runtime they ran on

**What it does.** A question such as "does adding a column need a full refresh?" becomes a set of claims, one per case. A script runs each claim on a fresh source database and prints PASS or FAIL, then deletes everything. Each result records the exact runtime every pipeline update ran on (DBR version, channel, release build, CPU architecture) and the settings it ran with. It also shows what changed since the last run of the same pipeline kind on that workspace. [lfc-schema-change-test](../skills/lfc-schema-change-test/SKILL.md), [bin/lfc_runtime.py](../bin/lfc_runtime.py).

| | Rerunnable | Tied to the runtime | Covers |
|---|---|---|---|
| Docs and FAQ pages | no | no | the documented behavior |
| A product's own test suites | yes | yes, the build under test | the product's code, not a customer's setup |
| **This repo** | **yes, on a fresh source each time** | **yes, every update's runtime and settings** | **the customer's question, end to end in a real workspace** |

**What's different here:** each answer is checked by rerunning it, on the exact runtime it ran on. It catches behavior that differs from the docs, and when a release changes it.

## Scripts, not instructions, made better by every failure

**What it does.** A skill does its work by running the bash and Python scripts in its own `scripts/` folder, not by an agent improvising commands from prose. The skill's text says when to run which script and what its output means. So anyone can repeat a test without the AI, by running the same scripts in the same order.

When a run fails on a case the scripts didn't handle, the fix goes into the script, with the evidence in the skill, and the next run handles it. On the Zerobus tests, in two days:
- a database export that wrote decimals as text under a decimal type;
- a DDL import that couldn't parse a typed default;
- a cleanup that stopped at a missing container;
- a macOS TCP check that hung;
- a schema re-dump that failed silently;
- an IP change that broke every claim mid-run.

Each became a script change, and each is now handled.

A repeatable runner then runs the committed scripts, from a clean checkout, and records which commit passed. It refuses to run with uncommitted changes, and writes the passing runs into the results table itself. Its first run caught a regression that hand runs had missed. [run-all.sh](../skills/zerobus-debezium-test/scripts/run-all.sh), [the results](lakeflow-connect-zerobus.md#successful-runs).

| | Repeatable without the AI | Gets better from failures | A ✅ means |
|---|---|---|---|
| Instructions an agent follows (prompts, runbooks) | only by redoing what the agent did | only if someone edits the text | the agent got it right that time |
| Scripts kept outside the skill | yes | if someone maintains them | the script ran, on some version |
| **This repo** | **yes: the skill's own scripts** | **yes: every unhandled case becomes a script change** | **these scripts, at this commit, passed every claim** |

**What's different here:** the scripts are the skill, so what was tested is what anyone runs. The agent's job is to run them, and to improve them when they fail.
