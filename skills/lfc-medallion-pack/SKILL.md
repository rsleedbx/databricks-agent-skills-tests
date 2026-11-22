---
name: lfc-medallion-pack
description: "Build a customer's own n:1 lakehouse from their schemas: each schema (or a TPC model) is a module repeated across many lines of business (LOBs, tenants, divisions), ingested by Lakeflow Connect into a bronze schema per LOB, stacked n:1 and modelled into one silver and gold. A model pack holds what knows one model (silver and gold SQL, the signal per 20-70-10 tier, how gold reads it back); the runner plans the ingestion pipelines within Lakeflow Connect's limits, renders the bundle, builds the sources on Lakebase empty (the no-data test: do the objects work at this many LOBs) or filled (the data test: does the signal survive to gold), and verifies each layer. Use when a customer has the same schema in many databases or schemas and wants one silver and gold, when sizing how many LOBs a deployment takes, or to write the marts for a new schema from the TPC examples."
parent: lfc-medallion
---

# A customer's n:1 lakehouse, one module per schema

A customer's repo holds their own modules: each is one of their schemas, repeated across LOBs (a database or schema
per line of business, tenant or acquired company), built into one bronze per LOB, one n:1 layer and one silver and
gold. This skill is the runner and the examples; the customer's repo is theirs and nothing goes back here.

| Piece | Where | Knows |
|---|---|---|
| Runner (`scripts/lfc_medallion_pack`): `lobs`, `plan`, `source`, `check`, `verify` | this skill | no model |
| Model pack: `pack.yaml`, `silver.sql`, `gold.sql` | TPC packs here (`packs/tpcb`); a customer's in their repo (`packs/<name>`) | one model |
| n:1 (`stack_all`) and the merge route | **lfc-medallion** | no model |
| The sources: DDL, fill, signals | **lfc-source-datagen**, **lfc-source-statschema** (the canonical schema) | the model's schema |
| Logins, connections, slots | **lfc-source-contract** | the engine |

## Steps

```bash
A=https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip   # or a branch or tag
alias lmp='uvx --from "lfc-medallion-pack @ $A#subdirectory=skills/lfc-medallion-pack/scripts" \
  --with "lfc-source-datagen @ $A#subdirectory=skills/lfc-source-datagen/scripts" lfc-medallion-pack'
lmp init my-lakehouse --name acme --archive "$A"     # both bundles, the notebooks, acme.yaml
# edit acme.yaml: the modules (pack, LOBs, seed, data), then
lmp render my-lakehouse/acme.yaml    # resources/<module>.yml, marts/<pack>/, source/plan.json
(cd my-lakehouse/source && databricks bundle deploy && databricks bundle run setup_source)
(cd my-lakehouse && databricks bundle deploy && databricks bundle run <module>_refresh)   # ingest, transform, verify
```

Each package by name (`name @ URL#subdirectory=...`): two requirements from one archive URL by subdirectory alone
confuse uv ("Package metadata name ... does not match"). `BUNDLE_VAR_prefix` (and the other variables) give each person their own names. `<module>_refresh` runs every
ingestion pipeline of the module, then the transform, then `checks/verify.py`, which fails the job when a check does.
When done: `databricks bundle run setup_source --params step=drop_slots` (a slot keeps WAL), then `bundle destroy`.

## The project file

```yaml
archive: <tests repo archive URL>     # what the pipelines and notebooks install
source: {slot_prefix: acme}           # slots and publications <slot_prefix>_<module>_<k>; max_slots: the source's
limits: {tables_per_pipeline: 250}
modules:
  sales:   {pack: packs/sales, lobs: [east, west, north], data: false}
  bank:    {pack: tpcb, lobs: 10, seed: 7, data: true}
```

A module may split its tables by role: `split_by_role: true` puts its facts (fact, log, queue, by the schema
YAML's roles) in ingestion pipelines of their own, `fact_mode: continuous` by default, and its dimensions (dimension,
lookup, bridge) in others, `dimension_mode: triggered` (the refresh job starts them, then merges and transforms).
A busy fact then doesn't wait on many idle dimensions in the same update. Each pipeline needs its own gateway and
slot, so a split takes up to twice the slots for the same tables. Without it (the default) every table of a LOB is in
one pipeline (`mode: triggered` or `continuous`).

`plan` puts as many LOBs in one ingestion pipeline (and its gateway, slot and publication) as keep it at or under
`tables_per_pipeline` (Lakeflow Connect: 1,000 tables hard, one of them the staging table, so 999; 250 recommended),
and stops when the pipelines need more replication slots than `max_slots`. The source job checks the real
`max_replication_slots` and `max_wal_senders` before it creates any.

## Two tests

1. **No data** (`data: false`): the tables empty, everything else real: one bronze schema per LOB, every ingestion
   pipeline, n:1 over every LOB, silver and gold. It answers whether this many LOBs and objects work before any data
   moves. Needs only the canonical schema.
2. **Data** (`data: true`): each LOB filled at its tier's scale with its tier's signal numbers (20-70-10: lobs.py),
   then `verify` reads the signal back from gold per LOB, against the rate its numbers give each row, and checks the
   tiers come out in order. A customer's schema gets this test once they give canonical statistics and the
   generator's rules (lfc-source-statschema, lfc-source-datagen).

## Writing a pack for a new schema

Start from `packs/tpcb` (and, in order of size, the other TPC packs as they're added). A pack is:

- `pack.yaml`: `model` (statschema's `schemas/<model>.yaml`, or the customer's canonical schema: its tables and each
  table's `role`, fact or dimension, come from there, not from the pack), `source_column`, `tiers` (per tier: `sf`
  and the numbers of the model's signals, in its `signals:` order), `marts`, `configuration` (pipeline settings the
  marts read as `${name}`), `readout` (the signal, gold's table and label, and the signal rule's columns and formulas
  as gold names them: the expected rate comes from the rule itself) and `contract` (grain, keys, label, features).
- `silver.sql`, `gold.sql`: materialized views over `all_<table>`. Every key and join includes the source column:
  keys repeat across LOBs.

The data is generated from the YAML alone: the schema's `generation:` rules and `formulas:` (statschema shape) and its
`signals:` rules (lfc_source_datagen.signals.rule_sql: a per-mille rate from a base and causes, the values of marked
and unmarked rows, and parent totals). No Python names a model, table or column, so a customer's schema and statistics
drive the same code.

Then `lfc-medallion-pack check <pack>`: every `all_<table>` is a table of the model, every column read is one of its
columns, every join equates the source column on both sides. Keep the signal and its readout in the same pack: a
change to one is a change to the other.

## The merge route's work per refresh

lfc-medallion's merge task (`merge_route`) reads only what changed, and each thing once:
- the sources, their tables and columns: one `information_schema` query; the state, its keys and times: one read;
- which LOBs' tables got rows or a snapshot since their state: one query over the ingestion pipelines' event logs
  (`--events`, rendered from the module's pipelines); a table no row reached isn't read at all;
- per changed source: one history read (the latest version and what changed), the key from the state;
- per table: one MERGE for all its changed LOBs (their change feeds unioned, read once), then the rows per LOB for
  the merge log from the target's own change feed for that commit (only the files the MERGE wrote), one state write;
  tables at once (`parallel`, 8), metadata reads on threads (16): all on the task's one compute.

Measured on TPC-B, 10 LOBs, 3 merge-route tables (2026-11-22): the task 516 s before, 85 s after a batch, 64 s with
nothing to merge (its start: installing and reading the catalog); a batch to gold 875 s before, about 370 s after.



After every run, record it, so the next, bigger deployment can be planned from measurements:

```bash
lmp timings --plan source/plan.json --module <m> --run <refresh run id> [--source-run <setup_source run id>] \
  [--deploy-seconds S] --kind first|steady|grown [--task-kind ingest_1=grown] --note "..." --out runs.jsonl
lmp predict runs.jsonl --tables 2000      # each step's seconds at that size: a line through two sizes or more
lmp report runs.jsonl                     # the records as a table
```

A record names no workspace, id or user. The measured runs so far: [references/scale.md](references/scale.md).

| Measured (TPC-B on Lakebase, 2026-11-21) | 3 LOBs, 12 tables | 250 LOBs, 1,000 tables in 5 pipelines |
|---|---|---|
| Source, empty (the job) | 124 s | 1,700 s (247 LOBs added: about 7 s each, one JDBC session per statement) |
| Deploy | 11 s | 28 s (255 resources) |
| Ingest, first snapshot of a pipeline | 124 s (12 tables) | about 810 s per 248-table pipeline: initializing 170, setting up tables 165, running 445 |
| Ingest, steady (no new tables, no data) | | about 680 s per 248-table pipeline: initializing 175, setting up 100, running 345 |
| Transform (n:1, silver, gold) | 84 s | 356 s first, 617 s next (initializing 301: finding 250 sources' tables) |
| Verify | 63 s (mostly installing) | 66 s |

The data test (10 LOBs at SF 1, 1,000,000 history rows): source 392 s, ingest 235 s, transform 94 s; gold's unusual
share per tier 0.49% / 3.05% / 7.02% (high / core / low), as the rule gives each row, and every balance equal to the
sum of its history (TPC-B's consistency rule) through Lakeflow Connect.

An ingestion pipeline's time grows with its tables even when they're empty: about 2.6 s per table per update at 248.
Plan the refresh interval from it: a 248-table pipeline can't refresh more often than about every 11 minutes.

## The control schema and the dashboard

`render` writes `resources/_control.yml`, `<prefix>_control`, and puts in it what a deployment needs to be watched and
restarted, from its first deploy (adding an event log to an ingestion pipeline later recreates it, and every table
of it is snapshotted again: measured 2026-11-21):

| In the control schema | Written by |
|---|---|
| `events_<pipeline>`: every gateway's, ingestion pipeline's and transform pipeline's event log | the pipelines (`event_log:`) |
| the gateways' staging volumes | the gateways (`gateway_storage_schema`) |
| `merge_log_<module>`: each copy or merge per table and LOB: versions read, inserted, updated, deleted, seconds, run id | lfc-medallion's merge task (`--log`) |
| `batches`: each batch of changes per LOB, and when | the source job's `batch` step |
| `checksums`: rows and an MD5 digest per table per LOB in the source, bronze and n:1 (`checksum.py`) | verify |
| views `v_changes`, `v_updates`, `v_latency`, `v_latency_table`, `v_refresh`, `v_quality`, `v_checksums` (`observe.py`) | verify, on every refresh |

The merge route's watermark is lfc-medallion's own state (`_lfc_medallion_merge_state`, in the silver schema): saved
after each table and LOB, so a run that fails part way restarts from there.

`dashboards/lakehouse.lvdash.json` (`dashboard.py`, a bundle resource) reads only those views, so it is the same for
any model. Its pages:
- **Changes:** rows through each stage (source capture, bronze, n:1 by merge or pipeline, silver, gold) by table, LOB,
  pipeline and update, with inserts, updates, upserts, deletes and snapshot rows apart.
- **Pipelines:** each update's state and phases, and each view's refresh technique and why not a cheaper one.
- **Latency:** from each batch to bronze, the n:1 merge and gold, per LOB and per table or view.
- **Quality:** each expectation's rows passed and failed.
- **Checksums:** every table of every LOB, matched or not.

The checksum: each row's columns as canonical text by their YAML type (decimals at their scale, timestamps as epoch
microseconds, chars trimmed, NULL as `chr(1)`), MD5, the first 8 hex digits as a number, summed with `COUNT(*)`: the
same on every engine with MD5 (an XOR aggregate isn't on all), computed where the rows are (the source through
`remote_query`). Measured: 40 of 40 match on 10 TPC-B LOBs, 2,011,110 rows, through both routes.

 (measured 2026-11-21, TPC-B, 10 LOBs with data)

A selective full refresh of one LOB's fact (`history`) and dimension (`account`) on its ingestion pipeline
(`start-update --json '{"full_refresh_selection": ["<catalog>.<schema>.<table>", ...]}'`, about 2 minutes):

- **Bronze:** the table is replaced (a `DLT REFRESH` commit), then reloaded by MERGEs; only the selected tables
  change. Its change data feed, deletion vectors and row tracking stay on (row tracking under new column names: a new
  generation of the table), so no table property needs setting.
- **n:1:** the two `all_<table>` views recomputed in full across every LOB (`DATA_SCHEMA_CHANGED`); the other two
  did nothing. **Silver and gold** refreshed row-based, as on any change. The next ordinary change was incremental
  everywhere again (`all_history` append-only, `all_account` row-based).
- **Correct:** the LOB's rows in n:1 equal bronze exactly (`EXCEPT ALL`: 0), gold's rows and every balance unchanged.

Materialized views are what makes this safe: a streaming table reading bronze (as the Zerobus and Debezium tests do,
on append-only bronze) would stop at the replaced table. The cost is the recompute: one LOB's refresh recomputes its
table's n:1 view over every LOB, which at hundreds of LOBs is the whole table. Where refreshes are expected, the
`merge` route (lfc-medallion `merge_all`) copies only the refreshed source; not tried here yet.

A keyless table (TPC-B's history) is merged on every column (`<=>` on all of them): identical duplicate rows in the
source would land as one.

 (the batch is built; the ramp job isn't yet)

Objects first, then data, then change rate, each step only after verify passes on the one before, with its
timings recorded, so a slow snapshot never hides an object problem:

```yaml
modules:
  bank:
    pack: tpcb
    lobs: 10
    ramp:
      data: [0.01, 0.1, 1]               # scale factors, one step at a time (the sources grow by append)
      dml: {every_minutes: 1, rows: [10, 100, 1000, 10000]}   # rows per batch per LOB and log or fact table
```

- **Writes through the UC JDBC connection** the source job already holds (no password in code), as one batch a
  minute from a scheduled job in the source bundle: the generator makes rows in bulk inside the database, so a batch
  a minute is enough; nothing runs continuously.
- **The DML comes from the YAML:** each table's role says what changes (log and fact tables get new rows by the
  generation rules, continuing their row numbers: `append`), the signal's rule marks the new rows, and its `totals:`
  add the batch's sums to their parents (TPC-B: a transaction's balances). No model or table is named in Python.
- The batch is `lfc_source_datagen.signals.batch(connect, model, sources, rows)`, on whichever DB-API connection
  `connect()` gives: psycopg, or the UC JDBC connection (`lfc_source_contract.jdbc.dbapi`). Measured 2026-11-21 on a
  local PostgreSQL 14, TPC-B at SF 1: 500 history rows a batch in 0.6 s through both, the dimension tables' counts
  unchanged, every account and teller balance still the sum of its history. Through UC JDBC the rows changed come back
  as unknown (`None`): Spark's `sessionInitStatement` returns no count. From the workspace (`setup_source` with
  `step=batch`, the UC JDBC connection to Lakebase): 500 rows on each of 10 LOBs in 170 s, most of it installing; the
  rows and balance updates reached gold through Lakeflow Connect CDC, every balance gap 0. A failed or retried run
  appends again: the data stays consistent, the rows are more than asked (seen: one LOB with 1,000 extra).
- `append` (whole scale factors of every table) is not the batch: TPC-B's branch is one row per scale factor, so its
  smallest step is 100,000 accounts.
- The ingestion pipelines are triggered; their steady time per update (above) sets how far behind bronze runs at a
  given batch size, which is what the ramp measures.


- One gateway, slot and publication per ingestion pipeline; the source's slot and sender limits cap the pipelines. A
  Lakebase project allows 10 of each and holds 2 physical slots of its own (`wal_proposer_slot`,
  `wal_retention_slot`), so it takes 8 ingestion pipelines (measured 2026-11-21); set `source.max_slots: 8` for it.
- **Adding LOBs to a deployed pipeline needs its gateway restarted.** A gateway reads the source's tables when it
  starts: given more schemas, the ingestion pipeline found none of the new ones (measured: 59 LOBs, 236 tables, absent
  after a full update; verify's bronze check fails and says so). Restart the gateway, then refresh.
- Every bronze table also gets a backing table (`__materialization_mat_*`): Unity Catalog objects are about twice
  the tables; the first LOB's bronze schema holds its pipeline's `cdc_staging_table` and event log.
- Every LOB of a tier is the same data (the generator is deterministic and the tier's numbers are shared): the score
  decides the tier only. Spread within a tier is not done yet.
- Tiers are by rank among the module's LOBs: adding LOBs can move a LOB to another tier, while its data keeps the
  numbers it was built with (a built LOB is kept). Draw the full count once for a data module.
- A table without a primary key (TPC-B's history) has no key for SCD type 1: see the measured results.
- Not covered yet: the merge route (lfc-medallion) for the quiet tables at many LOBs; an app over the contract.
