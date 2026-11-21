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

- `pack.yaml`: `model` (statschema's `schemas/<model>.yaml`, or the customer's canonical schema), `source_column`,
  `tables`, `tiers` (per tier: `sf` and the model's signal numbers, in its `signals:` order), `marts`,
  `configuration` (pipeline settings the marts read as `${name}`), `readout` (gold table, label, and the per-row
  rate from the signal's parameters) and `contract` (grain, keys, label, features, measure).
- `silver.sql`, `gold.sql`: materialized views over `all_<table>`. Every key and join includes the source column:
  keys repeat across LOBs.

Then `lfc-medallion-pack check <pack>`: every `all_<table>` is a table of the model, every column read is one of its
columns, every join equates the source column on both sides. Keep the signal and its readout in the same pack: a
change to one is a change to the other.

## Limits

- One gateway, slot and publication per ingestion pipeline; the source's slot and sender limits cap the pipelines.
- A table without a primary key (TPC-B's history) has no key for SCD type 1: see the measured results.
- Not covered yet: the merge route (lfc-medallion) for the quiet tables at many LOBs; an app over the contract.
