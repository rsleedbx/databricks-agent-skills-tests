---
name: cdc-table-plan
description: "Guess each source table's role (dimension, lookup, bridge, fact, log, queue) and silver (scd1, scd2, append) from a canonical schema.yaml, its foreign keys, row counts and activity, into a plan.yaml the customer checks and edits; diff a deployed plan against an edited one into the actions that change silver (add, rebuild, drop, refuse). Checked against the TPC models and Mautic."
parent: zerobus-debezium-test
---

# Which tables get history, which get current rows, which are appended

A change log carries every table the same way; silver doesn't have to. A dimension wants its current rows (SCD type 1)
and its history (SCD type 2); a fact wants its current rows; an insert-only log wants neither MERGE, only the rows
appended. A customer's schema rarely says which table is which, and usually has no documentation to look it up in.

So the plan is built in three passes, each recorded in the plan as `decided_by`:

| Pass | From | `decided_by` |
|---|---|---|
| 1. Guess | only what every schema has: keys, foreign keys (`fk_constraints` and columns' `references`), row counts, column names, and the activity when the database keeps it | `relations` |
| 2. Review | for public software, its docs or its code: `--review review.yaml` corrects a table with a citation | `docs` |
| 3. Edit | the customer, in `plan.yaml` | `customer` |

The rules are in `lfc-medallion`'s package (`lfc_medallion.roles`), so a pipeline or job decides the same way from
Unity Catalog (its `table_plan`: the source's catalog through the connection, or bronze, then hints); `classify.py`
runs them from files.

## Run

```bash
PY="uv run"; S=skills/cdc-table-plan/scripts
# the schema: lfc-source-statschema's collector and to-canonical.py (schema.yaml); the activity, when there is some:
mysql -N -B -r -e "SET @db='sales'; SOURCE skills/lfc-source-statschema/scripts/collect-activity-mysql.sql" > activity.json
$PY $S/classify.py schema.yaml --activity activity.json > plan.yaml          # the first guess, with its evidence
$PY $S/classify.py schema.yaml --truth truth.yaml                             # against known roles: what it gets wrong
$PY $S/classify.py schema.yaml --truth schema                                 # against the roles the schema's tables carry (role:)
$PY $S/plan.py diff deployed-plan.yaml plan.yaml --schema schema.yaml         # what changing silver to the edit takes
```

Each table in `plan.yaml` has `role`, `silver`, `confidence`, `decided_by`, `why` (the evidence) and, where a guess rests on
little, `ask`: the question to put to the customer ("Is page_hits insert-only?"). Silver builds what `silver` says
(`zerobus-debezium-test`'s `silver-from-schema.py` reads it as `<volume>/_plan.yaml`).

| Role | Silver | Signals |
|---|---|---|
| dimension | scd1, scd2 | referenced by other tables; a name column; a modified/updated column |
| lookup | scd1 | a fixed size in a scaled model (TPC), a few columns, parents only lookups |
| bridge | scd1 | a key made of its foreign keys; or (an ORM's own `id`) exactly two parents, a few columns, no measures |
| fact | scd1 | several parents and nothing below; the header of fact rows whose key holds its key (order -> lines); a leaf dated when added with no column saying it changes; updated or deleted per the activity |
| log | append | no primary key; named like a log (log, history, audit, events, stats, hits) with no modified column and nothing below; inserts only per the activity |
| queue | scd1 | named like a queue; inserts and deletes per the activity |

## What it gets right

The rules one by one, what each miss costs (changes lost, history lost, or only extra work), and the signals from
published work and the databases' catalogs not used yet: [references/heuristics.md](references/heuristics.md).

Relations only, against the specifications' roles (each TPC table's `role:` in its schema): TPC-H 8 of 8, TPC-B 4 of 4, TPC-DI 18 of 18, TPC-C 8 of 9,
TPC-DS 22 of 24, TPC-E 27 of 32. Mautic 7.2.1 (128 tables, 199 foreign keys, collected from an installed database),
against the 25 tables whose writes were traced in its code: 23 of 25 from relations, 24 of 25 with the activity of two
rounds of its own traffic (`mautic-source`'s `traffic.sh`).

- **Activity settles what structure can't** (TPC-C `new_order` and TPC-E `trade_request` are queues only by how they're
  used). It also corrected the code reading: Mautic updates every `page_hits` row once after inserting it, so
  `[append]` would have dropped those updates.
- **ORM schemas hide the header signal.** TPC's detail rows carry the header's key in their own (lineitem's key holds
  its order's); an ORM gives every table its own `id`, so Mautic's facts are found by names, a creation-time column and
  the absence of a modified column instead.
- **A short quiet window isn't insert-only.** A fact others reference (an e-mail sent, later opened) can have no updates
  while the activity is counted; it stays scd1 with a question, and only a table nothing references becomes a log.
- **Left as questions:** TPC-DS `ship_mode` and `household_demographics`, TPC-E `exchange`, `last_trade`,
  `holding_summary`, `account_permission` (lookup vs dimension, bridge vs fact: the same silver but scd2 or not), and
  Mautic's `dynamic_content_stats` (no traffic touched it).

## Trade-offs

- **A foreign key's ON DELETE overrides the guess.** A table that CASCADE or SET NULL changes with its parent gets
  `[scd1]` even when named like a log with inserts only, and `plan.py` warns about an edit that appends it. Its rows
  can't be insert-only, so the guess was wrong.
- **`[append]` drops what isn't an insert.** A table appended to keeps the snapshot rows and the inserts; an update or a
  delete of it is left out of silver (the producer still writes it to bronze, so a rebuild as scd1 gets it back).
- **A collected row count is only today's size.** Fixed-size reference data is recognised only in a scaled model
  (TPC's `row_count_per_sf`); in a collected schema size doesn't decide a role.
- **The activity counts since the server started** (MySQL's `performance_schema`) **or the statistics were reset**
  (PostgreSQL); a fixture load counts as inserts, so collect it after real traffic and truncate it first.
- **`plan.py` refuses** scd1 or scd2 without a primary key, append together with scd1 or scd2, and a table the producer
  doesn't capture yet; it asks before dropping a target (a history table goes with its history). The pipeline only stops updating a
  target the plan drops: the table stays in the catalog until a `DROP TABLE`.
