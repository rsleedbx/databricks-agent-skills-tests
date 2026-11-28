# Lakebase shapes fixture

Source tables covering every **source shape** the query-based connector has to handle, built on
a Lakebase project (the only source in these tests where we can create tables). Used for two
things:

1. **Product test (no agent):** `run.py` creates pipelines from `pipelines/*.json`, runs them,
   applies `mutate.sql`, runs again, and prints `verify.sql`. This shows what the product
   actually does, including what it doesn't check.
2. **Skill test:** `prompts/lfc-matrix/preflight-lakebase.txt` checks that the agent reports
   the problems below before creating a pipeline, and lets the user skip them.

## Tables (schema `lfc_shapes`)

| Table | Key | Cursor | Notes |
|---|---|---|---|
| `pk_ts` | primary key `id` | `updated_at`, update trigger, indexed | `is_deleted` soft-delete flag |
| `pk_num` | primary key `id` | `ver` bigint from a sequence, insert/update trigger, indexed | numeric cursor |
| `uk_only` | `UNIQUE (code)`, no primary key | `updated_at`, trigger, indexed | |
| `nokey_cursor` | none (rows distinct) | `updated_at`, trigger, **not indexed** | |
| `nokey_dups` | none, each row 3x | `updated_at` | fully duplicate rows |
| `nokey_nocursor` | none | none | |
| `pk_nocursor` | primary key `id` | none | rows hard-deleted in `mutate.sql` |
| `null_cursor` | primary key `id` | `updated_at`, nullable, NULL on 10 rows | |
| `fake_pk` | none; `id` has 10 duplicates and 5 NULLs | `updated_at`, **not indexed** | meant to be declared `primary_keys: [id]` |
| `insert_only_cursor` | primary key `id` | `updated_at` set on insert only (no trigger) | |
| `pk_hard` | primary key `id` | `updated_at`, trigger | rows hard-deleted in `mutate.sql` |

## Set up

```bash
LFC_ENV=<name> source env.sh
F=fixtures/lakebase-shapes
databricks psql --project $LFC_LAKEBASE_PROJECT --profile $PIPELINE_WS -- -X -q -v ON_ERROR_STOP=1 -f $F/seed.sql
# reader role + connection (writes conn.json in the current directory; delete it after)
databricks psql --project $LFC_LAKEBASE_PROJECT --profile $PIPELINE_WS -- -X -At -v ON_ERROR_STOP=1 -v host=<endpoint-host> -f $F/role.sql
databricks psql --project $LFC_LAKEBASE_PROJECT --profile $PIPELINE_WS -- -X -q -v ON_ERROR_STOP=1 -f $F/grants.sql
databricks connections create --json @conn.json --profile $PIPELINE_WS && rm conn.json
$F/run.py
```

Query-based pipelines work on a Default Storage catalog, so any `LFC_CATALOG` will do.

## Expected results (2026-10-15, Postgres 17 on Lakebase, through a UC connection)

| Pipeline | Table | After run 2 | Meaning |
|---|---|---|---|
| SCD1 | `pk_ts` (+ `deletion_condition`) | 100 rows: 5 new, 5 updated, soft-deleted 6-10 gone | works |
| SCD1 | `pk_num` | 105, 5 updated | numeric cursor works |
| SCD1 | `uk_only` (`primary_keys: [code]`) | 105, 5 updated | unique key works as the key |
| SCD1 | `null_cursor` | 101: id 101 (inserted with NULL cursor) missing | run 1's snapshot loaded the NULL-cursor rows; later ones are skipped |
| SCD1 | `fake_pk` (`primary_keys: [id]`) | **86 of 102**; new duplicate rows overwrote existing ones | **no key check; silent data loss** |
| SCD1 | `insert_only_cursor` | 105, **0 of 5 updates** | updates missed, no warning |
| SCD1 | `pk_hard` (`hard_deletion_sync_min_interval_in_seconds: 60`) | 100: hard-deleted 6-10 gone | hard deletes work |
| SCD1 | `pk_nocursor` | 100: updates and hard deletes reflected | full load each run |
| SCD2 | `pk_ts` / `pk_num` / `uk_only` | 110 versions; 100 / 105 / 105 current | soft delete closes the version |
| APPEND_ONLY | `pk_ts` / `nokey_dups` / `nokey_cursor` | 115 / 108 / 110 | every change appended; duplicates kept |
| probe | SCD1 `nokey_cursor`, no `primary_keys` | runtime `CANNOT_INGEST_TABLE_WITHOUT_PRIMARY_KEY` | no synthetic key (the docs say there is one) |
| probe | SCD2 `nokey_dups`, no `primary_keys` | same error | |
| probe | SCD1 `nokey_nocursor`, no key or cursor | 105, full replace each run | works |
| probe | APPEND_ONLY `nokey_nocursor` (no cursor) | rejected at create: "cursor_columns must be set" | the docs say this is the required mode |
| probe | `cursor_column` (string, as some docs spell it) | CLI warns "unknown field", pipeline created **without a cursor** | use `cursor_columns` |

No pipeline produced any warning about `fake_pk`'s key, the cursors without an index, or the NULL
cursors. The only warning was about lineage (`CREATE EXTERNAL METADATA`).
