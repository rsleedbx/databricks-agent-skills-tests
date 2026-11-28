# The full Lakeflow Connect matrix

"Run the full matrix" means running every cell below against **one workspace**, from a plain
prompt, and checking each cell against the pass criteria. A cell passes when the skill does the
right thing for that workspace, which includes stopping cleanly when the workspace or source
doesn't support the mode.

## Cells: 4 sources x 4 modes, plus DML

| Source | QBC via connection (`qbc-*`) | QBC via foreign catalog (`fcqbc-*`) | Integrated CDC (`icdc-*`) | Standard CDC (`scdc-*`) |
|---|---|---|---|---|
| **Lakebase** (Postgres stand-in) | bare "demo" prompt: creates the Lakebase project, seed tables, reader role, connection, pipeline; snapshot run | creates a foreign catalog on the Lakebase connection, named `<firstname><lastname>-<connection>`; pipeline reads through it | not available for Postgres: says so, creates nothing | tries `CREATE PUBLICATION`; if Lakebase refuses, stops and leaves nothing |
| **Postgres** (external) | pipeline from the UC connection | reuses the existing foreign catalog on the connection (or creates one) | not available for Postgres: says so, creates nothing | own publication + slot per pipeline; or the workspace 403 with nothing left on the source |
| **MySQL** | pipeline from the UC connection | reuses / creates the foreign catalog | one pipeline, `connector_type: CDC` | gateway (`-gw`) + ingestion (`-ap`); or the workspace 403 |
| **SQL Server** | pipeline from the UC connection | reuses / creates the foreign catalog | one pipeline, `connector_type: CDC` | gateway (`-gw`) + ingestion (`-ap`) |

Plus **DML round trip** (`dml-lakebase`): from only the pipeline name, the skill introspects the
source through the connection in one shot, generates 10 inserts / 10 updates / 10 deletes per
table, runs them on Lakebase, runs the pipeline, and shows that the changes arrived.

Every prompt is two turns: a plain request (the skill must ask before creating anything), then
the answers and the go-ahead. Pipelines read `intpk` (primary key `pk`, cursor `dt`) and `dtix`
(no primary key) on the external sources, and `customers` / `orders` on Lakebase.

## Data shapes: what each pipeline config does with each source shape

The 4 x 4 grid above uses two simple tables. Real tables differ along four more dimensions, which
are per-table settings, so they're covered by one fixture with a table per shape
([fixtures/lakebase-shapes](fixtures/lakebase-shapes/README.md)) rather than more pipelines:

| Dimension | Values |
|---|---|
| Source key | primary key; unique key only; no key; no key with duplicate rows; a declared key that isn't unique (duplicates, NULLs) |
| Cursor | timestamp; numeric; none; NULLs present; set on insert only; not indexed |
| Target | `SCD_TYPE_1`, `SCD_TYPE_2`, `APPEND_ONLY` (query-based only; CDC offers SCD1 / SCD2) |
| Deletes | none; soft (`deletion_condition`); hard (`hard_deletion_sync_min_interval_in_seconds`, not with SCD2) |

Valid combinations for query-based (✅ works, ⚠️ accepted but loses data silently, ❌ rejected or fails):

| | SCD1 | SCD2 | APPEND_ONLY |
|---|---|---|---|
| key + cursor | ✅ | ✅ | ✅ |
| key, no cursor | ✅ full load | — | ❌ needs a cursor |
| no key + cursor | ❌ needs `primary_keys` | ❌ needs `primary_keys` | ✅ |
| no key, no cursor | ✅ full replace | — | ❌ needs a cursor |
| declared key not unique | ⚠️ | ⚠️ | n/a |
| cursor insert-only or with NULLs | ⚠️ | ⚠️ | ⚠️ |

For CDC sources the key dimension also depends on the source's change capture: SQL Server change
tracking needs a primary key (use CDC for keyless tables, and SCD2 needs CDC). Postgres needs
`REPLICA IDENTITY FULL` for keyless tables. MySQL needs `binlog_row_image=FULL`.

Two tests use the fixture:

- `fixtures/lakebase-shapes/run.py` (product, no agent): expected results are in its README.
- `prompts/lfc-matrix/preflight-lakebase.txt` (skill): turn 1 must report that `fake_pk`'s key
  isn't a key (with duplicate and NULL counts), that `fake_pk` / `nokey_cursor` have unindexed
  cursors, that `null_cursor` has NULL cursors, that `insert_only_cursor` has no update trigger,
  and that `nokey_cursor` needs a key under SCD1. Turn 2 ("only a snapshot test") must go ahead
  with the user's choices and change only what would fail.

## Pass criteria, every cell

1. **Asks first:** turn 1 creates nothing and asks for every missing input in one message.
2. **Names:** pipelines are `<firstname><lastname>-<connection>`. Later modes on the same
   connection get `-2`, `-3`, ...; standard CDC gets `-gw` / `-ap` with the same number. Bundle
   names are unique (no `mode: development` prefix).
3. **Runs, and the counts match:** the rows-per-run report's snapshot counts equal the source row
   counts. For a CDC table without a primary key, they equal the number of distinct rows.
4. **Expected failures are clean:** a workspace 403, an unavailable mode, or a refused
   publication stops the cell with a clear reason. Nothing is left on the source (Postgres slots
   and publications), and the skill offers to remove empty schemas.
5. **No secrets:** no password in the transcript, and no `conn.json` left on disk or uploaded with
   a bundle.
6. **Long CDC updates:** integrated CDC is started without blocking. Anything still running at
   30 minutes is stopped by hand (`bin/stop_after.py 30 <pipeline-id>...`).

Additional criteria per mode:

- **QBC via foreign catalog:** the skill reuses an existing foreign catalog on the connection if
  there is one, whatever its name. Otherwise it checks `CREATE_CATALOG` first and creates
  `<firstname><lastname>-<connection>`. The pipeline uses `ingest_from_uc_foreign_catalog: true`
  with `source_catalog` set to the foreign catalog.
- **DML:** SCD1 tables show the inserts plus updates as `upserted` and the deletes as `deleted`.
  SCD2 tables are checked with the per-key count from the table (10 / 10 / 10), not the event-log
  counts.

## Workspace requirements

- **Target catalog:** for CDC cells, a catalog with its **own managed location**. CDC pipelines
  are rejected on a Default Storage catalog ("Default Storage for Unity Catalog is not supported
  on LakeFlow Connect pipelines"); query-based cells work on either.
- **Feature gates:** standard CDC for Postgres and MySQL is enabled per workspace. Where it isn't,
  the expected result is a clean 403 stop.
- **Compute: `LFC_SERVERLESS`** (in `env.sh`; `true` by default) fills `${LFC_COMPUTE_ASK}` in every
  VM-source prompt: `true` or `false` asks for that setting on every pipeline and gateway, `unset`
  asks to leave it out and take the product's default. `true` is the default only because of the
  test setup: lfc-source-firewall opens the sources to the workspace's serverless egress ranges,
  not to its classic egress IP, so a pipeline the default puts on classic starts and then can't
  reach the source. The public skill leaves `serverless` unset. A run with another setting writes
  to `/tmp/lfc-tests/<name>-serverless-<setting>/`, so the two can be compared.
- **Record compute for every pipeline cell**, from `bin/compute_report.py <profile> <name-prefix>`:
  what was sent (`unset`, `true`, `false`), what it ran on, whether the compute started, and
  whether it then reached the source, plus the workspace type (`workspace.compute`:
  `serverless-only` or `classic`). Keep these apart: **not started** (compute not allowed,
  refused at create, in seconds), **started but the source unreachable** (the firewall; on
  classic this shows only after the cluster's ~8 min start), and a rerun **switched** to
  serverless because of the firewall (`switched_from`, `switch_reason: firewall`). To see what
  the default picks on each workspace type, run the matrix with `LFC_SERVERLESS=unset`; with the
  test firewall, a classic pick records as started, source not reached (firewall).
- Record each run in `results/<date>-<workspace>.md`.

## Running it

```bash
cp env.example.sh env.<name>.local.sh     # once per workspace
LFC_ENV=<name> source env.sh
bin/run_matrix.py                         # qbc -> fcqbc -> icdc -> scdc -> dml, one batch at a time
bin/stop_after.py 30 <integrated-cdc-pipeline-id>...
```

Results land in `/tmp/lfc-tests/<name>/<test>-results/p1.json` (final answers and tool calls),
with logs in `/tmp/lfc-tests/<name>/<test>.log`. Afterwards, record each cell as pass /
expected-stop / fail, with the reason, and clean up (see README).
