---
name: lfc-change-source
description: "The change-source contract: what every source of database changes gives its consumer per source and table, whatever carries them (a Lakeflow Connect bronze table, an append-only change log from Debezium through Zerobus, the gateway's staging volume, files in object storage): a snapshot as of a position, the changes after one (with _op and _seq, as AUTO CDC takes them), a watermark up to which every change has arrived, and stats from metadata only. Use when an n:1 layer, an applier or a check reads changes and shouldn't care how they arrived."
---

# Change-source contract (contract 1)

Changes reach the lakehouse several ways. Each delivers the same two things per source and table, a snapshot and the
changes after it, so a consumer (the fan-in planner in **lfc-medallion**, an applier of one's own, a check) reads them
through one contract and doesn't depend on the transport. The contract takes AUTO CDC's inputs as its vocabulary:
keys, a sequence, and deletes.

| Part | Gives | AUTO CDC's name |
|---|---|---|
| `snapshot_sql(at)` | the table's full rows as of a position, as SQL | `create_auto_cdc_from_snapshot_flow` |
| `changes_sql(after, to)` | its changes after a position up to another: the table's columns, `_op` (`upsert` or `delete`) and `_seq` (a later change sorts higher; a delete before an insert of one key at one position) | `sequence_by`, `apply_as_deletes` |
| `watermark(session)` | the position up to which every change has arrived | (none: CockroachDB's `RESOLVED`, the gateway's extraction watermark, a bronze table's latest version) |
| `stats(session, after)` | change rows, bytes and files since a position, from metadata only | the materialized-view planner's `source_table_information` |
| `shared` | whether one physical object carries many tables' or sources' changes, so one scan can serve them all | |

A position is a kind and a value (`delta_version:19`, `hlc:...`, `lsn:...`). Positions of one kind compare; a
consumer keeps them as text. `last_change_sql(changes, keys)` is the consumer's reduction to each key's last change,
the same for every source.

Code: [scripts/lfc_change_source/](scripts/lfc_change_source/) (`contract.py`, one module per adapter), installed as
the package `lfc-change-source` (`pip install <archive>#subdirectory=skills/lfc-change-source/scripts`).

## Adapters

| Adapter | Carries | Positions | Watermark | Shared | State |
|---|---|---|---|---|---|
| `lfc_bronze` | a Lakeflow Connect bronze table: the ingestion pipeline's streaming table, one a source and table; changes from its change data feed (`table_changes`) | Delta versions, or commit times | the latest version | no | built; lfc-medallion's merge route reads through it |
| `cdc_log` | an append-only table of Debezium change events (through Zerobus), every table's: `destination`, `key:`, `value:after:`, op | the source's own order (`SEQUENCES`: LSN, binlog position, SCN, event time) | the latest event's position | yes | built; passes conformance; **zerobus-debezium-test**'s silver generator to read through it |
| `gateway_staging` | the gateway's staging volume, read-only: `snapshot/<table id>/<time>/*.parquet` (with the cursor it was taken at), `cdc/` JSON lines, columns by id through `metadata/table_schemas/` | the gateway's cursor (`cursor.sequence`) | the newest cursor in the change files or the table's snapshot | yes: one folder a gateway | built; passes conformance; a table rebuilt from staging alone (snapshot and the changes after it) equals bronze |
| `changefeed_log` | a CockroachDB changefeed's files (`format='json', updated, resolved`) in cloud storage or a Volume, one folder a table, landed by `land.changefeed` into the shared log: `after` (null: a delete), `key` (an array in key order), `updated` | the HLC (`hlc`, text of fixed width) | the newest `.RESOLVED` landed | yes, once landed | built; the initial scan is in the log, so the snapshot is the log up to a position |

**One log for every transport.** `land.gateway` copies a gateway's change files, each file once (`COPY INTO`), into
the shared append-only change log a Debezium source writes through Zerobus, in the same shape (`destination`,
`operation`, `key`, `value`; clustered by destination). With `keys=land.gateway_keys(schemas)` each row's key is filled
in by one expression for every table (a map of key columns by table id), as Debezium and a changefeed fill theirs;
`land.backfill_keys` fills rows landed without. Then `cdc_log.from_gateway` reads a gateway's table from it, columns
by id from the gateway's schema files and positions the gateway's cursor (`gateway_cursor`). A table is its snapshot
(with the position it was taken at) and the changes after that position, each key's last one, deletes dropped: the
same reduction for every transport (conformance `gateway_log`). A CockroachDB changefeed lands the same way
(`land.changefeed`, read by `changefeed_log`). Use `format='json'`: its parquet writer fills a DECIMAL with the
number's text, so other readers get wrong numbers.

**Which log, which snapshot method.** [routing.py](scripts/lfc_change_source/routing.py) gives each table a class
from its snapshot's bytes and its change rate, or by name, and with it how its silver is kept and how its snapshot is
taken (`snapshot_by`: `qbc` by default, the customer's `files` past 10 TB). Every snapshot is a Delta table:

| Class | Changes | Silver |
|---|---|---|
| `small_slow` | the shared log | shared: a group's table of changes, a view a table over it and its snapshot table |
| `small_fast` | the shared log | shared, or a MERGE of its own every round |
| `big_slow` | the shared log | a MERGE when changed |
| `big_fast` | a log of its own, sharded by key hash once one stream can't carry it | AUTO CDC, a continuous pipeline |

A busy table in the shared log makes every other table's read skip its events, so a fast table gets its own log, and
its own gateway or Debezium target table, since a gateway's change files mix its tables. The log carries changes
only: each table's snapshot is a Delta table of its own (`qbc`, or `intake` for any other arrival, a gateway's
staging included: `intake.gateway` copies it by name with its position). The suggestion from a
source's statistics, and what changing a route later takes (most changes are a cut-over at a position, no new
snapshot), and when a small table leaves its group (`routing.plan`): [references/routing.md](references/routing.md).

A shared adapter matters for cost: one scan filtered by source and table can replace a change-feed plan per source,
which is what grows with the sources (lfc-medallion's `references/performance-reasoning.md`).

FlowThrough, the four layers (transport, a table's silver, many sources into one, gold), each in one table: [docs/design.md](../../docs/design.md).

## Conformance: an adapter is done when this passes

[conformance.py](scripts/lfc_change_source/conformance.py) holds one scenario as data:
- a snapshot;
- inserts and updates;
- a delete;
- a key changed twice in one range;
- a replacement (a delete and an insert of one key at one position);
- a key inserted, then deleted.

Each adapter's `build()` turns it into its own transport and returns the positions. The checks then run the
adapter's SQL on a warehouse:
- **snapshot:** the snapshot's rows;
- **last change:** each key's final state;
- **ranges:** resuming range by range gives the same result;
- **`_seq`:** it increases for a key, with a delete before an insert at one position;
- **watermark:** it is never behind the last position;
- **stats:** they come from metadata.

```bash
python -m lfc_change_source.conformance --adapter lfc_bronze --warehouse <id> --schema <catalog>.<schema>
```

It makes one scratch table, `lfc_change_source_conformance`, in that schema.

## Adding an adapter

1. **Read the transport's samples.** [tests/samples/](tests/samples/) has real files of each, so the work can start
   offline.
2. **Write the module.** Copy `lfc_bronze.py`'s shape: the docstring with the transport, its position kind, a class
   with `snapshot_sql`, `changes_sql`, `watermark`, `stats` and `shared`, then `build()` for the scenario.
3. **Register it** in `ADAPTERS` in [scripts/lfc_change_source/__init__.py](scripts/lfc_change_source/__init__.py).
4. **Make it pass.** Run the offline tests, then conformance on a warehouse, until both pass.

A wrong position kind raises an error that names the expected kind.

## Checks

`python3 -m unittest skills/lfc-change-source/tests/test_change_source.py` checks, offline:
- the contract;
- each adapter's SQL, watermark and stats, on fakes;
- the scenario's truth;
- that every registered adapter has the contract's methods and `build()`;
- that the samples are consistent with their schema files.
