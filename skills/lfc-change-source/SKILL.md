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
| `lfc_bronze` | a Lakeflow Connect bronze table: the ingestion pipeline's streaming table, one a source and table; changes from its change data feed (`table_changes`) | Delta versions | the latest version | no | built; lfc-medallion's merge route reads through it |
| change log | an append-only table of Debezium change events (through Zerobus), every table's: `destination`, `key:`, `value:after:`, op | the source's position (LSN, HLC) | the highest committed position | yes | next: from **zerobus-debezium-test**'s silver generator |
| gateway staging | the gateway's staging volume: `snapshot/<T>/*.parquet`, `cdc/<segment>/<n>` JSON lines with columns by id | the gateway's cursor | the extraction watermark, the committed LSN | yes: one folder a pipeline | then: formats in **lfc-medallion-pack**'s `references/gateway-staging.md` |
| object storage | a changefeed's files in cloud storage or a Volume (CockroachDB): `__crdb__updated` (HLC), `__crdb__event_type`; column families merged first | HLC | the newest `.RESOLVED` file | a folder a table | last |

A shared adapter matters for cost: one scan filtered by source and table can replace a change-feed plan per source,
which is what grows with the sources (lfc-medallion's `references/performance-reasoning.md`).

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
