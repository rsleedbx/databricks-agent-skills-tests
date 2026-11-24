# Routes: a suggestion from statistics, and what changing one later takes

Every table gets a route: its class, its snapshot's path, its change log, and its shards
([routing.py](../scripts/lfc_change_source/routing.py)). Workloads change, so a route is a starting point the user
edits, and a later change should cost a cut-over, not a new pipeline.

## The suggestion

cdc-table-plan's `classify.py` adds a `route` to each table of the plan it writes, from lfc-source-statschema's
statistics of the source:

| Input | From | Gives |
|---|---|---|
| bytes | `stats.yaml`'s `data_size_bytes`, else `row_count` x the columns' `avg_width_bytes` | big or small |
| change rate | the activity's inserts, updates and deletes (`collect-activity-*.sql`) over `--activity-seconds` (how long the counts cover); SQL Server's hourly journal gives the busiest hour | fast or slow |

```bash
classify.py schema.yaml --stats stats.yaml --activity activity.yaml --activity-seconds 86400 > plan.yaml
```

Each route says why (`why`: the bytes, the rate, or that one is unknown). A table of unknown size is `auto_slow` or
`auto_fast`: its snapshot's real bytes decide small or big when it lands. An unknown rate counts as slow. The user edits `route` like `silver`.

| Class | Snapshot | Changes | Silver |
|---|---|---|---|
| `small_slow` (small, few changes) | `log`: landed into the change log | the shared log | `view`: no work a round |
| `small_fast` (small, many changes) | `log` | the shared log | `merge_round`: a MERGE every round |
| `big_slow` (big, few changes) | `direct`: read where the producer wrote it | the shared log | `merge_changed`: a MERGE in rounds that change it |
| `big_fast` (big, many changes) | `direct` | its own log, sharded by key hash once one stream can't carry it | `auto_cdc`: flows in a continuous pipeline, setup paid once |

A small table costs per object (a MERGE has a fixed floor of seconds whatever it merges), a big one per byte. Most
tables are small and slow and hold almost no data; most bytes are in a few big, fast tables. Any table whose rate
passes `own_log_events_per_s` also gets its own log.

Thresholds (`Policy`): `fast_events_per_s` (default 1), `own_log_events_per_s` (10,000), `max_log_snapshot_bytes` (1 GiB), and
`shard_events_per_s` (Zerobus's documented 100,000 records a second per stream). They are starting points, set per
deployment.

## What can change later, and how

`plan.py diff <deployed plan> <edited plan>` names each change (`snapshot`, `cutover`, `reshard`) with what it takes.

| Change | How | Takes a new snapshot? | State |
|---|---|---|---|
| snapshot `direct` -> `log` | land the kept snapshot into the log once (`land.gateway_snapshots`) | no | built, tested |
| snapshot `log` -> `direct` | read the kept snapshot in place; its rows in the log are ignored (`gateway_log.current_sql`) | no | built, tested |
| changes shared -> own log (slow -> fast) | at a position P: land the table's events after P into its own log (`land.gateway(only=...)`, the shared landing `skip=...`); read the shared log up to P and its own after P (`routing.history_fq`) | no | built; the reader tested live (two logs to silver, every table equal to the source) |
| changes own -> shared log (fast -> slow) | the same cut-over the other way | no | built, tested offline |
| shards | at a position P: split the events after P by the new key hash | no | designed, not built |
| the gateway or Debezium target a table is on | the new one starts with the table's snapshot | **yes** | an applier of your own adds the table to the new gateway's task |

Positions of one source compare across logs (one source, one kind of position), so a cut-over needs only P, kept with
the table's route history. Each needs the producer's snapshot kept (staging cleanup deletes only landed change files).

## What to choose at the start

Only one choice is costly to change: **which gateway (or Debezium target table) a table is on**. Moving costs a new
snapshot of it, a full read of the source table. Everything else is a cut-over.

- Group the many small, slow tables, a few thousand to a gateway, on a shared log.
- Put each table that is big, or likely to become fast, on a gateway of its own from the start, even while its
  changes still go to the shared log. If it turns fast, its cut-over to its own log is a landing filter and a position,
  and other tables' change files never carry its events.
- Let the snapshot path follow the bytes (`auto`). Change it per table any time.
- Start every table on the shared log unless the statistics show it fast. Measure the real rates from the log
  (`routing.rates`) and edit the plan.
