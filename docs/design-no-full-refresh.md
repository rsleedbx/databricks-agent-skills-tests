# FlowThrough: snapshot (layer 2) and no full refresh

A table's silver is its snapshot plus every change after it ([design.md](design.md)). The two come apart: the
transport carries the changes ([design-transport.md](design-transport.md)), and a snapshot method takes the snapshot,
chosen a table at a time like its class. So a gateway doing the CDC can take QBC's snapshot, its task set to take none
of its own; Debezium through Zerobus carries a CockroachDB table's changes while a changefeed exports its 10 TB
snapshot; another method later is one more entry.

## The order

The change capture runs first, and a change of its own (a marker: a no-op update of one row) is seen in the log; only
then does the snapshot start. The log then holds every change from before the snapshot on. It carries whole rows, so a
change the snapshot already holds replays to the same row, and the overlap is harmless. A table without a key can't
tell a replay from a new row: it needs no writes in between. Since the snapshot takes time anyway, the rates the plan
needs are measured meanwhile ([design-intelligent.md](design-intelligent.md)).

## The contract: a snapshot is a Delta table

A snapshot is a Delta table (a streaming table too) with the source's column names and types. Views and MERGE read it
in batch, AUTO CDC streams it, each with the log's changes after its position over it (lfc-log-silver
`Table.snapshot_table`, lfc-change-source `CdcLog.current_sql`). Files alone aren't a snapshot: lfc-change-source
`intake` turns every arrival into the contract's table (a table as it is, Parquet converted in place, other files
copied once and typed, a gateway's staging copied by name with its position), so no silver cell knows a format,
decodes text or lists folders.

| Size | Snapshot by | Becomes the Delta table by |
|---|---|---|
| most tables | `qbc`: a Lakeflow Connect query-based pipeline through a UC connection, each table once | itself: it writes a streaming table |
| big (past `snapshot_big_bytes`, 10 TB) | `files`: the customer's export into storage we're told of, e.g. a CockroachDB `CREATE CHANGEFEED ... WITH initial_scan = 'only', format = 'parquet'`; or the transport's own | `intake.parquet_in_place` (`CONVERT TO DELTA`, no copy; the files mustn't change), else `intake.files` (copied once) |
| a transport that takes its own (a gateway) | the transport | `intake.gateway`: copied by name, with its position |

## The log carries CDC only

A snapshot is never in the change log. That keeps each part easy to see and to keep: raw CDC (the log, a table's
events), raw snapshot (its table) and the two combined (silver) are three things to look at, and the dashboard links
each. The log holds no second copy of every row as a VARIANT, so it grows with changes only; no reader filters out
snapshot rows or picks the newest of several. A new snapshot is a new table, swapped in: a table built from another
snapshot table (or from the log alone, before its snapshot was ready) is rebuilt from the new one, or its view
recreated (lfc-log-silver `silver.renewed`).

A shared group keeps only its members' changes: each key's last change, a delete kept as a marker. Each member's view
is its rows in the group plus its snapshot table's rows for the keys the group hasn't changed or deleted, matched by
the key typed from the group's stored key (`stored_key_values`). So the group's table and its one MERGE stay the same
for every member, and a member's new snapshot only recreates its view ([design-scale.md](design-scale.md)).

## No full refresh

The source is read once a table, for its snapshot; every later state comes from the log. A table that must change
shape (a type change) is rebuilt from its snapshot table and the log, never from the source.

**A checkpoint is an internal resnapshot.** A year of changes can rewrite a table; a rebuild or a view shouldn't replay
them from the first snapshot. `flowthrough.checkpoint` writes each table's current rows as of a position P into a new
snapshot table, from silver (a MERGE table's is a copy of itself; a view's or a shared member's is its rows), records
it so every later round starts there (a MERGE table isn't rebuilt; views are recreated over it), then deletes the
log's changes up to P for those tables (lfc-change-source `land.trim`). The log then holds only what's after the last
checkpoint.
