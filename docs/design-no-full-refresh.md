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

**A schema change is an event at its position, not a refresh.** Databases treat `ADD COLUMN ... DEFAULT x`, a
rename and a type change as metadata: existing rows aren't rewritten, and the change is a fact at a moment. The change
log records it the same way, at its position P (layer 1), and silver reads each row by which side of P its last
change is on. No new snapshot, no rebuild from the source:

| Change at P | In the log at P | A row whose last change is before P (snapshot rows taken before P too) | after P |
|---|---|---|---|
| `ADD COLUMN c DEFAULT x` | `{operation: schema, add: c, default: x}` | `c` reads `x` | carries `c` |
| rename `a` -> `b` | `{rename: a -> b}` | `b` reads `a` | carries `b` |
| type change of `c` | `{retype: c, from, to}` | decoded as the old type, cast to the new (a narrowing change is flagged, not applied) | the new type |
| drop `c` | `{drop: c}` | as it was | `c` NULL, or dropped: routing's choice |

Routing reads the schema events and decides each table's action; silver carries it out: a view or a shared member's
view is recreated with the rules (one `CASE` on the row's position a schema event), a MERGE table gets the column
added or widened (`ALTER TABLE`) and its existing rows set by the rule. A snapshot taken after P already has the new
shape. n:1 and gold see an ordinary column change.

**A refresh is partial: only what's suspect is read again.** When part of a table is known to be wrong, a patch is
taken at a position P, the same way as any snapshot (its CDC running, its marker seen first), and the rest of the
table stays as it is:

| Patch | Read from the source | When |
|---|---|---|
| columns | the key and the suspect columns, every row | a column known to be wrong: a missed default, a type decoded wrongly, a rename with no schema event |
| rows | every column of the suspect keys or a key range | rows known to be wrong: a gap in the log for a window, one source partition |

For each key and patched column, the newest by position wins: a change after P from the log, else the patch's value
at P, else the snapshot and the changes before it, for what the patch doesn't cover. That's silver's one rule with one
more input at P, and the overlap is harmless as before: a change after P carries the whole row. Routing decides a
patch is needed and what it covers (from a failed check against the source, the drift record, or a schema event the
log can't resolve); the snapshot layer takes it (a query-based read of those columns or keys, or `intake` over files
holding them); silver recreates views over it, or runs one MERGE into a MERGE table setting only those columns; a
checkpoint later folds it into a whole snapshot table. A column patch of a wide table reads a fraction of its bytes,
and a row patch only its range; neither rewrites the table.

**A checkpoint is an internal resnapshot.** A year of changes can rewrite a table; a rebuild or a view shouldn't replay
them from the first snapshot. A checkpoint isn't a layer of its own: it's a loop, silver's output going back in as a
snapshot, each step in the layer that already does that job:

| Step | Layer |
|---|---|
| decide when: a table's changes since its snapshot pass a share of its rows, or a schedule | 3. Routing |
| write its current rows as of a position P into a new snapshot table: silver is one more snapshot source, beside QBC, files and a gateway (a MERGE table's is a copy of itself; a view's or a shared member's is its rows) | 2. Snapshot |
| switch to it: views recreated over it, a MERGE table recorded at P, not rebuilt (`silver.renewed`) | 4. Silver |
| trim the log up to P, once every table reading that log has a checkpoint at or past it (`land.trim`) | 1. CDC |

The log then holds only what's after the last checkpoint. A rebuild after a schema change is different: silver
recomputing from its own inputs, the snapshot table and the log, with no new snapshot.
