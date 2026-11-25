# FlowThrough: scale, one table's silver

Layer 2 ([design.md](design.md)) keeps each table's silver by the cell its class picks.

A table's cell is set by its size and by how fresh its silver must be (slow, fast, realtime): its class
([design-intelligent.md](design-intelligent.md)). Every cell reads the table's current rows one way (its snapshot
table, then each key's last change after it: [design-no-full-refresh.md](design-no-full-refresh.md)), so every cell
gives the same table (lfc-log-silver).

| | slow | fast | realtime |
|---|---|---|---|
| **small** (most tables) | shared: a group's table of changes, a view a table over it and its snapshot table | shared (or a MERGE of its own) | AUTO CDC flows in a continuous pipeline, and a flow for the snapshot table |
| **big** (most of the bytes) | a MERGE only in a round that changed it (or a view) | a MERGE every round | AUTO CDC, and a flow for the snapshot table |

A small table costs per statement (a MERGE has a fixed floor of seconds whatever it merges; a warehouse runs a few a
second), a big one per byte. So small tables share: a group keeps its members' changes in one table (the
row as a VARIANT, clustered by table), one MERGE a round for the group, and each table is a typed view over it and
its snapshot table.

`routing.plan` takes a small table out of its group, for a table of its own (a view if slow, a MERGE if fast), when:
- it needs what only its own table gives: streaming reads, its own change data feed, time travel (named in the
  policy);
- it carries more than a share of its group's changes (10% to start): its rows would slow every member's MERGE;
- it's read more than a number of times a day (from query history): each read casts its row, with no column
  statistics;
- it has a change log of its own (a busy table).

Any table's cell can be chosen by name. Moving a table into or out of a group builds that one table (or its rows in
the group) from the log, and swaps a view or a table; nothing else changes (`routing.transitions`).

What a group gives up for its one MERGE: per-table Delta features (time travel, change data feed, history are the
group's), streaming reads (a view can't be read as a stream), column statistics, and a failure holds back the whole
group that round. Its MERGE reads every member's changes since the last round, so it grows with their total.

What each cell can carry, and what sets its latency and throughput:

| Cell | Limited by | Tables per unit | Latency, change to silver | Throughput |
|---|---|---|---|---|
| shared | one MERGE a group a round, growing with the group's changed rows | tables per group (no SQL a table: the key is filled in when landing, the row kept as the event has it) | the log's latency + one MERGE | per MERGE: the group's changed rows |
| view | nothing a round; a read rebuilds the table | tables per schema (Unity Catalog: 10,000) | the log's latency; each read pays its rebuild | per read: the table's events in the log |
| MERGE every round | a fixed cost per MERGE; MERGEs at once per cluster | tables per cluster per round | the log's latency + tables / (clusters x MERGEs a second per cluster) | per MERGE: its changed rows (and the files they touch, for a big table) |
| MERGE when changed | the same, for changed tables only | changed tables per cluster per round | the same, over the changed tables | the same |
| AUTO CDC | flows per pipeline (a pipeline takes up to 1,000 tables); its setup grows with its flows, paid once when continuous | tables per pipeline | the log's latency + the pipeline's micro-batch | per flow; per byte for big tables |
| the snapshot table | a QBC pipeline per source, or one copy (intake) | per table, once | once, before the plan is pinned | per byte, once |
