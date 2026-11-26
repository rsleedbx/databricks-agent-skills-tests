# FlowThrough: intelligent routing

Routing (layer 1, [design.md](design.md)) comes first and decides how each table combines its CDC and its snapshot,
from what the source already knows about it, not asked of the user table by table: its change log and shards (CDC),
its snapshot method (snapshot), its silver cell (view, shared group, MERGE, AUTO CDC), what a schema change does to it
(from the schema events in the log: [design-no-full-refresh.md](design-no-full-refresh.md)), and when it's
checkpointed. The layers carry each decision out; none of them decides it.

## The steps

| # | Step | Runs |
|---|---|---|
| 1 | **Introspect**: the source's schema and statistics | first, and again on demand from the dashboard |
| 2 | **Provisional CDC**: every table into one log, to observe | after 1; again when the plan changes |
| 3 | **Provisional snapshot**: what the classification needs of the rows | after 2 |
| 4 | **Classify**: the plan from 1 to 3, the observed log and the decisions | every round |
| 5 | **A person confirms** the plan in the dashboard (or changes it, and 2 to 4 run again) | when asked |
| 6 | **Real CDC**: the logs, splits and producers the plan says | after 5 |
| 7 | **Real snapshot**: each table by the method the plan says | after 6 (its CDC started first) |
| 8, 9, 10 | **Silver, n:1, gold** | from 6 and 7 |

**The provisional snapshot is for small tables with statistics only** (step 3): a table whose introspection gives
its size, and is small, is read by our own `spark.read.jdbc` through its connection, after the provisional CDC has
started, so a person sees real rows in silver while deciding. A table with no statistics gets none (it could take
long). It is never taken twice without need: it is a snapshot table with its position like any other, and step 7
keeps it as the real one while that position is still in a log the table reads from (a split overlaps, so it stays);
step 7 takes a new one only for a table that had none, or whose schema or method the decisions changed.

**The mapping is a person's to override too.** The dashboard shows each source column's place and type in
Databricks: catalog, schema, table, column, data type, from introspection and the routing rules; a person overrides any
of them (`mapping_overrides`, NULL meaning the default), and silver builds from the result.

**Classify finds the n:1 on its own.** Tables that repeat across databases or schemas (the same columns and keys, per
customer or per shard) are one table to decide, not thousands: classify groups them by their shape and shows a group
as one row with its members, so the dashboard shows the 50 shapes of a 10,000-table source, and a decision on the group
applies to each member. Later: show each shape and where it repeats.

Steps 2 to 5 are a loop: a start, a restart, a change in the data or a change of mind goes round it again. Each
step a person starts is a row in a table a job watches (a table update trigger): `introspection_request` runs 1,
`routing_iteration` runs 4, `ingestion_start` runs 6 and 7. Nothing is overwritten: each introspection is a run in
`introspection_schema` and `introspection_stats`, and each plan a round in `routing_plan` that names the introspection
run it came from, so the dashboard shows how the source and the plan changed over time.

## Two boxes: introspect, then classify

Routing is two boxes with files between them, so each one runs, fails and is tested on its own:

| Box | Inputs | Outputs | Runs |
|---|---|---|---|
| **Introspect** | the source database (a read-only login) | `schema` (tables, columns, keys, foreign keys) and `stats` (rows, bytes, inserts, updates and deletes over a known time) | once, before change capture starts; again when asked |
| **Classify** | `schema`, `stats`, `observed` (each table's events counted in its change log, by round), `decisions` (what a person chose) | `plan`: each table's role, class, change log and shards, snapshot method and silver cell, with why | every round |

Between the two, the change capture starts, every table into one change log. Classify then runs in a loop: round n
reads what the log shows and what a person decided since round n-1, and writes plan n. The layers below carry out
plan n; nothing below decides. A plan is a file, so a round can be replayed from its inputs and two plans compared.

**A person in the loop, through tables.** The plan is a table (`routing_plan`: each table's heuristic role, class,
silver cell, snapshot method, log and why, by round) that the dashboard shows. Beside it, `routing_decisions` has the
same choices as override columns, NULL meaning "take the heuristic", which a person sets in the dashboard's editable table
(the least work that is usable for testing; an app later). Editing doesn't start a round: a person adds a row to `routing_iteration`
(n + 1, who, when), and a job with a table update trigger on that table runs the next round: the non-null overrides
become `decisions`, classify writes plan n + 1. A log split is an override of a table's log, carried out by the
producer module below.

**A log can be split while it runs.** A change log is append-only, so a split never rewrites one: the plan names a
new log for some tables, the producer (a gateway, a Debezium Server) is stopped and started again from its last
position, sending those tables to the new log and the rest to the old one, and silver reads each table from its log
after that position. This works after the snapshot has started too, as long as the change capture started before it:
the position the snapshot is combined from is still in a log. A person can ask for a split (`decisions`), or the plan
can propose one when a table's rate outgrows its log.

**Restarting a producer is one abstraction for every transport** (CDC layer, used by every split and by any recovery):
`position()` (where the producer has got to), `stop()`, and `start(at, routes)` (from a position, with a destination
log for each table). Each transport is an adapter behind it, tested on its own with fakes and live:

| Adapter | Position | Destination per table |
|---|---|---|
| Debezium Server | its offset store (an LSN, an SCN, a binlog file and position) | its sink's target table: a new log is a new Zerobus target table, so the Zerobus side changes with it |
| Lakeflow Connect gateway | its cursor in the staging volume | the gateway's tables and the landing that copies them into a log |

The plan only names the new routes; this module carries them out, and silver reads each table from its log after the
position it was restarted at.

**Make before break.** A new producer's position starts where it is created (a PostgreSQL slot at the current WAL), not
where the old one was, so a split starts the new producer for the moved tables first and only then restarts the old one
without them. The two logs overlap for those tables (never a gap); both carry the source's own positions, and silver
applies an event at a position once, so a moved table reads its old log up to the cut and its new log after it. The
cut is the old producer's position when it stopped carrying the table.

**The plan, before anything is captured.** Routing reads, for every source:
1. the schema: tables, keys, foreign keys (each table's role, below);
2. the statistics: rows, bytes, and inserts, updates and deletes over a known time;
3. how many tables are in scope, per source;

and decides each table's CDC log and shards, its snapshot method (a direct JDBC read for the many small tables, QBC
for medium ones within its 250 tables a pipeline, a gateway's own when the gateway carries thousands, the customer's
files past 10 TB: [design-no-full-refresh.md](design-no-full-refresh.md)), and its first silver cell. Then every
round it re-decides cells from the measured rates.

**From relationships: its role.** cdc-table-plan `classify.py` reads a canonical schema's keys and foreign keys (and
row counts and activity, when known) and names each table's role, for the customer to check:

| Role | Shape |
|---|---|
| dimension | referenced by others, few or no parents |
| lookup | small reference data, referenced, no parents |
| bridge | a key made of two or more foreign keys |
| fact | several parents, the largest of them, few or no children |
| log | no primary key, or inserts only, named like a log |
| queue | inserts and deletes, few updates |

**From statistics: its class.** lfc-routing `routing` puts each table in one of four classes by its snapshot's
size and its change rate:

| Class | Size, changes | Its share |
|---|---|---|
| `small_slow` | small, few | most tables, almost no data (dimensions, lookups) |
| `small_fast` | small, many | busy small tables (queues, counters) |
| `big_slow` | big, few | large tables changed now and then (facts) |
| `big_fast` | big, many | realtime: few tables, most of the bytes |

The class picks the table's change log, its shards, its silver cell ([design-scale.md](design-scale.md)) and its
snapshot method ([design-no-full-refresh.md](design-no-full-refresh.md)).

**Busy is measured, not asked.** Before the change capture starts, the source's own counters give each table's
inserts, updates and deletes over a known time (lfc-source-statschema: a database's statistics, a change journal by
hour). After it starts, the change log gives each table's rate every round (lfc-log-silver `flowthrough`), and a table
moves when it turns busy or quiet. A move rebuilds that one table from its snapshot table and the log; nothing else
changes (`routing.transitions`).

**What the data can't say is chosen by name.** How fresh a table must be for its readers: a quiet table someone needs
within seconds is chosen as realtime. Any table's class, silver cell or snapshot method can be chosen by name
(`Policy.chosen`, `silver_chosen`, `snapshot_chosen`), and a small table can be kept out of a group for what only a
table of its own gives (`own_table`: streaming reads, its own change data feed, time travel).

**The plan is pinned after the snapshot, not before.** The snapshot takes time, and the change capture runs first,
so its rates are measured while the snapshot is taken. Meanwhile every table is a view (`flowthrough.observe`) and
the route the plan would give it is shown as proposed; the customer pins the plan, and `flowthrough.step` moves each
view to its route. The dashboard shows changes coming in, snapshots coming in, and the routes as they form.
