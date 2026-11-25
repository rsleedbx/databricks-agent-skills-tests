# FlowThrough: intelligent

Each table's route is decided from what the source already knows about it, not asked of the user table by table
([design.md](design.md)).

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

**From statistics: its class.** lfc-change-source `routing` puts each table in one of four classes by its snapshot's
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
