---
name: lfc-routing
description: "FlowThrough's routing layer: each table's class from its snapshot's size and its change rate (small or big, slow or fast), and from it its change log and shards, its snapshot method and its silver cell (a shared group, a view, a MERGE every round or when changed, AUTO CDC); the rules that take a small table out of its group; what changing a route takes (a cut-over at a position, no new snapshot). Use to plan or explain how each table of a change log is kept, or why a table has the route it has."
---

# Routing

Routing decides each table's way through FlowThrough ([docs/design-intelligent.md](../../docs/design-intelligent.md));
the other layers carry it out: lfc-change-source lands the changes and takes the snapshots, lfc-log-silver keeps
silver by the cell routing chose (its `flowthrough` runner measures the rates every round and moves a table when its
route changes).

**Which log, which snapshot method.** [routing.py](scripts/lfc_routing/routing.py) gives each table a class
from its snapshot's bytes and its change rate, or by name, and with it how its silver is kept and how its snapshot is
taken (`snapshot_by`: `qbc` by default, the customer's `files` past 10 TB). Every snapshot is a Delta table:

| Class | Changes | Silver |
|---|---|---|
| `small_slow` | the shared log | shared: a group's table of changes, a view a table over it and its snapshot table |
| `small_fast` | the shared log | shared, or a MERGE of its own every round |
| `big_slow` | the shared log | a MERGE when changed |
| `big_fast` | a log of its own, sharded by key hash once one stream can't carry it | AUTO CDC, a continuous pipeline |

A busy table in the shared log makes every other table's read skip its events, so a fast table gets its own log, and
its own gateway or Debezium target table, since a gateway's change files mix its tables. The suggestion from a
source's statistics, and what changing a route later takes (most changes are a cut-over at a position, no new
snapshot), and when a small table leaves its group (`routing.plan`): [references/routing.md](references/routing.md).


**Classify: the plan from files.** [classify.py](scripts/lfc_routing/classify.py) is routing's second box: the schema,
the statistics, what the change log showed (`observed`) and what a person chose (`decisions`) in, a plan out, with
each table's role (lfc-medallion `roles`), its keys (the primary key, else every column, as QBC and a gateway take a
keyless table) and its route. No database or workspace, so a round replays from its files. cdc-table-plan
`classify.py` is its command line.

**Repeating tables: one decision a shape.** [shapes.py](scripts/lfc_routing/shapes.py) groups the tables that
repeat across databases or schemas by their name and columns (a deviating member is a variant, its own group) and
classifies once a group (`classify_many`): 5,000 tenants of 2 tables are 2 plan rows with 5,000 members each. A
decision on a group, a mapping override too, applies to every member.

```python
from lfc_routing import routing
p = routing.Policy(shared_log="`main`.`ctl`.`cdc_log`")
routes = routing.plan({"db.s.orders": (5 << 40, 250_000), "db.s.region": (4096, 0.01)}, p)
```

Plain Python, no Spark: it imports lfc-change-source's position kinds (`cdc_log.SEQUENCES`) for `history_fq`.

## Checks

`python3 -m unittest skills/lfc-routing/tests/test_routing.py` (offline): the four classes, groups and the rules out,
the suggestion from statistics, cut-overs across logs, what a route change takes, the snapshot method by size.
`python3 -m unittest skills/lfc-routing/tests/test_classify.py` (offline): keys, the observed rate over the activity,
decisions, the round, the schema's role lines never read.
