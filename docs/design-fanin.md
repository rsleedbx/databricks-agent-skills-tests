# FlowThrough: fan-in and gold

Layers 3 and 4 ([design.md](design.md)): each source's silver tables from layer 2 ([design-scale.md](design-scale.md))
combined into one table each, then marts on them.

## Layer 3: many sources into one (n:1)

Each source (a line of business, a tenant, a region) has its own silver tables from layer 2. Layer 3 combines them
into one table each, whatever cell each was built in (lfc-medallion).

| Shape | Reads | Writes | How | Code |
|---|---|---|---|---|
| one source | its silver tables | nothing more | layer 2's table is the answer | none |
| n:1 stack | each source's table | a view a table across sources, with a source column | `UNION ALL`, columns widened to one type (`coltypes`) | lfc-medallion `stack` |
| n:1 merge | each source's changes | a table a table across sources, with a source column | a MERGE a table of the changed sources; the fan-in planner picks the engine and how many MERGEs | lfc-medallion merge route (`paths.py`) |
| n:1 pipeline | each source's bronze | a streaming table a table | an AUTO CDC flow a source into one target | lfc-medallion `flows` |

What limits it: sources per table (a MERGE plans each changed source's changes in turn), tables per pipeline and
per schema, and the n:1 table's commits (its MERGEs commit one after another).

## Layer 4: gold

Gold reads the n:1 tables (or one source's silver) and builds marts for the questions asked of them.

| Shape | Reads | Writes | How | Code |
|---|---|---|---|---|
| SQL marts | the n:1 tables | tables | a job's SQL a run | lfc-medallion-pack |
| materialized views | the n:1 tables | materialized views | incremental refresh where the planner can, else a recompute | lfc-medallion-pack |

What limits it: how much of the source a refresh reads (incremental or a full recompute, the materialized-view
planner's choice), and how often it runs.
