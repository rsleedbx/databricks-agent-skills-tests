---
name: lfc-medallion
description: "Build the layers downstream of Lakeflow Connect from many sources (databases, schemas, tenants, lines of business) as reusable Python: discover the sources and their tables, then stack each table n:1 into one materialized view with its source, so silver and gold are only business SQL. Use when several Lakeflow Connect destination schemas hold the same tables and need one view."
---

# Layers downstream of Lakeflow Connect, from many sources

Lakeflow Connect writes each source (a database, schema, tenant or line of business) into its own destination schema,
and the same table names repeat across them. Three layers keep the generic part apart from the business part:

| Layer | What it does | Knows tables or columns | Module |
|---|---|---|---|
| n:1 | each table of every source in one materialized view, plus a column naming the source | no | `stack_all` |
| silver | joins and cleans the stacked tables, one table per business entity | yes (your SQL) | — |
| gold | shapes silver for its readers | yes (your SQL) | — |

Why separate n:1: a view that joins tables for each source has one join per source per table, and past the join limit
for incremental refresh a materialized view is recomputed in full on every change (the event log's planning
information says `NUM_JOINS_THRESHOLD_EXCEEDED`). Stacked first, a silver view joins a fixed number of tables, however
many sources there are. Keys usually start at 1 in every source, so every silver key and join includes the source
column.

Materialized views, not streaming tables: Lakeflow Connect's SCD type 1 destination tables take updates and deletes,
which a streaming read can't follow; a materialized view refreshes incrementally from the tables' change feed.

## Use

A pipeline source file (Lakeflow Spark Declarative Pipelines, serverless):

```python
from pyspark import pipelines as dp
from lfc_medallion import stack_all

stack_all(dp, spark, catalog="main", prefix="sales_bronze_", source_column="division",
          only=spark.conf.get("divisions", ""))   # empty: every schema sales_bronze_*
```

That defines `all_<table>` for every table any source has (`name="n1_{table}"` changes the pattern). A table only
some sources have is stacked from those. Silver and gold are SQL files in the same pipeline that read `all_<table>`.
The sources and tables are found when each update starts, so a new source or table needs only its ingestion.

The package is `scripts/lfc_medallion` (no dependencies beyond the pipeline's Spark). A serverless pipeline installs it
from this repo through its environment (bundle `resources.pipelines.<name>`):

```yaml
environment:
  dependencies:
    - "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-medallion/scripts"
```

The archive URL, not `lfc-medallion @ git+https://...`: the pipeline passes the line to `%pip install` unquoted, so the
spaces split it and pip looks for `lfc-medallion` on PyPI (`ENVIRONMENT_PIP_INSTALL_ERROR`). A branch or tag name in
place of `main` pins a version. Offline tests: `python3 -m unittest skills/lfc-medallion/tests/test_stack.py`.

## Fact or dimension: `table_plan`

Facts change all the time; dimensions rarely. With a few tables both can be views in one pipeline. With hundreds of
sources or thousands of tables, every table in the pipeline's graph lengthens each update's start, so the quiet ones
are better kept by a MERGE job. `table_plan` decides each table's role from evidence, then hints:

```python
from lfc_medallion import table_plan
p = table_plan(spark, "main", "sales_bronze_east")                          # bronze only
p = table_plan(spark, "main", "sales_bronze_east", connection="pg_conn",    # the source's own catalog
               database="sales", source_schema="east", hints={"nation": "lookup"})
p["tables"]["lineitem"]   # role, silver (scd1, scd2, append), route (pipeline, merge), confidence, decided_by, why
```

| Evidence, in order | What it gives |
|---|---|
| The source through the Unity Catalog connection (`remote_query`; PostgreSQL) | declared primary and foreign keys, row counts, inserts / updates / deletes since its statistics were reset |
| Bronze | the key Lakeflow Connect records (table property `spark.internal.streaming_table.apply_changes.keys`, internal and undocumented), row counts, each MERGE's inserts / updates / deletes after the snapshot (table history), foreign keys inferred from names (`l_suppkey` -> `s_suppkey`) |
| `hints` | `{table: role}` or `{table: {role, silver}}`; they win (`decided_by: hint`, the guess kept in `guessed`) |

The rules are cdc-table-plan's (its accuracy against the TPC specifications and Mautic is in that skill's SKILL.md);
they live in `lfc_medallion.roles`, and its `classify.py` runs them from files. Routes: facts, logs and queues go to the
pipeline; dimensions, lookups and bridges to a MERGE job (`routes` overrides per role).

## Tested

The same layering with three sources of eight tables each (TPC-H, PostgreSQL through Lakeflow Connect standard CDC):
one changed line and one renamed reference row (a nation) each refreshed row-based in every layer, and the tables the
change didn't touch did nothing (`NO_OP`); before the split, one silver view of 18 joins recomputed in full.
Installed from this repo's archive URL in that pipeline, `stack_all` defined the same views as the hand-written file
had: every view `NO_OP` on the next update, nothing recomputed. Not run yet: more than three sources; a MERGE job for the `merge` route.

## Trade-offs

- Every table is stacked, including ones silver doesn't use: storage and an update step each (`NO_OP` when unchanged).
- A schema that matches the prefix but isn't a source is read as one; `only` lists the sources instead.
- A source whose schema exists before its tables land is stacked without them, until the next update after ingestion.
