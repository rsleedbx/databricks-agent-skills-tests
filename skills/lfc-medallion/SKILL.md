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
| The source through its Unity Catalog connection (`remote_query`), the engine read from the connection's type (`engine` overrides) | declared primary and foreign keys, row counts, inserts / updates / deletes from the engine's counters (optional: without the privilege, row counts only) |
| Bronze: when there's no connection, or its engine has no catalog queries here (the plan's `evidence` says so) | the key Lakeflow Connect records (table property `spark.internal.streaming_table.apply_changes.keys`, internal and undocumented), row counts (`count_rows=False` skips them), each write's inserts / updates / deletes after the snapshot (table history), foreign keys inferred from names |
| `hints` | `{table: role}` or `{table: {role, silver}}`; they win (`decided_by: hint`, the guess kept in `guessed`) |

| Engine (connection type) | Keys | Counters |
|---|---|---|
| `POSTGRESQL` | `pg_catalog` (a SELECT-only login doesn't see constraints in `information_schema`) | `pg_stat_user_tables` |
| `MYSQL` | `information_schema.key_column_usage` | `performance_schema` (needs SELECT on it) |
| `SQLSERVER` | `sys.indexes`, `sys.foreign_keys` | `sys.dm_db_index_operational_stats` (needs `VIEW SERVER STATE`) |
| any other | bronze | bronze |

What else it handles: a write of any kind in the history (MERGE for SCD type 1 and 2 destinations, WRITE and
STREAMING UPDATE for append-only ones, DELETE, UPDATE; a replace, truncate or overwrite starts a new snapshot);
system columns left out (`__START_AT`, `__END_AT`, `_rescued_data`); views and a pipeline's own backing tables
(`__materialization_mat_*`) not planned; foreign keys by the conventions schemas follow without declaring them (the key's
own name, a table's column prefix as in TPC, `<name>_id` to `<name>(s)` keyed `id` as in most ORMs, and composite keys).

The rules are cdc-table-plan's (its accuracy against the TPC specifications and Mautic is in that skill's SKILL.md);
they live in `lfc_medallion.roles`, and its `classify.py` runs them from files. Routes: facts, logs and queues go to the
pipeline; dimensions, lookups and bridges to a MERGE job (`routes` overrides per role). Measured activity overrides
the role: at least `busy_changes` changes (default 1,000) and at least `busy_per_row` per row (default 2) send a
table to the pipeline whatever its role, since a table with no relations falls to the dimension default yet can
change all the time; a one-time bulk update that touches every row once doesn't count as busy.

## The `merge` route: `merge_all`

A job, not the pipeline, keeps the n:1 table of each quiet table current, so the pipeline's graph holds only the
tables that change all the time. Run it as a job task before the pipeline that reads the targets:

```python
from lfc_medallion import merge_all, stack_all, table_plan
plan = table_plan(spark, "main", "sales_bronze_east")
quiet = [n for n, t in plan["tables"].items() if t["route"] == "merge"]
merge_all(spark, "main", "sales_bronze_", target="sales_silver", tables=quiet, source_column="division")
# and in the pipeline: stack_all(dp, spark, "main", "sales_bronze_", source_column="division", skip=quiet)
```

For each table and source, `merge_all` reads the source's change feed since the version it last read, keeps each
key's last change, and MERGEs it into `<target>.all_<table>` keyed by the source column and the primary key (inserts,
updates, deletes). The first read, or a full refresh in bronze (replace, truncate, overwrite: the change feed starts
over), copies the source whole. Versions read are kept in `<target>._lfc_medallion_merge_state`. A source whose new
versions wrote no rows (every ingestion update adds versions to every table) is skipped from the history alone, so a
quiet table costs a metadata read, not a MERGE. New bronze columns are added first; an SCD type 2 destination's key
gets `__START_AT`. The targets have the change data feed and row tracking on, so the silver views reading them still
refresh incrementally.

## Tested

The same layering with three sources of eight tables each (TPC-H, PostgreSQL through Lakeflow Connect standard CDC):
one changed line and one renamed reference row (a nation) each refreshed row-based in every layer, and the tables the
change didn't touch did nothing (`NO_OP`); before the split, one silver view of 18 joins recomputed in full.
Installed from this repo's archive URL in that pipeline, `stack_all` defined the same views as the hand-written file
had: every view `NO_OP` on the next update, nothing recomputed.

`table_plan`, from a serverless job installing the package from a branch archive:

| Source | Evidence | Result |
|---|---|---|
| TPC-H, one line of business (PostgreSQL) | connection, and bronze | against TPC-H's truth file: 6 of 8 roles, 8 of 8 routes both ways. `region` and `nation` come out dimensions, not lookups: the lookup rule needs a fixed size, which only a scaled model shows, so a hint settles them; both go to `merge` either way |
| Two tables changed every minute (a keyed one, a keyless one), MySQL 8.4, SQL Server 2022, PostgreSQL 16 | connection, engine read from each | keys and counters read on all three; the keyless table a queue (inserts and deletes), the keyed one busy (about 87,000 changes on 1,000 rows): pipeline |

Found on the way, all fixed: Spark escapes quotes with a backslash (`''` is two strings joined); a SELECT-only login
doesn't see PostgreSQL's constraints in `information_schema`; `SHOW TBLPROPERTIES` doesn't return the keys property
(the Unity Catalog API does); a two-column foreign key (lineitem to partsupp) needs inferring, or partsupp looks like a
fact; `information_schema` lists a pipeline's backing tables next to its streaming tables.
`merge_all` on the same three sources, the six tables `table_plan` routes to `merge` (customer, nation, part,
partsupp, region, supplier), into a scratch schema: every target equal to the pipeline's own `all_<table>` view in both
directions (`EXCEPT ALL`: 0 and 0) after the first copy, after a nation renamed and one inserted, and after that one
deleted and the name restored; each change MERGEd only the one table and source it touched (17 of 18 unchanged).
Not run yet: more than three sources; bronze of an append-only or SCD type 2 destination (offline tests only).

## Trade-offs

- Moving a table to the `merge` route: remove it from the pipeline (`skip`) before `merge_all` writes a table of the
  same name; a pipeline's materialized view and a job's table can't share one.

- Every table is stacked, including ones silver doesn't use: storage and an update step each (`NO_OP` when unchanged).
- A schema that matches the prefix but isn't a source is read as one; `only` lists the sources instead.
- A source whose schema exists before its tables land is stacked without them, until the next update after ingestion.
- Sources stacked by column name must agree on each column's type (Spark widens compatible ones, like int to bigint;
  others fail the view): the sources' own design.
- The activity is a lower bound from bronze (inserts before the first update count as snapshot) and since the last
  statistics reset from the source; `busy_changes` and `busy_per_row` are counts over that window, not rates in time, so a long window counts more.
- Foreign keys inferred from names can be wrong for a schema that follows none of the conventions; the connection's
  declared keys are the better evidence.
