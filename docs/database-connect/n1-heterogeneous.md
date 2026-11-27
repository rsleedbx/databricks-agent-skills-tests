# Many engines into one n:1: SQL Server, MySQL, PostgreSQL and Oracle

An advanced use case: the same tables in sources of different engines (a line of business on SQL Server, another
on MySQL, PostgreSQL or Oracle) kept as one set of tables, each row with the source it came from. Each source is
captured on its own, its silver kept from its own change log, and each table's sources stacked into one n:1 view.
Back to [database connectors](README.md).

**Tested:** SQL Server 2022, MySQL, PostgreSQL 16 and Oracle 23ai Free, each through a Lakeflow Connect gateway,
two tables each, into one n:1 view per table and a gold view over them. Every silver table matched its snapshot
plus its changes in two rounds, with the tables both in a shared group and merged, and each n:1 view held exactly
its members' rows.

## How it fits together

| Step | For each source | Across sources | Skill |
|---|---|---|---|
| Introspection | its tables, columns, keys, rows and bytes from its catalog, through a Unity Catalog JDBC connection, kept per run | | [lfc-introspection](../../skills/lfc-introspection/SKILL.md) |
| Group | | tables grouped by name and column shape, so 10 shapes are decided instead of 10,000 tables; an Oracle `NUMBER` key groups with an integer | [lfc-routing](../../skills/lfc-routing/SKILL.md) |
| Change capture | its Lakeflow Connect gateway, set up as the [connector docs](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview) create it | | |
| Land | the gateway's snapshots copied once, its change files landed into its own change log | | [lfc-change-source](../../skills/lfc-change-source/SKILL.md) |
| Silver | as soon as its own tables are in, without waiting for the other sources | | [lfc-log-silver](../../skills/lfc-log-silver/SKILL.md) |
| n:1 | | each group's members stacked with a `_source` column, types widened to one | [lfc-medallion](../../skills/lfc-medallion/SKILL.md) |
| Gold | | marts on the n:1 tables | [lfc-medallion-pack](../../skills/lfc-medallion-pack/SKILL.md) |

Watch each gateway while it starts, and before reading what it captured:
[lfc-pipeline-watch](../../skills/lfc-pipeline-watch/SKILL.md). Every workspace call goes through
[lfc-rest](../../skills/lfc-rest/SKILL.md), so each step runs from a laptop or a notebook.

## What each engine needs

| Engine | Gateway | Its tables arrive as | Introspection |
|---|---|---|---|
| SQL Server | the [SQL Server connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline) | `<database>.<schema>.<table>` | `information_schema`; sizes from `sys.partitions` when the login can't read `sys.dm_db_partition_stats` |
| MySQL | the [MySQL connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/mysql-pipeline) | `default_catalog.<database>.<table>` | `information_schema`; sizes without `mysql.innodb_table_stats` when the login can't read it |
| PostgreSQL | the [PostgreSQL connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql-pipeline), with a replication slot and publication per pipeline | `<database>.<schema>.<table>` | `information_schema` and `pg_catalog`, filtered to the database |
| Oracle | the [Oracle connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/oracle-integrated-overview); a multitenant database's gateway names the CDB's service | `<PDB>.<SCHEMA>.<TABLE>`, upper case | the `all_*` views through a JDBC connection to the PDB (a connection whose service is the CDB's sees no PDB tables there) |

- **A replication slot per PostgreSQL pipeline:** make one with the pipeline's publication through a UC JDBC
  connection whose login has REPLICATION: [lfc-source-contract](../../skills/lfc-source-contract/SKILL.md)
  `cdc.pipeline_slot`. A slot holds WAL until it is read: drop it when its pipeline is deleted.
- **Names are matched case-insensitively** across sources (Oracle's are upper case); each group is keyed by its
  lower-case name.
- **Catalog and schema:** introspection filters every query by both, so a schema of the same name in another
  database isn't read. The target catalog is the workspace's default unless one is given; none is assumed.

## Correct with a log that has gaps

A source's change log can hold older history: events from an earlier capture, with deletes that never landed. Each
table's silver reads only the changes after its own snapshot's position, so rows the snapshot no longer has never
come back. A table whose silver method changes between rounds (shared, merged, a view) is rebuilt in its new form.

## Run it

```python
from lfc_rest import Rest, target_catalog
from lfc_introspection import history
from lfc_routing import rounds
api = Rest.connect()                         # in a notebook; Rest.connect("<profile>") on a laptop
catalog = target_catalog(api)                # the workspace's default catalog
# 1. each source introspected through its UC JDBC connection (an introspection_request row per source)
history.run_pending(spark, f"`{catalog}`.`<schema>`", via="jdbc", connection_for=lambda c: "<its JDBC connection>")
# 2. one round across the sources: groups and each table's route, recorded in routing_plan
plan = rounds.run_round(spark, f"`{catalog}`.`<schema>`", {"<connection>": {"log": "<its change log>",
                                                                             "prefix": "<catalog>.<schema>."}})
```

Then per source: its gateway's snapshots and change files landed (lfc-change-source), its silver
(lfc-log-silver `planned.run_families`), and each group's members stacked (lfc-medallion `stack_sql`).
