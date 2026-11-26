---
name: lfc-introspection
description: "A source database's schema.yaml and stats.yaml (statschema's canonical format), read from its catalog through a Unity Catalog connection: tables, columns and types, primary and foreign keys, rows and bytes; no table scans, no row data. Routing's first box (classify reads the two files), and the same input for data generation when nothing is ingested. Use to describe a source's tables before planning CDC or shaping test data."
---

# Introspection

Routing's first box ([docs/design-intelligent.md](../../docs/design-intelligent.md), "Two boxes"): a source
database in, two files out, nothing else, so routing (lfc-routing `classify`), data generation (lfc-source-datagen)
and sizing (lfc-source-sizing) each take it without what the others need.

| File | Holds | Read from |
|---|---|---|
| `schema.yaml` | each base table's columns (canonical type, native type, not null, default), primary key, foreign keys with their actions | [`sql/infoschema-columns.sql`](scripts/lfc_introspection/sql/infoschema-columns.sql), [`sql/pg-keys.sql`](scripts/lfc_introspection/sql/pg-keys.sql) or [`sql/infoschema-keys.sql`](scripts/lfc_introspection/sql/infoschema-keys.sql), [`sql/mysql-*.sql`](scripts/lfc_introspection/sql/mysql-columns.sql) |
| `stats.yaml` | each table's rows and data bytes | [`sql/size/<engine>.sql`](scripts/lfc_introspection/sql/size/), the queries lfc-source-sizing runs |

```bash
uv run scripts/introspect.py --profile P --warehouse ID --connection lfcddemo-azurevm-pg --engine postgres \
  --database postgres --schema lfcddemo --out out/
```

In Python: through a UC **JDBC** connection with Spark's jdbc source (`via="jdbc"`, the default: a notebook or job,
any engine with a driver, Spark's read options later), or a standard connection with `remote_query` on a SQL
warehouse (`via="remote_query"`: from the laptop, the command line above). Either way
`from lfc_introspection.introspect import introspect; schema, stats = introspect(session, conn, "mysql", schema="app")`.
The documents are statschema's (`statschema.tools.canonical.convert`, the conversion its own collectors use).

Engines: PostgreSQL, YugabyteDB, CockroachDB (keys from `pg_catalog`: `information_schema`'s constraint views show
only tables the login owns), SQL Server, MySQL and MariaDB (`information_schema`; MySQL names its columns in upper
case). Oracle, Db2 and Informix have size queries but no columns or keys query yet.

## On demand, kept by run

A person asks for a run by adding a row to `introspection_request` (n, source, engine, database, src_schema), from the
dashboard's editable table; the bundle's job ([bundle/](bundle/databricks.yml), a table update trigger on that table)
runs [`history.run_pending`](scripts/lfc_introspection/history.py) through the request's JDBC connection. Each run is appended: `introspection_runs` (status,
error, the documents classify reads, what changed since that source's previous run: tables and columns added or
dropped, a type or key changed, rows grown), `introspection_schema` (columns) and `introspection_stats` (rows, bytes).
`history.latest(session, schema, source)` gives a source's last documents. Live (2026-11-26): a row added, the
triggered run 76 s later; a bad connection recorded as a failed run.

## Checks

`python3 -m unittest skills/lfc-introspection/tests/test_introspect.py` (offline, a fake session): the three
statements, type text per engine, a composite foreign key and its action, a keyless table, rows and bytes of the
schema asked for. `tests/test_history.py` (offline): rows, changes between runs, pending requests, a failed run.
Live (2026-11-26): the demo VM's PostgreSQL, MySQL and SQL Server `lfcddemo` tables, keys and
row counts as the databases have them.
