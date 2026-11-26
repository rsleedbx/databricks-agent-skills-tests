# Samples of each transport

Real files an adapter reads, so an adapter can be written and tested offline. Names are generic; the data is
synthetic (TPC-B from the test generator).

| Folder | What | Source |
|---|---|---|
| `gateway/` | `cdc_history_update.jsonl`: two lines of a gateway `cdc/<segment>/<n>` file, an update of a keyless table as a DELETE and an INSERT in one transaction (one cursor `sequence`), columns by id; `target_schema.0.json`: the table's schema file from `metadata/table_schemas/<table id>/`, mapping each column id to its name and type (a keyless table gets a primary key of every column) | captured from a PostgreSQL gateway's staging volume |
| `gateway/sqlserver_ct_*` | `sqlserver_ct_t0001.jsonl`: a SQL Server change-tracking gateway's keyed table, one `REPLACE` (its insert), `UPDATE` and `DELETE`; `sqlserver_ct_t0001_schema.json`: its schema file | captured from a 500-table SQL Server gateway's staging volume |
| `gateway/sqlserver_cdc_*` | `sqlserver_cdc_t1.jsonl`: a SQL Server CDC gateway's keyed table, one `INSERT`, `UPDATE` and `DELETE`; `sqlserver_cdc_t1_schema.json`: its schema file | captured from an Always On test gateway's staging volume |
| `object_store/` | `changefeed_formats.json`: CockroachDB parquet changefeed row columns and `.RESOLVED` file names; `changefeed_orders.ndjson`: a JSON changefeed's events (initial scan, updates, a delete with no after image, exact decimals) and a RESOLVED line | formats checked against CockroachDB's source; the JSON events captured from CockroachDB v25.4.13 into node-local storage |
| `debezium/` | `<engine>.jsonl`: real Debezium events through Zerobus for PostgreSQL, SQL Server, MySQL, Oracle, Informix and CockroachDB: a create and an update a table (each its key's last event), a delete, each with the canonical columns and the row the source had | captured from kept zerobus-debezium-test runs' change logs (`../capture_debezium.py`); Db2 and YugabyteDB not recorded yet |

Each source's shape, the canonical definition: [`lfc_change_source/formats.py`](../../scripts/lfc_change_source/formats.py).
`../check_formats.py` decodes every Debezium sample on a warehouse and compares it with the source's row (all six pass).
