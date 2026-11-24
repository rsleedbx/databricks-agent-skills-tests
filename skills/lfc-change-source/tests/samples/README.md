# Samples of each transport

Real files an adapter reads, so an adapter can be written and tested offline. Names are generic; the data is
synthetic (TPC-B from the test generator).

| Folder | What | Source |
|---|---|---|
| `gateway/` | `cdc_history_update.jsonl`: two lines of a gateway `cdc/<segment>/<n>` file, an update of a keyless table as a DELETE and an INSERT in one transaction (one cursor `sequence`), columns by id; `target_schema.0.json`: the table's schema file from `metadata/table_schemas/<table id>/`, mapping each column id to its name and type (a keyless table gets a primary key of every column) | captured from a PostgreSQL gateway's staging volume |
| `object_store/` | `changefeed_formats.json`: CockroachDB parquet changefeed row columns and `.RESOLVED` file names; `changefeed_orders.ndjson`: a JSON changefeed's events (initial scan, updates, a delete with no after image, exact decimals) and a RESOLVED line | formats checked against CockroachDB's source; the JSON events captured from CockroachDB v25.4.13 into node-local storage |
| (change log) | a Debezium event row from a Zerobus change-log table | to capture with the change-log adapter |
