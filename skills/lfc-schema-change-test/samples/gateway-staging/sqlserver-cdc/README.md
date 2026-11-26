# SQL Server CDC: what the gateway stages for each edge case

Real files from one standard-CDC gateway's staging volume (2026-11-26, SQL Server 2022 CU27 Developer, utility
objects 1.7, serverless gateway with table and column ids on), for writing and testing a reader or applier of the
staging offline. One source table per case in `edgecases.edge`; the 25 DDL cases are [ddl-cases.yaml](../../../scripts/ddl-cases.yaml)'s
(`ddl-matrix.py sql sqlserver cdc --setup utility-1.7`), the 10 data-change cases are `dml_*`. The data is synthetic.

| File | What |
|---|---|
| `records.jsonl` | every change record the gateway wrote, one line each: `{"case": <table>, "record": <the line from cdc/<producer start>/<segment>>}`; `dml_bulk` keeps 3 of its 20,000 (`of`) |
| `schemas.json` | each table's schema files from `metadata/table_schemas/<table id>/target_schema.<n>.json`, by table then version (17 tables have a version 1 after their DDL) |
| `gateway_events.json` | the gateway's `cdc_extraction_details` events (one per DDL it parsed) and its two flow failures, with their messages |

Record shape: `table_id` (catalog, schema, name, `uoid`), `cursor.sequence` (`~v00001-<commit LSN>-<counter>`, sorts in
commit order), `schemaVersion` (the `target_schema.<n>` the record follows), `txnID`, `operation` (`INSERT`, `UPDATE`,
`DELETE`), `data` (columns by column id; `schemas.json` maps them to names).

## Data changes (after the snapshot)

| Case | Input -> staged |
|---|---|
| `dml_basic` | insert 4, update 2 (`n` 2 -> 20), delete 3 -> `INSERT`, `UPDATE` (the after image only, every column), `DELETE` (every column) |
| `dml_pk_update` | `UPDATE ... SET id = 100 WHERE id = 1` -> `DELETE id=1`, `INSERT id=100`: one commit LSN, counters `…01`, `…02` |
| `dml_txn_commit` | one transaction: 2 inserts, 1 update, 1 delete -> 4 records with one commit LSN, counters `…01`..`…04` in statement order; `txnID` `"0"` |
| `dml_txn_rollback` | insert rolled back, then a committed insert -> only the committed row; nothing marks the rollback |
| `dml_same_row_many` | one row set to 1..20 in 20 statements -> 20 `UPDATE`s, one commit LSN each, in order |
| `dml_wide_one_col` | only `c3` changed -> the whole row, with the unchanged 100,000-character `NVARCHAR(MAX)` |
| `dml_nulls_lob` | every nullable column set NULL -> JSON `null`s; a 30,000-character value -> inline in the record. A 1 MB value failed at the source (Msg 7139: over `max text repl size`, 65,536 bytes), so nothing was captured |
| `dml_keyless` | no primary key: update one of two equal rows, delete another -> `DELETE` + `INSERT` with the **same** `sequence` (only their order in the file separates them), then `DELETE` |
| `dml_types` | each type at an extreme -> `DATETIME2(7)` `.9999999` as `.999999` (6 digits); `DATETIMEOFFSET` `00:00 -14:00` as `14:00` (UTC, offset dropped); `TIME(7)` a string with 7 digits; `DECIMAL(38,10)` `-0.0000000001` as `"-1E-10"`; `MONEY` as `DECIMAL(19,4)`; GUID, XML as strings. (`FLOAT` `-2.2e-308` was already `-0` in SQL Server.) |
| `dml_bulk` | 20,000 rows in one `INSERT ... SELECT` -> 20,000 `INSERT`s in one change file |

## DDL (each followed by one insert)

| Case | Input -> staged |
|---|---|
| `add_nullable`, `add_nullable_with_default`, `add_not_null_with_default`, `add_identity_column` | `ADD c INT ...` -> `target_schema.1` with the column; the next `INSERT` has `schemaVersion: 1` and the value (default 5, identity 3) |
| `add_nullable_keyless` | `ADD c` on a keyless table -> version 1, and its stand-in key (every column) grows to include `c` |
| `drop_column` | `DROP COLUMN v` -> version 1 without `v`; the next record has no `v` |
| `int_to_bigint`, `bigint_to_int`, `int_to_string`, `decimal_10_2_to_18_4`, `int_to_decimal_18_2`, `date_to_timestamp` | `ALTER COLUMN n <type>` -> version 1 with the new `type`; values follow it (decimals as strings) |
| `string_length_narrow`, `varchar_to_nvarchar`, `datetime_precision_up` | the same generic type -> version 1 where only `sourceDataType` changes (`NVARSTRING(50)` -> `(20)`, `VARSTRING` -> `NVARSTRING`, `TIMESTAMP(3)` -> `(7)`) |
| `null_to_not_null`, `not_null_to_null` | -> version 1, only `notNull` flips |
| `add_computed_column` | `ADD cc AS (n * 2)` -> no version 1, no record after; the table's flow FAILED: "Cannot parse generated column ... requires a full refresh on the target table" |
| `add_unique` | `ADD CONSTRAINT ... UNIQUE (v)` -> no record after; flow FAILED: "`ADD unique key` requires a full refresh" |
| `rename_table` | `sp_rename` to `rename_table_new` -> no new version, no event; rows inserted into the new name keep arriving as `rename_table`, `schemaVersion: 0` |
| `drop_table` | `DROP TABLE` -> nothing staged, no event |
| `drop_primary_key`, `rename_column`, `truncate_table` | refused by SQL Server on a CDC table (Msg 22979, 4928, 4711): no change to capture |
| `start_first_update` | no DDL (the control) -> nothing after the snapshot |

The same 25 cases on the destination side: [results/ddl-matrix/sqlserver-cdc-utility-1.7-2026-10-26.yaml](../../../../../results/ddl-matrix/sqlserver-cdc-utility-1.7-2026-10-26.yaml).
The `truncate` case's table is `truncate_table`: `ddl-matrix.py` named it `truncate` before, which SQL Server refuses
as a table name (finding `ddl-matrix-sqlserver-reserved-table-name`, fixed with these samples).
