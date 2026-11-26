"""formats: the canonical definition of each change source's events: how an event is laid out, where its position
is, how its key comes, how each type's values are encoded, and which recorded sample (tests/samples) holds real ones.
The adapters read these shapes (cdc_log, gateway_log, changefeed_log; debezium.decode for the values); synthetic
tests write them; tests/check_formats.py decodes every sample on a warehouse and compares it with the row the source
had, so a producer whose output changes shows up there first. Plain data, no Spark.

Debezium Server through Zerobus, one row an event in the change log: destination (<server>.<schema>.<table>),
operation (create, update, delete, read), key and value (VARIANT); value: after (the row; NULL for a delete), op
(c, u, d, r), source (where the event is from, its position), ts_ns. Values, unless an engine says otherwise: an
integer a number, a decimal its text (decimal.handling.mode=string), a date its days since 1970-01-01, a timestamp its
epoch milli-, micro- or nanoseconds by the column's digits of fraction (3 or fewer, 4 to 6, 7 to 9), a time its
microseconds of the day (nanoseconds at 7 to 9 digits), a string its text.
"""
from __future__ import annotations

OPS = {"c": "create", "u": "update", "d": "delete", "r": "read"}

DEBEZIUM = {
    "POSTGRESQL": {"sample": "debezium/pg.jsonl", "position": "lsn",
                   "source": "lsn (a number), txId, sequence ([last commit lsn, lsn] as text), ts_ms, ts_ns",
                   "key": "an object of the key columns, as numbers"},
    "SQLSERVER": {"sample": "debezium/mssql.jsonl", "position": "change_lsn",
                  "source": "change_lsn and commit_lsn (hex vlf:block:slot text, 0000002e:00003ea0:0002), "
                            "event_serial_no, ts_ms, ts_ns",
                  "key": "an object of the key columns, as numbers",
                  "values": "DATETIME2(7) and TIME(7): nanoseconds"},
    "MYSQL": {"sample": "debezium/mysql.jsonl", "position": "binlog",
              "source": "file (binlog.000003), pos, row, server_id, thread, ts_ms, ts_ns",
              "key": "an object of the key columns, as numbers",
              "values": "binary: hex text (binary.handling.mode=hex); ON DELETE CASCADE and SET NULL not in the binlog "
                        "through 9.5 (lfc-log-silver fk derives them)"},
    "ORACLE": {"sample": "debezium/oracle.jsonl", "position": "scn",
               "source": "scn and commit_scn (numbers as text), txId, ts_ms, ts_ns; names upper case",
               "key": "an object of the key columns, numbers as text ('5')",
               "values": "DATE holds a time: epoch milliseconds, a TIMESTAMP(0) column"},
    "INFORMIX": {"sample": "debezium/informix.jsonl", "position": "ifx_lsn",
                 "source": "change_lsn and commit_lsn (numbers as text), txId, ts_ms, ts_ns",
                 "key": "an object of the key columns, as numbers"},
    "COCKROACHDB": {"sample": "debezium/crdb.jsonl", "position": "ts_ns",
                    "source": "none: no source block; ordered by the event's ts_ns",
                    "key": "an object of the key columns, as numbers"},
    "DB2": {"sample": None, "position": "db2_lsn",
            "source": "change_lsn or commit_lsn (00000000:0000:0000000000a1b2c3 text: its last 15 hex digits order it)",
            "key": "not recorded yet"},
}

GATEWAY = {"sample": "gateway/cdc_history_update.jsonl", "position": "gateway_cursor",
           "sqlserver_ct": "gateway/sqlserver_ct_t0001.jsonl and _schema.json: a SQL Server change-tracking gateway's "
                           "keyed table, REPLACE (an insert, as an upsert), UPDATE and DELETE; cursor.sequence 20 "
                           "digits, seqNum base64; no INSERT",
           "sqlserver_cdc": "gateway/sqlserver_cdc_t1.jsonl and _schema.json: a SQL Server CDC gateway's keyed table, "
                            "INSERT, UPDATE and DELETE; cursor.sequence ~v<version>-<LSN hex>-<n> (fixed width, ordered "
                            "as text), lsn and seqNum base64",
           "event": "table_id {catalog, schema, name, uoid}, cursor {sequence (fixed-width text, ordered as text), "
                    "timestamp, extractionTimestamp}, txnID, operation (INSERT, UPDATE, DELETE; REPLACE an upsert), "
                    "data (columns by id, C_<uoid>_<n>)",
           "schema": "gateway/target_schema.0.json: metadata/table_schemas/<table id>/, each column id's name and type",
           "not recorded yet": "a snapshot's parquet (read through intake.gateway, checked live on a 500-table run)"}

CHANGEFEED = {"sample": "object_store/changefeed_orders.ndjson", "position": "hlc",
              "event": "after (the row; null for a delete), key (an array, key order), updated (<wall ns>.<logical>)",
              "resolved": "<time>.RESOLVED files: {resolved: <hlc>}, every change up to it arrived",
              "formats": "object_store/changefeed_formats.json"}

KAFKA_CONNECT = {"sample": None, "note": "YugabyteDB through Kafka Connect's Zerobus endpoint: another row layout "
                                         "(the Kafka record), read by zerobus-debezium-test's silver only; not recorded"}
