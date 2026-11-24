"""land: a Lakeflow Connect gateway's change files into one append-only Delta change log, the shape a Debezium change
log through Zerobus has (cdc_log), so every gateway and every Debezium source is read the same way. The gateway only
writes files; landing copies each new file once (COPY INTO records the files it loaded), every gateway into the same
log, each event as it is (VARIANT):

  destination   <catalog>.<schema>.<table>, from the event's table_id
  operation     create, update or delete (the gateway's INSERT, UPDATE, DELETE, in Debezium's words; REPLACE, a SQL Server
                change-tracking gateway's insert or upsert, is an update)
  key           NULL (a gateway event's data holds the key)
  value         the whole event: table_id, cursor.sequence, operation, data (columns by id)
  gateway, file where it came from

    from lfc_change_source import land
    land.ensure_log(spark, "`main`.`ctl`.`cdc_log`")
    land.gateway(spark, "/Volumes/.../<gateway id>", "`main`.`ctl`.`cdc_log`", "sales_gw_1")   # rows added

A CockroachDB changefeed (format='json', updated, resolved) into object storage or a Volume, one folder a table
(<date>/<files>.ndjson and <time>.RESOLVED), lands the same way (land.changefeed): the key as an object by the key
columns' names (the event's key is an array in key order), operation delete when the event has no after image, else
update; each RESOLVED file is one row, operation resolved, its position in value:resolved. Use format='json', not
parquet: CockroachDB's parquet writer fills a DECIMAL column with the number's text, so readers get wrong numbers.
"""
from __future__ import annotations


def ensure_log(session, log: str) -> None:
    """The change log, clustered by destination so one table's read skips the others' files."""
    session.sql(f"CREATE TABLE IF NOT EXISTS {log} (destination STRING, operation STRING, key VARIANT, value VARIANT, "
                "gateway STRING, file STRING) CLUSTER BY (destination) COMMENT 'lfc_change_source: every source''s "
                "change events, append only (a gateway''s change files landed, or Debezium through Zerobus)'")


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def gateway(session, root: str, log: str, name: str) -> int | None:
    """The gateway's change files not landed before into the log; the rows added."""
    return _copy(session, log, "concat_ws('.', event:table_id.catalog::STRING, event:table_id.schema::STRING, "
                 "event:table_id.name::STRING)", "CASE event:operation::STRING WHEN 'INSERT' THEN 'create' "
                 "WHEN 'UPDATE' THEN 'update' WHEN 'REPLACE' THEN 'update' WHEN 'DELETE' THEN 'delete' END", "CAST(NULL AS VARIANT)", name,
                 f"{root}/cdc/", "*/*")


def changefeed(session, root: str, log: str, destination: str, keys: list[str], name: str) -> int | None:
    """A CockroachDB changefeed's files (format='json') for one table, not landed before, into the log; the rows
    added. destination: <database>.<schema>.<table>; keys: the primary key's columns in order."""
    key = ", ".join(f"{_lit(k)}, event:key[{i}]" for i, k in enumerate(keys))
    return _copy(session, log, _lit(destination), "CASE WHEN event:resolved IS NOT NULL THEN 'resolved' "
                 "WHEN event:after IS NULL OR is_variant_null(event:after) THEN 'delete' ELSE 'update' END",
                 f"CASE WHEN event:key IS NOT NULL THEN to_variant_object(named_struct({key})) END", name,
                 f"{root}/", "*/*{.ndjson,.RESOLVED}")


def _copy(session, log: str, destination: str, operation: str, key: str, name: str, src: str, pattern: str):
    """One COPY INTO of a transport's JSON files into the log, each event as it is (VARIANT): the rows added."""
    rows = session.sql(
        f"COPY INTO {log} FROM (SELECT {destination} AS destination, {operation} AS operation, {key} AS key, "
        f"event AS value, {_lit(name)} AS gateway, _metadata.file_path AS file FROM '{src}') "
        f"FILEFORMAT = JSON PATTERN = '{pattern}' FORMAT_OPTIONS ('singleVariantColumn' = 'event')").collect()
    try:
        return int(rows[0].asDict().get("num_inserted_rows")) if rows else 0
    except Exception:  # noqa: BLE001  a session that doesn't return the counts
        return None
