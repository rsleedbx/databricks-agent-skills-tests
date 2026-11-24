"""land: a Lakeflow Connect gateway's change files into one append-only Delta change log, the shape a Debezium change
log through Zerobus has (cdc_log), so every gateway and every Debezium source is read the same way. The gateway only
writes files; landing copies each new file once (COPY INTO records the files it loaded), every gateway into the same
log, each event as it is (VARIANT):

  destination   <catalog>.<schema>.<table>, from the event's table_id
  operation     create, update or delete (the gateway's INSERT, UPDATE, DELETE, in Debezium's words)
  key           NULL (a gateway event's data holds the key)
  value         the whole event: table_id, cursor.sequence, operation, data (columns by id)
  gateway, file where it came from

    from lfc_change_source import land
    land.ensure_log(spark, "`main`.`ctl`.`cdc_log`")
    land.gateway(spark, "/Volumes/.../<gateway id>", "`main`.`ctl`.`cdc_log`", "sales_gw_1")   # rows added
"""
from __future__ import annotations


def ensure_log(session, log: str) -> None:
    """The change log, clustered by destination so one table's read skips the others' files."""
    session.sql(f"CREATE TABLE IF NOT EXISTS {log} (destination STRING, operation STRING, key VARIANT, value VARIANT, "
                "gateway STRING, file STRING) CLUSTER BY (destination) COMMENT 'lfc_change_source: every source''s "
                "change events, append only (a gateway''s change files landed, or Debezium through Zerobus)'")


def gateway(session, root: str, log: str, name: str) -> int | None:
    """The gateway's change files not landed before into the log; the rows added."""
    lit = "'" + name.replace("'", "''") + "'"
    rows = session.sql(
        f"COPY INTO {log} FROM (SELECT concat_ws('.', event:table_id.catalog::STRING, event:table_id.schema::STRING, "
        "event:table_id.name::STRING) AS destination, CASE event:operation::STRING WHEN 'INSERT' THEN 'create' "
        "WHEN 'UPDATE' THEN 'update' WHEN 'DELETE' THEN 'delete' END AS operation, CAST(NULL AS VARIANT) AS key, "
        f"event AS value, {lit} AS gateway, _metadata.file_path AS file FROM '{root}/cdc/') "
        "FILEFORMAT = JSON PATTERN = '*/*' FORMAT_OPTIONS ('singleVariantColumn' = 'event')").collect()
    try:
        return int(rows[0].asDict().get("num_inserted_rows")) if rows else 0
    except Exception:  # noqa: BLE001  a session that doesn't return the counts
        return None
