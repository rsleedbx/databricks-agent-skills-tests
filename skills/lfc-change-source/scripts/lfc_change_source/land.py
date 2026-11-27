"""land: a Lakeflow Connect gateway's change files into one append-only Delta change log, the shape a Debezium change
log through Zerobus has (cdc_log), so every gateway and every Debezium source is read the same way. The gateway only
writes files; landing copies each new file once (COPY INTO records the files it loaded), every gateway into the same
log, each event as it is (VARIANT):

  destination   <catalog>.<schema>.<table>, from the event's table_id
  operation     create, update or delete (the gateway's INSERT, UPDATE, DELETE, in Debezium's words; REPLACE, a SQL Server
                change-tracking gateway's insert or upsert, is an update)
  key           the key's values as a JSON array of text, in key order (gateway_keys: each table's key columns by
                id, from its schema files), or NULL when landed without them; a gateway event's data holds the
                columns
  value         the whole event: table_id, cursor.sequence, operation, data (columns by id)
  gateway, file where it came from

    from lfc_change_source import land
    land.ensure_log(spark, "`<catalog>`.`ctl`.`cdc_log`")
    land.gateway(spark, "/Volumes/.../<gateway id>", "`<catalog>`.`ctl`.`cdc_log`", "sales_gw_1")   # rows added

The log carries changes only: a gateway's snapshot goes to a Delta table of its own (intake.gateway), never into
the log (docs/design-no-full-refresh.md).

A CockroachDB changefeed (format='json', updated, resolved) into object storage or a Volume, one folder a table
(<date>/<files>.ndjson and <time>.RESOLVED), lands the same way (land.changefeed): the key as an object by the key
columns' names (the event's key is an array in key order), operation delete when the event has no after image, else
update; each RESOLVED file is one row, operation resolved, its position in value:resolved. Use format='json', not
parquet: CockroachDB's parquet writer fills a DECIMAL column with the number's text, so readers get wrong numbers.
Use initial_scan = 'no' on that changefeed: the table's snapshot comes from a snapshot method into a table (qbc, or an
initial_scan = 'only' export through intake.files); an initial scan here would land every row as an update.
"""
from __future__ import annotations


def ensure_log(session, log: str) -> None:
    """The change log, clustered by destination so one table's read skips the others' files."""
    session.sql(f"CREATE TABLE IF NOT EXISTS {log} (destination STRING, operation STRING, key VARIANT, value VARIANT, "
                "gateway STRING, file STRING) CLUSTER BY (destination) COMMENT 'lfc_change_source: every source''s "
                "change events, append only (a gateway''s change files landed, or Debezium through Zerobus)'")


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# a gateway event (VARIANT column `event`) as a change log row: the same for COPY INTO (gateway) and Auto Loader
# (autoloader.gateway_stream)
GATEWAY_DESTINATION = ("concat_ws('.', event:table_id.catalog::STRING, event:table_id.schema::STRING, "
                       "event:table_id.name::STRING)")
GATEWAY_OPERATION = ("CASE event:operation::STRING WHEN 'INSERT' THEN 'create' WHEN 'UPDATE' THEN 'update' "
                     "WHEN 'REPLACE' THEN 'update' WHEN 'DELETE' THEN 'delete' END")


def gateway_keys(entries: dict) -> dict:
    """{table id: [its key columns' ids, in key order]} from gateway_staging.schemas()'s entries (keyless tables
    left out)."""
    out = {}
    for e in entries.values():
        ids = {c: cid for c, _, cid in e["columns"]}
        if e["keys"]:
            out[e["id"]] = [ids[k] for k in e["keys"]]
    return out


def gateway_key_sql(keys: dict | None, event: str = "event") -> str:
    """A gateway event's key as a VARIANT (a JSON array of each key column's value as text), from `keys`
    (gateway_keys) by the event's table id: one expression for every table (a map literal, no branch a table), the
    same text for a snapshot row and a change of the same key. NULL with no keys, or for a table not in them. Column
    ids are letters, digits and underscores, so a plain $.<id> path needs no quoting."""
    if not keys:
        return "CAST(NULL AS VARIANT)"
    m = "map(" + ", ".join(f"{_lit(t)}, array({', '.join(_lit(c) for c in cols)})" for t, cols in keys.items()) + ")"
    return (f"parse_json(to_json(transform({m}[{event}:table_id.uoid::STRING], c -> "
            f"variant_get({event}:data, concat('$.', c), 'string'))))")


def backfill_keys(session, log: str, name: str, keys: dict) -> int | None:
    """Fill the key of this gateway's rows landed without one (before gateway_keys): one UPDATE; rows changed. A
    streaming reader of the log fails on it (DELTA_SOURCE_TABLE_IGNORE_CHANGES) unless it reads with
    skipChangeCommits (lfc-log-silver's AUTO CDC pipeline does): run it before such readers start, or with that."""
    rows = session.sql(f"UPDATE {log} SET key = {gateway_key_sql(keys, 'value')} WHERE gateway = {_lit(name)} AND "
                       "key IS NULL AND operation <> 'resolved'").collect()
    return _inserted(rows, "num_affected_rows")


def gateway(session, root: str, log: str, name: str, only: list | None = None, skip: list | None = None,
            keys: dict | None = None) -> int | None:
    """The gateway's change files not landed before into the log; the rows added. only / skip: the tables
    (<catalog>.<schema>.<table>) whose events go to this log, or don't: one gateway's events split between logs (a
    big_fast table's own log, lfc-routing), each log landing whole files with its own filter. keys (gateway_keys):
    each row's key filled in (silver's shared groups need it)."""
    where = ""
    if only:
        where = f" WHERE lower({GATEWAY_DESTINATION}) IN ({', '.join(_lit(t.lower()) for t in only)})"   # any spelling
    elif skip:
        where = f" WHERE lower({GATEWAY_DESTINATION}) NOT IN ({', '.join(_lit(t.lower()) for t in skip)})"
    return _copy(session, log, GATEWAY_DESTINATION, GATEWAY_OPERATION, gateway_key_sql(keys), name, f"{root}/cdc/",
                 "*/*", where)


def gateway_keyed(session, root: str, log: str, name: str, only: list | None = None, skip: list | None = None) -> int | None:
    """gateway() with each table's keys from the gateway's own schema files, into a log made if missing: what a
    producer lands each round. The rows added."""
    from . import gateway_staging
    ensure_log(session, log)
    return gateway(session, root, log, name, only, skip, keys=gateway_keys(gateway_staging.schemas(session, root)))


def changefeed(session, root: str, log: str, destination: str, keys: list[str], name: str) -> int | None:
    """A CockroachDB changefeed's files (format='json') for one table, not landed before, into the log; the rows
    added. destination: <database>.<schema>.<table>; keys: the primary key's columns in order."""
    key = ", ".join(f"{_lit(k)}, event:key[{i}]" for i, k in enumerate(keys))
    return _copy(session, log, _lit(destination), "CASE WHEN event:resolved IS NOT NULL THEN 'resolved' "
                 "WHEN event:after IS NULL OR is_variant_null(event:after) THEN 'delete' ELSE 'update' END",
                 f"CASE WHEN event:key IS NOT NULL THEN to_variant_object(named_struct({key})) END", name,
                 f"{root}/", "*/*{.ndjson,.RESOLVED}")




def trim(session, log: str, destinations: list, kind: str, upto: str) -> int | None:
    """The log's change events of `destinations` at or before position `upto` (a position kind of cdc_log
    SEQUENCES) deleted: safe once each of those tables has a checkpoint at `upto` or later (lfc_log_silver
    silver.checkpoint) and every reader of them keeps a position at or after it. One statement for every table
    checkpointed at that position; resolved marks stay. The rows deleted."""
    from .cdc_log import SEQUENCES, TEXT
    if not destinations:
        return 0
    lit = _lit(upto) if kind in TEXT else str(int(upto))
    rows = session.sql(f"DELETE FROM {log} WHERE destination IN ({', '.join(_lit(d) for d in destinations)}) "
                       f"AND operation <> 'resolved' AND {SEQUENCES[kind]} <= {lit}").collect()
    return _inserted(rows, "num_affected_rows") if rows else None

def _inserted(rows, field: str = "num_inserted_rows") -> int | None:
    try:
        return int(rows[0].asDict().get(field)) if rows else 0
    except Exception:  # noqa: BLE001  a session that doesn't return the counts
        return None


def _copy(session, log: str, destination: str, operation: str, key: str, name: str, src: str, pattern: str,
          where: str = ""):
    """One COPY INTO of a transport's JSON files into the log, each event as it is (VARIANT): the rows added."""
    cols = (f"{destination} AS destination, {operation} AS operation, {key} AS key, event AS value, "
            f"{_lit(name)} AS gateway, _metadata.file_path AS file")
    if where:   # COPY INTO takes no WHERE: a filtered landing reads the files itself, skipping the ones it landed
        rows = session.sql(
            f"INSERT INTO {log} SELECT {cols} FROM read_files('{src}{pattern}', format => 'json', "
            f"singleVariantColumn => 'event'){where} AND _metadata.file_path NOT IN "
            f"(SELECT DISTINCT file FROM {log} WHERE gateway = {_lit(name)} AND file IS NOT NULL)").collect()
        return _inserted(rows)
    rows = session.sql(
        f"COPY INTO {log} FROM (SELECT {cols} FROM '{src}') "
        f"FILEFORMAT = JSON PATTERN = '{pattern}' FORMAT_OPTIONS ('singleVariantColumn' = 'event')").collect()
    return _inserted(rows)
