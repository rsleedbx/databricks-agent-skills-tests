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

A gateway's snapshots take one of two paths, per table (snapshot_path): `log`, landed by land.gateway_snapshots, one
COPY INTO a table (their columns differ), each row as an event of operation read, so a table is rebuilt from the log
alone (many small tables); or `direct`, left where the gateway wrote it and read in place, with only the changes from
the log (a large table: no second copy). By the snapshot's bytes (MAX_LOG_SNAPSHOT_BYTES) unless chosen.

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


# a gateway event (VARIANT column `event`) as a change log row: the same for COPY INTO (gateway) and Auto Loader
# (autoloader.gateway_stream)
GATEWAY_DESTINATION = ("concat_ws('.', event:table_id.catalog::STRING, event:table_id.schema::STRING, "
                       "event:table_id.name::STRING)")
GATEWAY_OPERATION = ("CASE event:operation::STRING WHEN 'INSERT' THEN 'create' WHEN 'UPDATE' THEN 'update' "
                     "WHEN 'REPLACE' THEN 'update' WHEN 'DELETE' THEN 'delete' END")


def gateway(session, root: str, log: str, name: str, only: list | None = None, skip: list | None = None) -> int | None:
    """The gateway's change files not landed before into the log; the rows added. only / skip: the tables
    (<catalog>.<schema>.<table>) whose events go to this log, or don't: one gateway's events split between logs (a
    big_fast table's own log, routing.py), each log landing whole files with its own filter."""
    where = ""
    if only:
        where = f" WHERE {GATEWAY_DESTINATION} IN ({', '.join(_lit(t) for t in only)})"
    elif skip:
        where = f" WHERE {GATEWAY_DESTINATION} NOT IN ({', '.join(_lit(t) for t in skip)})"
    return _copy(session, log, GATEWAY_DESTINATION, GATEWAY_OPERATION, "CAST(NULL AS VARIANT)", name, f"{root}/cdc/",
                 "*/*", where)


def changefeed(session, root: str, log: str, destination: str, keys: list[str], name: str) -> int | None:
    """A CockroachDB changefeed's files (format='json') for one table, not landed before, into the log; the rows
    added. destination: <database>.<schema>.<table>; keys: the primary key's columns in order."""
    key = ", ".join(f"{_lit(k)}, event:key[{i}]" for i, k in enumerate(keys))
    return _copy(session, log, _lit(destination), "CASE WHEN event:resolved IS NOT NULL THEN 'resolved' "
                 "WHEN event:after IS NULL OR is_variant_null(event:after) THEN 'delete' ELSE 'update' END",
                 f"CASE WHEN event:key IS NOT NULL THEN to_variant_object(named_struct({key})) END", name,
                 f"{root}/", "*/*{.ndjson,.RESOLVED}")


MAX_LOG_SNAPSHOT_BYTES = 1 << 30   # a bigger snapshot stays where the gateway wrote it (path direct)


def snapshot_path(nbytes: int, max_bytes: int = MAX_LOG_SNAPSHOT_BYTES, chosen: str | None = None) -> str:
    """How a table's snapshot reaches its reader: `log`, landed into the change log as read events (many small
    tables: one query rebuilds any table, and the cost is per object), or `direct`, read where the gateway wrote it
    (a large table: no second copy as VARIANT rows; the table is loaded once from its parquet and only its changes
    come from the log). By bytes unless chosen."""
    if chosen in ("log", "direct"):
        return chosen
    if chosen:
        raise ValueError(f"snapshot path {chosen!r}: log or direct")
    return "log" if nbytes <= max_bytes else "direct"


def gateway_snapshot(session, root: str, log: str, name: str, destination: str, entry: dict,
                     max_bytes: int = MAX_LOG_SNAPSHOT_BYTES, path: str | None = None, landed: set | None = None) -> dict:
    """One table's newest snapshot (gateway_staging's snapshot/<table id>/<time>/*.parquet): {path, bytes, snapshot,
    rows}. Path log: its rows into the log, each as a change event of the gateway's shape (operation read, value
    {table_id, cursor.sequence (the row's __sequence), operation READ, snapshot (the folder's time), data (columns by
    id)}), so a reader rebuilds the table from the log alone; rows added, 0 when this snapshot is in the log already
    (landed: the (destination, snapshot time) pairs there, landed_snapshots). An INSERT of read_files, not COPY INTO:
    many COPY INTOs into one log at once failed on COPY INTO's own state (COPY_INTO_ROCKSDB_MAX_RETRY_EXCEEDED at 24),
    and Delta takes concurrent appends. Path direct: nothing copied (rows None); read it in place
    (gateway_log.current_sql). Path None: no snapshot yet."""
    tid = entry["id"]
    times = [int(e.name.strip("/")) for e in session.w.files.list_directory_contents(f"{root}/snapshot/{tid}")
             if e.is_directory and e.name.strip("/").isdigit()]
    if not times:
        return {"path": None, "bytes": 0, "snapshot": None, "rows": None}
    newest = max(times)
    nbytes = sum(e.file_size or 0 for e in session.w.files.list_directory_contents(f"{root}/snapshot/{tid}/{newest}")
                 if (e.name or e.path).endswith(".parquet"))
    out = {"path": snapshot_path(nbytes, max_bytes, path), "bytes": nbytes, "snapshot": newest, "rows": None}
    if out["path"] == "direct":
        return out
    cat, sch, tbl = destination.split(".", 2)
    value = (f"to_variant_object(named_struct('table_id', named_struct('uoid', {_lit(tid)}, 'catalog', {_lit(cat)}, "
             f"'schema', {_lit(sch)}, 'name', {_lit(tbl)}), 'cursor', named_struct('sequence', __sequence), "
             f"'operation', 'READ', 'snapshot', {newest}, 'data', struct(* EXCEPT (__sequence, __operation, _rescued_data))))")
    if landed is not None and (destination, newest) in landed:
        out["rows"] = 0
        return out
    rows = session.sql(
        f"INSERT INTO {log} (destination, operation, key, value, gateway, file) SELECT {_lit(destination)}, 'read', "
        f"CAST(NULL AS VARIANT), {value}, {_lit(name)}, _metadata.file_path FROM read_files("
        f"'{root}/snapshot/{tid}/{newest}/', format => 'parquet', pathGlobFilter => '*.parquet')").collect()
    out["rows"] = _inserted(rows)
    return out


def landed_snapshots(session, log: str, name: str) -> set:
    """{(destination, snapshot time)} of the snapshots a gateway has in the log already (one query)."""
    rows = session.sql(f"SELECT DISTINCT destination, value:snapshot::BIGINT FROM {log} WHERE operation = 'read' "
                       f"AND gateway = {_lit(name)}").collect()
    return {(r[0], int(r[1])) for r in rows if r[1] is not None}


def gateway_snapshots(session, root: str, log: str, name: str, tables: dict, threads: int = 8,
                      max_bytes: int = MAX_LOG_SNAPSHOT_BYTES, paths: dict | None = None) -> dict:
    """Every table's newest snapshot (gateway_snapshot), `threads` at once: {destination: {path, bytes, snapshot,
    rows}}. tables: gateway_staging.schemas()'s {(catalog, schema, table): entry}; paths: {destination: log|direct}
    to choose a table's path instead of by bytes."""
    from concurrent.futures import ThreadPoolExecutor
    paths = paths or {}
    landed = landed_snapshots(session, log, name)

    def one(kv):
        dest = ".".join(kv[0])
        return dest, gateway_snapshot(session, root, log, name, dest, kv[1], max_bytes, paths.get(dest), landed)
    with ThreadPoolExecutor(threads) as pool:
        return dict(pool.map(one, tables.items()))


def _inserted(rows) -> int | None:
    try:
        return int(rows[0].asDict().get("num_inserted_rows")) if rows else 0
    except Exception:  # noqa: BLE001  a session that doesn't return the counts
        return None


def _copy(session, log: str, destination: str, operation: str, key: str, name: str, src: str, pattern: str,
          where: str = ""):
    """One COPY INTO of a transport's JSON files into the log, each event as it is (VARIANT): the rows added."""
    rows = session.sql(
        f"COPY INTO {log} FROM (SELECT {destination} AS destination, {operation} AS operation, {key} AS key, "
        f"event AS value, {_lit(name)} AS gateway, _metadata.file_path AS file FROM '{src}'{where}) "
        f"FILEFORMAT = JSON PATTERN = '{pattern}' FORMAT_OPTIONS ('singleVariantColumn' = 'event')").collect()
    return _inserted(rows)
