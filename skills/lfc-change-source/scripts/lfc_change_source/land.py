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


def gateway(session, root: str, log: str, name: str, only: list | None = None, keys: dict | None = None) -> int | None:
    """The gateway's change files not landed before into the log; the rows added. only: the tables
    (<catalog>.<schema>.<table>, any spelling) whose events go to this log: one gateway's events split between logs
    (a table's own log, lfc-routing), each log listing its own tables, read once a file and table (a ledger,
    <log>__scanned). A log takes either every table (no only) or a list: no "every table but" form, which would have
    to read every change file for the tables it holds (lfc-routing's ingest lists each log's tables). keys
    (gateway_keys): each row's key filled in (silver's shared groups need it)."""
    if only:   # COPY INTO takes no WHERE: a filtered landing reads only what it hasn't read for its tables
        return _filtered(session, log, gateway_key_sql(keys), name, f"{root}/cdc/", only)
    return _copy(session, log, GATEWAY_DESTINATION, GATEWAY_OPERATION, gateway_key_sql(keys), name, f"{root}/cdc/",
                 "*/*")


def gateway_keyed(session, root: str, log: str, name: str, only: list | None = None) -> int | None:
    """gateway() with each table's keys from the gateway's own schema files, into a log made if missing: what a
    producer lands each round. A staging laid out by name (no table ids to key by) lands without keys. The rows added."""
    from . import gateway_staging
    ensure_log(session, log)
    try:
        keys = gateway_keys(gateway_staging.schemas(session, root))
    except ValueError:   # laid out by name: no keys (only shared groups need them)
        keys = None
    return gateway(session, root, log, name, only, keys=keys)


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


def _copy(session, log: str, destination: str, operation: str, key: str, name: str, src: str, pattern: str):
    """One COPY INTO of a transport's JSON files into the log, each event as it is (VARIANT): the rows added."""
    cols = (f"{destination} AS destination, {operation} AS operation, {key} AS key, event AS value, "
            f"{_lit(name)} AS gateway, _metadata.file_path AS file")
    rows = session.sql(
        f"COPY INTO {log} FROM (SELECT {cols} FROM '{src}') "
        f"FILEFORMAT = JSON PATTERN = '{pattern}' FORMAT_OPTIONS ('singleVariantColumn' = 'event')").collect()
    return _inserted(rows)


SETTLE_S = 10   # a change file modified this recently may still be written: the next round takes it
BATCH = 200     # files a statement reads


def _bare(path: str) -> str:
    return path.removeprefix("dbfs:")


def ledger(log: str) -> str:
    """The table of what a filtered landing into `log` has read: <log>__scanned, a row a change file and table (its
    rows landed, or none in that file)."""
    head, _, last = log.rpartition(".")
    return f"{head}.`{last.strip('`')}__scanned`"


def _files(session, src: str, now_ms: float) -> list:
    """Every change file under src (<root>/cdc/: a folder a producer run, its files numbered), by LIST: metadata
    only, no file read; the ones modified in the last SETTLE_S seconds left for the next round."""
    out = []
    for d in session.sql(f"LIST '{src}'").collect():
        if not str(d[0]).endswith("/"):
            continue
        out += [_bare(str(f[0])) for f in session.sql(f"LIST '{d[0]}'").collect()
                if not str(f[0]).endswith("/") and int(f[3] or 0) <= now_ms - SETTLE_S * 1000]
    return sorted(out)


def _filtered(session, log: str, key: str, name: str, src: str, only: list) -> int:
    """A filtered landing, read once a file and table: the change files listed (_files); each read only for the
    tables of `only` it hasn't been read for (the ledger's (file, table) rows, or the log's own rows of that table
    from it: a round stopped before recording stays safe), so a table added to `only` later gets every file read for
    it once; files needing the same tables read together in batches of BATCH, then recorded (gateway() turns a skip
    into this list). The rows added."""
    import time
    led = ledger(log)
    session.sql(f"CREATE TABLE IF NOT EXISTS {led} (gateway STRING, file STRING, tbl STRING, at TIMESTAMP) COMMENT "
                "'lfc_change_source land: each change file and table a filtered landing into this log has read'").collect()
    if "tbl" not in {str(r[0]) for r in session.sql(f"DESCRIBE TABLE {led}").collect()}:   # a ledger of a file a row
        session.sql(f"ALTER TABLE {led} ADD COLUMNS (tbl STRING)").collect()   # its rows no table's: read again once
    dest = f"lower({GATEWAY_DESTINATION})"
    want = sorted({t.lower() for t in only})
    wl = ", ".join(_lit(t) for t in want)
    done = {(_bare(str(r[0])), str(r[1])) for r in session.sql(
        f"SELECT file, tbl FROM {led} WHERE gateway = {_lit(name)} AND tbl IN ({wl}) "
        f"UNION SELECT DISTINCT file, lower(destination) FROM {log} WHERE gateway = {_lit(name)} AND file IS NOT "
        f"NULL AND lower(destination) IN ({wl})").collect()}
    groups: dict = {}   # the tables a file still needs -> the files
    for f in _files(session, src, time.time() * 1000):
        need = tuple(t for t in want if (f, t) not in done)
        if need:
            groups.setdefault(need, []).append(f)
    cols = (f"{GATEWAY_DESTINATION} AS destination, {GATEWAY_OPERATION} AS operation, {key} AS key, event AS value, "
            f"{_lit(name)} AS gateway, _metadata.file_path AS file")
    added = 0
    for need, files in sorted(groups.items()):
        where = f" WHERE {dest} IN ({', '.join(_lit(t) for t in need)})"
        for i in range(0, len(files), BATCH):
            part = files[i:i + BATCH]
            reads = " UNION ALL ".join(f"SELECT {cols} FROM read_files('{f}', format => 'json', singleVariantColumn => "
                                       f"'event'){where}" for f in part)
            added += _inserted(session.sql(f"INSERT INTO {log} {reads}").collect()) or 0
            session.sql(f"INSERT INTO {led} (gateway, file, tbl, at) VALUES " + ", ".join(
                f"({_lit(name)}, {_lit(f)}, {_lit(t)}, current_timestamp())" for f in part for t in need)).collect()
    return added
