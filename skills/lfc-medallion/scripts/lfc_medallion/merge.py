"""merge: the `merge` route. A job keeps the n:1 table of each quiet table current from the sources' change feeds, so
the table needn't be a view in the pipeline's graph (each one there lengthens every update's start; with thousands of
tables a MERGE job doesn't: zerobus-debezium-test's many-tables measurements).

For each table and each source that has it, merge_all reads the source's change feed since the version it last read
(table_changes), keeps each key's last change, and MERGEs it into <target>.all_<table> keyed by the source column and
the primary key: inserts, updates (post-images) and deletes. A source read for the first time, or whose table was
replaced, truncated or overwritten since (a full refresh: its change feed starts over), is copied whole instead. The
versions read are kept in <target>._lfc_medallion_merge_state. Bronze columns the target lacks are added first, so a
source's new column arrives, and a column a source holds wider is widened (_ensure_target). The targets have the
change data feed and row tracking on, so the silver views that read them still refresh incrementally.

Lakeflow Connect's destination tables have the change data feed on (delta.enableChangeDataFeed=true: seen on its
PostgreSQL connector's SCD type 1 tables, 2026-11-15). An SCD type 2 destination keeps a row per version of a key, so
__START_AT joins the key. Run it as a job task before the pipeline that reads the targets.

An SCD type 2 target keeps its history across a full refresh of bronze (which reloads one row per live key):
- the versions made since the last read and before the refresh are read first: the change feed up to the version
  before the refresh still reads after it, while a range across it doesn't, nor the reload's own versions (SQL Server
  CDC, standard and integrated, 2026-11-22: lfc-downstream-history scripts/sqlserver-scd2-refresh-probe.sh);
- then the whole copy closes the target's open versions the reload doesn't have (__END_AT: the refresh's time, and
  "lfc_medallion: closed by full refresh at version <n>"), never deleting one;
- a snapshot row (the first load, a reload) has an empty start (__START_AT.__cdc_timestamp_value null), so a reload's
  row would match the first load's row of its key: its start is filled with its snapshot's time (the latest reset at
  or before the version read, else the table's first version), in every read, so the target's rows keep their keys.
A copy for another reason (the first read, a recreated table, versions log retention dropped) of a source the target
already has also closes rather than deletes. A source whose newest refresh has no write after it yet (reloading) is
left for the next run.

    from lfc_medallion import merge_all, table_plan
    plan = table_plan(spark, "main", "sales_bronze_east")
    quiet = [n for n, t in plan["tables"].items() if t["route"] == "merge"]
    merge_all(spark, "main", "sales_bronze_", target="sales_silver", tables=quiet, source_column="division")
"""
import json
import datetime as dt
import time

from . import coltypes, discover, evidence, paths
from .stack import spark_string

STATE = "_lfc_medallion_merge_state"
CHANGE_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")
TARGET_PROPERTIES = "'delta.enableChangeDataFeed' = 'true', 'delta.enableRowTracking' = 'true'"


def _q(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _fq(catalog, schema, table) -> str:
    return f"{_q(catalog)}.{_q(schema)}.{_q(table)}"


def _rows(spark, query):
    return spark.sql(query).collect()


def columns(spark, fq: str) -> list[tuple[str, str]]:
    """(name, type) of a table's columns, up to the first blank or "# ..." section of DESCRIBE TABLE."""
    out = []
    for r in _rows(spark, f"DESCRIBE TABLE {fq}"):
        if not r.col_name or r.col_name.startswith("#"):
            break
        out.append((r.col_name, r.data_type))
    return out


def latest_version(spark, fq: str) -> int:
    return int(_rows(spark, f"SELECT MAX(version) AS v FROM (DESCRIBE HISTORY {fq})")[0].v)


def table_id(spark, fq: str) -> str:
    """The table's id (DESCRIBE DETAIL): a dropped and recreated table has a new one, and its history starts over."""
    return str(_rows(spark, f"DESCRIBE DETAIL {fq}")[0].id)


DATA_OPS = ("MERGE", "WRITE", "STREAMING UPDATE", "DELETE", "UPDATE")   # their metrics say how many rows
NO_DATA_OPS = ("OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ADD COLUMNS",
               "CHANGE COLUMN", "RENAME COLUMN", "UPGRADE PROTOCOL", "DLT SETUP", "ADD CONSTRAINT", "DROP CONSTRAINT",
               "CLUSTER BY", "SET TABLE COMMENT")
ROW_METRICS = ("numTargetRowsInserted", "numTargetRowsMatchedUpdated", "numTargetRowsUpdated", "numTargetRowsDeleted",
               "numTargetRowsMatchedDeleted", "numOutputRows", "numAddedRows", "numDeletedRows", "numUpdatedRows")


def history_since(spark, fq: str, version: int) -> tuple[int, bool, bool]:
    """(the latest version, refreshed, changed) from ONE read of the table's history after `version`: the version
    read and since() together, not a DESCRIBE HISTORY each."""
    rows = _rows(spark, f"SELECT version, operation, operationParameters, operationMetrics "
                        f"FROM (DESCRIBE HISTORY {fq}) WHERE version > {int(version)}")
    if not rows:
        return int(version), False, False
    return max(int(r.version) for r in rows), *_judge(rows, version)


def since(spark, fq: str, version: int) -> tuple[bool, bool]:
    """From the table's history after `version` (metadata only, no data read): whether it was replaced, truncated,
    overwritten or restored (its change feed starts over), and whether it may have changed. An ingestion update adds
    versions to every table, changed or not (an empty MERGE, housekeeping DELETEs: seen on Lakeflow Connect's
    destination tables 2026-11-15), so a new version alone isn't a change; but only known operations reporting zero
    rows, or known to touch no data, count as no change: anything else (an unknown operation, missing metrics) does."""
    return history_since(spark, fq, version)[1:]


def _judge(rows, version: int) -> tuple[bool, bool]:
    """since()'s verdict on history rows after `version`: (refreshed, changed)."""
    refreshed, changed = False, False
    # log retention may have dropped versions since: then the history can't say what changed, so copy whole
    if not rows or min(int(r.version) for r in rows) != int(version) + 1:
        return True, True
    for r in rows:
        if evidence.is_reset(r.operation, r.operationParameters):
            refreshed = True
            continue
        if r.operation in NO_DATA_OPS:
            continue
        m = getattr(r, "operationMetrics", None) or {}
        m = json.loads(m) if isinstance(m, str) else m
        if r.operation not in DATA_OPS or not any(k in m for k in ROW_METRICS):
            changed = True   # can't tell: read the change feed
        elif sum(evidence._write_counts(r.operation, m)) or \
                any(int(v or 0) for k, v in m.items() if k.startswith("numTargetRows") and k != "numTargetRowsCopied"):
            changed = True   # also MERGE's WHEN NOT MATCHED BY SOURCE updates and deletes
    return refreshed, changed


START, END = "__START_AT", "__END_AT"
# Lakeflow Connect's SCD type 2 start and end: struct<__cdc_internal_value:string,__cdc_timestamp_value:timestamp>
# (SQL Server CDC, 2026-11-22; a string time is taken too); a snapshot row's start has an empty internal value and no time
CDC_STRUCT = "struct<__cdc_internal_value:string,__cdc_timestamp_value:"
RELOAD_OPS = ("MERGE", "WRITE", "STREAMING UPDATE")   # a reload's writes


def _kind(spark_type: str | None) -> str | None:
    """How an SCD type 2 start or end column is filled: 'cdc' (Lakeflow Connect's struct, its time a timestamp),
    'cdc_string' (the same with the time as a string), 'timestamp', or None (another shape: left as it is)."""
    t = (spark_type or "").lower().replace(" ", "")
    if t.startswith(CDC_STRUCT):
        rest = t[len(CDC_STRUCT):]
        return "cdc" if rest.startswith("timestamp") and rest.count(",") == 0 else "cdc_string" if rest == "string>" else None
    return "timestamp" if t.startswith("timestamp") else None


def _time_sql(kind: str, at: str) -> str:
    """A time literal for a column of `kind`: a TIMESTAMP, or the string Lakeflow Connect would write."""
    return spark_string(at) if kind == "cdc_string" else f"TIMESTAMP {spark_string(at)}"


def _iso(ts) -> str:
    """A history timestamp as Lakeflow Connect writes its CDC times: 2026-11-22T17:04:00.437Z."""
    if isinstance(ts, str):
        return ts
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def generations(rows) -> list[tuple[int, str]]:
    """[(version, time)] at which a snapshot of the table begins: its first version, then each reset (evidence.is_reset:
    a DLT REFRESH, a replace, ...), in order. rows: DESCRIBE HISTORY rows (version, timestamp, operation,
    operationParameters)."""
    rows = sorted(rows, key=lambda r: int(r.version))
    if not rows:
        return []
    out = [(int(rows[0].version), _iso(rows[0].timestamp))]
    out += [(int(r.version), _iso(r.timestamp)) for r in rows[1:]
            if evidence.is_reset(r.operation, getattr(r, "operationParameters", None))]
    return out


def keep_generations(stored, seen: list[tuple[int, str]], resets: set | None = None) -> list[tuple[int, str]]:
    """The generations to use: those kept in the state (stored), then the resets seen after the newest of them.
    The oldest version DESCRIBE HISTORY still shows moves once log retention trims the log, so the first load's
    time is taken from the history only the first time (nothing stored), never again; seen's first entry is kept
    after that only when it is itself a reset (resets: their versions), a refresh the trimmed log now starts at."""
    if not stored:
        return seen
    stored = [(int(v), str(t)) for v, t in stored]
    return stored + [g for n, g in enumerate(seen) if g[0] > stored[-1][0] and (n or g[0] in (resets or ()))]


def generation_at(gens: list[tuple[int, str]], version: int) -> tuple[int, str] | None:
    """The snapshot a version's rows belong to: the latest generation at or before it."""
    at = [g for g in gens if g[0] <= int(version)]
    return at[-1] if at else (gens[0] if gens else None)


WAIT_MAX_S = 3600   # a DLT REFRESH with no write after it this long: an empty reload, not one still running


def _older(ts: str, seconds: float, now_s: float | None) -> bool:
    """Whether time `ts` (ISO, Z) is more than `seconds` before now (now_s, epoch seconds); False when it can't tell."""
    from datetime import datetime
    try:
        at = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return False
    return (time.time() if now_s is None else now_s) - at > seconds


def refresh_plan(rows, last: int, now: int, now_s: float | None = None) -> dict | None:
    """How an SCD type 2 source read up to `last` catches up to `now` across the resets between them, or None (no
    reset: the change feed reads). {"waiting": the newest reset has no write after it yet (its reload hasn't
    landed: only after a DLT REFRESH, whose reload comes in later commits, and for at most WAIT_MAX_S, after which an
    empty reload is taken as done), "steps": [("changes", since, until) | ("copy", version)]}: the change feed up to the
    version before the
    first reset (it reads after the reset, a range across it doesn't); for each later reset a copy of the version
    before it (the reload before it can't be read as changes: Invalid Streaming Table CDF); then a copy at `now`."""
    resets = [int(r.version) for r in rows if int(last) < int(r.version) <= int(now)
              and evidence.is_reset(r.operation, getattr(r, "operationParameters", None))]
    if not resets:
        return None
    resets.sort()
    newest = next(r for r in rows if int(r.version) == resets[-1])
    waiting = newest.operation == "DLT REFRESH" and \
        not any(resets[-1] < int(r.version) <= int(now) and r.operation in RELOAD_OPS for r in rows) and \
        not _older(newest.timestamp, WAIT_MAX_S, now_s)
    steps = []
    if int(last) >= 0 and resets[0] - 1 > int(last):
        steps.append(("changes", int(last), resets[0] - 1))
    steps += [("copy", r - 1) for r in resets[1:]]
    steps.append(("copy", int(now)))
    return {"waiting": waiting, "steps": steps}


def start_expr(kind: str | None, fill: str) -> str:
    """__START_AT with a snapshot row's empty start filled with its snapshot's time (no alias)."""
    s = _q(START)
    if kind == "timestamp":
        return f"coalesce({s}, {_time_sql(kind, fill)})"
    return (f"CASE WHEN {s} IS NULL OR {s}.__cdc_timestamp_value IS NULL THEN named_struct('__cdc_internal_value', "
            f"coalesce({s}.__cdc_internal_value, ''), '__cdc_timestamp_value', {_time_sql(kind, fill)}) ELSE {s} END")


def start_sql(kind: str | None, fill: str | None) -> str:
    """The SELECT expression of __START_AT: a snapshot row's empty start filled with its snapshot's time."""
    s = _q(START)
    return s if not fill or kind is None else f"{start_expr(kind, fill)} AS {s}"


def unfilled_sql(kind: str | None) -> str | None:
    """The condition of a row whose start is a snapshot's, unfilled (a target merged before the times were filled)."""
    s = _q(START)
    return None if kind is None else f"{s} IS NULL" if kind == "timestamp" else \
        f"({s} IS NULL OR {s}.__cdc_timestamp_value IS NULL)"


def close_sql(kind: str | None, at: str, note: str) -> str | None:
    """The __END_AT a version gets when a whole copy doesn't have it (a full refresh dropped it): time `at`, and in
    Lakeflow Connect's struct `note` saying why. None: a shape that can't be filled."""
    if kind == "timestamp":
        return _time_sql(kind, at)
    if kind in ("cdc", "cdc_string"):
        return f"named_struct('__cdc_internal_value', {spark_string(note)}, '__cdc_timestamp_value', {_time_sql(kind, at)})"
    return None


def absent_at(spark, i: dict, version: int) -> dict:
    """{column: type} of source i's columns its table didn't have yet at `version` (only asked for a version before
    now: one schema read)."""
    if int(version) == int(i["now"]):
        return {}
    have = set(spark.sql(f"SELECT * FROM {i['src']} VERSION AS OF {int(version)} LIMIT 0").columns)
    return {c: t for c, t in i["cols"] if c not in have}


def history_rows(spark, fq: str) -> list:
    """The table's whole history (metadata): version, timestamp (as text, UTC, as Lakeflow Connect writes its CDC
    times: the session's time zone and a client's local time don't enter), operation, operationParameters."""
    return _rows(spark, "SELECT version, date_format(to_utc_timestamp(timestamp, current_timezone()), "
                        "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\") AS timestamp, operation, operationParameters "
                        f"FROM (DESCRIBE HISTORY {fq})")


def _state_full(spark, catalog, target) -> dict:
    """{(target table, source): (version, table id, key columns or None, updated_at, snapshots or None)}: the state,
    its key columns kept so a source's key is read once, not every run; snapshots: an SCD type 2 source's
    generations [[version, time]], kept so its snapshot rows' filled start never changes (keep_generations)."""
    st = _fq(catalog, target, STATE)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {st} (target_table STRING, source STRING, version BIGINT, "
              "table_id STRING, updated_at TIMESTAMP, key_columns STRING) COMMENT 'lfc_medallion merge_all: the "
              "version of each source table read into each target, its table id and key'")
    have = {r.col_name for r in _rows(spark, f"DESCRIBE TABLE {st}")}
    for c in ("table_id", "key_columns", "snapshots"):   # a state from before
        if c not in have:
            spark.sql(f"ALTER TABLE {st} ADD COLUMNS ({c} STRING)")
    out = {}
    for r in _rows(spark, f"SELECT target_table, source, version, table_id, key_columns, updated_at, snapshots "
                          f"FROM {st}"):
        k, g = getattr(r, "key_columns", None), getattr(r, "snapshots", None)
        out[(r.target_table, r.source)] = (int(r.version), r.table_id, json.loads(k) if k else None,
                                           getattr(r, "updated_at", None), json.loads(g) if g else None)
    return out


def _save_states(spark, catalog, target, target_table, saves: list[tuple], read_at: float | None = None) -> None:
    """Every source's new version of one table in one MERGE: saves [(source, version, table id[, key columns[,
    snapshots]])].
    read_at: when those versions were read (epoch seconds), saved as updated_at: changed_since compares the event
    log's times with it, so rows landing while the MERGEs ran aren't taken as merged."""
    if not saves:
        return
    at = "current_timestamp()" if read_at is None else f"timestamp_millis({int(read_at * 1000)})"
    vals = ", ".join(f"({spark_string(target_table)}, {spark_string(x[0])}, {int(x[1])}, {spark_string(x[2])}, "
                     f"{_json_or_null(x, 3)}, {_json_or_null(x, 4)})" for x in saves)
    sql = (f"MERGE INTO {_fq(catalog, target, STATE)} s USING (SELECT * FROM VALUES {vals} "
           "AS n(target_table, source, version, table_id, key_columns, snapshots)) n "
           "ON s.target_table = n.target_table AND s.source = n.source "
           "WHEN MATCHED THEN UPDATE SET s.version = n.version, s.table_id = n.table_id, "
           f"s.key_columns = coalesce(n.key_columns, s.key_columns), s.snapshots = coalesce(n.snapshots, s.snapshots), "
           f"s.updated_at = {at} "
           "WHEN NOT MATCHED THEN INSERT (target_table, source, version, table_id, key_columns, snapshots, updated_at) "
           f"VALUES (n.target_table, n.source, n.version, n.table_id, n.key_columns, n.snapshots, {at})")
    _shared(spark, sql)


def _json_or_null(x: tuple, n: int) -> str:
    """Field n of a state save as a JSON string literal, or NULL (absent or empty)."""
    return spark_string(json.dumps(x[n])) if len(x) > n and x[n] else "NULL"


def _shared(spark, sql: str, tries: int = 6) -> None:
    """A write to a table every table's merge writes at once (the state, the merge log): a concurrent-write conflict,
    or a CREATE ... IF NOT EXISTS racing another, is retried."""
    for attempt in range(tries):
        try:
            spark.sql(sql)
            return
        except Exception as e:  # noqa: BLE001
            msg = str(e).upper()
            if attempt == tries - 1 or not ("CONCURRENT" in msg or "ALREADY_EXISTS" in msg or "ALREADY EXISTS" in msg):
                raise
            time.sleep(0.5 * 2 ** attempt)


def _ensure_target(spark, tgt: str, exists: bool, source_column: str, cols, have: dict) -> None:
    """Create the target (source column first, then the bronze columns), or add the bronze columns it lacks and widen
    the ones a source holds wider (coltypes.widest: an INT target and a BIGINT source -> BIGINT), in place with Delta's
    type widening. A MERGE into the narrower column would fail (an overflow) or round (a decimal's scale) silently;
    a widening Delta can't do in place (a number and a date: STRING) is an error naming the column."""
    if not exists:
        body = ", ".join([f"{_q(source_column)} STRING"] + [f"{_q(c)} {t}" for c, t in cols])
        spark.sql(f"CREATE TABLE {tgt} ({body}) TBLPROPERTIES ({TARGET_PROPERTIES})")
        return
    new = [(c, t) for c, t in cols if c not in have]
    if new:
        spark.sql(f"ALTER TABLE {tgt} ADD COLUMNS ({', '.join(f'{_q(c)} {t}' for c, t in new)})")
    wider = []
    for c, t in cols:
        if c not in have or coltypes.from_spark(have[c]) == coltypes.from_spark(t):
            continue
        w = coltypes.widest([have[c], t])
        if coltypes.from_spark(w) == coltypes.from_spark(have[c]):   # the target holds the source's already
            continue
        if not coltypes.widens_in_place(have[c], w):
            raise ValueError(f"{tgt}.{_q(c)}: {have[c]} in the target, {t} in a source; the type that holds both, {w}, "
                             "isn't a change Delta makes in place: cast the column in the source, or drop the target "
                             "for a full copy")
        wider.append((c, w))
    if wider:
        # a TIMESTAMP_NTZ column also needs its table feature on (DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT: seen on a
        # serverless warehouse 2026-11-16, as INT -> BIGINT, DECIMAL(10,2) -> DECIMAL(12,4) and DATE -> TIMESTAMP_NTZ)
        ntz = ", 'delta.feature.timestampNtz' = 'supported'" if any(w == "TIMESTAMP_NTZ" for _, w in wider) else ""
        spark.sql(f"ALTER TABLE {tgt} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true'{ntz})")
        for c, w in wider:
            spark.sql(f"ALTER TABLE {tgt} ALTER COLUMN {_q(c)} TYPE {w}")


def changes_sql(src: str, source: str, source_column: str, key: list[str], cols: list[str], since: int | None,
                until: int, start: str | None = None, absent: dict | None = None, after_ts: str | None = None) -> str:
    """One source's rows for a MERGE: the whole table at `until` (since=None), or its changes since `since` (each key's
    last change, update pre-images dropped), with the source column and the change in `__lfc_change`. start: the
    SELECT expression for __START_AT (start_sql: a snapshot row's start filled), in place of the column. absent:
    {column: type} the table didn't have yet at `until` (a column added since): NULL of that type."""
    from lfc_change_source.contract import Position, last_change_sql
    from lfc_change_source.lfc_bronze import KIND, Bronze
    lit = spark_string(source)
    absent = absent or {}
    data = ", ".join(f"CAST(NULL AS {absent[c]}) AS {_q(c)}" if c in absent else start if start and c == START
                     else _q(c) for c in cols)
    bronze = Bronze(source, src, "", list(key), list(cols))
    at = Position(KIND, str(int(until)))
    if since is None:
        # pinned to the version saved in the state, so a commit in between isn't applied twice
        return (f"SELECT {lit} AS {_q(source_column)}, {data}, 'insert' AS `__lfc_change` "
                f"FROM ({bronze.snapshot_sql(at, data)})")
    # each key's last change (the change source's _seq: the version, a delete before an insert in one version);
    # after_ts: from a saved time to the latest commit (lfc_bronze commit_ts), no version needed
    from lfc_change_source.lfc_bronze import TS
    rng = (Position(TS, after_ts), None) if after_ts else (Position(KIND, str(int(since))), at)
    names = ", ".join(_q(c) for c in cols)
    return (f"SELECT {lit} AS {_q(source_column)}, {names}, _change_type AS `__lfc_change` FROM ("
            f"{last_change_sql(bronze.changes_sql(*rng, data), list(key))})")


def merge_into(tgt: str, rows: str, source_column: str, key: list[str], cols: list[str],
               whole_source: str | None = None, close: str | None = None) -> str:
    """The MERGE of `rows` (changes_sql, or a UNION ALL of several sources') keyed by the source column and the key.
    whole_source: a whole copy of that source, whose rows the copy lacks are deleted in the same statement; with
    close (an __END_AT expression, close_sql), its open versions the copy lacks are closed instead, and none deleted."""
    names = [source_column] + cols
    on = " AND ".join(f"t.{_q(c)} = c.{_q(c)}" for c in [source_column] + key)
    sets = ", ".join(f"t.{_q(c)} = c.{_q(c)}" for c in cols if c not in key)
    ins_cols = ", ".join(_q(c) for c in names)
    ins_vals = ", ".join(f"c.{_q(c)}" for c in names)
    update = f"WHEN MATCHED THEN UPDATE SET {sets} " if sets else ""
    mine = f"t.{_q(source_column)} = {spark_string(whole_source)}" if whole_source is not None else ""
    stale = ("" if whole_source is None else
             f" WHEN NOT MATCHED BY SOURCE AND {mine} THEN DELETE" if close is None else
             f" WHEN NOT MATCHED BY SOURCE AND {mine} AND t.{_q(END)} IS NULL THEN UPDATE SET t.{_q(END)} = {close}")
    return (f"MERGE INTO {tgt} t USING ({rows}) c ON {on} "
            "WHEN MATCHED AND c.`__lfc_change` = 'delete' THEN DELETE "
            f"{update}WHEN NOT MATCHED AND c.`__lfc_change` != 'delete' THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
            f"{stale}")


def merge_sql(tgt: str, src: str, source: str, source_column: str, key: list[str], cols: list[str],
              since: int | None, until: int) -> str:
    """The MERGE for one source: its rows (since=None: the whole table) or its changes since `since`."""
    return merge_into(tgt, changes_sql(src, source, source_column, key, cols, since, until), source_column, key, cols,
                      whole_source=source if since is None else None)


LOG = "_lfc_medallion_merge_log"


def _log_rows(spark, fq_log: str, rows: list[tuple]) -> None:
    """Append what merge_all did to the merge log: one row per table and source it copied or merged."""
    if not rows:
        return
    _shared(spark, f"CREATE TABLE IF NOT EXISTS {fq_log} (run_id STRING, target_table STRING, source STRING, done STRING, "
              "version_from BIGINT, version_to BIGINT, inserted BIGINT, updated BIGINT, deleted BIGINT, "
              "seconds DOUBLE, at TIMESTAMP) COMMENT 'lfc_medallion merge_all: each copy or merge, per table and source'")
    lit = lambda v: "NULL" if v is None else str(v) if isinstance(v, (int, float)) else spark_string(str(v))  # noqa: E731
    values = ", ".join("(" + ", ".join(lit(v) for v in r[:-1]) + ", current_timestamp())" for r in rows)
    _shared(spark, f"INSERT INTO {fq_log} VALUES {values}")


def _last_metrics(spark, fq: str) -> tuple:
    """(inserted, updated, deleted) of a table's last commit (a MERGE's operationMetrics)."""
    r = _rows(spark, f"SELECT operationMetrics AS m FROM (DESCRIBE HISTORY {fq} LIMIT 1)")
    m = (r[0].m or {}) if r else {}
    g = lambda k: int(m.get(k) or 0)   # noqa: E731
    return g("numTargetRowsInserted"), g("numTargetRowsUpdated"), g("numTargetRowsDeleted")   # totals: they include
    # the Matched (and NotMatchedBySource) counts


def merge_all(spark, catalog: str, prefix: str, target: str, tables: list[str], source_column: str = "_source",
              only=None, name: str = "all_{table}", keys: dict | None = None, workspace=None,
              take_over: bool = False, run_id: str = "", log: str | None = None, changed: set | None = None,
              context: dict | None = None, keep_gone: bool = False, planner=None, read_by: str = "version") -> dict:
    """Keep <catalog>.<target>.<name> current for each table in `tables`, from every source (schema <prefix><source>)
    that has it. keys: {table: [columns]} where the recorded key isn't the one to use. take_over: a target that is
    still a view (the pipeline's, left over when the table moved to this route) is dropped and the table copied whole;
    without it, a view there is an error (a pipeline still defining it would recreate it). Returns, per target table and
    source, what was done: copied (first read or after a full refresh), merged (from the change feed) or unchanged,
    and the version read. Each table's copies and merges are appended to the merge log (`log`, a table name, by default
    <target>._lfc_medallion_merge_log) when the table is done, even when it fails part way: run_id, table, source,
    done, the versions read, the target's inserted, updated and deleted rows, and the seconds; for a dashboard.
    changed: the (source, table) pairs that may have changed (merge_route reads them from the ingestion pipelines'
    event logs): any other source with a state is left as it is, with no read at all; None: every source is read.
    context: what merge_route already read once for every table (sources, their tables and columns, the state, the
    target's objects), so a call per table doesn't read them again. keep_gone: a source gone (or left out by
    `only`) keeps its rows and state (a history schema keeps what it has), instead of losing them."""
    fq_log = log or _fq(catalog, target, LOG)
    ctx = context or load_context(spark, catalog, prefix, target, only)
    names, held, state, kinds = ctx["names"], ctx["held"], ctx["state"], dict(ctx["kinds"])
    existing = set(kinds)
    report, fresh_targets = {}, {}
    for table in tables:
        out_name = name.format(table=table)
        tgt = _fq(catalog, target, out_name)
        report[out_name] = {}
        logged: list[tuple] = []
        try:
            _merge_table(spark, catalog, prefix, target, table, out_name, tgt, names, held, state, kinds, existing,
                         fresh_targets, report, source_column, keys, workspace, take_over, logged, run_id, changed,
                         ctx.get("cols"), keep_gone, planner, read_by, ctx.get("refreshed") or set())
        finally:
            _log_rows(spark, fq_log, logged)
    return report


def load_context(spark, catalog: str, prefix: str, target: str, only=None) -> dict:
    """What every table's merge needs, read once: the sources, their tables and columns (one information_schema
    query; a SHOW TABLES per source where it can't run), the state with its keys and times, the target's objects."""
    names = discover.sources(spark, catalog, prefix, only)
    held, cols = discover.held_tables(spark, catalog, prefix, names)
    kinds = {r.table_name: r.table_type for r in _rows(
        spark, f"SELECT table_name, table_type FROM {_q(catalog)}.information_schema.tables "
               f"WHERE table_schema = {spark_string(target)}")}
    return {"names": names, "held": held, "cols": cols, "state": _state_full(spark, catalog, target), "kinds": kinds}


def _inspect(spark, catalog, prefix, table, out_name, s, keys, workspace, state, fresh, cols_known=None,
             by_time=False) -> dict:
    """One source of one table, read only (metadata: no data, no writes): its columns, key, version, table id, the
    version last merged and what changed since. Run for every source at once (threads). by_time: a source the event
    logs flag as changed (no snapshot) and with a saved state is read from its saved time (after_ts), with no history
    or table id read, when it's SCD type 1 (no __START_AT); its saved version stays as it was (a lower bound)."""
    src = _fq(catalog, f"{prefix}{s}", table)
    saved = state.get((out_name, s))
    cols = [(c, t) for c, t in (cols_known or columns(spark, src)) if c not in CHANGE_COLUMNS]
    if (by_time and not fresh and saved and len(saved) > 3 and saved[0] is not None and saved[1] and saved[3]
            and START not in {c for c, _ in cols}):
        key = list((keys or {}).get(table) or (saved[2] if len(saved) > 2 and saved[2] else None) or [])
        if key:
            from .route import MARGIN_S
            t = saved[3] if saved[3].tzinfo else saved[3].replace(tzinfo=dt.timezone.utc)   # Spark: naive, UTC
            after = (t - dt.timedelta(seconds=MARGIN_S)).astimezone(dt.timezone.utc)
            return {"s": s, "src": src, "cols": cols, "key": key, "now": saved[0], "tid": saved[1],
                    "last": saved[0], "whole": False, "changed": True,
                    "after_ts": after.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
    key = list((keys or {}).get(table) or (saved[2] if saved and len(saved) > 2 and saved[2] else None) or
               evidence.recorded_keys(spark, src, f"{catalog}.{prefix}{s}.{table}", workspace))
    if not key:
        raise ValueError(f"{catalog}.{prefix}{s}.{table}: no primary key recorded; give keys={{{table!r}: [...]}}")
    if "__START_AT" in {c for c, _ in cols} and "__START_AT" not in key:
        key.append("__START_AT")   # SCD type 2: a row per version of the key
    last, last_id = (None, None) if fresh else (saved[:2] if saved else (None, None))
    tid = table_id(spark, src)
    if last is None or not last_id or last_id != tid:   # first read, a state without an id, or a recreated table
        now = latest_version(spark, src)
        if last is not None and (last_id and last_id != tid or now < last):   # its history starts over
            last = -1
        refreshed = changed = False
        if last is not None and last >= 0 and now != last:
            refreshed, changed = since(spark, src, last)
    else:   # the latest version and what changed, from one read of the history after the version saved
        now, refreshed, changed = history_since(spark, src, last)
        if now < last:
            last = -1
    info = {"s": s, "src": src, "cols": cols, "key": key, "now": now, "tid": tid, "last": last,
            "whole": last is None or last < 0 or refreshed, "changed": changed}
    types = dict(cols)
    if START in types and now != last:   # SCD type 2 with something to read: its snapshots, from one history read
        hist = history_rows(spark, src)
        stored = saved[4] if saved and len(saved) > 4 and last_id == tid else None   # a recreated table: its own
        gens = keep_generations(stored, generations(hist), {int(r.version) for r in hist if evidence.is_reset(
            r.operation, getattr(r, "operationParameters", None))})
        info.update(gens=gens, times={int(r.version): _iso(r.timestamp) for r in hist},
                    start_kind=_kind(types[START]), end_kind=_kind(types.get(END)),
                    plan=refresh_plan(hist, last, now) if last is not None and last >= 0 and refreshed else None,
                    # a target merged before the snapshot times were kept: its snapshot rows' start unfilled, once
                    migrate=bool(saved) and stored is None and last is not None and last >= 0)
    return info


def _current(spark, i: dict) -> dict:
    """Source i with its latest version and table id, for a whole copy: one read by time has neither (its saved
    version is only a lower bound), and copying that version would copy old rows."""
    if i.get("after_ts"):
        i.update(now=latest_version(spark, i["src"]), tid=table_id(spark, i["src"]))
        i.pop("after_ts")
    return i


def _saved(i: dict) -> tuple:
    """What the state keeps of source i after a run: its version, table id, key and (SCD type 2) generations."""
    return (i["s"], i["now"], i["tid"], i["key"], [list(g) for g in i["gens"]] if i.get("gens") else None)


def _start(i: dict, version: int) -> str | None:
    """The __START_AT expression for rows of version `version` of source i (None: not SCD type 2, or no history)."""
    g = generation_at(i.get("gens") or [], version)
    return start_sql(i.get("start_kind"), g[1]) if g else None


def _close(i: dict, version: int) -> str | None:
    """The __END_AT of a target version a whole copy at `version` lacks: the time of the reset it came after (a full
    refresh since the last read), else of the version copied (the first read, a recreated table, a history gap)."""
    g = generation_at(i.get("gens") or [], version)
    last = i["last"] if i["last"] is not None else -1
    if g and g[0] > last and g[0] != (i["gens"][0][0] if i.get("gens") else None):
        return close_sql(i.get("end_kind"), g[1], f"lfc_medallion: closed by full refresh at version {g[0]}")
    at = (i.get("times") or {}).get(int(version))
    return close_sql(i.get("end_kind"), at, f"lfc_medallion: not in a whole copy at version {int(version)}") if at else None


def _head(spark, tgt: str) -> int:
    """The table's latest version (one history row: metadata)."""
    return int(_rows(spark, f"SELECT version FROM (DESCRIBE HISTORY {tgt} LIMIT 1)")[0].version)


def _merged_counts(spark, tgt: str, source_column: str, before: int | None) -> dict | None:
    """{source: (inserted, updated, deleted)} of the MERGE just run on `tgt` (its version was `before`), from the
    target's own change feed for the versions after `before` (TARGET_PROPERTIES turns it on): only the files the MERGE
    wrote, where counting the change rows first would read every source's change feed a second time. Only this
    table's thread writes `tgt`. {} when the MERGE committed nothing; None when the feed can't be read (unknown)."""
    if before is None:
        return None
    try:
        v = _head(spark, tgt)
        if v <= before:
            return {}
        got = _rows(spark, f"SELECT {_q(source_column)} AS s, "
                           "SUM(CASE WHEN _change_type = 'insert' THEN 1 ELSE 0 END) AS i, "
                           "SUM(CASE WHEN _change_type = 'update_postimage' THEN 1 ELSE 0 END) AS u, "
                           "SUM(CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END) AS d "
                           f"FROM table_changes({spark_string(tgt.replace('`', ''))}, "
                           f"{before + 1}, {v}) GROUP BY 1")
    except Exception:  # noqa: BLE001  a target from before, without the feed
        return None
    return {r.s: (int(r.i or 0), int(r.u or 0), int(r.d or 0)) for r in got}


UNKNOWN = (None, None, None)   # a source's counts when the target's feed couldn't say


def _of(counts: dict | None, source: str) -> tuple:
    return UNKNOWN if counts is None else counts.get(source, (0, 0, 0))


def _before(spark, tgt: str) -> int | None:
    """The target's version before a MERGE; None when its history can't be read (the counts are then unknown)."""
    try:
        return _head(spark, tgt)
    except Exception:  # noqa: BLE001
        return None


def _merge_split(eng, tgt: str, parts: list[list[str]], source_column: str, key: list[str], cols: list[str],
                 tries: int = 8) -> tuple[int, list[str]]:
    """One MERGE per part (each a UNION ALL of its sources' changes), the parts at once on threads, on one engine; a
    MERGE that meets another's commit (CONCURRENT...) is tried again. (The retries, the statements' ids where the
    engine gives them: a warehouse's, for their phases in query history), for the paths log."""
    def one(p):
        q = merge_into(tgt, " UNION ALL ".join(p), source_column, key, cols)
        for attempt in range(tries):
            try:
                return attempt, getattr(eng.sql(q), "statement_id", None)
            except Exception as e:  # noqa: BLE001
                if attempt == tries - 1 or "CONCURRENT" not in str(e).upper():
                    raise
                time.sleep(0.2 * 2 ** attempt)
    if len(parts) == 1:
        got = [one(parts[0])]
    else:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=len(parts)) as ex:
            got = list(ex.map(one, parts))
    return sum(a for a, _ in got), [i for _, i in got if i]


def _counts_by_source(spark, tgt: str, source_column: str) -> dict:
    """Each source's rows in the target, one GROUP BY: the planner's denominator for merge or copy."""
    return {r[0]: int(r[1]) for r in spark.sql(f"SELECT {_q(source_column)}, COUNT(*) FROM {tgt} GROUP BY 1").collect()}


def _insights(spark, tgt: str) -> list[str]:
    """What the target's Delta features say about its MERGE cost, as the materialized-view planner's insights:
    without deletion vectors a MERGE rewrites every file a changed key is in; row tracking is kept for incremental
    readers. Read once per table and run."""
    try:
        props = {str(r[0]).lower(): str(r[1]).lower() for r in spark.sql(f"SHOW TBLPROPERTIES {tgt}").collect()}
    except Exception:  # noqa: BLE001  not there yet: nothing to say
        return []
    out = []
    if props.get("delta.enabledeletionvectors") == "false" or (
            "delta.feature.deletionvectors" not in props and "delta.enabledeletionvectors" not in props):
        out.append("DELTA_FEATURE_NOT_ENABLED: deletion vectors (a MERGE rewrites whole files)")
    if props.get("delta.enablerowtracking") != "true":
        out.append("DELTA_FEATURE_NOT_ENABLED: row tracking")
    return out


def _merge_table(spark, catalog, prefix, target, table, out_name, tgt, names, held, state, kinds, existing,
                 fresh_targets, report, source_column, keys, workspace, take_over, logged, run_id, changed=None,
                 cols_known=None, keep_gone=False, planner=None, read_by="version", refreshed=frozenset()):
    """merge_all's work on one table: every source read at once (metadata, threads), then a whole copy for each source
    that needs one, ONE MERGE for all the sources whose changes come from the change feed, and one state write: so a
    table of hundreds of sources costs a few statements, not a few per source (logged: what it copied or merged)."""
    from concurrent.futures import ThreadPoolExecutor
    srcs = [x for x in names if table in held[x]]
    started = time.time()   # to the first MERGE: this table's planning (paths log planning_s)
    if kinds.get(out_name) in ("MATERIALIZED_VIEW", "VIEW"):   # a pipeline's view (moved to this route)
        if not take_over:
            raise ValueError(f"{catalog}.{target}.{out_name} is a view (a pipeline's?): leave it out of the "
                             "pipeline (stack_all skip=...), then take_over=True drops it for the table")
        spark.sql(f"DROP {kinds[out_name].replace('_', ' ')} {tgt}")
        existing.discard(out_name)
        kinds.pop(out_name)
    created = out_name not in existing   # made now (or dropped by hand since): every source copied
    fresh = fresh_targets.setdefault(out_name, created)
    quiet = [] if fresh or changed is None else [x for x in srcs if (table, x) not in changed and (
        (out_name, x) in state)]   # nothing landed in their bronze since their state: no read at all
    for x in quiet:
        report[out_name][x] = {"done": "unchanged", "version": state[(out_name, x)][0]}
    look = [x for x in srcs if x not in quiet]
    read_at = time.time()   # before any version is read: the watermark (_save_states)
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(look)))) as ex:
        by_time = read_by == "time" and changed is not None   # the event logs say what changed: no history read
        info = list(ex.map(lambda s: _inspect(spark, catalog, prefix, table, out_name, s, keys, workspace, state,
                                              fresh, (cols_known or {}).get(s, {}).get(table),
                                              by_time and (table, s) not in refreshed), look))
    shapes = {tuple(i["cols"]) for i in info}   # names and types: a source holding a column wider widens the target
    for i in (info if len(shapes) > 1 or created else info[:1]) if info else []:   # every source's columns in the target
        have = {} if out_name not in existing else dict(columns(spark, tgt))
        _ensure_target(spark, tgt, out_name in existing, source_column, i["cols"], have)
        existing.add(out_name)
    for i in info:   # a target merged before the snapshot times were kept: fill its snapshot rows' start, once
        if i.get("migrate") and out_name in existing and unfilled_sql(i.get("start_kind")):
            g = generation_at(i["gens"], i["last"])
            if g:
                spark.sql(f"UPDATE {tgt} SET {_q(START)} = {start_expr(i['start_kind'], g[1])} "
                          f"WHERE {_q(source_column)} = {spark_string(i['s'])} AND {unfilled_sql(i['start_kind'])}")
    saves, feed, failed = [], [], []
    try:   # what finished is saved even when a later copy or MERGE fails: the next run doesn't redo it
        for i in info:
            if i["last"] is not None and i["now"] == i["last"]:
                report[out_name][i["s"]] = {"done": "unchanged", "version": i["now"]}
            elif i.get("plan") and i["plan"]["waiting"]:   # a full refresh not reloaded yet: the next run
                report[out_name][i["s"]] = {"done": "waiting (full refresh reloading)", "version": i["last"]}
            elif i.get("plan"):   # SCD type 2 across a full refresh: the versions before it, then a copy that closes
                try:   # one source's failure doesn't stop the others; its state stays, the next run retries it
                    _catch_up(spark, tgt, i, source_column, logged, run_id, out_name, report)
                    saves.append(_saved(i))
                except Exception as e:  # noqa: BLE001
                    failed.append((i["s"], e))
                    report[out_name][i["s"]] = {"done": f"failed: {str(e).splitlines()[0][:150]}", "version": i["last"]}
            elif not i["whole"] and not i["changed"]:   # new versions, no rows written: nothing to MERGE
                saves.append(_saved(i))
                report[out_name][i["s"]] = {"done": "unchanged", "version": i["now"]}
            elif i["whole"]:
                _copy(spark, tgt, i, source_column, logged, run_id, out_name, report, "copied")
                saves.append(_saved(i))
            else:
                feed.append(i)
        if planner is not None and feed and planner.needs_counts(table, [i["s"] for i in feed]):
            # a source whose changes are a large share of its rows is copied whole (paths.technique)
            how = planner.techniques(table, [i["s"] for i in feed], _counts_by_source(spark, tgt, source_column))
            for i in [i for i in feed if how[i["s"]][0] == "copy"]:
                _copy(spark, tgt, _current(spark, i), source_column, logged, run_id, out_name, report,
                      f"copied ({how[i['s']][1]})")
                saves.append(_saved(i))
            feed = [i for i in feed if how[i["s"]][0] != "copy"]
        insights = _insights(spark, tgt) if planner is not None and feed else []
        groups: dict[tuple, list] = {}   # sources with the same key and columns share one MERGE
        for i in feed:
            groups.setdefault((tuple(i["key"]), tuple(c for c, _ in i["cols"])), []).append(i)
        for (key, cols), part in groups.items():
            each = {i["s"]: changes_sql(i["src"], i["s"], source_column, list(key), list(cols), i["last"], i["now"],
                                        _start(i, i["now"]), after_ts=i.get("after_ts")) for i in part}
            # the path: the engine and how many MERGEs at once (paths.Planner, by cost); none: one MERGE on spark
            fp = paths.fingerprint(list(key), list(cols))
            e, eng, k, crows, preds = ("spark", spark, 1, 0, None) if planner is None else \
                planner.pick(table, [i["s"] for i in part], out_name, fp)
            try:
                t0 = time.time()
                before = _before(eng, tgt)
                retries, ids = _merge_split(eng, tgt, [[each[i["s"]] for i in g] for g in paths.groups(part, k)],
                                            source_column, list(key), list(cols))
                n = _merged_counts(eng, tgt, source_column, before)
                if planner is not None:
                    planner.record(out_name, e, k, len(part), crows, preds, time.time() - t0, retries, fp=fp,
                                   insights=insights, statements=ids, planning_s=t0 - started)
            # the change data vacuumed or expired, or the feed off: the error class varies, so each source alone
            # (on spark; a source's changes merged again change nothing: the same rows match)
            except Exception as err:  # noqa: BLE001
                if planner is not None:
                    planner.record(out_name, e, k, len(part), crows, preds, time.time() - t0, 0,
                                   f"failed, each source alone: {str(err).splitlines()[0][:120]}", fp=fp,
                                   insights=insights)
                for i in part:
                    try:
                        t1 = time.time()
                        one = changes_sql(i["src"], i["s"], source_column, list(key), list(cols), i["last"], i["now"],
                                          _start(i, i["now"]), after_ts=i.get("after_ts"))
                        before = _before(spark, tgt)
                        spark.sql(merge_into(tgt, one, source_column, list(key), list(cols)))
                        _done(logged, report, run_id, out_name, i, "merged",
                              _of(_merged_counts(spark, tgt, source_column, before), i["s"]), t1)
                    except Exception as e:  # noqa: BLE001
                        from lfc_change_source.lfc_bronze import nothing_newer
                        if i.get("after_ts") and nothing_newer(e):   # no commit after its start: nothing changed
                            report[out_name][i["s"]] = {"done": "unchanged", "version": i["last"]}
                        else:
                            _copy(spark, tgt, _current(spark, i), source_column, logged, run_id, out_name, report,
                                  f"copied (change feed unreadable: {str(e).splitlines()[0][:100]})")
                    saves.append(_saved(i))
                continue
            for i in part:   # seconds: the shared MERGE's, for every source in it
                _done(logged, report, run_id, out_name, i, "merged", _of(n, i["s"]), t0)
                saves.append(_saved(i))
    finally:
        _save_states(spark, catalog, target, out_name, saves, read_at)
    # sources gone (schema dropped, table removed, left out by `only`): their rows and state go too
    gone = [g for g in sorted({src for (t, src) in state if t == out_name} - set(srcs))
            if g not in names or table not in discover.tables_or_none(spark, catalog, prefix, g)]   # a listed source:
    # the catalog query can lag a table just made, so SHOW TABLES confirms it's gone
    if gone and out_name in existing and not keep_gone:
        listed = ", ".join(spark_string(g) for g in gone)
        spark.sql(f"DELETE FROM {tgt} WHERE {_q(source_column)} IN ({listed})")
        _shared(spark, f"DELETE FROM {_fq(catalog, target, STATE)} WHERE target_table = {spark_string(out_name)} "
                       f"AND source IN ({listed})")
        for g in gone:
            report[out_name][g] = {"done": "removed"}
    if failed:   # after every other source's work and state: the table counts as failed (merge_route keeps it)
        raise RuntimeError(f"{out_name}: {len(failed)} source(s) failed, first {failed[0][0]}: "
                           f"{str(failed[0][1]).splitlines()[0][:200]}") from failed[0][1]


def _done(logged, report, run_id, out_name, i, done, counts, started) -> None:
    report[out_name][i["s"]] = {"done": done, "version": i["now"]}
    logged.append((run_id, out_name, i["s"], done.split(" ")[0], None if i["last"] is None or i["last"] < 0
                   else int(i["last"]), int(i["now"]), *counts, round(time.time() - started, 2), None))


def _copy(spark, tgt, i, source_column, logged, run_id, out_name, report, done, at: int | None = None) -> None:
    """A whole copy of one source at version `at` (default: now) (the first read, a full refresh, a recreated table,
    an unreadable feed). The rows the copy lacks are deleted; an SCD type 2 source the target already has keeps them:
    its open versions are closed (_close), or left as they are where its end column can't be filled."""
    t0 = time.time()
    v = i["now"] if at is None else at
    cols = [c for c, _ in i["cols"]]
    keep = bool(i.get("gens"))   # SCD type 2, even on a first read: a target holding rows (a lost state) keeps them
    close = _close(i, v) if keep else None
    spark.sql(merge_into(tgt, changes_sql(i["src"], i["s"], source_column, i["key"], cols, None, v, _start(i, v),
                                          absent_at(spark, i, v)),
                         source_column, i["key"], cols, whole_source=None if keep and close is None else i["s"],
                         close=close))
    if at is None:
        _done(logged, report, run_id, out_name, i, done, _last_metrics(spark, tgt), t0)


def _catch_up(spark, tgt, i, source_column, logged, run_id, out_name, report) -> None:
    """An SCD type 2 source across full refreshes (refresh_plan): its changes up to the version before the first, a
    copy of the version before each later one, then a copy now; each copy closes the open versions it lacks. A step
    that fails (the change data or the files vacuumed) falls back to the copy now, which still closes rather than
    deletes; the versions in between are then lost, and the merge log says so."""
    try:
        _steps(spark, tgt, i, source_column, logged, run_id, out_name, report)
    except Exception as e:   # only data that is gone falls back; anything else is retried next run
        if not data_gone(e):
            raise
        _copy(spark, tgt, i, source_column, logged, run_id, out_name, report,
              f"refreshed (catch-up failed, versions since the last read lost: {str(e).splitlines()[0][:100]})")


# the errors that say the versions a catch-up reads are gone for good: the change data vacuumed or never recorded
# (DELTA_MISSING_CHANGE_DATA), a data or change-data file vacuumed (DELTA_FILE_NOT_FOUND...,
# DELTA_CHANGE_DATA_FILE_NOT_FOUND, FileNotFoundException), a version no longer in the log (DELTA_LOG_FILE_NOT_FOUND,
# DELTA_TRUNCATED_TRANSACTION_LOG, VersionNotFoundException, DELTA_VERSIONS_NOT_CONTIGUOUS, "Cannot time travel ...
# to version").
# Nothing else: a schema change in the range or a wrong range (Invalid Streaming Table CDF) is a bug to see, and a
# conflict, throttling or a network error is retried. One not listed is raised every run until someone looks.
GONE = ("DELTA_MISSING_CHANGE_DATA", "DELTA_FILE_NOT_FOUND", "DELTA_CHANGE_DATA_FILE_NOT_FOUND", "DELTA_LOG_FILE_NOT_FOUND",
        "DELTA_TRUNCATED_TRANSACTION_LOG", "FILENOTFOUNDEXCEPTION", "VERSIONNOTFOUNDEXCEPTION",
        "DELTA_VERSIONS_NOT_CONTIGUOUS", "CANNOT TIME TRAVEL DELTA TABLE TO VERSION")


def data_gone(e: Exception) -> bool:
    """Whether an error means the data a catch-up reads is gone (GONE), not a failure worth retrying."""
    msg = str(e).upper()
    return any(g in msg for g in GONE)


def _steps(spark, tgt, i, source_column, logged, run_id, out_name, report) -> None:
    t0 = time.time()
    cols = [c for c, _ in i["cols"]]
    for step in i["plan"]["steps"]:
        if step[0] == "changes":
            _, since, until = step
            spark.sql(merge_into(tgt, changes_sql(i["src"], i["s"], source_column, i["key"], cols, since, until,
                                                  _start(i, until), absent_at(spark, i, until)),
                                 source_column, i["key"], cols))
        else:
            _copy(spark, tgt, i, source_column, logged, run_id, out_name, report, "", at=step[1])
    _done(logged, report, run_id, out_name, i, "refreshed (history kept)", _last_metrics(spark, tgt), t0)


def merged_targets(spark, catalog: str, target: str) -> set[str]:
    """The target tables merge_all finished merging at least once (in its state)."""
    if not _rows(spark, f"SHOW TABLES IN {_q(catalog)}.{_q(target)} LIKE {spark_string(STATE)}"):
        return set()
    return {r.target_table for r in _rows(spark, f"SELECT DISTINCT target_table FROM {_fq(catalog, target, STATE)}")}


def release(spark, catalog: str, target: str, tables: list[str], name: str = "all_{table}") -> list[str]:
    """Hand tables back to the pipeline (the route back from `merge`): drop merge_all's tables and their state, so the
    pipeline's views can take the names. Only tables merge_all wrote (in its state) are dropped. Returns those."""
    st = _fq(catalog, target, STATE)
    if not _rows(spark, f"SHOW TABLES IN {_q(catalog)}.{_q(target)} LIKE {spark_string(STATE)}"):
        return []
    written = {r.target_table for r in _rows(spark, f"SELECT DISTINCT target_table FROM {st}")}
    dropped = [name.format(table=t) for t in tables if name.format(table=t) in written]
    for out_name in dropped:
        spark.sql(f"DROP TABLE IF EXISTS {_fq(catalog, target, out_name)}")
        spark.sql(f"DELETE FROM {st} WHERE target_table = {spark_string(out_name)}")
    return dropped


ROUTE_TAG = "lfc_medallion_route"


def routes_to_apply(spark, catalog: str, target: str, planned: dict, name: str = "all_{table}",
                    tag: str = ROUTE_TAG, previous: dict | None = None) -> tuple[dict, dict]:
    """The route each table gets this run, and the changes the plan proposes without making them. A switch costs a
    whole copy (or a full recompute) of the table and its readers, so it happens only on request: the tag `tag`
    ('merge' or 'pipeline') on <target>.<name>, set by an operator. The request is used up by the switch, since the
    object it is on (the pipeline's view or merge_all's table) is dropped and replaced. Without a tag a table keeps its
    route (a table here: merge; a view: pipeline). previous: {table: route} applied last run (the caller keeps it): a
    table whose object is missing keeps that route, so a run that failed between a switch's drop and the new object
    (the pipeline's view, or merge_all's copy) finishes the switch instead of undoing it; with neither, the plan's.
    planned: {table: 'merge' | 'pipeline'}. Returns ({table: route}, {table: {"current": ..., "planned": ...}})."""
    rows = _rows(spark, f"SELECT table_name, table_type FROM {_q(catalog)}.information_schema.tables "
                        f"WHERE table_schema = {spark_string(target)}")
    kinds = {r.table_name: r.table_type for r in rows}
    asked = {r.table_name: str(r.tag_value).strip().lower() for r in _rows(
        spark, f"SELECT table_name, tag_value FROM {_q(catalog)}.information_schema.table_tags "
               f"WHERE schema_name = {spark_string(target)} AND tag_name = {spark_string(tag)}")}
    apply, proposed = {}, {}
    for table, plan in planned.items():
        out = name.format(table=table)
        kind = kinds.get(out)
        current = None if kind is None else ("pipeline" if kind in ("MATERIALIZED_VIEW", "VIEW") else "merge")
        request = asked.get(out)
        if request in ("merge", "pipeline"):
            apply[table] = request
        else:
            current = current or (previous or {}).get(table)
            apply[table] = current or plan
            if current and current != plan:
                proposed[table] = {"current": current, "planned": plan}
    return apply, proposed
