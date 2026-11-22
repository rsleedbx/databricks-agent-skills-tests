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

    from lfc_medallion import merge_all, table_plan
    plan = table_plan(spark, "main", "sales_bronze_east")
    quiet = [n for n, t in plan["tables"].items() if t["route"] == "merge"]
    merge_all(spark, "main", "sales_bronze_", target="sales_silver", tables=quiet, source_column="division")
"""
import json
import time

from . import coltypes, discover, evidence
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


def _state_full(spark, catalog, target) -> dict:
    """{(target table, source): (version, table id, key columns or None, updated_at)}: the state, its key columns kept
    so a source's key is read once, not every run."""
    st = _fq(catalog, target, STATE)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {st} (target_table STRING, source STRING, version BIGINT, "
              "table_id STRING, updated_at TIMESTAMP, key_columns STRING) COMMENT 'lfc_medallion merge_all: the "
              "version of each source table read into each target, its table id and key'")
    have = {r.col_name for r in _rows(spark, f"DESCRIBE TABLE {st}")}
    for c in ("table_id", "key_columns"):   # a state from before
        if c not in have:
            spark.sql(f"ALTER TABLE {st} ADD COLUMNS ({c} STRING)")
    out = {}
    for r in _rows(spark, f"SELECT target_table, source, version, table_id, key_columns, updated_at FROM {st}"):
        k = getattr(r, "key_columns", None)
        out[(r.target_table, r.source)] = (int(r.version), r.table_id, json.loads(k) if k else None,
                                           getattr(r, "updated_at", None))
    return out


def _save_states(spark, catalog, target, target_table, saves: list[tuple]) -> None:
    """Every source's new version of one table in one MERGE: saves [(source, version, table id[, key columns])]."""
    if not saves:
        return
    vals = ", ".join(f"({spark_string(target_table)}, {spark_string(x[0])}, {int(x[1])}, {spark_string(x[2])}, "
                     f"{spark_string(json.dumps(x[3])) if len(x) > 3 and x[3] else 'NULL'})" for x in saves)
    sql = (f"MERGE INTO {_fq(catalog, target, STATE)} s USING (SELECT * FROM VALUES {vals} "
           "AS n(target_table, source, version, table_id, key_columns)) n "
           "ON s.target_table = n.target_table AND s.source = n.source "
           "WHEN MATCHED THEN UPDATE SET s.version = n.version, s.table_id = n.table_id, "
           "s.key_columns = coalesce(n.key_columns, s.key_columns), s.updated_at = current_timestamp() "
           "WHEN NOT MATCHED THEN INSERT (target_table, source, version, table_id, key_columns, updated_at) "
           "VALUES (n.target_table, n.source, n.version, n.table_id, n.key_columns, current_timestamp())")
    _shared(spark, sql)


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
                until: int) -> str:
    """One source's rows for a MERGE: the whole table at `until` (since=None), or its changes since `since` (each key's
    last change, update pre-images dropped), with the source column and the change in `__lfc_change`."""
    lit = spark_string(source)
    data = ", ".join(_q(c) for c in cols)
    if since is None:
        # pinned to the version saved in the state, so a commit in between isn't applied twice
        return (f"SELECT {lit} AS {_q(source_column)}, {data}, 'insert' AS `__lfc_change` "
                f"FROM {src} VERSION AS OF {int(until)}")
    part = ", ".join(_q(k) for k in key)
    return (f"SELECT {lit} AS {_q(source_column)}, {data}, _change_type AS `__lfc_change` FROM ("
            # each key's last change; in one version a delete and an insert of a key is a replacement: the insert
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY _commit_version DESC, "
            "CASE _change_type WHEN 'delete' THEN 1 ELSE 0 END) AS `__lfc_n` "
            f"FROM table_changes({spark_string(src.replace('`', ''))}, {int(since) + 1}, {int(until)}) "
            "WHERE _change_type != 'update_preimage') WHERE `__lfc_n` = 1")


def merge_into(tgt: str, rows: str, source_column: str, key: list[str], cols: list[str],
               whole_source: str | None = None) -> str:
    """The MERGE of `rows` (changes_sql, or a UNION ALL of several sources') keyed by the source column and the key.
    whole_source: a whole copy of that source, whose rows the copy lacks are deleted in the same statement."""
    names = [source_column] + cols
    on = " AND ".join(f"t.{_q(c)} = c.{_q(c)}" for c in [source_column] + key)
    sets = ", ".join(f"t.{_q(c)} = c.{_q(c)}" for c in cols if c not in key)
    ins_cols = ", ".join(_q(c) for c in names)
    ins_vals = ", ".join(f"c.{_q(c)}" for c in names)
    update = f"WHEN MATCHED THEN UPDATE SET {sets} " if sets else ""
    stale = (f" WHEN NOT MATCHED BY SOURCE AND t.{_q(source_column)} = {spark_string(whole_source)} THEN DELETE"
             if whole_source is not None else "")
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
              context: dict | None = None) -> dict:
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
    target's objects), so a call per table doesn't read them again."""
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
                         ctx.get("cols"))
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


def _inspect(spark, catalog, prefix, table, out_name, s, keys, workspace, state, fresh, cols_known=None) -> dict:
    """One source of one table, read only (metadata: no data, no writes): its columns, key, version, table id, the
    version last merged and what changed since. Run for every source at once (threads)."""
    src = _fq(catalog, f"{prefix}{s}", table)
    saved = state.get((out_name, s))
    cols = [(c, t) for c, t in (cols_known or columns(spark, src)) if c not in CHANGE_COLUMNS]
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
    return {"s": s, "src": src, "cols": cols, "key": key, "now": now, "tid": tid, "last": last,
            "whole": last is None or last < 0 or refreshed, "changed": changed}


def _counts(spark, rows: str, source_column: str) -> dict:
    """{source: (inserted, updated, deleted)} of the change rows about to be merged."""
    got = _rows(spark, f"SELECT {_q(source_column)} AS s, SUM(CASE WHEN `__lfc_change` = 'insert' THEN 1 ELSE 0 END) "
                       "AS i, SUM(CASE WHEN `__lfc_change` = 'update_postimage' THEN 1 ELSE 0 END) AS u, "
                       "SUM(CASE WHEN `__lfc_change` = 'delete' THEN 1 ELSE 0 END) AS d "
                       f"FROM ({rows}) GROUP BY 1")
    return {r.s: (int(r.i or 0), int(r.u or 0), int(r.d or 0)) for r in got}


def _merge_table(spark, catalog, prefix, target, table, out_name, tgt, names, held, state, kinds, existing,
                 fresh_targets, report, source_column, keys, workspace, take_over, logged, run_id, changed=None,
                 cols_known=None):
    """merge_all's work on one table: every source read at once (metadata, threads), then a whole copy for each source
    that needs one, ONE MERGE for all the sources whose changes come from the change feed, and one state write: so a
    table of hundreds of sources costs a few statements, not a few per source (logged: what it copied or merged)."""
    from concurrent.futures import ThreadPoolExecutor
    srcs = [x for x in names if table in held[x]]
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
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(look)))) as ex:
        info = list(ex.map(lambda s: _inspect(spark, catalog, prefix, table, out_name, s, keys, workspace, state,
                                              fresh, (cols_known or {}).get(s, {}).get(table)), look))
    shapes = {tuple(i["cols"]) for i in info}   # names and types: a source holding a column wider widens the target
    for i in (info if len(shapes) > 1 or created else info[:1]) if info else []:   # every source's columns in the target
        have = {} if out_name not in existing else dict(columns(spark, tgt))
        _ensure_target(spark, tgt, out_name in existing, source_column, i["cols"], have)
        existing.add(out_name)
    saves, feed = [], []
    for i in info:
        if i["last"] is not None and i["now"] == i["last"]:
            report[out_name][i["s"]] = {"done": "unchanged", "version": i["now"]}
        elif not i["whole"] and not i["changed"]:   # new versions, no rows written: nothing to MERGE
            saves.append((i["s"], i["now"], i["tid"], i["key"]))
            report[out_name][i["s"]] = {"done": "unchanged", "version": i["now"]}
        elif i["whole"]:
            _copy(spark, tgt, i, source_column, logged, run_id, out_name, report, "copied")
            saves.append((i["s"], i["now"], i["tid"], i["key"]))
        else:
            feed.append(i)
    groups: dict[tuple, list] = {}   # sources with the same key and columns share one MERGE
    for i in feed:
        groups.setdefault((tuple(i["key"]), tuple(c for c, _ in i["cols"])), []).append(i)
    for (key, cols), part in groups.items():
        rows = " UNION ALL ".join(changes_sql(i["src"], i["s"], source_column, list(key), list(cols), i["last"],
                                              i["now"]) for i in part)
        try:
            t0 = time.time()
            n = _counts(spark, rows, source_column)
            spark.sql(merge_into(tgt, rows, source_column, list(key), list(cols)))
        # the change data vacuumed or expired, or the feed off: the error class varies, so each source alone
        except Exception:  # noqa: BLE001
            for i in part:
                try:
                    t1 = time.time()
                    one = changes_sql(i["src"], i["s"], source_column, list(key), list(cols), i["last"], i["now"])
                    c = _counts(spark, one, source_column).get(i["s"], (0, 0, 0))
                    spark.sql(merge_into(tgt, one, source_column, list(key), list(cols)))
                    _done(logged, report, run_id, out_name, i, "merged", c, t1)
                except Exception as e:  # noqa: BLE001
                    _copy(spark, tgt, i, source_column, logged, run_id, out_name, report,
                          f"copied (change feed unreadable: {str(e).splitlines()[0][:100]})")
                saves.append((i["s"], i["now"], i["tid"], i["key"]))
            continue
        for i in part:   # seconds: the shared MERGE's, for every source in it
            _done(logged, report, run_id, out_name, i, "merged", n.get(i["s"], (0, 0, 0)), t0)
            saves.append((i["s"], i["now"], i["tid"], i["key"]))
    _save_states(spark, catalog, target, out_name, saves)
    # sources gone (schema dropped, table removed, left out by `only`): their rows and state go too
    gone = sorted({src for (t, src) in state if t == out_name} - set(srcs))
    if gone and out_name in existing:
        listed = ", ".join(spark_string(g) for g in gone)
        spark.sql(f"DELETE FROM {tgt} WHERE {_q(source_column)} IN ({listed})")
        _shared(spark, f"DELETE FROM {_fq(catalog, target, STATE)} WHERE target_table = {spark_string(out_name)} "
                       f"AND source IN ({listed})")
        for g in gone:
            report[out_name][g] = {"done": "removed"}


def _done(logged, report, run_id, out_name, i, done, counts, started) -> None:
    report[out_name][i["s"]] = {"done": done, "version": i["now"]}
    logged.append((run_id, out_name, i["s"], done.split(" ")[0], None if i["last"] is None or i["last"] < 0
                   else int(i["last"]), int(i["now"]), *counts, round(time.time() - started, 2), None))


def _copy(spark, tgt, i, source_column, logged, run_id, out_name, report, done) -> None:
    """A whole copy of one source (the first read, a full refresh, a recreated table, an unreadable feed)."""
    t0 = time.time()
    cols = [c for c, _ in i["cols"]]
    spark.sql(merge_into(tgt, changes_sql(i["src"], i["s"], source_column, i["key"], cols, None, i["now"]),
                         source_column, i["key"], cols, whole_source=i["s"]))
    _done(logged, report, run_id, out_name, i, done, _last_metrics(spark, tgt), t0)


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
