# Databricks notebook source
# history-snapshot-scd2: keep the versions of a Lakeflow Connect SCD type 2 destination table across a full refresh,
# in a pipeline of its own. A full refresh reloads an SCD type 2 table with one row per live key: the older versions
# and the deleted keys are gone. Each update of this pipeline reads the whole table and AUTO CDC FROM SNAPSHOT keeps
# every version it has seen, keyed by (keys, the version's start):
#   history   every version seen; a version a full refresh dropped is closed here (__END_AT), never deleted. The
#             source's __START_AT / __END_AT are src_start / src_end (AUTO CDC owns those names); a version closing in
#             the source updates src_end in place, not as a new version.
#   versions  one row per version as the source had it: src_start_at, src_end_at; a refresh's reload of an unchanged
#             live row is joined to the version before it; a dropped open version ends where the next one starts.
# The snapshot comes from a function (it runs when the flow runs, not while the pipeline builds its graph): it fails the
# update while a full refresh is reloading the table (a DLT REFRESH after the last snapshot, and no end of the reload
# in the ingestion pipeline's events after it), while it is emptied, and when it is empty after rows were written to it.
# The guards read the source's Delta history with filters and count() only: no rows come back to Python (a collect()
# is flagged as unsupported in a pipeline). A snapshot's version is the time it was taken, in ms; one is taken per
# update, when the source has a commit after the last one.
# Settings: src (the SCD type 2 table, catalog.schema.table), keys (comma-separated key columns, default id),
# allow_empty (true: a snapshot with no rows may close every version; default false: such a run fails), reload_check
# (events, the default: the run-as identity needs to see the ingestion pipeline; history: the Delta history rules
# only, which can't tell a reload written in several commits is unfinished). Run it after each ingestion
# update; versions made and dropped between two of its runs are not seen. `spark` comes from the pipeline runtime.
# ruff: noqa: F821
import re
import time

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

SRC = spark.conf.get("src")
KEYS = [k.strip() for k in spark.conf.get("keys", "id").split(",") if k.strip()]
ALLOW_EMPTY = spark.conf.get("allow_empty", "false").lower() == "true"
RELOAD_CHECK = spark.conf.get("reload_check", "events")
UPDATE_START_MS = int(time.time() * 1000)  # the notebook runs again at the start of each update
SCHEMA = spark.read.table(SRC).schema
DATA = [f.name for f in SCHEMA.fields if f.name not in KEYS and f.name not in ("__START_AT", "__END_AT")]


def part(col, field):
    """__START_AT / __END_AT: a struct (internal value: sortable, timestamp) or a plain value."""
    t = SCHEMA[col].dataType
    if isinstance(t, StructType):
        names = [f.name for f in t.fields]
        inner = "__cdc_internal_value" if field == "seq" else "__cdc_timestamp_value"
        if inner in names:
            return F.col(f"{col}.{inner}")
    return F.col(col).cast("string") if field == "seq" else F.col(col).cast("timestamp")


WRITES = ("MERGE", "WRITE", "STREAMING UPDATE")
ROW_METRICS = ("numOutputRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsCopied")


def count(value):
    """A metric as a number; one that isn't a number is 0 (as TRY_CAST in WROTE_ROWS)."""
    try:
        return int(value or 0)
    except ValueError:
        return 0


def wrote_rows(metrics):
    """A write's operationMetrics show rows; no metrics at all counts as rows (older tables don't record them)."""
    if not metrics:
        return True
    return any(count(metrics.get(k)) > 0 for k in ROW_METRICS)


def refresh_in_progress(ops):
    """ops: (version, operation, operationMetrics) from DESCRIBE HISTORY. A full refresh empties the table (DLT REFRESH)
    before it reloads it: True while the newest DLT REFRESH has no write with rows after it."""
    refresh = max((v for v, op, _ in ops if op == "DLT REFRESH"), default=None)
    if refresh is None:
        return False
    return not any(v > refresh and op in WRITES and wrote_rows(m) for v, op, m in ops)


def empty_after_rows(snapshot_rows, ops, allow_empty):
    """A snapshot with no rows of a table that had rows written (in the history read) would close every version:
    refuse it unless allow_empty says the table is meant to be empty. Reads the source's history, not this pipeline's
    own tables (no cycle in the graph)."""
    had_rows = any(op in WRITES and wrote_rows(m) for _, op, m in ops)
    return snapshot_rows == 0 and had_rows and not allow_empty


# the same rules as refresh_in_progress / empty_after_rows above (the offline tests check those), on the history DataFrame
WROTE_ROWS = ("(operationMetrics IS NULL OR size(operationMetrics) <= 0 OR "
              + " OR ".join(f"coalesce(TRY_CAST(operationMetrics['{k}'] AS BIGINT), 0) > 0" for k in ROW_METRICS) + ")")


def source_state():
    """(refresh_in_progress, had_rows) from the source's last 50 commits, with filters and count() only."""
    h = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 50")
    refreshes = h.where("operation = 'DLT REFRESH'")
    writes = h.where(F.col("operation").isin(*WRITES) & F.expr(WROTE_ROWS))
    if refreshes.limit(1).count() == 0:
        return False, writes.limit(1).count() > 0
    newest = refreshes.agg(F.max("version").alias("refresh_version"))
    reloaded = writes.crossJoin(newest).where("version > refresh_version").limit(1).count() > 0
    return not reloaded, writes.limit(1).count() > 0


def committed_since(latest_ms):
    """True when the source has a commit after the last snapshot (its version is the time it was taken, in ms)."""
    h = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 50")
    return h.where(F.expr(f"unix_millis(timestamp) > {int(latest_ms)}")).limit(1).count() > 0


def reload_done(message, src):
    """An ingestion pipeline event that says a full refresh of src has been applied (the lifecycle event, or the
    snapshot progress event where the lifecycle events are off)."""
    # names compared without backticks: the event may quote the name (`catalog`.`schema`.`table`) or not
    named = re.search(rf"(?<![\w.]){re.escape(src.replace('`', '').lower())}(?!\w)", message.replace("`", "").lower())
    return bool(named) and ("Full Refresh completed for dataset" in message or
                            ("Snapshot of" in message and "APPLIED" in message))


def completed_ms(since_ms):
    """The newest time the source's ingestion pipeline logged the end of a full refresh of the table, looking back to
    since_ms (the last snapshot) and at most 10000 events; None when there is none. A full refresh reloads the table in
    one commit or several, and nothing in the table marks the end: only the ingestion pipeline's events do."""
    from datetime import datetime

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    pid = w.tables.get(SRC).pipeline_id
    if not pid:
        raise RuntimeError(f"{SRC} has no ingestion pipeline: set reload_check=history to guard by its Delta history only")
    for n, e in enumerate(w.pipelines.list_pipeline_events(pipeline_id=pid, max_results=250)):
        ts = int(datetime.fromisoformat((e.timestamp or "").replace("Z", "+00:00")).timestamp() * 1000)  # noqa: FURB162  (Python < 3.11)
        if (since_ms is not None and ts < since_ms) or n >= 10000:
            return None
        if reload_done(e.message or "", SRC):
            return ts
    return None


def reload_pending(latest_ms):
    """True while a full refresh newer than the last snapshot has no end event after it (fails closed: no end event
    found counts as not finished)."""
    h = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 50").where(
        F.expr(f"operation = 'DLT REFRESH' AND unix_millis(timestamp) > {int(latest_ms)}"))
    if h.limit(1).count() == 0:
        return False
    done = completed_ms(latest_ms)
    return done is None or h.where(F.expr(f"unix_millis(timestamp) > {done}")).limit(1).count() > 0


def next_snapshot(latest_version):
    """The snapshot source: (the table's rows, the time in ms), or None to take no snapshot this run. The flow calls
    it until it returns None: one snapshot per update (a snapshot's version is the time it was taken), and after the
    first, only when the source has a commit after the last snapshot. The first snapshot skips the event check: the
    history is empty, so there is nothing to close (the table-history rules still apply)."""
    if latest_version is not None and (latest_version >= UPDATE_START_MS or not committed_since(latest_version)):
        return None
    if RELOAD_CHECK == "events" and latest_version is not None and reload_pending(latest_version):
        raise RuntimeError(f"{SRC} is being reloaded by a full refresh (no end of the reload in its ingestion pipeline's "
                           "events yet): run this pipeline again after the ingestion update finishes")
    in_refresh, had_rows = source_state()
    # the table-history rule: for reload_check=history and the first snapshot (after that, the events say whether the
    # reload finished, and a refresh of an empty source writes no rows: this rule would never let it through)
    by_history = RELOAD_CHECK != "events" or latest_version is None
    if in_refresh and by_history and not ALLOW_EMPTY:
        raise RuntimeError(f"{SRC} is being reloaded by a full refresh (DLT REFRESH, no write with rows yet): "
                           "run this pipeline again after the ingestion update finishes")
    table = spark.read.table(SRC)
    if table.limit(1).count() == 0 and had_rows and not ALLOW_EMPTY:
        raise RuntimeError(f"{SRC} is empty after rows were written to it: a snapshot would close every version. "
                           "Run again once the table is loaded, or set allow_empty=true if it is meant to be empty")
    now_ms = max(int(time.time() * 1000), (latest_version or 0) + 1)
    return (table.select(
        *KEYS, *DATA,
        part("__START_AT", "seq").alias("src_start"), part("__START_AT", "ts").alias("src_start_ts"),
        part("__END_AT", "seq").alias("src_end"), part("__END_AT", "ts").alias("src_end_ts")), now_ms)


# reset not allowed: a full refresh of this pipeline would start the history over from the current snapshot
dlt.create_streaming_table("history", table_properties={"pipelines.reset.allowed": "false"})
dlt.apply_changes_from_snapshot(
    target="history", source=next_snapshot, keys=[*KEYS, "src_start"], stored_as_scd_type=2,
    track_history_except_column_list=["src_end", "src_end_ts"])


@dlt.table(name="versions", comment="one row per source version, across full refreshes of the source table")
def versions():
    h = spark.read.table("history")
    # __START_AT / __END_AT here are snapshot versions: the source table's commit time in ms
    seen_at, gone_at = F.timestamp_millis(F.col("__START_AT")), F.timestamp_millis(F.col("__END_AT"))
    dropped = F.col("__END_AT").isNotNull()          # gone from the source table: a full refresh removed it
    # (its end stays the source's when the source recorded one: dropped_by_refresh says the row left the table, not when)
    # A row with no source start (src_start empty) came from a snapshot: the first load (before every CDC version of
    # its key) or a full refresh's reload (after every version the refresh dropped: a drop of this key at or before
    # the time this pipeline first saw the row). The reload is placed at that time.
    first_drop = F.min(F.when(dropped, F.col("__END_AT"))).over(Window.partitionBy(*KEYS))
    snap = F.coalesce(F.col("src_start"), F.lit("")) == ""
    h = h.withColumn("order_ts", F.when(~snap, F.col("src_start_ts"))
                     .when(first_drop <= F.col("__START_AT"), seen_at)
                     .otherwise(F.lit("1900-01-01").cast("timestamp")))
    w = Window.partitionBy(*KEYS).orderBy("order_ts", "src_start")
    v = (h.withColumn("dh", F.xxhash64(*[F.col(c) for c in DATA]) if DATA else F.lit(0))
          .withColumn("next_start_ts", F.lead("order_ts").over(w))
          .withColumn("prev_dh", F.lag("dh").over(w))
          .withColumn("prev_open_dropped", F.lag(dropped & F.col("src_end").isNull()).over(w)))
    # a refresh reloads a live row as a new version with the same values: it continues the dropped open one before it
    v = v.withColumn("continues", F.coalesce(F.col("prev_open_dropped") & (F.col("dh") == F.col("prev_dh")), F.lit(False)))
    v = v.withColumn("grp", F.sum(F.when(F.col("continues"), 0).otherwise(1)).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    # the end of a version: the source's, else (dropped while open) where the next one starts, else when it was dropped
    v = v.withColumn("end_ts", F.coalesce(F.col("src_end_ts"), F.when(dropped, F.coalesce(F.col("next_start_ts"), gone_at))))
    last = Window.partitionBy(*KEYS, "grp").orderBy(F.col("order_ts").desc(), F.col("src_start").desc())
    # a version's start: the source's; a reload's (no source start): where the dropped version before it ended
    start = F.when(F.col("order_ts") > F.lit("1900-01-01").cast("timestamp"), F.col("order_ts"))
    v = v.withColumn("rn", F.row_number().over(last)).withColumn("first_start_ts", F.min(start).over(Window.partitionBy(*KEYS, "grp")))
    return (v.where("rn = 1")
             .select(*KEYS, *DATA, F.col("first_start_ts").alias("src_start_at"), F.col("end_ts").alias("src_end_at"),
                     dropped.alias("dropped_by_refresh")))
