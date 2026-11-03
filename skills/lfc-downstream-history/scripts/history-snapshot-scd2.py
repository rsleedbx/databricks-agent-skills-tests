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
# Settings: src (the SCD type 2 table), keys (comma-separated key columns, default id). Run it after each ingestion
# update; versions made and dropped between two of its runs are not seen. `spark` comes from the pipeline runtime.
# ruff: noqa: F821
import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

SRC = spark.conf.get("src")
KEYS = [k.strip() for k in spark.conf.get("keys", "id").split(",") if k.strip()]
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


def refresh_in_progress():
    """A full refresh empties the table (DLT REFRESH) before it reloads it (MERGE): a snapshot read in between would
    close every version. True when the newest DLT REFRESH has no data write after it."""
    ops = spark.sql(f"DESCRIBE HISTORY {SRC} LIMIT 50").select("version", "operation").collect()
    refresh = max((r.version for r in ops if r.operation == "DLT REFRESH"), default=None)
    writes = max((r.version for r in ops if r.operation in ("MERGE", "WRITE", "STREAMING UPDATE")), default=None)
    return refresh is not None and (writes is None or writes < refresh)


@dlt.view(name="src_versions")
def src_versions():
    if refresh_in_progress():
        raise RuntimeError(f"{SRC} is being reloaded by a full refresh (DLT REFRESH, no MERGE yet): "
                           "run this pipeline again after the ingestion update finishes")
    return spark.read.table(SRC).select(
        *KEYS, *DATA,
        part("__START_AT", "seq").alias("src_start"), part("__START_AT", "ts").alias("src_start_ts"),
        part("__END_AT", "seq").alias("src_end"), part("__END_AT", "ts").alias("src_end_ts"))


dlt.create_streaming_table("history")
dlt.apply_changes_from_snapshot(
    target="history", source="src_versions", keys=[*KEYS, "src_start"], stored_as_scd_type=2,
    track_history_except_column_list=["src_end", "src_end_ts"])


@dlt.table(name="versions", comment="one row per source version, across full refreshes of the source table")
def versions():
    h = spark.read.table("history")
    dropped = F.col("__END_AT").isNotNull()          # gone from the source table: a full refresh dropped it
    # A row with no source start (src_start empty) came from a snapshot: the first load (before every CDC version of
    # its key) or a full refresh's reload (after every version the refresh dropped: a drop of this key at or before
    # the time this pipeline first saw the row). The reload is placed at that time.
    first_drop = F.min(F.when(dropped, F.col("__END_AT"))).over(Window.partitionBy(*KEYS))
    snap = F.coalesce(F.col("src_start"), F.lit("")) == ""
    h = h.withColumn("order_ts", F.when(~snap, F.col("src_start_ts"))
                     .when(first_drop <= F.col("__START_AT"), F.col("__START_AT").cast("timestamp"))
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
    v = v.withColumn("end_ts", F.coalesce(F.col("src_end_ts"), F.when(dropped, F.coalesce(F.col("next_start_ts"), F.col("__END_AT").cast("timestamp")))))
    last = Window.partitionBy(*KEYS, "grp").orderBy(F.col("order_ts").desc(), F.col("src_start").desc())
    # a version's start: the source's; a reload's (no source start): where the dropped version before it ended
    start = F.when(F.col("order_ts") > F.lit("1900-01-01").cast("timestamp"), F.col("order_ts"))
    v = v.withColumn("rn", F.row_number().over(last)).withColumn("first_start_ts", F.min(start).over(Window.partitionBy(*KEYS, "grp")))
    return (v.where("rn = 1")
             .select(*KEYS, *DATA, F.col("first_start_ts").alias("src_start_at"), F.col("end_ts").alias("src_end_at"),
                     dropped.alias("dropped_by_refresh")))
