# silver-from-schema.py: a Lakeflow Spark Declarative Pipelines source file that builds silver from the canonical
# schema (statschema's schema.yaml, from the lfc-source-statschema skill) in a Unity Catalog Volume. The one generator
# for every source, every model and every bronze layout: nothing in it names a table, a column or a type.
#
# Sources: <volume>/_sources.yaml lists every source database (host) feeding this silver, each with the change log's
# destination prefix (destination = <prefix>.<table>, e.g. crdb1.public.orders), its T, and its snapshot format. The
# same logical table from many sources merges into one silver table; with source_key: true the key is (_source, the
# primary key), so identical keys from different hosts stay apart, and each key's changes are ordered within its own
# source only. Every silver row carries _source (the destination prefix).
#
# For every table in <volume>/_schema/schema.yaml (or _schema/tables/<table>.yaml, which wins: one table's evolved
# schema, written without touching the others) it defines, when the pipeline starts:
#   <table>_snapshot        every source's snapshot as of its T, from <volume>/<table>/<prefix>/ (Parquet or CSV); a
#                           table with no files there (CockroachDB writes none for an empty table) has an empty one.
#                           <volume>/_snapshot.tsv (table, prefix, format, files), when there is one, says which do,
#                           so 10,000s of folders aren't listed when the pipeline starts
#   <table>_changes         the table's events in the change log, sequenced by the event's position in its source
#   <table>_changes_dedup   the same, producer replays removed
#   <table>                 current rows (AUTO CDC, SCD type 1)
#   <table>_history         every version (AUTO CDC, SCD type 2), replays removed; the suffix is cdc.history_suffix
#                           (default _history; TPC-E has both holding and holding_history, so there it can't be)
# and, for the table in cdc.nodedup_table, <table>_history_nodedup (SCD type 2 with the replays). A table without a
# primary key (TPC-B's and TPC-C's history) is append-only: <table> gets the snapshot rows and the inserts, appended.
# Dates, timestamps and times are decoded from either form a producer sends: text (CockroachDB's changefeed, the
# snapshot) or a number (Debezium's PostgreSQL connector: days, epoch milli- or microseconds, microseconds of the day).
# A time is kept as text, HH:mm:ss.SSSSSS.
#
# Schema drift (cdc.on_drift = record, the default): silver keeps flowing on the columns it knows. A value that doesn't
# fit its column's type becomes NULL, a column that's gone gets its default (or NULL), a new column waits; and each one
# is recorded once per (table, source, kind, column) in schema_drift (kind: type, missing, new; with the last position
# and a sample). Evolution is asynchronous and per table: write that table's new schema to _schema/tables/<table>.yaml
# and full-refresh it (the change log is append-only VARIANT, so it catches up from bronze); the other tables never
# stop, and neither does the change log: a new column or a new table needs no DDL there. cdc.on_drift = fail casts
# strictly, so an update stops at the first value that doesn't fit.
#
# Pipeline configuration:
#   cdc.volume            /Volumes/<catalog>/<schema>/<volume>
#   cdc.bronze            the change log's shape: zerobus (Debezium Server's sink) or kafka (Kafka Connect, Kafka API)
#   cdc.bronze_table      the change-log table each silver table reads: cdc_log (one log for every table, the default),
#                         cdc_log_{table} (one per table) or cdc_log_s{shard} (cdc.bronze_shards logs, by table)
#   cdc.bronze_table.<t>  one table's own bronze table, over cdc.bronze_table: a table moved out of the shared log
#                         (fanout.py --tables <t>, or a producer routing it), while the others stay; changing it
#                         takes a full refresh of that table, from a bronze table that holds its history from T
#   cdc.bronze_shards     for {shard}: how many (fanout.py writes them with the same rule)
#   cdc.shard             i/n: this pipeline builds only the tables with crc32(name) % n == i, so 10,000s of tables
#                         spread over n pipelines (default: every table)
#   cdc.history_suffix    the SCD type 2 tables' suffix (default _history); a source table with that name stops the start
#   cdc.nodedup_table, cdc.on_drift (record | fail), cdc.drift_sample (check one event in n for drift, default 1)
#   cdc.seq               what orders a key's events: ts_ns (the event's time, the default) or binlog (MySQL: the binlog
#                         file's number, the event's position in it and the row in the event, (file << 44) + (pos << 12) +
#                         row, exact and the same for a replay; a source's T is the snapshot's position in that form)
#                         or lsn (PostgreSQL: the change's LSN; a source's T is its replication slot's), or change_lsn
#                         (SQL Server: source.change_lsn's file, offset and slot as one number; T is the log's last LSN),
#                         or scn (Oracle: the change's SCN; T is the database's current SCN), or db2_lsn (Db2: the low 8
#                         bytes of source.change_lsn, the third of its parts; T is the ASN capture's synch point), or
#                         ifx_lsn (Informix: source.change_lsn, a number; T is 0, Debezium starting after the snapshot)
#   cdc.binary            hex: binary values come as hex text (Debezium's binary.handling.mode=hex, and the snapshot)
#   cdc.fk_actions        apply: the change log lacks the rows foreign keys' ON DELETE CASCADE / SET NULL change (MySQL
#                         through 9.5, MariaDB), so silver derives them from each parent delete and the schema's
#                         on_delete, into the child's targets and fk_actions (the trace); logged (default): it has them
#   cdc.fk_depth          how many levels of derived deletes to follow (chains, a table referencing itself; default 4)
# A source in _sources.yaml may carry csv: {option: value} for its CSV snapshot files (MySQL's INTO OUTFILE writes NULL
# as \N and escapes with a backslash).
#
# The table plan: <volume>/_plan.yaml, when there is one (cdc-table-plan's classify.py, checked and edited by the
# customer), says what each table gets: silver: [scd1] (current rows), [scd2] (history only), [scd1, scd2], or [append]
# (the snapshot rows and the inserts, appended: no MERGE; an update or delete of such a table is left out, and the
# harness counts them). A table it doesn't list gets the default: scd1 and scd2 with a key, append without one.
import functools
import os
import re
import sys

import yaml
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

VOL = spark.conf.get("cdc.volume")
BRONZE = spark.conf.get("cdc.bronze", "zerobus")
BRONZE_TABLE = spark.conf.get("cdc.bronze_table", "cdc_log")
BRONZE_SHARDS = int(spark.conf.get("cdc.bronze_shards", "1"))
SHARD = spark.conf.get("cdc.shard", "")
NODEDUP = spark.conf.get("cdc.nodedup_table", "")
ON_DRIFT = spark.conf.get("cdc.on_drift", "record")
DRIFT_SAMPLE = int(spark.conf.get("cdc.drift_sample", "1"))
TRY = ON_DRIFT == "record"
HIST = spark.conf.get("cdc.history_suffix", "_history")
SEQ = spark.conf.get("cdc.seq", "ts_ns")
BINHEX = spark.conf.get("cdc.binary", "") == "hex"
# canonical_sql.py sits next to this file: the pipeline's folder first on sys.path (cdc.code when there's no __file__)
sys.path.insert(0, os.path.dirname(globals().get("__file__") or "") or spark.conf.get("cdc.code"))
from canonical_sql import load, path, shard_of, snapshot_value, spark_type, typed  # noqa: E402
SEQ_EXPR = {"ts_ns": "value:ts_ns::BIGINT",
            "binlog": "CAST(regexp_extract(value:source:file::STRING, '([0-9]+)$', 1) AS BIGINT) * 17592186044416"
                      " + value:source:pos::BIGINT * 4096 + COALESCE(value:source:row::BIGINT, 0)",
            "lsn": "value:source:lsn::BIGINT",
            "scn": "value:source:scn::BIGINT",
            "ifx_lsn": "value:source:change_lsn::BIGINT",
            "db2_lsn": "CAST(conv(substring(split_part(COALESCE(value:source:change_lsn, value:source:commit_lsn)::STRING, ':', 3), -15), 16, 10) AS BIGINT)",
            "change_lsn": "CAST(conv(split_part(value:source:change_lsn::STRING, ':', 1), 16, 10) AS BIGINT) * 281474976710656 + CAST(conv(split_part(value:source:change_lsn::STRING, ':', 2), 16, 10) AS BIGINT) * 65536 + CAST(conv(split_part(value:source:change_lsn::STRING, ':', 3), 16, 10) AS BIGINT)"}[SEQ]
GROUP = spark.conf.get("cdc.placement_group", "")   # composite: only this group's tables, from <volume>/_placement.tsv
FK_ACTIONS = spark.conf.get("cdc.fk_actions", "logged")   # apply: the log lacks what foreign keys' actions change
FK_DEPTH = int(spark.conf.get("cdc.fk_depth", "4"))

with open(f"{VOL}/_sources.yaml") as f:
    _S = yaml.safe_load(f)
SOURCES = _S["sources"]                     # [{prefix, t_ns, format}]
SOURCE_KEY = bool(_S.get("source_key", len(SOURCES) > 1))
SCHEMA = load(VOL)
if SHARD:
    _i, _n = (int(x) for x in SHARD.split("/"))
    SCHEMA = {t: c for t, c in SCHEMA.items() if shard_of(t, _n) == _i}
OWN = set()
if GROUP:
    with open(f"{VOL}/_placement.tsv") as f:
        _p = [r.rstrip("\n").split("\t") for r in f if r.strip()]
    SCHEMA = {t: c for t, c in SCHEMA.items() if any(r[0] == t and r[1] == GROUP for r in _p)}
    OWN = {r[0] for r in _p if r[1] == GROUP and r[2] == "own"}   # own bronze: cdc_log_<table>, from the fan-out
_clash = sorted(t for t in SCHEMA if (t + HIST).lower() in {x.lower() for x in SCHEMA})
if _clash:
    raise ValueError(f"cdc.history_suffix {HIST}: {', '.join(t + HIST for t in _clash[:5])} would be a source table too; set another")
PLAN = {}
if os.path.exists(f"{VOL}/_plan.yaml"):
    with open(f"{VOL}/_plan.yaml") as f:
        PLAN = {t: set(v.get("silver") or []) for t, v in ((yaml.safe_load(f) or {}).get("tables") or {}).items()}
_bad = sorted(t for t, v in PLAN.items() if t in SCHEMA and (not v or v - {"scd1", "scd2", "append"} or "append" in v and len(v) > 1
              or v & {"scd1", "scd2"} and not any(c.get("primary_key") for c in SCHEMA[t])))
if _bad:
    raise ValueError(f"_plan.yaml: silver must be scd1 and/or scd2 (with a key), or append alone: {', '.join(_bad[:5])}")
DRIFT = "schema_drift" + (f"_s{SHARD.replace('/', '_of_')}" if SHARD else "")
FKLOG = "fk_actions" + (f"_s{SHARD.replace('/', '_of_')}" if SHARD else "")
_views = set()
INFO = {}   # per table: cols, pk, keys, targets [(name, scd type)], versions (a function: the versions as of now)
FK_VIEWS = set()   # the tables with derived foreign key changes (<table>_fk), filled before any view is evaluated


def _snapshot_files():
    """{(table, format)} with snapshot files: from _snapshot.tsv, else by listing the volume."""
    m = f"{VOL}/_snapshot.tsv"
    if os.path.exists(m):
        with open(m) as f:
            return {(r[0], r[2]) for r in (line.rstrip("\n").split("\t") for line in f) if len(r) == 4 and int(r[3]) > 0}
    out = set()
    for s in SOURCES:
        fmt = s.get("format", "parquet")
        for table in SCHEMA:
            d = f"{VOL}/{table}/{s['prefix']}"
            if os.path.isdir(d) and any(n.endswith(f".{fmt}") for n in os.listdir(d)):
                out.add((table, fmt))
    return out


SNAPSHOT_FILES = _snapshot_files()


def bronze_of(table):
    default = "cdc_log_{table}" if table in OWN else BRONZE_TABLE
    return spark.conf.get(f"cdc.bronze_table.{table}", default).format(table=table, shard=shard_of(table, BRONZE_SHARDS))


def norm_view(bronze):
    """The change log in one shape for both sinks: destination, operation, idempotency_key, key, value, _seq, _event_ns.
    Kafka Connect's records carry only the event: the operation comes from op, the key from after or before, and the
    sequence from the slot's LSN (commit order, within its source). One view per bronze table, shared by its tables."""
    name = f"_norm_{bronze}"
    if name in _views:
        return name
    _views.add(name)

    dp.temporary_view(name=name)(lambda: normed(spark.readStream.table(bronze)))

    @dp.temporary_view(name=f"_dedup_{bronze}")
    def dedup():
        # a replay has the same key and the same row image as the original; the key alone may be shared by two changes
        return (spark.readStream.table(name)
                .withColumn("_event_time", F.timestamp_micros(F.expr("_event_ns DIV 1000")))
                .withColumn("_after", F.expr("TO_JSON(value:after)"))
                .withWatermark("_event_time", "10 minutes")
                .dropDuplicatesWithinWatermark(["destination", "idempotency_key", "operation", "_after"])
                .drop("_event_time", "_after"))
    return name


def normed(log):
    """The change log (a stream, or a table read as of now) in norm_view's shape."""
    if BRONZE == "kafka":
        return log.select(
            "destination",
            F.expr("CASE value:op::STRING WHEN 'c' THEN 'create' WHEN 'u' THEN 'update' WHEN 'd' THEN 'delete' "
                   "WHEN 'r' THEN 'read' END").alias("operation"),
            F.expr("value:source:lsn::STRING").alias("idempotency_key"),   # none from CockroachDB: replays match on the row
            F.expr("CASE value:op::STRING WHEN 'd' THEN value:before ELSE value:after END").alias("key"), "value",
            F.expr("COALESCE(value:source:lsn::BIGINT, value:ts_ns::BIGINT)").alias("_seq"), F.expr("value:ts_ms::BIGINT * 1000000").alias("_event_ns"))
    return log.withColumn("_seq", F.expr(SEQ_EXPR)).withColumn("_event_ns", F.expr("value:ts_ns::BIGINT"))


def define(table, cols):
    pk = [c["name"] for c in cols if c.get("primary_key")]
    keys = (["_source"] + pk) if SOURCE_KEY and pk else pk
    names = [c["name"] for c in cols]
    norm = norm_view(bronze_of(table))
    # A column added after T is in neither the snapshot nor the changes made before it was added. A PostgreSQL-protocol
    # source doesn't resend old rows when a column is added, so they hold the column's default (a SQL literal in
    # schema.yaml): use it where the field is missing (SQL NULL from the path), not where it's a JSON null.
    def dflt(c):
        return c.get("default") or "NULL"

    def get(base, c, strict=False):   # a field of the event, as the column's type (record: NULL where it doesn't fit)
        return typed(c, path(base, c["name"]), "TRY_CAST" if TRY and not strict else "CAST", BINHEX)

    def change(c):
        n, ty = c["name"], spark_type(c)
        if n in pk:
            return get("key", c, strict=True)
        if c.get("default"):
            return (f"CASE WHEN value:after IS NOT NULL AND NOT is_variant_null(value:after) AND {path('value:after', n)} IS NULL "
                    f"THEN CAST({c['default']} AS {ty}) ELSE {get('value:after', c)} END")
        return get("value:after", c)

    chg_cols = [F.expr(change(c)).alias(c["name"]) for c in cols]
    # the source: everything in destination before the table's name (crdb1.public.orders -> crdb1.public)
    src = F.expr(f"substring(destination, 1, length(destination) - {len(table) + 1})").alias("_source")
    mine = F.expr(f"substring_index(destination, '.', -1) = '{table}'")

    def snapshot_rows(stream):
        """Every source's snapshot rows as of its T (_seq) or None: one Auto Loader stream per format over
        <volume>/<table>/<prefix>/ (stream), or the files read as they are (the foreign key actions' versions)."""
        parts = []
        kinds = {(s.get("format", "parquet"), tuple(sorted((s.get("csv") or {}).items()))) for s in SOURCES
                 if (table, s.get("format", "parquet")) in SNAPSHOT_FILES}
        for fmt, opts in sorted(kinds):
            r = (spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt) if stream
                 else spark.read.format(fmt).option("recursiveFileLookup", "true")).option("pathGlobFilter", f"*.{fmt}")
            if fmt == "csv":
                r = r.option("header", "true")
                for k, v in opts:
                    r = r.option(k, str(v).lower() if isinstance(v, bool) else str(v))
            # the sources of this kind only, when the kinds differ (each source's files are under its own folder)
            mine_src = [s["prefix"] for s in SOURCES if (s.get("format", "parquet"), tuple(sorted((s.get("csv") or {}).items()))) == (fmt, opts)]
            df = r.load(f"{VOL}/{table}")
            have = set(df.columns)
            parts.append(df.select(*[F.expr(snapshot_value(c, have, BINHEX)).alias(c["name"]) for c in cols],
                                   F.regexp_extract(F.col("_metadata.file_path"), f"/{re.escape(table)}/([^/]+)/", 1)
                                   .alias("_source")).where(F.col("_source").isin(mine_src)))
        if not parts:
            return None
        t = spark.createDataFrame([(s["prefix"], int(s["t_ns"])) for s in SOURCES], "_source STRING, _seq BIGINT")
        return (functools.reduce(lambda a, b: a.unionByName(b), parts).join(F.broadcast(t), "_source")
                .select(*names, "_source", "_seq", F.lit("read").alias("_op")))

    @dp.temporary_view(name=f"{table}_snapshot")
    def snapshot():
        rows = snapshot_rows(True)
        if rows is None:   # no snapshot files: no rows at T; an empty stream of the view's columns (no join: it folds away)
            return spark.readStream.table(norm).where("false").select(
                *[F.expr(f"CAST(NULL AS {spark_type(c)})").alias(c["name"]) for c in cols], F.lit(None).cast("string").alias("_source"),
                F.lit(0).cast("bigint").alias("_seq"), F.lit("read").alias("_op"))
        return rows

    def changes(view):
        df = spark.readStream.table(view).where(mine).select(*chg_cols, src, F.col("_seq"), F.col("operation").alias("_op"))
        if table in FK_VIEWS:   # the changes its foreign keys' ON DELETE made, which the log lacks (cdc.fk_actions)
            df = df.unionByName(spark.readStream.table(f"{table}_fk").select(*names, "_source", "_seq", "_op"))
        return df

    # a TIMESTAMP_NTZ column needs its Delta table feature enabled (an append-only table's create fails without it)
    props = {"delta.feature.timestampNtz": "supported"} if any(spark_type(c) == "TIMESTAMP_NTZ" for c in cols) else {}
    dp.temporary_view(name=f"{table}_changes")(lambda: changes(norm))
    if TRY:
        drift(table, cols, pk, norm, mine, src)
    want = PLAN.get(table) or ({"scd1", "scd2"} if pk else {"append"})

    def versions():   # every version of each row, from T, as of now: its _seq and the next one's (_next)
        ch = normed(spark.read.table(bronze_of(table))).where(mine).select(*chg_cols, src, "_seq", F.col("operation").alias("_op"))
        rows = snapshot_rows(False)
        v = (ch if rows is None else rows.unionByName(ch)).dropDuplicates(["_source", *pk, "_seq"])
        return v.withColumn("_next", F.lead("_seq").over(Window.partitionBy("_source", *pk).orderBy("_seq")))
    INFO[table] = {"cols": cols, "pk": pk, "keys": keys, "targets": [], "versions": versions}
    if "append" in want:   # append-only: the snapshot rows, then each insert (a table without a key gets no updates or deletes)
        dp.create_streaming_table(name=table, table_properties=props)
        dp.append_flow(target=table, name=f"{table}_from_snapshot")(   # not once: a once append flow takes a batch only
            lambda: spark.readStream.table(f"{table}_snapshot").drop("_seq", "_op"))
        # the inserts with producer replays removed (the dedup view): without a key, a replay would be a second row.
        # Two identical rows inserted within the dedup watermark count as one
        dp.temporary_view(name=f"{table}_changes_dedup")(lambda: changes(f"_dedup_{bronze_of(table)}"))
        # only inserts after the source's T: a producer started before the snapshot (MySQL's) also sends rows it has
        after_t = " ".join(f"WHEN '{s['prefix']}' THEN {int(s['t_ns'])}" for s in SOURCES)
        dp.append_flow(target=table, name=f"{table}_from_changes")(
            lambda: spark.readStream.table(f"{table}_changes_dedup").where("_op = 'create'")
            .where(f"_seq > CASE _source {after_t} ELSE -1 END").drop("_seq", "_op"))
        return
    dp.temporary_view(name=f"{table}_changes_dedup")(lambda: changes(f"_dedup_{bronze_of(table)}"))

    # clustered by the key, but for the columns Delta can't skip on (a BINARY key: Mautic's cache_items; BOOLEAN)
    ctype_of = {c["name"]: spark_type(c) for c in cols}
    ckeys = [k for k in keys if ctype_of.get(k, "STRING") not in ("BINARY", "BOOLEAN")]
    # Delta keeps stats for the first 32 columns, and clustering needs them for its columns: with many sources the key
    # starts with _source, the last column, so a wide table (Mautic's emails) names its stats columns itself
    order = names + ["_source"]
    if any(order.index(k) >= 32 for k in ckeys if k in order):
        rest = [n for n in order if n not in ckeys and ctype_of.get(n, "STRING") not in ("BINARY", "BOOLEAN")]   # no stats for those
        props = dict(props, **{"delta.dataSkippingStatsColumns": ",".join(f"`{n}`" for n in ckeys + rest[:32 - len(ckeys)])})
    targets = ([(table, 1, f"{table}_changes")] if "scd1" in want else []) \
        + ([(f"{table}{HIST}", 2, f"{table}_changes_dedup")] if "scd2" in want else [])
    if table == NODEDUP:
        targets.append((f"{table}{HIST}_nodedup", 2, f"{table}_changes"))
    INFO[table]["targets"] = [(target, scd) for target, scd, _ in targets]
    for target, scd, change_view in targets:
        dp.create_streaming_table(name=target, cluster_by=ckeys or None, table_properties=props)
        # once: the snapshot is loaded on the first update only (and again on a full refresh), so later updates don't
        # start a snapshot stream per table
        dp.create_auto_cdc_flow(name=f"{target}_from_snapshot", target=target, source=f"{table}_snapshot", keys=keys,
                                sequence_by="_seq", except_column_list=["_seq", "_op"], stored_as_scd_type=scd, once=True)
        dp.create_auto_cdc_flow(name=f"{target}_from_changes", target=target, source=change_view, keys=keys,
                                sequence_by="_seq", apply_as_deletes=F.expr("_op = 'delete'"),
                                except_column_list=["_seq", "_op"], stored_as_scd_type=scd)


def drift(table, cols, pk, norm, mine, src):
    """What doesn't fit the table's schema, from its events (one in cdc.drift_sample): a value its type can't hold
    (type), a known column without a default missing from the row (missing: dropped at the source), a column the
    schema doesn't have (new). AUTO CDC keeps one row per (table, source, kind, column): the last position, a sample."""
    items = []
    for c in cols:
        if c["name"] in pk:
            continue
        v = path("value:after", c["name"])
        items.append(f"IF({v} IS NOT NULL AND NOT is_variant_null({v}) AND {typed(c, v, 'TRY_CAST', BINHEX)} IS NULL, "
                     f"named_struct('kind', 'type', 'col', '{c['name']}'), NULL)")
        if not c.get("default"):
            items.append(f"IF({v} IS NULL, named_struct('kind', 'missing', 'col', '{c['name']}'), NULL)")
    known = ", ".join(f"'{c['name']}'" for c in cols)
    new = (f"transform(array_except(json_object_keys(to_json(value:after)), array({known})), "
           f"x -> named_struct('kind', 'new', 'col', x))")
    none = "CAST(NULL AS STRUCT<kind: STRING, col: STRING>)"
    found = f"filter(concat(array({', '.join(items) or none}), {new}), x -> x IS NOT NULL)"

    @dp.temporary_view(name=f"{table}_drift")
    def view():
        return (spark.readStream.table(norm).where(mine)
                .where("value:after IS NOT NULL AND NOT is_variant_null(value:after)").where(f"_seq % {DRIFT_SAMPLE} = 0")
                .select(src, "_seq", F.expr("substr(to_json(value:after), 1, 500)").alias("sample"),
                        F.explode(F.expr(found)).alias("d"))
                .select(F.lit(table).alias("tbl"), "_source", F.col("d.kind").alias("kind"), F.col("d.col").alias("col"),
                        "_seq", "sample"))
    if DRIFT not in _views:
        _views.add(DRIFT)
        dp.create_streaming_table(name=DRIFT, comment="Schema drift seen in the change log, one row per table, source, "
                                  "kind (type, missing, new) and column: what an evolution (the table's new schema, "
                                  "then its full refresh) resolves")
    # named after the bronze table it reads: a table moved to its own bronze gets a new drift flow (a new checkpoint)
    dp.create_auto_cdc_flow(name=f"{DRIFT}_{table}_{bronze_of(table)}", target=DRIFT, source=f"{table}_drift",
                            keys=["tbl", "_source", "kind", "col"], sequence_by="_seq", stored_as_scd_type=1)


def load_fks():
    """{child: [(parent, columns, parent columns, action)]}: the foreign keys whose ON DELETE changes the child's rows
    (CASCADE deletes them, SET NULL sets the columns to NULL), from schema.yaml's fk_constraints (on_delete), a table's
    own _schema/tables/<table>.yaml over it; only children built here."""
    docs = [f"{VOL}/_schema/schema.yaml"]
    d = f"{VOL}/_schema/tables"
    docs += [f"{d}/{n}" for n in sorted(os.listdir(d)) if n.endswith(".yaml")] if os.path.isdir(d) else []
    by = {}
    for doc in docs:
        with open(doc) as f:
            by.update({t["name"]: t.get("fk_constraints") or [] for t in yaml.safe_load(f)["tables"]})
    out = {}
    for child, fks in by.items():
        for f in fks:
            act = str(f.get("on_delete") or "").upper().replace("_", " ")
            if act in ("CASCADE", "SET NULL") and child in INFO and INFO[child]["pk"]:
                out.setdefault(child, []).append((f["parent_table"], f["columns"], f["parent_columns"], act))
    return out


def fk_actions():
    """cdc.fk_actions = apply: the rows a foreign key's ON DELETE changes, which the source's change log leaves out
    (MySQL through 9.5 and MariaDB: InnoDB's own cascades aren't in the binlog). For each parent delete (sequence S),
    the child rows whose version at S references it are deleted (CASCADE) or get the columns NULL (SET NULL), as
    changes at S, into the child's own targets: AUTO CDC orders them with the child's events. A child deleted so is a
    parent at the next level, to cdc.fk_depth levels (chains, and a table referencing itself). Each change is also in
    fk_actions (table, source, key, sequence, operation, the foreign key), the trace."""
    fks = load_fks()
    if FK_ACTIONS != "apply" or not fks:
        return
    deletes = {0: set(INFO)}   # level -> tables with deletes at that level (0: the change log's own)
    levels = {}                # (child, n) -> the edges feeding it
    for n in range(1, FK_DEPTH + 1):
        deletes[n] = set()
        for child, edges in sorted(fks.items()):
            feed = [e for e in edges if n == 1 or e[0] in deletes[n - 1]]
            if feed:
                levels[(child, n)] = feed
                if any(e[3] == "CASCADE" for e in feed):
                    deletes[n].add(child)

    def level(child, n, edges):
        info = INFO[child]
        cmap = {c["name"]: c for c in info["cols"]}

        @dp.temporary_view(name=f"{child}_fk_d{n}")
        def view():
            ver = spark.read.table(f"_fkv_{child}").alias("v")
            parts = []
            for parent, fcols, pcols, act in edges:
                if n == 1:   # the parent's deletes in the change log: the referenced values from the row before
                    d = (spark.readStream.table(norm_view(bronze_of(parent)))
                         .where(F.expr(f"substring_index(destination, '.', -1) = '{parent}' AND operation = 'delete'"))
                         .select(*[F.expr(typed(cmap[fc], f"COALESCE({path('value:before', pc)}, {path('key', pc)})", "TRY_CAST", BINHEX))
                                   .alias(f"_p{i}") for i, (fc, pc) in enumerate(zip(fcols, pcols))],
                                 F.expr(f"substring(destination, 1, length(destination) - {len(parent) + 1})").alias("_source"),
                                 "_seq", "_event_ns"))
                else:        # the parent's rows deleted at the level above
                    d = (spark.readStream.table(f"{parent}_fk_d{n - 1}").where("_op = 'delete'")
                         .select(*[F.col(f"`{pc}`").cast(spark_type(cmap[fc])).alias(f"_p{i}") for i, (fc, pc) in enumerate(zip(fcols, pcols))],
                                 "_source", "_seq", "_event_ns"))
                d = d.alias("d")
                on = [F.col(f"v.`{fc}`") == F.col(f"d._p{i}") for i, fc in enumerate(fcols)] + [
                    F.col("v._source") == F.col("d._source"), F.col("v._seq") < F.col("d._seq"),
                    F.col("v._next").isNull() | (F.col("v._next") > F.col("d._seq")), F.col("v._op") != "delete"]
                rule = f"{child}({','.join(fcols)}) -> {parent}({','.join(pcols)}) ON DELETE {act}"
                parts.append(d.join(ver, functools.reduce(lambda a, b: a & b, on)).select(
                    *[(F.lit(None).cast(spark_type(c)) if act == "SET NULL" and c["name"] in fcols else F.col(f"v.`{c['name']}`"))
                      .alias(c["name"]) for c in info["cols"]],
                    F.col("d._source").alias("_source"), F.col("d._seq").alias("_seq"),
                    F.lit("delete" if act == "CASCADE" else "update").alias("_op"), F.lit(rule).alias("_fk"),
                    F.col("d._event_ns").alias("_event_ns")))
            # two foreign keys to the same parent give the same change twice
            return (functools.reduce(lambda a, b: a.unionByName(b), parts)
                    .withColumn("_event_time", F.timestamp_micros(F.expr("_event_ns DIV 1000")))
                    .withWatermark("_event_time", "10 minutes")
                    .dropDuplicatesWithinWatermark(["_source", *info["pk"], "_seq", "_op"]).drop("_event_time"))

    # each child's versions once per update, a materialized view: the levels' joins read it rather than each
    # re-reading the child's snapshot files and change log (they made resolving the pipeline's graph take 27 minutes
    # for Mautic's 125 children)
    for child in sorted({c for c, _ in levels}):
        dp.materialized_view(name=f"_fkv_{child}", comment=f"{child}'s versions from T, for its foreign keys' ON DELETE "
                             "changes (cdc.fk_actions): its key, columns, source, sequence, operation and the next "
                             "version's sequence", table_properties={"delta.feature.timestampNtz": "supported"}   # as the targets'
                             )(lambda child=child: INFO[child]["versions"]())
    for (child, n), edges in sorted(levels.items(), key=lambda x: x[0][1]):
        level(child, n, edges)
    # each child's levels in one view; its own change views take it in (define's changes), so the flows that build its
    # targets apply these changes too, with no flow of their own: an update starts one flow per target, not three
    children = sorted({c for c, _ in levels})
    for child in children:
        ns = sorted(n for c, n in levels if c == child)
        dp.temporary_view(name=f"{child}_fk")(
            lambda ns=ns, child=child: functools.reduce(lambda a, b: a.unionByName(b),
                                                        [spark.readStream.table(f"{child}_fk_d{n}") for n in ns]))
        FK_VIEWS.add(child)
    dp.create_streaming_table(name=FKLOG, comment="The rows foreign keys' ON DELETE actions changed, which the source's "
                              "change log doesn't carry: one row per change (table, source, key, sequence, operation, "
                              "foreign key), applied to the table's targets (an append-only table's are not)")

    @dp.append_flow(target=FKLOG, name=f"{FKLOG}_all")
    def trace():   # one flow for every child's changes
        return functools.reduce(lambda a, b: a.unionByName(b), [
            spark.readStream.table(f"{c}_fk").select(
                F.lit(c).alias("tbl"), "_source", F.to_json(F.struct(*INFO[c]["pk"])).alias("key"), "_seq",
                F.col("_op").alias("operation"), F.col("_fk").alias("foreign_key"),
                F.lit(bool(INFO[c]["targets"])).alias("applied")) for c in children])

for name, columns in SCHEMA.items():
    define(name, columns)
fk_actions()
