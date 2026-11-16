# merge-silver.py: silver without a pipeline. A serverless job (spark_python_task) builds the same silver tables as
# silver-from-schema.py, from the same volume (_sources.yaml, _schema/, _snapshot.tsv, the snapshot files) and the same
# change log, with MERGE instead of AUTO CDC. A pipeline declares every table, view and flow when each update starts,
# so its start-up grows with the number of tables, and a pipeline holds only so many; here nothing is declared per
# table: one stream reads the change log, and each micro-batch MERGEs only the tables that changed in it.
#
#   merge-silver.py --volume /Volumes/<c>/<s>/<v> --source <c>.<s>.cdc_log --target <c>.<s> --checkpoint <dir>
#                   [--bronze zerobus|kafka] [--shard i/n] [--history-suffix _history] [--threads 16]
#                   [--create eager|lazy] [--on-drift record|fail] [--refresh t1,t2] [--bronze-table t=cdc_log_t]
#                   [--binary hex] [--once]
#
# Tables: <t> (current rows, SCD type 1) and <t><history-suffix> (every version, SCD type 2, __START_AT/__END_AT: the
# version's sequence and the next one's) for a table with a key; <t> append-only for one without. Each row has _source
# and, in <t>, _seq (its change's sequence: AUTO CDC keeps it hidden, MERGE needs it in the row). --create eager
# creates every table at the start (with its snapshot rows); lazy creates a table when it first has rows.
# Ordering, as AUTO CDC's: a change applies only when its sequence is newer than the row's; a delete leaves a
# tombstone (_cdc_tombstones: table, source, key, sequence, event; every delete), so a replayed older change can't bring the
# row back. SCD type 2: replays (same key and sequence) are dropped; each version ends at its key's next change (the
# next version or a delete), recomputed over the key's whole history each time the key changes, so a version that
# arrives late (older than one already there) still lands in order. A producer's replay (the same event under a new
# sequence, after a restart) is dropped, as the pipeline's dedup view does, kept in the history's _idem.
# Schema drift (--on-drift record, the default), as silver-from-schema.py's: a value that doesn't fit becomes NULL, a
# column that's gone gets its default, a new column waits; each is recorded in schema_drift (one row per table,
# source, kind, column: the last sequence and a sample). fail casts strictly: the run stops at the first misfit.
# --refresh t1,t2: those tables again from nothing, with the schema in the volume now (_schema/tables/<t>.yaml, a
# table's evolved schema): replaced, their snapshot loaded, then every event in their change log at once (the log is
# append-only VARIANT, so it holds them all); the evolution path, as a pipeline's full refresh of those tables.
# --bronze-table t=cdc_log_t: a table moved out of the shared log reads its own (fanout.py --tables t): a stream of
# its own (checkpoint <checkpoint>-<bronze>), and the shared log's stream skips it; refresh it when it moves.
# --shard i/n: only the tables with crc32(name) % n == i (silver-from-schema.py's rule), so n jobs share the work.
# The helpers below are silver-from-schema.py's (types, VARIANT paths, temporal decoding), so both build the same rows.
import argparse
import functools
import os
import time
import sys
from concurrent.futures import ThreadPoolExecutor

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--volume", required=True)
ap.add_argument("--source", required=True)
ap.add_argument("--target", required=True)
ap.add_argument("--checkpoint", required=True)
ap.add_argument("--bronze", default="zerobus")
ap.add_argument("--shard", default="")
ap.add_argument("--history-suffix", default="_history")
ap.add_argument("--threads", type=int, default=16)
ap.add_argument("--create", default="eager", choices=["eager", "lazy"])
ap.add_argument("--on-drift", default="record", choices=["record", "fail"])
ap.add_argument("--refresh", default="")
ap.add_argument("--bronze-table", action="append", default=[])
ap.add_argument("--placement-group", default="")   # composite: this group's tables, from <volume>/_placement.tsv
ap.add_argument("--drift-table", default="schema_drift")
ap.add_argument("--once", action="store_true")
ap.add_argument("--binary", default="", choices=["", "hex"])   # hex: binary as hex text (Debezium's binary.handling.mode)
a = ap.parse_args()
spark = SparkSession.builder.getOrCreate()
VOL, TGT, HIST = a.volume, a.target, a.history_suffix
BINHEX = a.binary == "hex"
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # canonical_sql.py sits next to this file
from canonical_sql import load, path, shard_of, snapshot_value, spark_type, typed  # noqa: E402
log = lambda *m: print(time.strftime("%H:%M:%S"), *m, flush=True)

with open(f"{VOL}/_sources.yaml") as f:
    _S = yaml.safe_load(f)
SOURCES = _S["sources"]
SOURCE_KEY = bool(_S.get("source_key", len(SOURCES) > 1))
T_NS = {s["prefix"]: int(s["t_ns"]) for s in SOURCES}
SCHEMA = load(VOL)
if a.shard:
    _i, _n = (int(x) for x in a.shard.split("/"))
    SCHEMA = {t: c for t, c in SCHEMA.items() if shard_of(t, _n) == _i}
if a.placement_group:   # composite: the group's tables; own bronze (cdc_log_<table>, from the fan-out) for those that have it
    with open(f"{VOL}/_placement.tsv") as f:
        _p = [r.rstrip("\n").split("\t") for r in f if r.strip()]
    _g = {r[0]: r[2] for r in _p if r[1] == a.placement_group}
    SCHEMA = {t: c for t, c in SCHEMA.items() if t in _g}
    a.bronze_table += [f"{t}=cdc_log_{t}" for t, b in _g.items() if b == "own" and t in SCHEMA]
LOWER = {t.lower(): t for t in SCHEMA}
BRONZE_OF = {t: f"{TGT}.{q}" for t, q in (x.split("=", 1) for x in a.bronze_table)}
REFRESH = [t for t in a.refresh.split(",") if t in SCHEMA]
TRY = a.on_drift == "record"
# a producer's replay arrives within minutes of the original (after a restart); the same event later is a real change
# that happens to repeat a row image (A -> B -> A). The pipeline's dedup view uses the same 10-minute watermark.
REPLAY_NS = 10 * 60 * 10**9
DRIFT = a.drift_table + (f"_s{a.shard.replace('/', '_of_')}" if a.shard else "")
# the state tables (_cdc_tombstones, _cdc_appended, _cdc_loaded), one set per group and shard: jobs run at once, and
# two writing one Delta table can conflict
STATE = (f"_{a.placement_group}" if a.placement_group else "") + (f"_s{a.shard.replace('/', '_of_')}" if a.shard else "")
SNAP = set()
if os.path.exists(f"{VOL}/_snapshot.tsv"):
    with open(f"{VOL}/_snapshot.tsv") as f:
        SNAP = {(r[0], r[1], r[2]) for r in (x.rstrip("\n").split("\t") for x in f) if len(r) == 4 and int(r[3]) > 0}


def q(n):
    return f"`{n}`"


def pk(table):
    return [c["name"] for c in SCHEMA[table] if c.get("primary_key")]


def keys(table):
    k = pk(table)
    return (["_source"] + k) if SOURCE_KEY and k else k


def ddl(table, replace=False):
    """CREATE TABLE IF NOT EXISTS statements for the table's silver tables (replace: CREATE OR REPLACE, so a view
    over them sees the same tables, rebuilt)."""
    cols = ", ".join(f"{q(c['name'])} {spark_type(c)}" for c in SCHEMA[table])
    # row tracking and deletion vectors: a materialized view over the table can refresh incrementally (MERGE updates
    # and deletes rows, so the view needs to know which)
    props = ("TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported', 'delta.enableRowTracking' = 'true', "
             "'delta.enableDeletionVectors' = 'true')")
    k = keys(table)
    c = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    if not k:
        return [f"{c} {TGT}.{q(table)} ({cols}, _source STRING) {props}"]
    cb = ", ".join(q(x) for x in k)
    return [f"{c} {TGT}.{q(table)} ({cols}, _source STRING, _seq BIGINT) CLUSTER BY ({cb}) {props}",
            (f"{c} {TGT}.{q(table + HIST)} ({cols}, _source STRING, _idem STRING, __START_AT BIGINT, __END_AT BIGINT) "
             f"CLUSTER BY ({cb}) {props}")]


def snapshot(table):
    """The table's snapshot rows from every source, typed, with _source, _seq (the source's T) and _op 'read'."""
    parts = []
    for s in SOURCES:
        fmt = s.get("format", "parquet")
        if (table, s["prefix"], fmt) not in SNAP:
            continue
        r = spark.read.format(fmt)
        if fmt == "csv":
            r = r.option("header", "true")
        df = r.load(f"{VOL}/{table}/{s['prefix']}/*.{fmt}")
        have = set(df.columns)
        sel = [F.expr(snapshot_value(c, have, BINHEX)).alias(c["name"]) for c in SCHEMA[table]]
        parts.append(df.select(*sel, F.lit(s["prefix"]).alias("_source"), F.lit(T_NS[s["prefix"]]).alias("_seq"),
                               F.lit("read").alias("_op"), F.lit(None).cast("string").alias("_idem")))
    out = None
    for p in parts:
        out = p if out is None else out.unionByName(p)
    return out


def changes(batch, table):
    """The table's events in a normalized batch, typed (record: TRY_CAST, a value that doesn't fit becomes NULL)."""
    k, cast = pk(table), "TRY_CAST" if TRY else "CAST"
    sel = []
    for c in SCHEMA[table]:
        n = c["name"]
        if n in k:
            sel.append(F.expr(typed(c, path("key", n), "CAST", BINHEX)).alias(n))
        elif c.get("default"):
            sel.append(F.expr(f"CASE WHEN value:after IS NOT NULL AND NOT is_variant_null(value:after) AND {path('value:after', n)} "
                              f"IS NULL THEN CAST({c['default']} AS {spark_type(c)}) ELSE {typed(c, path('value:after', n), cast, BINHEX)} END").alias(n))
        else:
            sel.append(F.expr(typed(c, path("value:after", n), cast, BINHEX)).alias(n))
    return (batch.where(F.col("_table") == table)
            .select(*sel, F.expr(f"substring(destination, 1, length(destination) - {len(table) + 1})").alias("_source"),
                    "_seq", F.col("operation").alias("_op"),
                    # a producer's replay (after a restart) is the same event under a new sequence: the same event key,
                    # operation and row image (what silver-from-schema.py's _dedup view drops)
                    F.expr("sha2(concat_ws('|', destination, idempotency_key, operation, to_json(value:after)), 256)").alias("_idem")))


def drift(batch, table):
    """What in the table's events doesn't fit its schema, as silver-from-schema.py's drift(): a value its type can't
    hold (type), a known column without a default missing from the row (missing), a column the schema lacks (new)."""
    items = []
    for c in SCHEMA[table]:
        if c.get("primary_key"):
            continue
        v = path("value:after", c["name"])
        items.append(f"IF({v} IS NOT NULL AND NOT is_variant_null({v}) AND {typed(c, v, 'TRY_CAST', BINHEX)} IS NULL, "
                     f"named_struct('kind', 'type', 'col', '{c['name']}'), NULL)")
        if not c.get("default"):
            items.append(f"IF({v} IS NULL, named_struct('kind', 'missing', 'col', '{c['name']}'), NULL)")
    known = ", ".join(f"'{c['name']}'" for c in SCHEMA[table])
    new = (f"transform(array_except(json_object_keys(to_json(value:after)), array({known})), "
           f"x -> named_struct('kind', 'new', 'col', x))")
    none = "CAST(NULL AS STRUCT<kind: STRING, col: STRING>)"
    found = f"filter(concat(array({', '.join(items) or none}), {new}), x -> x IS NOT NULL)"
    return (batch.where(F.col("_table") == table).where("value:after IS NOT NULL AND NOT is_variant_null(value:after)")
            .select(F.expr(f"substring(destination, 1, length(destination) - {len(table) + 1})").alias("_source"), "_seq",
                    F.expr("substr(to_json(value:after), 1, 500)").alias("sample"), F.explode(F.expr(found)).alias("d"))
            .select(F.lit(table).alias("tbl"), "_source", F.col("d.kind").alias("kind"), F.col("d.col").alias("col"),
                    "_seq", "sample"))


def apply(table, rows):
    """MERGE the table's rows (snapshot or changes: columns, _source, _seq, _op) into its silver tables. Returns the
    rows' deletes (tbl, _source, k, _seq), for _cdc_tombstones: written once a batch, by the caller (one writer)."""
    k = keys(table)
    cols = [c["name"] for c in SCHEMA[table]]
    if not k:   # append-only: the snapshot rows and the inserts; a producer's replay of an insert (the same _idem
        # within REPLAY_NS) once, against this batch and _cdc_appended (what was appended before). Returns what it
        # appended (tbl, idem, _seq) for _cdc_appended. Two identical rows inserted within REPLAY_NS count as one
        v = f"_a_{abs(hash(table))}"
        rows.where("_op IN ('read', 'create')").createOrReplaceTempView(v + "_in")
        spark.sql(f"""SELECT * FROM {v}_in r
          WHERE NOT (r._idem IS NOT NULL AND EXISTS (SELECT 1 FROM {TGT}._cdc_appended{STATE} x WHERE x.tbl = '{table}'
                     AND x.idem = r._idem AND abs(x._seq - r._seq) < {REPLAY_NS}))
          QUALIFY r._idem IS NULL OR r._seq - COALESCE(LAG(r._seq) OVER (PARTITION BY r._idem ORDER BY r._seq), r._seq - {REPLAY_NS}) >= {REPLAY_NS}""").createOrReplaceTempView(v)
        spark.table(v).drop("_seq", "_op", "_idem").write.mode("append").saveAsTable(f"{TGT}.{q(table)}")
        return spark.sql(f"SELECT '{table}' AS tbl, _idem AS idem, _seq FROM {v} WHERE _idem IS NOT NULL")
    on = " AND ".join(f"t.{q(x)} <=> s.{q(x)}" for x in k)
    pkeys = ", ".join(q(x) for x in k)
    kj = f"to_json(named_struct({', '.join(f'{chr(39)}{x}{chr(39)}, {q(x)}' for x in pk(table))}))"
    # replays: the same key, sequence and operation once; a producer's replay (the same _idem under a new sequence,
    # within REPLAY_NS of it) once, the first: in this batch, or against what's in the history and the tombstones already
    rows = rows.dropDuplicates(k + ["_seq", "_op"]).withColumn("_k", F.expr(kj))
    h = f"{TGT}.{q(table + HIST)}"
    v = f"_r_{abs(hash(table))}"
    rows.createOrReplaceTempView(v + "_in")
    spark.sql(f"""SELECT * FROM {v}_in r
      WHERE NOT (r._op = 'delete' AND EXISTS (SELECT 1 FROM {TGT}._cdc_tombstones{STATE} x WHERE x.tbl = '{table}'
                 AND x._source = r._source AND x.k = r._k AND x.idem = r._idem AND x._seq <> r._seq AND abs(x._seq - r._seq) < {REPLAY_NS}))
        AND NOT (r._op <> 'delete' AND EXISTS (SELECT 1 FROM {h} x WHERE {' AND '.join(f'x.{q(c)} = r.{q(c)}' for c in k)}
                 AND x._idem = r._idem AND x.__START_AT <> r._seq AND abs(x.__START_AT - r._seq) < {REPLAY_NS}))
      QUALIFY r._idem IS NULL OR r._seq - COALESCE(LAG(r._seq) OVER (PARTITION BY {', '.join('r.' + q(x) for x in k)}, r._idem ORDER BY r._seq), r._seq - {REPLAY_NS}) >= {REPLAY_NS}""").createOrReplaceTempView(v)
    # SCD type 1, from the rows before replay removal (as the pipeline's SCD type 1 flow reads the changes, not the
    # dedup view): a replay comes in order with a newer sequence, so it converges, while removing it could drop a real
    # change back to an earlier image (A -> B -> A within the window). Each key's newest change (at the same sequence, a
    # delete before the reinsert), applied when newer than the row; one no newer than the key's last delete doesn't
    # bring the row back
    last = (f"(SELECT r.* FROM {v}_in r LEFT JOIN (SELECT _source, k, MAX(_seq) AS _seq FROM {TGT}._cdc_tombstones{STATE} "
            f"WHERE tbl = '{table}' GROUP BY 1, 2) x ON r._source = x._source AND r._k = x.k WHERE x._seq IS NULL OR r._seq > x._seq "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join('r.' + q(x) for x in k)} ORDER BY r._seq DESC, r._op = 'delete') = 1)")
    sets = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in cols + ["_source", "_seq"])
    ins = ", ".join(q(c) for c in cols + ["_source", "_seq"])
    spark.sql(f"""MERGE INTO {TGT}.{q(table)} t USING {last} s ON {on}
      WHEN MATCHED AND s._seq > t._seq AND s._op = 'delete' THEN DELETE
      WHEN MATCHED AND s._seq > t._seq THEN UPDATE SET {sets}
      WHEN NOT MATCHED AND s._op <> 'delete' THEN INSERT ({ins}) VALUES ({', '.join('s.' + q(c) for c in cols + ['_source', '_seq'])})""")
    # SCD type 2: the new versions (one per key and sequence; a replayed batch's are there already), then every
    # version of the keys the rows touch ends at the key's next change: the next version or a delete (this batch's,
    # or a tombstone), whichever comes first; at the same sequence a delete comes before the version
    hc = ", ".join(q(c) for c in cols + ["_source", "_idem"])
    spark.sql(f"""MERGE INTO {h} t
      USING (SELECT * FROM {v} WHERE _op <> 'delete' QUALIFY ROW_NUMBER() OVER (PARTITION BY {pkeys}, _seq
             ORDER BY CASE _op WHEN 'update' THEN 0 WHEN 'create' THEN 1 ELSE 2 END) = 1) s ON {on} AND t.__START_AT = s._seq
      WHEN NOT MATCHED THEN INSERT ({hc}, __START_AT, __END_AT) VALUES ({', '.join('s.' + q(c) for c in cols + ['_source', '_idem'])}, s._seq, NULL)""")
    touched = f"(SELECT DISTINCT {pkeys}{'' if '_source' in k else ', _source'}, _k FROM {v})"
    points = (f"SELECT {', '.join('h.' + q(x) for x in k)}, h.__START_AT AS p, 1 AS ver FROM {h} h LEFT SEMI JOIN {touched} s ON "
              f"{' AND '.join(f'h.{q(x)} <=> s.{q(x)}' for x in k)} "
              f"UNION ALL SELECT {pkeys}, _seq, 0 FROM {v} WHERE _op = 'delete' "
              f"UNION ALL SELECT {', '.join('s.' + q(x) for x in k)}, x._seq, 0 FROM {TGT}._cdc_tombstones{STATE} x JOIN {touched} s "
              f"ON x.tbl = '{table}' AND x._source = s._source AND x.k = s._k")
    spark.sql(f"""MERGE INTO {h} t
      USING (SELECT * FROM (SELECT *, LEAD(p) OVER (PARTITION BY {pkeys} ORDER BY p, ver) AS e FROM ({points})) WHERE ver = 1) s
      ON {on} AND t.__START_AT = s.p
      WHEN MATCHED AND NOT (t.__END_AT <=> s.e) THEN UPDATE SET t.__END_AT = s.e""")
    # the view stays until the batch's state is written (the next batch replaces it)
    # tombstones from every delete, before replay removal: the SCD type 1 check takes each key's latest, so a replayed
    # delete only raises it, and a real second delete taken for a replay (the same key within REPLAY_NS, on a route with
    # no idempotency key) still keeps an older change from bringing the row back
    return spark.sql(f"SELECT DISTINCT '{table}' AS tbl, _source, _k AS k, _seq, _idem AS idem FROM {v}_in WHERE _op = 'delete'")


def write_state(tombs, drifts, appended=()):
    """This batch's deletes into _cdc_tombstones, its drift into schema_drift, and what append-only tables appended
    into _cdc_appended: one MERGE each, 200 tables a time."""
    def chunks(dfs):
        dfs = [d for d in dfs if d is not None]
        for i in range(0, len(dfs), 200):
            yield functools.reduce(lambda x, y: x.unionByName(y), dfs[i:i + 200])
    for u in chunks(tombs):
        u.createOrReplaceTempView("_tomb_new")
        spark.sql(f"""MERGE INTO {TGT}._cdc_tombstones{STATE} t USING (SELECT DISTINCT * FROM _tomb_new) s
          ON t.tbl = s.tbl AND t._source = s._source AND t.k = s.k AND t._seq = s._seq
          WHEN NOT MATCHED THEN INSERT (tbl, _source, k, _seq, idem) VALUES (s.tbl, s._source, s.k, s._seq, s.idem)""")
    for u in chunks(list(appended)):
        u.createOrReplaceTempView("_app_new")
        spark.sql(f"""MERGE INTO {TGT}._cdc_appended{STATE} t USING (SELECT DISTINCT * FROM _app_new) s
          ON t.tbl = s.tbl AND t.idem = s.idem AND t._seq = s._seq
          WHEN NOT MATCHED THEN INSERT (tbl, idem, _seq) VALUES (s.tbl, s.idem, s._seq)""")
    for u in chunks(drifts):
        u.createOrReplaceTempView("_drift_new")
        spark.sql(f"""MERGE INTO {TGT}.{DRIFT} t
          USING (SELECT tbl, _source, kind, col, MAX(_seq) AS _seq, MAX_BY(sample, _seq) AS sample FROM _drift_new GROUP BY 1, 2, 3, 4) s
          ON t.tbl = s.tbl AND t._source = s._source AND t.kind = s.kind AND t.col = s.col
          WHEN MATCHED AND s._seq > t._seq THEN UPDATE SET t._seq = s._seq, t.sample = s.sample
          WHEN NOT MATCHED THEN INSERT (tbl, _source, kind, col, _seq, sample) VALUES (s.tbl, s._source, s.kind, s.col, s._seq, s.sample)""")


def each(fn, items):
    """fn over items, --threads at a time; the first error is raised after all have run."""
    errs = []

    def run(x):
        try:
            fn(x)
        except Exception as e:   # noqa: BLE001
            errs.append((x, e))
    with ThreadPoolExecutor(a.threads) as ex:
        list(ex.map(run, items))
    if errs:
        log(f"{len(errs)} failed; first: {errs[0][0]}: {str(errs[0][1])[:500]}")
        raise errs[0][1]


# --- start: the state tables, the silver tables (eager), the snapshot (once per table, recorded in _cdc_loaded) -----
t0 = time.time()
spark.sql(f"CREATE TABLE IF NOT EXISTS {TGT}._cdc_tombstones{STATE} (tbl STRING, _source STRING, k STRING, _seq BIGINT, idem STRING) "
          "CLUSTER BY (tbl)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TGT}._cdc_loaded{STATE} (tbl STRING, at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TGT}._cdc_appended{STATE} (tbl STRING, idem STRING, _seq BIGINT) CLUSTER BY (tbl)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TGT}.{DRIFT} (tbl STRING, _source STRING, kind STRING, col STRING, _seq BIGINT, "
          f"sample STRING) COMMENT 'Schema drift seen in the change log, one row per table, source, kind (type, missing, "
          f"new) and column: what an evolution (the table''s new schema, then --refresh of it) resolves'")
existing = {r[0].lower() for r in spark.sql(f"SHOW TABLES IN {TGT}").select("tableName").collect()}
loaded = {r[0] for r in spark.table(f"{TGT}._cdc_loaded{STATE}").collect()}
todo = [t for t in SCHEMA if t not in loaded or t in REFRESH]
has_rows = {t for t, _, _ in SNAP}
create = [t for t in todo if a.create == "eager" or t in has_rows or t in REFRESH]
log(f"{len(SCHEMA)} tables in this shard; {len(existing)} tables in {TGT}; snapshot to load for {len(todo)}, "
    f"tables to create {len(create)} ({a.create}); to refresh: {','.join(REFRESH) or 'none'}")


def norm(log_df):
    """The change log in one shape for both sinks: destination, operation, key, value, _seq, and _table (its name)."""
    if a.bronze == "kafka":
        out = log_df.select(
            "destination",
            F.expr("CASE value:op::STRING WHEN 'c' THEN 'create' WHEN 'u' THEN 'update' WHEN 'd' THEN 'delete' "
                   "WHEN 'r' THEN 'read' END").alias("operation"),
            F.expr("CASE value:op::STRING WHEN 'd' THEN value:before ELSE value:after END").alias("key"), "value",
            F.expr("COALESCE(value:source:lsn::BIGINT, value:ts_ns::BIGINT)").alias("_seq"),
            F.expr("value:source:lsn::STRING").alias("idempotency_key"))   # none from CockroachDB: replays match on the row
    else:
        out = log_df.select("destination", "operation", "key", "value", F.expr("value:ts_ns::BIGINT").alias("_seq"), "idempotency_key")
    return out.withColumn("_table", F.substring_index("destination", ".", -1))


def named(b, t):
    """The batch with _table as the schema spells the table (a producer may change its case)."""
    return b.withColumn("_table", F.expr(f"CASE WHEN lower(_table) = '{t.lower()}' THEN '{t}' ELSE _table END"))


if REFRESH:   # their tables' tombstones go too: the replay below writes them again
    spark.sql(f"DELETE FROM {TGT}._cdc_tombstones{STATE} WHERE tbl IN ({', '.join(chr(39) + t + chr(39) for t in REFRESH)})")
    spark.sql(f"DELETE FROM {TGT}._cdc_appended{STATE} WHERE tbl IN ({', '.join(chr(39) + t + chr(39) for t in REFRESH)})")
tombs, appends = [], []


def start(table):
    replace = table in REFRESH
    for s in ddl(table, replace):
        if replace or s.split(" (")[0].split(".")[-1].strip("`").lower() not in existing:
            spark.sql(s)
    rows = snapshot(table)
    if rows is not None:
        apply(table, rows)
    if replace:   # every event in its change log, as one batch
        r = apply(table, changes(named(norm(spark.read.table(BRONZE_OF.get(table, a.source))), table), table))
        (tombs if keys(table) else appends).append(r)


each(start, create)
write_state(tombs, [], appends)
new = [t for t in todo if t not in loaded]
if new:
    spark.createDataFrame([(t,) for t in new], "tbl STRING").withColumn("at", F.current_timestamp()) \
        .write.mode("append").saveAsTable(f"{TGT}._cdc_loaded{STATE}")
log(f"start: {len(create)} table(s) created and loaded in {time.time() - t0:.0f}s")


# --- the change logs: one stream each (the shared log, and a moved table's own); each micro-batch MERGEs the tables
# it has events for, then writes their tombstones and drift ------------------------------------------------------------
def batch_fn(mine):
    def fn(batch, batch_id):
        b = norm(batch)
        names = [r[0] for r in b.select("_table").distinct().collect()]
        tables = [t for t in (LOWER.get(n.lower(), n) for n in names) if t in mine]
        if not tables:
            return
        t1 = time.time()
        for t in tables:   # a table first seen in the log (lazy, or new since the start)
            if t not in loaded and t not in create:
                for s in ddl(t):
                    spark.sql(s)
        dels = {LOWER.get(r[0].lower(), r[0]) for r in b.where("operation = 'delete'").select("_table").distinct().collect()}
        out, drifts, apps = [], [], []

        def one(t):
            bt = named(b, t)
            d = apply(t, changes(bt, t))
            if not keys(t):
                apps.append(d)
            elif t in dels:
                out.append(d)
            if TRY:
                drifts.append(drift(bt, t))
        each(one, tables)
        write_state(out, drifts, apps)
        log(f"batch {batch_id} of {streams_of.get(id(mine), '?')}: {len(tables)} table(s) in {time.time() - t1:.0f}s")
    return fn


groups = {a.source: {t for t in SCHEMA if t not in BRONZE_OF}}
for t, bt in BRONZE_OF.items():
    if t in SCHEMA:
        groups.setdefault(bt, set()).add(t)
streams_of = {id(m): src for src, m in groups.items()}
queries = []
for src, mine in groups.items():
    cp = a.checkpoint if src == a.source else f"{a.checkpoint}-{src.split('.')[-1]}"
    w = spark.readStream.table(src).writeStream.foreachBatch(batch_fn(mine)).option("checkpointLocation", cp)
    if a.once:   # one after another: each takes what's in its log now, then stops
        w.trigger(availableNow=True).start().awaitTermination()
    else:
        queries.append(w.trigger(processingTime="10 seconds").start())
if queries:
    spark.streams.awaitAnyTermination()
log(f"done in {time.time() - t0:.0f}s")
