"""merge: the `merge` route. A job keeps the n:1 table of each quiet table current from the sources' change feeds, so
the table needn't be a view in the pipeline's graph (each one there lengthens every update's start; with thousands of
tables a MERGE job doesn't: zerobus-debezium-test's many-tables measurements).

For each table and each source that has it, merge_all reads the source's change feed since the version it last read
(table_changes), keeps each key's last change, and MERGEs it into <target>.all_<table> keyed by the source column and
the primary key: inserts, updates (post-images) and deletes. A source read for the first time, or whose table was
replaced, truncated or overwritten since (a full refresh: its change feed starts over), is copied whole instead. The
versions read are kept in <target>._lfc_medallion_merge_state. Bronze columns the target lacks are added first, so a
source's new column arrives. The targets have the change data feed and row tracking on, so the silver views that read
them still refresh incrementally.

Lakeflow Connect's destination tables have the change data feed on (delta.enableChangeDataFeed=true: seen on its
PostgreSQL connector's SCD type 1 tables, 2026-11-15). An SCD type 2 destination keeps a row per version of a key, so
__START_AT joins the key. Run it as a job task before the pipeline that reads the targets.

    from lfc_medallion import merge_all, table_plan
    plan = table_plan(spark, "main", "sales_bronze_east")
    quiet = [n for n, t in plan["tables"].items() if t["route"] == "merge"]
    merge_all(spark, "main", "sales_bronze_", target="sales_silver", tables=quiet, source_column="division")
"""
import json

from . import discover, evidence
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


def since(spark, fq: str, version: int) -> tuple[bool, int]:
    """From the table's history after `version` (metadata only, no data read): whether it was replaced, truncated or
    overwritten (its change feed starts over), and how many rows its writes inserted, updated or deleted. An ingestion
    update adds versions to every table, changed or not (an empty MERGE, housekeeping DELETEs: seen on Lakeflow
    Connect's destination tables 2026-11-15), so a new version alone doesn't mean a change."""
    refreshed, rows = False, 0
    for r in _rows(spark, f"SELECT version, operation, operationParameters, operationMetrics "
                          f"FROM (DESCRIBE HISTORY {fq}) WHERE version > {int(version)}"):
        params = r.operationParameters or {}
        params = json.loads(params) if isinstance(params, str) else params
        mode = str(params.get("mode", "")).lower()
        if r.operation in evidence.RESETS or r.operation.startswith("CREATE OR REPLACE") or \
                (r.operation == "WRITE" and mode == "overwrite"):
            refreshed = True
        m = getattr(r, "operationMetrics", None) or {}
        rows += sum(evidence._write_counts(r.operation, json.loads(m) if isinstance(m, str) else m))
    return refreshed, rows


def _state(spark, catalog, target) -> dict:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {_fq(catalog, target, STATE)} (target_table STRING, source STRING, "
              "version BIGINT, updated_at TIMESTAMP) COMMENT 'lfc_medallion merge_all: the version of each source "
              "table read into each target'")
    return {(r.target_table, r.source): int(r.version)
            for r in _rows(spark, f"SELECT target_table, source, version FROM {_fq(catalog, target, STATE)}")}


def _save_state(spark, catalog, target, target_table, source, version):
    spark.sql(f"MERGE INTO {_fq(catalog, target, STATE)} s USING (SELECT {spark_string(target_table)} AS target_table, "
              f"{spark_string(source)} AS source, {int(version)} AS version) n "
              "ON s.target_table = n.target_table AND s.source = n.source "
              "WHEN MATCHED THEN UPDATE SET s.version = n.version, s.updated_at = current_timestamp() "
              "WHEN NOT MATCHED THEN INSERT (target_table, source, version, updated_at) "
              "VALUES (n.target_table, n.source, n.version, current_timestamp())")


def _ensure_target(spark, tgt: str, exists: bool, source_column: str, cols, have: set) -> None:
    """Create the target (source column first, then the bronze columns), or add the bronze columns it lacks."""
    if not exists:
        body = ", ".join([f"{_q(source_column)} STRING"] + [f"{_q(c)} {t}" for c, t in cols])
        spark.sql(f"CREATE TABLE {tgt} ({body}) TBLPROPERTIES ({TARGET_PROPERTIES})")
        return
    new = [(c, t) for c, t in cols if c not in have]
    if new:
        spark.sql(f"ALTER TABLE {tgt} ADD COLUMNS ({', '.join(f'{_q(c)} {t}' for c, t in new)})")


def merge_sql(tgt: str, src: str, source: str, source_column: str, key: list[str], cols: list[str],
              since: int | None, until: int) -> str:
    """The MERGE for one source: its rows (since=None: the whole table) or its changes since `since` (each key's last
    change, update pre-images dropped), keyed by the source column and the primary key."""
    names = [source_column] + cols
    lit = spark_string(source)
    data = ", ".join(_q(c) for c in cols)
    if since is None:
        rows = f"SELECT {lit} AS {_q(source_column)}, {data}, 'insert' AS `__lfc_change` FROM {src}"
    else:
        part = ", ".join(_q(k) for k in key)
        rows = (f"SELECT {lit} AS {_q(source_column)}, {data}, _change_type AS `__lfc_change` FROM ("
                # each key's last change; in one version a delete and an insert of a key is a replacement: the insert
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY _commit_version DESC, "
                "CASE _change_type WHEN 'delete' THEN 1 ELSE 0 END) AS `__lfc_n` "
                f"FROM table_changes({spark_string(src.replace('`', ''))}, {int(since) + 1}, {int(until)}) "
                "WHERE _change_type != 'update_preimage') WHERE `__lfc_n` = 1")
    on = " AND ".join(f"t.{_q(c)} = c.{_q(c)}" for c in [source_column] + key)
    sets = ", ".join(f"t.{_q(c)} = c.{_q(c)}" for c in cols if c not in key)
    ins_cols = ", ".join(_q(c) for c in names)
    ins_vals = ", ".join(f"c.{_q(c)}" for c in names)
    update = f"WHEN MATCHED THEN UPDATE SET {sets} " if sets else ""
    return (f"MERGE INTO {tgt} t USING ({rows}) c ON {on} "
            "WHEN MATCHED AND c.`__lfc_change` = 'delete' THEN DELETE "
            f"{update}WHEN NOT MATCHED AND c.`__lfc_change` != 'delete' THEN INSERT ({ins_cols}) VALUES ({ins_vals})")


def merge_all(spark, catalog: str, prefix: str, target: str, tables: list[str], source_column: str = "_source",
              only=None, name: str = "all_{table}", keys: dict | None = None, workspace=None) -> dict:
    """Keep <catalog>.<target>.<name> current for each table in `tables`, from every source (schema <prefix><source>)
    that has it. keys: {table: [columns]} where the recorded key isn't the one to use. Returns, per target table and
    source, what was done: copied (first read or after a full refresh), merged (from the change feed) or unchanged,
    and the version read."""
    names = discover.sources(spark, catalog, prefix, only)
    held = discover.tables(spark, catalog, prefix, names)
    state = _state(spark, catalog, target)
    existing = {r.tableName for r in _rows(spark, f"SHOW TABLES IN {_q(catalog)}.{_q(target)}")}
    report = {}
    for table in tables:
        out_name = name.format(table=table)
        tgt = _fq(catalog, target, out_name)
        report[out_name] = {}
        for s in [x for x in names if table in held[x]]:
            src = _fq(catalog, f"{prefix}{s}", table)
            cols = [(c, t) for c, t in columns(spark, src) if c not in CHANGE_COLUMNS]
            key = list((keys or {}).get(table) or evidence.recorded_keys(spark, src, f"{catalog}.{prefix}{s}.{table}",
                                                                        workspace))
            if not key:
                raise ValueError(f"{catalog}.{prefix}{s}.{table}: no primary key recorded; "
                                 f"give keys={{{table!r}: [...]}}")
            if "__START_AT" in {c for c, _ in cols} and "__START_AT" not in key:
                key.append("__START_AT")   # SCD type 2: a row per version of the key
            have = {c for c, _ in columns(spark, tgt)} if out_name in existing else set()
            _ensure_target(spark, tgt, out_name in existing, source_column, cols, have)
            existing.add(out_name)
            now, last = latest_version(spark, src), state.get((out_name, s))
            if last is not None and now == last:
                report[out_name][s] = {"done": "unchanged", "version": now}
                continue
            refreshed, changed = (False, 0) if last is None else since(spark, src, last)
            whole = last is None or refreshed
            if not whole and not changed:   # new versions, no rows written: nothing to MERGE
                _save_state(spark, catalog, target, out_name, s, now)
                report[out_name][s] = {"done": "unchanged", "version": now}
                continue
            if whole and last is not None:   # a full refresh: this source's rows are copied again
                spark.sql(f"DELETE FROM {tgt} WHERE {_q(source_column)} = {spark_string(s)}")
            spark.sql(merge_sql(tgt, src, s, source_column, key, [c for c, _ in cols], None if whole else last, now))
            _save_state(spark, catalog, target, out_name, s, now)
            report[out_name][s] = {"done": "copied" if whole else "merged", "version": now}
    return report
