"""discover: the sources and their tables, found in Unity Catalog when a pipeline update starts.

A source is a schema named <prefix><source> in one catalog, as an ingestion pipeline writes one destination schema per
source database, schema, tenant or line of business. Nothing here names a table or a column, so a new source or table
needs only its ingestion; `only` restricts the sources (a pipeline setting, comma-separated).
"""
import re


def sources(spark, catalog: str, prefix: str, only: str | list[str] | None = None) -> list[str]:
    """The sources: `only` (a list or a comma-separated string) if given, else every schema <prefix>* in the catalog."""
    if only:
        names = only.split(",") if isinstance(only, str) else only
        found = [n.strip() for n in names if n.strip()]
    else:
        rows = spark.sql(f"SHOW SCHEMAS IN `{catalog}` LIKE '{prefix}*'").collect()
        found = sorted(r.databaseName[len(prefix):] for r in rows if r.databaseName.startswith(prefix))
    if not found:
        raise ValueError(f"no sources: no schema {catalog}.{prefix}* and no list given")
    return found


PIPELINE_TABLE = re.compile(r"^(event_log_[0-9a-f]{8}(_[0-9a-f]{4}){3}_[0-9a-f]{12}|__.*)$")


def is_pipeline_table(name: str) -> bool:
    """A pipeline's own table, not a source's: its event log (event_log_<pipeline id>, in the pipeline's schema: seen
    next to an ingestion pipeline's destination tables 2026-11-15) or a backing table (__materialization_mat_...)."""
    return bool(PIPELINE_TABLE.match(name))


def tables(spark, catalog: str, prefix: str, names: list[str]) -> dict[str, set[str]]:
    """Each source's tables: {source: {table, ...}}, without pipelines' own tables. A source missing a table another
    has is fine (see stack)."""
    return {s: {r.tableName for r in spark.sql(f"SHOW TABLES IN `{catalog}`.`{prefix}{s}`").collect()
                if not is_pipeline_table(r.tableName)} for s in names}


def catalog_columns(spark, catalog: str, prefix: str, names: list[str]) -> dict[str, dict[str, list[tuple]]]:
    """Every source's tables and their columns with types, {source: {table: [(column, type), ...]}}, in ONE query
    (information_schema.columns), not a SHOW TABLES and a schema read per source and table: at hundreds of sources
    those round trips are most of a pipeline update's start. Pipelines' own tables are left out."""
    want = {f"{prefix}{n}": n for n in names}
    esc = prefix.replace("\\", "\\\\").replace("'", "\\'")
    rows = spark.sql(f"SELECT table_schema, table_name, column_name, full_data_type FROM "
                     f"`{catalog}`.information_schema.columns WHERE startswith(table_schema, '{esc}') "
                     f"ORDER BY table_schema, table_name, ordinal_position").collect()
    out: dict[str, dict[str, list[tuple]]] = {}
    for r in rows:
        s = want.get(r.table_schema)
        if s is None or is_pipeline_table(r.table_name):
            continue
        out.setdefault(s, {}).setdefault(r.table_name, []).append((r.column_name, r.full_data_type))
    return out
