"""discover: the sources and their tables, found in Unity Catalog when a pipeline update starts.

A source is a schema named <prefix><source> in one catalog, as an ingestion pipeline writes one destination schema per
source database, schema, tenant or line of business. Nothing here names a table or a column, so a new source or table
needs only its ingestion; `only` restricts the sources (a pipeline setting, comma-separated).
"""


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


def tables(spark, catalog: str, prefix: str, names: list[str]) -> dict[str, set[str]]:
    """Each source's tables: {source: {table, ...}}. A source missing a table another has is fine (see stack)."""
    return {s: {r.tableName for r in spark.sql(f"SHOW TABLES IN `{catalog}`.`{prefix}{s}`").collect()} for s in names}
