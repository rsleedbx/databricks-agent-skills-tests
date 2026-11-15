"""stack: the n:1 layer. Each table of every source in one materialized view, with a column naming its source.

Keys usually repeat across sources (each starts at 1), so the source column is part of every key downstream. The view
is a UNION ALL of the sources' tables, with no joins: silver and gold (the business logic, in SQL) join the stacked
tables, so their join count doesn't grow with the number of sources. Bronze written by Lakeflow Connect takes updates
and deletes (SCD type 1), which a streaming read can't follow; a materialized view refreshes incrementally from the
tables' change feed.

In a Lakeflow Spark Declarative Pipelines source file:

    from pyspark import pipelines as dp
    from lfc_medallion import discover, stack_all
    stack_all(dp, spark, catalog="main", prefix="sales_bronze_", source_column="division",
              only=spark.conf.get("divisions", ""))
"""
from . import discover


def union_sql(catalog: str, prefix: str, table: str, names: list[str], source_column: str = "_source") -> str:
    """SELECT '<source>' AS <source_column>, * FROM <catalog>.<prefix><source>.<table>, for each source, UNION ALL."""
    return " UNION ALL ".join(
        f"SELECT '{s.replace(chr(39), chr(39) * 2)}' AS `{source_column}`, * FROM `{catalog}`.`{prefix}{s}`.`{table}`"
        for s in names)


def stack_all(dp, spark, catalog: str, prefix: str, source_column: str = "_source", only=None,
              name: str = "all_{table}") -> dict[str, list[str]]:
    """Define one materialized view per table found in any source (named by `name`), stacked from the sources that
    have it. Returns {view name: [sources]} for logging or checks."""
    names = discover.sources(spark, catalog, prefix, only)
    held = discover.tables(spark, catalog, prefix, names)
    defined = {}
    for table in sorted(set().union(*held.values())):
        having = [s for s in names if table in held[s]]
        view = name.format(table=table)
        query = union_sql(catalog, prefix, table, having, source_column)
        dp.materialized_view(name=view, comment=f"{table} of every source in one table, with its source in "
                             f"{source_column} (n:1)")(_reader(spark, query))
        defined[view] = having
    return defined


def _reader(spark, query: str):
    """The view's function, bound to its own query (a closure in the loop would see only the last table's)."""
    def build():
        return spark.sql(query)
    return build
