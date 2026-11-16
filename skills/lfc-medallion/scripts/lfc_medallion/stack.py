"""stack: the n:1 layer. Each table of every source in one materialized view, with a column naming its source.

Keys usually repeat across sources (each starts at 1), so the source column is part of every key downstream. The view
stacks the sources' tables by column name (unionByName, a column some source lacks is NULL there), with no joins:
silver and gold (the business logic, in SQL) join the stacked tables, so their join count doesn't grow with the
number of sources. By name, not position: a source whose table gained a column first, or holds its columns in
another order, still lines up. A column whose type differs between sources (an INT in one, a BIGINT in another) is
cast to the type that holds every source's (coltypes.widest; types that don't widen into each other: STRING) in the
sources whose type is narrower; a column the sources agree on is left as it is. Bronze written by Lakeflow Connect
takes updates and deletes (SCD type 1), which a streaming read can't follow; a materialized view refreshes
incrementally from the tables' change feed.

In a Lakeflow Spark Declarative Pipelines source file:

    from pyspark import pipelines as dp
    from lfc_medallion import discover, stack_all
    stack_all(dp, spark, catalog="main", prefix="sales_bronze_", source_column="division",
              only=spark.conf.get("divisions", ""))
"""
from . import coltypes, discover


def spark_string(s: str) -> str:
    """A Spark SQL string literal. Spark escapes with a backslash: '' isn't a quote there ('a''b' is two strings
    joined), so a quote or backslash in a name is backslashed."""
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def source_sql(catalog: str, prefix: str, table: str, source: str, source_column: str = "_source") -> str:
    """One source's rows: SELECT '<source>' AS <source_column>, * FROM <catalog>.<prefix><source>.<table>."""
    return (f"SELECT {spark_string(source)} AS `{source_column}`, * "
            f"FROM `{catalog}`.`{prefix}{source}`.`{table}`")



def aligned(frames):
    """The frames with each column whose Spark type differs between them cast to the widest (coltypes.widest), in the
    frames that have it narrower; a column with one type everywhere is left as it is."""
    seen: dict[str, set[str]] = {}
    for f in frames:
        for c, t in f.dtypes:
            seen.setdefault(c, set()).add(t)
    wide = {c: coltypes.widest(sorted(ts)) for c, ts in seen.items() if len(ts) > 1}
    out = []
    for f in frames:
        for c, t in f.dtypes:
            if c in wide and coltypes.from_spark(t) != coltypes.from_spark(wide[c]):
                f = f.withColumn(c, f[c].cast(wide[c]))
        out.append(f)
    return out

def stacked(spark, catalog: str, prefix: str, table: str, names: list[str], source_column: str = "_source"):
    """The sources' rows of one table in one DataFrame, matched by column name; a column a source lacks is NULL."""
    frames = aligned([spark.sql(source_sql(catalog, prefix, table, s, source_column)) for s in names])
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    return out


def stack_all(dp, spark, catalog: str, prefix: str, source_column: str = "_source", only=None,
              name: str = "all_{table}", skip=()) -> dict[str, list[str]]:
    """Define one materialized view per table found in any source (named by `name`), stacked from the sources that
    have it, except the tables in `skip` (those merge_all keeps, the `merge` route). Returns {view name: [sources]}."""
    names = discover.sources(spark, catalog, prefix, only)
    held = discover.tables(spark, catalog, prefix, names)
    defined = {}
    for table in sorted(set().union(*held.values()) - set(skip)):
        having = [s for s in names if table in held[s]]
        view = name.format(table=table)
        dp.materialized_view(name=view, comment=f"{table} of every source in one table, with its source in "
                             f"{source_column} (n:1)")(_reader(spark, catalog, prefix, table, having, source_column))
        defined[view] = having
    return defined


def _reader(spark, *args):
    """The view's function, bound to its own table and sources (a closure in the loop would see only the last's)."""
    def build():
        return stacked(spark, *args)
    return build
