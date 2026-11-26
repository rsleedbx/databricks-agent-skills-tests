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


def stack_sql(catalog: str, prefix: str, table: str, cols: dict[str, list[tuple]], source_column: str = "_source",
              labels: dict | None = None) -> str:
    """One table of every source in ONE statement: SELECT each source's rows, its name in the source column, UNION
    ALL, by column name (a column a source lacks is a typed NULL there; a column whose type differs between sources
    is cast to the widest, coltypes.widest, where it's narrower): the same rows as stacked(), with no schema read per
    source. cols: {source: [(column, type)]} for this table (discover.catalog_columns). labels: {source: the value its
    rows carry in the source column} where that isn't the schema's suffix (a source named pg1.tpch1, a schema can't be)."""
    order: list[str] = []
    types: dict[str, list[str]] = {}
    for s in cols:
        for c, t in cols[s]:
            if c not in types:
                order.append(c)
                types[c] = []
            types[c].append(t)
    wide = {c: coltypes.widest(sorted(set(ts))) if len(set(ts)) > 1 else ts[0] for c, ts in types.items()}
    parts = []
    for s in sorted(cols):
        have = dict(cols[s])
        sel = []
        for c in order:
            q = f"`{c.replace('`', '``')}`"
            if c not in have:
                sel.append(f"CAST(NULL AS {wide[c]}) AS {q}")
            elif coltypes.from_spark(have[c]) != coltypes.from_spark(wide[c]):
                sel.append(f"CAST({q} AS {wide[c]}) AS {q}")
            else:
                sel.append(q)
        parts.append(f"SELECT {spark_string((labels or {}).get(s, s))} AS `{source_column}`, {', '.join(sel)} "
                     f"FROM `{catalog}`.`{prefix}{s}`.`{table}`")
    return "\nUNION ALL\n".join(parts)


def stack_all(dp, spark, catalog: str, prefix: str, source_column: str = "_source", only=None,
              name: str = "all_{table}", skip=()) -> dict[str, list[str]]:
    """Define one materialized view per table found in any source (named by `name`), stacked from the sources that
    have it, except the tables in `skip` (those merge_all keeps, the `merge` route). Returns {view name: [sources]}.
    The sources' tables and column types come from one information_schema query and each view is one statement
    (stack_sql); where that query can't run, a SHOW TABLES per source and a DataFrame per source (stacked)."""
    names = discover.sources(spark, catalog, prefix, only)
    held, cols = discover.held_tables(spark, catalog, prefix, names)   # a source it misses: the per-source way
    defined = {}
    for table in sorted(set().union(*held.values()) - set(skip)):
        having = [s for s in names if table in held[s]]
        view = name.format(table=table)
        comment = f"{table} of every source in one table, with its source in {source_column} (n:1)"
        if cols is not None and all(s in cols for s in having):
            sql = stack_sql(catalog, prefix, table, {s: cols[s][table] for s in having}, source_column)
            dp.materialized_view(name=view, comment=comment)(_sql_reader(spark, sql))
        else:
            dp.materialized_view(name=view, comment=comment)(_reader(spark, catalog, prefix, table, having,
                                                                      source_column))
        defined[view] = having
    return defined


def _sql_reader(spark, sql: str):
    """The view's function, bound to its own statement."""
    def build():
        return spark.sql(sql)
    return build


def _reader(spark, *args):
    """The view's function, bound to its own table and sources (a closure in the loop would see only the last's)."""
    def build():
        return stacked(spark, *args)
    return build
