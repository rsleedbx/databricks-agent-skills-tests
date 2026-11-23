"""flows: the built-in n:1 path, an AUTO CDC flow per source into one streaming table per table, in a Lakeflow
Spark Declarative Pipelines source file. The pipeline keeps each flow's position (its checkpoint) and runs the flows
at once, so no statement plans every source's change feed; the cost is the pipeline's per flow (Lakeflow Connect's
applier is the same kind of pipeline: about 1 s a flow a triggered update, 2026-11-22), which continuous mode pays
once, not each update. A path for paths.py to be measured against (references/merge-paths.md).

    from pyspark import pipelines as dp
    from lfc_medallion import flows
    flows.cdc_flows(dp, spark, catalog="main", prefix="sales_bronze_", tables={"customer": ["c_id"]},
                    source_column="division")

Each source's bronze table is read as a stream of its change feed (readChangeFeed: Lakeflow Connect's bronze takes
updates and deletes, which a plain streaming read can't follow), update pre-images dropped, the source in
`source_column`; the flow keys on the source column and the table's key, is sequenced by the commit version (a delete
before an insert of the key in the same version, a replacement), and applies deletes. A keyless table can't be an
AUTO CDC target: leave it to stack_all.
"""
from __future__ import annotations

from . import discover

ORDER = "__lfc_order"   # within a commit version, a delete before an insert of the same key


def cdc_flows(dp, spark, catalog: str, prefix: str, tables: dict[str, list[str]], source_column: str = "_source",
              only=None, name: str = "all_{table}") -> dict[str, list[str]]:
    """Per table in `tables` ({table: its key columns}), a streaming table `name` and an AUTO CDC flow into it from
    every source that has the table. Returns {target: [sources]}."""
    from pyspark.sql import functions as F
    names = discover.sources(spark, catalog, prefix, only)
    held, _ = discover.held_tables(spark, catalog, prefix, names)
    out = {}
    for table, key in tables.items():
        if not key:
            raise ValueError(f"{table}: no key, so no AUTO CDC target: leave it to stack_all")
        target = name.format(table=table)
        having = [s for s in names if table in held[s]]
        dp.create_streaming_table(name=target, comment=f"{table} of every source, by an AUTO CDC flow a source (n:1)",
                                  table_properties={"delta.enableChangeDataFeed": "true"})
        for s in having:
            view = f"__lfc_cdc_{table}_{s}"

            def changes(s=s, fq=f"`{catalog}`.`{prefix}{s}`.`{table}`"):
                return (spark.readStream.option("readChangeFeed", "true").table(fq)
                        .where("_change_type != 'update_preimage'")
                        .withColumn(source_column, F.lit(s))
                        .withColumn(ORDER, F.when(F.col("_change_type") == "delete", 0).otherwise(1)))
            (getattr(dp, "temporary_view", None) or dp.view)(name=view)(changes)
            dp.create_auto_cdc_flow(target=target, source=view, keys=[source_column, *key],
                                    sequence_by=F.struct("_commit_version", ORDER),
                                    apply_as_deletes=F.expr("_change_type = 'delete'"),
                                    except_column_list=["_change_type", "_commit_version", "_commit_timestamp", ORDER],
                                    stored_as_scd_type=1, name=f"{target}__{s}")
        out[target] = having
    return out
