"""n:1: every bronze table of every LOB of a module stacked into one materialized view, all_<table>, with the LOB in
the source column (lfc_medallion.stack_all). It names no table or column: the LOBs and their tables are found when
each update starts, so a new LOB needs only its ingestion. Every module's transform pipeline runs this file with its
own settings (resources/<module>.yml): catalog, bronze_prefix, source_column, silver_schema, and lobs to restrict them.
The tables the refresh job's merge task keeps (the schema YAML's dimensions: lfc-medallion's merge route) are left
out here: they're tables in the silver schema, all_<table> as well, read the same way by silver.
"""
from lfc_medallion import merged_tables, stack_all
from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")
stack_all(dp, spark, catalog=catalog, prefix=spark.conf.get("bronze_prefix"),
          source_column=spark.conf.get("source_column"), only=spark.conf.get("lobs", ""),
          skip=merged_tables(spark, catalog, spark.conf.get("silver_schema")))   # kept by the merge task
