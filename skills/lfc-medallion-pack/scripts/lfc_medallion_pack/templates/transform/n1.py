"""n:1: every bronze table of every LOB of a module stacked into one materialized view, all_<table>, with the LOB in
the source column (lfc_medallion.stack_all). It names no table or column: the LOBs and their tables are found when
each update starts, so a new LOB needs only its ingestion. Every module's transform pipeline runs this file with its
own settings (resources/<module>.yml): catalog, bronze_prefix, source_column, and lobs to restrict them.
"""
from lfc_medallion import stack_all
from pyspark import pipelines as dp

stack_all(dp, spark, catalog=spark.conf.get("catalog"), prefix=spark.conf.get("bronze_prefix"),
          source_column=spark.conf.get("source_column"), only=spark.conf.get("lobs", ""))
