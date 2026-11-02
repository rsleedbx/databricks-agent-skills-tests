# Databricks notebook source
# history-snapshot: SCD type 2 history of a Lakeflow Connect destination table from periodic snapshots of it, in a
# pipeline of its own. Each update reads the whole table and AUTO CDC FROM SNAPSHOT compares it with the previous
# snapshot: a new or changed row opens a version, a missing row closes one. No change data feed is read.
# Settings: src (the table), keys (comma-separated key columns, default id). `spark` comes from the pipeline runtime.
# ruff: noqa: F821
import dlt

SRC = spark.conf.get("src")
KEYS = [k.strip() for k in spark.conf.get("keys", "id").split(",") if k.strip()]

dlt.create_streaming_table("history")
dlt.apply_changes_from_snapshot(
    target="history", source=SRC, keys=KEYS, stored_as_scd_type=2
)
