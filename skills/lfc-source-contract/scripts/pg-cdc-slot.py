# Databricks notebook source
# MAGIC %md
# MAGIC # A replication slot for a Lakeflow Connect PostgreSQL pipeline, through UC JDBC
# MAGIC - Creates replication slot `lfc_<uuid>` on publication `lfcddemo_pub` (schema `lfcddemo`) through the UC JDBC
# MAGIC   connection in the widget, and prints the pipeline's `slot_config`. Each run makes a new slot.
# MAGIC - On the demo VM, slots idle for 2 hours or older than a day are deleted automatically.
# ruff: noqa: F821

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("jdbc_connection", "lfcddemo-jdbc-azurevm-pg")

# COMMAND ----------

import json

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi

out = cdc.pipeline_slot(dbapi(spark, dbutils.widgets.get("jdbc_connection").strip()).cursor(), "create",
                        schemas=["lfcddemo"], publication_name="lfcddemo_pub")
print("In the ingestion pipeline's source_configurations[].catalog, next to source_catalog:")
print(json.dumps(out["slot_config"], indent=2))
