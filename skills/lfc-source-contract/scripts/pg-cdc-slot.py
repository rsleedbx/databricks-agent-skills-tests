# Databricks notebook source
# MAGIC %md
# MAGIC # PostgreSQL replication slot for Lakeflow Connect
# MAGIC - Creates replication slot `lfc_<uuid>` on publication `publication_name` (default `lfcddemo_pub`, schema `lfcddemo`).
# MAGIC - Empty `publication_name`: also makes publication `lfc_pub_<uuid>` on the `schemas` (needs owning the tables).
# MAGIC - Prints the pipeline's `slot_config`.
# MAGIC - Each run makes a new slot.
# MAGIC - On the demo VM, slots idle for 2 hours or older than a day are deleted automatically.

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("uc_jdbc_connection", "lfcddemo-jdbc-azurevm-pg")
dbutils.widgets.text("publication_name", "lfcddemo_pub")
dbutils.widgets.text("schemas", "lfcddemo")

# COMMAND ----------

import json

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi

out = cdc.pipeline_slot(dbapi(spark, dbutils.widgets.get("uc_jdbc_connection").strip()).cursor(), "create",
                        schemas=[s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()],
                        publication_name=dbutils.widgets.get("publication_name").strip() or None)
print("In the ingestion pipeline's source_configurations[].catalog, next to source_catalog:")
print(json.dumps(out["slot_config"], indent=2))
