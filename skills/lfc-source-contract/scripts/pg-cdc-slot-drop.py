# Databricks notebook source
# MAGIC %md
# MAGIC # Drop a PostgreSQL replication slot
# MAGIC - Drops the slot named in the widget (the `slot_name` pg-cdc-slot printed);
# MAGIC   publication `lfcddemo_pub` stays. An active slot is refused: stop or delete its pipeline first.
# MAGIC - On the demo VM, old slots are deleted automatically anyway (idle 2 hours, or older than a day).

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("jdbc_connection", "lfcddemo-jdbc-azurevm-pg")
dbutils.widgets.text("slot_name", "")

# COMMAND ----------

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi

slot_name = dbutils.widgets.get("slot_name").strip()
if not slot_name.startswith("lfc_"):
    raise ValueError(f"slot_name {slot_name!r}: the lfc_<uuid> name pg-cdc-slot printed")
cdc.pipeline_slot(dbapi(spark, dbutils.widgets.get("jdbc_connection").strip()).cursor(), "drop",
                  id=slot_name[len("lfc_"):], publication_name="lfcddemo_pub")
