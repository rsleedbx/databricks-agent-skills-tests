# Databricks notebook source
# MAGIC %md
# MAGIC # A replication slot for a Lakeflow Connect PostgreSQL pipeline, through UC JDBC
# MAGIC - Uses the UC JDBC connection `lfcddemo-jdbc-azurevm-pg` to create replication slot `lfc_<uuid>` on publication
# MAGIC   `lfcddemo_pub` (schema `lfcddemo`); it prints the pipeline's `slot_config` and the `id` for `step` `drop`.
# MAGIC - On the demo VM, slots idle for 2 hours or older than a day are deleted automatically.
# ruff: noqa: F821

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

for name, default in (("jdbc_connection", "lfcddemo-jdbc-azurevm-pg"), ("schemas", "lfcddemo"), ("publication", "lfcddemo_pub"), ("id", ""), ("step", "create")):
    dbutils.widgets.text(name, default)
w = {k: dbutils.widgets.get(k).strip() for k in ("jdbc_connection", "schemas", "publication", "id", "step")}

# COMMAND ----------

import json

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi

if not w["jdbc_connection"]:
    raise ValueError("jdbc_connection: the TYPE JDBC connection to the source")
out = cdc.pipeline_slot(dbapi(spark, w["jdbc_connection"]).cursor(), w["step"], id=w["id"] or None,
                        schemas=w["schemas"].replace(",", " ").split(), publication_name=w["publication"] or None)
print("id:", out["id"], "(give it to step drop)")
if "slot_config" in out:
    print("In the ingestion pipeline's source_configurations[].catalog, next to source_catalog:")
    print(json.dumps(out["slot_config"], indent=2))
