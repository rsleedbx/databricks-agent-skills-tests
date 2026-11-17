# Databricks notebook source
# MAGIC %md
# MAGIC # A PostgreSQL publication and replication slot for a Lakeflow Connect pipeline, through UC JDBC
# MAGIC Everything goes through one TYPE JDBC connection, in its database (its login needs `REPLICATION`, and owns the
# MAGIC tables unless `publication` is given). No password in the notebook. `step` `create` makes
# MAGIC `lfc_<uuid>` and `lfc_pub_<uuid>` (a new uuid when `id` is empty) and prints the pipeline's `slot_config`;
# MAGIC `drop` with the same `id` drops the slot and the publication; an active slot stops it. To run create again, give the printed `id`:
# MAGIC with none, each run makes a new pair, and an unused slot holds WAL until dropped.
# MAGIC `publication`: an existing one to use instead (checked, never changed: it must have the schemas' tables), for a login that has
# MAGIC `REPLICATION` but doesn't own the tables (a FOR TABLE publication needs their owner); `drop` leaves it.
# MAGIC Import: Workspace > Import > URL, `https://raw.githubusercontent.com/rsleedbx/databricks-agent-skills-tests/main/skills/lfc-source-contract/scripts/pg-cdc-slot.py`.
# MAGIC Run it cell by cell (serverless), or as a job with the widgets as parameters.
# ruff: noqa: F821

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

for name, default in (("jdbc_connection", ""), ("schemas", ""), ("publication", ""), ("id", ""), ("step", "create")):
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
