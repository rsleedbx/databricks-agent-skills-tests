# Databricks notebook source
# MAGIC %md
# MAGIC # Create a fan-in ingestion
# MAGIC
# MAGIC Set up like an [integrated CDC pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline):
# MAGIC a name, the destination catalog and schema, and an ingestion definition with a connector type.
# MAGIC
# MAGIC | `connector_type` | Source, deployed by you | What it creates |
# MAGIC |---|---|---|
# MAGIC | `fanin_dbz_zbus` | Debezium Server through Zerobus: `change_log` (its log), `snapshot_volume` (each source's snapshot table and the schema), `source_type` | a view `cdc_log` over your log, and silver by FlowThrough's routing (no pipeline) |
# MAGIC | `fanin_crdb_cdf` | CockroachDB changefeed files (`format='json'`) in a Volume: `changefeed_path`, `tables` | the change log `cdc_log` |
# MAGIC
# MAGIC `catalog` and `schema` hold this ingestion's own tables, its state (`_fanin`) and its silver. Running the
# MAGIC notebook again changes nothing it made already, and brings silver up to date.

# COMMAND ----------

# ruff: noqa: F821  (a notebook: dbutils, spark and display are its built-ins)
dbutils.widgets.text("tests_repo_archive", "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip")
import subprocess
import sys

archive = dbutils.widgets.get("tests_repo_archive")
for skill in ("lfc-change-source", "lfc-routing", "lfc-medallion", "lfc-log-silver", "lfc-fanin"):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", f"{archive}#subdirectory=skills/{skill}/scripts"])

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the fan-in
# MAGIC
# MAGIC Edit the values here, or pass them as job parameters (the bundle's variables).

# COMMAND ----------

for k, v in {"pipeline_name": "<pipeline-name>", "connector_type": "fanin_dbz_zbus", "dest_catalog": "<destination-catalog>",
             "dest_schema": "<destination-schema>", "change_log": "", "snapshot_volume": "", "source_type": "",
             "changefeed_path": "", "tables": ""}.items():
    dbutils.widgets.text(k, v)
arg = {k: dbutils.widgets.get(k) for k in ("pipeline_name", "connector_type", "dest_catalog", "dest_schema", "change_log",
                                         "snapshot_volume", "source_type", "changefeed_path", "tables")}


def objects(tables: str) -> list:
    """database.schema.table:key1|key2, comma separated, as table objects."""
    out = []
    for t in (x.strip() for x in tables.split(",") if x.strip()):
        t, _, keys = t.partition(":")
        db, sch, name = t.split(".", 2)
        out.append({"table": {"source_catalog": db, "source_schema": sch, "source_table": name,
                              **({"primary_keys": keys.split("|")} if keys else {})}})
    return out

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from lfc_fanin import fanin

w = WorkspaceClient()

ingestion = fanin.create(
    w, spark,
    name=arg["pipeline_name"],
    catalog=arg["dest_catalog"],
    schema=arg["dest_schema"],
    ingestion_definition={
        "connector_type": arg["connector_type"],
        "objects": objects(arg["tables"]),
        "code_archive": dbutils.widgets.get("tests_repo_archive"),   # restartPython cleared the first cell's variables
        **{k: arg[k] for k in ("change_log", "snapshot_volume", "source_type", "changefeed_path") if arg[k]},
    },
)
print(f"Fan-in created: {ingestion['name']} ({ingestion['connector_type']}), change log {ingestion['log']}")
print(f"  {ingestion['parts']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bring silver up to date

# COMMAND ----------

landed = fanin.land(w, spark, ingestion)
print(f"Landed: {landed}")
