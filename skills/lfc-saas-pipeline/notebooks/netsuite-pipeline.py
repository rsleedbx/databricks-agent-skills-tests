# Databricks notebook source
# MAGIC %md
# MAGIC # Create a managed NetSuite ingestion pipeline
# MAGIC
# MAGIC The docs' notebook ([NetSuite pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/netsuite-pipeline)) with its helper cells installed as a package: only the spec cell is left. Run all; a job's parameters `connection`, `catalog`, `schema`, `name`, `netsuite_jar_path` override the defaults below.

# COMMAND ----------

# MAGIC %pip install --quiet "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-saas-pipeline/scripts"

# COMMAND ----------

from lfc_saas_pipeline import arg, create_pipeline, start_pipeline  # also edit_, delete_, list_, get_pipeline

# COMMAND ----------

# Modify this cell with your pipeline specifications. This creates the ingestion pipeline and starts its first update.
# If destination_table (optional) is not specified, the connector uses the source table name.
connection = arg("connection", "lfcsaas-netsuite")
catalog = arg("catalog", "") or spark.sql("SELECT current_catalog()").first()[0]   # the workspace default, never assume main
schema = arg("schema", "netsuite")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

pipeline_spec = {
    "name": arg("name", schema),
    "catalog": catalog,
    "schema": schema,
    "ingestion_definition": {
        "connection_name": connection,
        # NetSuite's SuiteAnalytics Connect JDBC driver, uploaded to a volume (the docs' "Download the ... driver")
        "netsuite_jar_path": arg("netsuite_jar_path", "/Volumes/main/lfcsaas_drivers/netsuite/8.10.190.0/NQjc.jar"),
        "objects": [
            {"table": {"source_schema": "default", "source_table": t,
                       "destination_catalog": catalog, "destination_schema": schema}}
            for t in ("transaction",)  # any table of the NetSuite2.com data source but DeletedRecords
        ],
    },
}
pipeline = create_pipeline(pipeline_spec)
start_pipeline(pipeline["pipeline_id"])
