# Databricks notebook source
# MAGIC %md
# MAGIC # Create a managed GitHub ingestion pipeline
# MAGIC
# MAGIC The docs' notebook ([GitHub pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/github-pipeline)) with its helper cells installed as a package: only the spec cell is left. Run all; a job's parameters `connection`, `catalog`, `schema`, `name`, `org` override the defaults below. `org` (required) is the GitHub organization the connection's user can read.

# COMMAND ----------

# MAGIC %pip install --quiet "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-saas-pipeline/scripts"

# COMMAND ----------

from lfc_saas_pipeline import arg, create_pipeline, start_pipeline  # also edit_, delete_, list_, get_pipeline

# COMMAND ----------

# Modify this cell with your pipeline specifications. This creates the ingestion pipeline and starts its first update.
# If destination_table (optional) is not specified, the connector uses the source table name.
connection = arg("connection", "lfcsaas-github")
catalog = arg("catalog", "main")
schema = arg("schema", "github")
org = arg("org", "")   # the source schema of every table
assert org, "set org to the GitHub organization name"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

pipeline_spec = {
    "name": arg("name", schema),
    "catalog": catalog,
    "schema": schema,
    "ingestion_definition": {
        "connection_name": connection,
        "objects": [
            {"table": {"source_schema": org, "source_table": t,
                       "destination_catalog": catalog, "destination_schema": schema}}
            for t in ("repositories", "pull_requests")
        ],
    },
}
pipeline = create_pipeline(pipeline_spec)
start_pipeline(pipeline["pipeline_id"])
