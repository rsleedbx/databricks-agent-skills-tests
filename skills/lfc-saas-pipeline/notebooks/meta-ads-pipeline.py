# Databricks notebook source
# MAGIC %md
# MAGIC # Create a managed Meta Ads ingestion pipeline
# MAGIC
# MAGIC The docs' notebook ([Meta Ads pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-pipeline)) with its helper cells installed as a package: only the spec cell is left. Run all; a job's parameters `connection`, `catalog`, `schema`, `name`, `account_id`, `insights_start_date` override the defaults below. `account_id` (required) is the ad account with its `act_` prefix, one the connection's Meta user can see.

# COMMAND ----------

# MAGIC %pip install --quiet "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-saas-pipeline/scripts"

# COMMAND ----------

from lfc_saas_pipeline import arg, create_pipeline, start_pipeline  # also edit_, delete_, list_, get_pipeline

# COMMAND ----------

# Modify this cell with your pipeline specifications. This creates the ingestion pipeline and starts its first update.
# If destination_table (optional) is not specified, the connector uses the source table name.
connection = arg("connection", "lfcsaas-metaads")
catalog = arg("catalog", "") or spark.sql("SELECT current_catalog()").first()[0]   # the workspace default, never assume main
schema = arg("schema", "meta_ads")
account_id = arg("account_id", "")   # act_<id>: the source schema of every table
assert account_id.startswith("act_"), "set account_id to the ad account id with its act_ prefix"
# ad_insights from this day (YYYY-MM-DD, within the last 36 months); without start_date the connector reads 36 months.
insights_start_date = arg("insights_start_date", "2026-10-01")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

table = {"source_schema": account_id, "destination_catalog": catalog, "destination_schema": schema}
pipeline_spec = {
    "name": arg("name", schema),
    "catalog": catalog,
    "schema": schema,
    "ingestion_definition": {
        "connection_name": connection,
        "objects": [{"table": {**table, "source_table": t}} for t in ("campaigns", "ad_sets", "ads")] + [
            {"table": {**table, "source_table": "ad_insights",
                       "connector_options": {"meta_ads_options": {"start_date": insights_start_date}}}}
        ],
    },
}
pipeline = create_pipeline(pipeline_spec)
start_pipeline(pipeline["pipeline_id"])
