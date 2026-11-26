# Databricks notebook source
# MAGIC %md
# MAGIC # Introspection requests
# MAGIC
# MAGIC Runs every request in `introspection_request` that has no run yet (lfc_introspection.history.run_pending): each
# MAGIC source's catalog read through its Unity Catalog connection, appended to `introspection_runs` (the documents and
# MAGIC what changed since its previous run), `introspection_schema` and `introspection_stats`. A table update trigger on
# MAGIC `introspection_request` runs this job; the first run creates the tables.

# COMMAND ----------

# ruff: noqa: F821  (a notebook: dbutils and spark are its built-ins)
dbutils.widgets.text("tests_repo_archive", "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip")
import subprocess
import sys

archive = dbutils.widgets.get("tests_repo_archive")
for skill in ("lfc-source-statschema", "lfc-introspection"):   # statschema first: introspection needs it
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", f"{archive}#subdirectory=skills/{skill}/scripts"])

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from lfc_introspection import history

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
q = lambda n: "`" + n.replace("`", "``") + "`"  # noqa: E731
target = f"{q(dbutils.widgets.get('catalog'))}.{q(dbutils.widgets.get('schema'))}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target}")
for stmt in history.ddl(target):
    spark.sql(stmt)
runs = history.run_pending(spark, target)
for r in runs:
    print(r)
if any(r["status"] != "ok" for r in runs):
    raise RuntimeError(f"{sum(r['status'] != 'ok' for r in runs)} of {len(runs)} introspections failed (see introspection_runs)")
