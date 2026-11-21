# Databricks notebook source
# MAGIC %md
# MAGIC # The source: every module's LOBs in one Lakebase database
# MAGIC The logins and connections, each module's LOBs (a schema each, filled or empty as `plan.json` says), and one
# MAGIC publication and replication slot per ingestion pipeline (lfc_medallion_pack.source). `step`: `source`, `batch`
# MAGIC (`rows` changes per LOB of the modules with data), or `drop_slots` when the test is done (a slot keeps WAL until its gateway reads it). Safe to rerun.

# COMMAND ----------

for name, default in (("project", ""), ("database", ""), ("role", ""), ("connection", ""), ("writer", ""),
                      ("jdbc_connection", ""), ("archive", ""), ("modules", ""), ("step", "source"), ("rows", "0")):
    dbutils.widgets.text(name, default)
w = {k: dbutils.widgets.get(k) for k in ("project", "database", "role", "connection", "writer", "jdbc_connection",
                                         "archive", "modules", "step")}

# COMMAND ----------

import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q",
                       f"lfc-source-datagen @ {w['archive']}#subdirectory=skills/lfc-source-datagen/scripts",
                       f"lfc-medallion-pack @ {w['archive']}#subdirectory=skills/lfc-medallion-pack/scripts"])
dbutils.library.restartPython()

# COMMAND ----------

w = {k: dbutils.widgets.get(k) for k in ("project", "database", "role", "connection", "writer", "jdbc_connection",
                                         "modules", "step", "rows")}
from lfc_medallion_pack.plan import read_plan
from lfc_medallion_pack.source import run
rows = run(spark, read_plan("plan.json"), project=w["project"], database=w["database"], role=w["role"], connection=w["connection"],
           writer=w["writer"], jdbc_connection=w["jdbc_connection"], step=w["step"],
           modules=w["modules"].split(",") if w["modules"] else None, rows=int(w["rows"] or 0))
display(spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in rows]) if rows else "nothing")
