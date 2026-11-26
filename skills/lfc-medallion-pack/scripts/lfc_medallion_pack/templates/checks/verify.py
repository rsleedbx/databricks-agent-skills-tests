# Databricks notebook source
# MAGIC %md
# MAGIC # Verify one module against its plan
# MAGIC Bronze (a schema per LOB, every table), n:1 (all_<table> with the LOB column), the marts' views; with data,
# MAGIC the rows carried through and the pack's signal read back from gold per LOB (lfc_medallion_pack.verify). The
# MAGIC refresh job's last task: it fails when a check does.

# COMMAND ----------

for name in ("module", "catalog", "prefix", "archive", "connection", "database", "control"):
    dbutils.widgets.text(name, "")

# COMMAND ----------

ARCHIVE = dbutils.widgets.get("archive")
import subprocess, sys
PKGS = {"lfc-statschema": "lfc-source-statschema", "lfc-source-contract": "lfc-source-contract/scripts",
        "lfc-source-datagen": "lfc-source-datagen/scripts", "lfc-change-source": "lfc-change-source/scripts",
        "lfc-medallion": "lfc-medallion/scripts",
        "lfc-medallion-pack": "lfc-medallion-pack/scripts"}
# every tests-repo package from the one archive (--no-deps: their own pins point at main), then what they need
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--no-deps"] +
                      [f"{n} @ {ARCHIVE}#subdirectory=skills/{d}" for n, d in PKGS.items()])
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg[binary]>=3.2", "pyyaml>=6", "sqlglot>=25"])
dbutils.library.restartPython()

# COMMAND ----------

from lfc_medallion_pack.verify import job
job(spark, {k: dbutils.widgets.get(k) for k in ("module", "catalog", "prefix", "connection", "database", "control")},
    plan_path="../source/plan.json", marts_dir="../marts", show=display)
