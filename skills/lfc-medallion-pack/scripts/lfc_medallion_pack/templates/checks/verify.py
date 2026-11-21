# Databricks notebook source
# MAGIC %md
# MAGIC # Verify one module against its plan
# MAGIC Bronze (a schema per LOB, every table), n:1 (all_<table> with the LOB column), the marts' views; with data,
# MAGIC the rows carried through and the pack's signal read back from gold per LOB (lfc_medallion_pack.verify). The
# MAGIC refresh job's last task: it fails when a check does.

# COMMAND ----------

for name in ("module", "catalog", "prefix", "archive"):
    dbutils.widgets.text(name, "")

# COMMAND ----------

ARCHIVE = dbutils.widgets.get("archive")
import subprocess, sys
PKGS = {"lfc-statschema": "lfc-source-statschema", "lfc-source-contract": "lfc-source-contract/scripts",
        "lfc-source-datagen": "lfc-source-datagen/scripts", "lfc-medallion-pack": "lfc-medallion-pack/scripts"}
# every tests-repo package from the one archive (--no-deps: their own pins point at main), then what they need
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--no-deps"] +
                      [f"{n} @ {ARCHIVE}#subdirectory=skills/{d}" for n, d in PKGS.items()])
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg[binary]>=3.2", "pyyaml>=6", "sqlglot>=25"])
dbutils.library.restartPython()

# COMMAND ----------

from lfc_medallion_pack.plan import read_plan
from lfc_medallion_pack.verify import run
w = {k: dbutils.widgets.get(k) for k in ("module", "catalog", "prefix")}
rows = run(spark, read_plan("../source/plan.json"), w["module"], catalog=w["catalog"], prefix=w["prefix"],
           marts_dir="../marts")
display(spark.createDataFrame(rows))
bad = [r for r in rows if not r["ok"]]
if bad:
    raise AssertionError(f"{len(bad)} of {len(rows)} checks failed: " + "; ".join(f"{r['check']}: want {r['want']}, got {r['got']} {r['detail']}" for r in bad[:10]))
print(f"all {len(rows)} checks passed")
