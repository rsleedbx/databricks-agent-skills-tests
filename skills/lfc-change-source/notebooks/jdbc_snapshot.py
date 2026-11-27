# Databricks notebook source
# MAGIC %md
# MAGIC # A snapshot through a Unity Catalog JDBC connection
# MAGIC
# MAGIC Each source table read once with Spark's jdbc source (`SELECT *`, lfc_change_source.intake.jdbc) into
# MAGIC `<target_schema>.<table>`, the snapshot contract silver reads. For a source with no query-based connector
# MAGIC (Db2, Informix): run it after the change capture has landed a change of its own, so each key's later changes
# MAGIC win (lfc_change_source.qbc's order). The connection holds the login and the driver (lfc_source_contract.jdbc).
# MAGIC
# MAGIC | Widget | |
# MAGIC |---|---|
# MAGIC | `package` | what to pip-install for lfc_change_source (a wheel path or the repo's archive URL) |
# MAGIC | `jdbc_connection` | the UC connection of TYPE JDBC |
# MAGIC | `tables` | the source's tables, comma separated, as it names them (`DBZ.CUSTOMERS`) |
# MAGIC | `target_schema` | catalog.schema for the snapshot tables (made if missing) |
# MAGIC | `position_kind` | empty: no position; `lsn`: PostgreSQL's, read just before each table |

# COMMAND ----------

# ruff: noqa: F821  (a notebook: dbutils and spark are its built-ins)
for k, v in {"package": "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip"
                        "#subdirectory=skills/lfc-change-source/scripts",
             "jdbc_connection": "", "tables": "", "target_schema": "", "position_kind": ""}.items():
    dbutils.widgets.text(k, v)
arg = {k: dbutils.widgets.get(k) for k in ("package", "jdbc_connection", "tables", "target_schema", "position_kind")}

import json
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--disable-pip-version-check", arg["package"]], check=True)
from lfc_change_source import intake  # noqa: E402

q = lambda n: ".".join("`" + p.replace("`", "``") + "`" for p in n.split("."))  # noqa: E731
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(arg['target_schema'])}")
out = []
for t in [x.strip() for x in arg["tables"].split(",") if x.strip()]:
    r = intake.jdbc(spark, arg["jdbc_connection"], t, f"{arg['target_schema']}.{t.split('.')[-1].lower()}",
                    arg["position_kind"] or None)
    print(json.dumps(r))
    out.append(r)
dbutils.notebook.exit(json.dumps(out))
