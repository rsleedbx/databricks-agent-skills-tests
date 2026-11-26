# Databricks notebook source
# MAGIC %md
# MAGIC # A classify round (routing step 4)
# MAGIC
# MAGIC Runs when a row is added to `routing_iteration` (a table update trigger): each source's latest introspection,
# MAGIC what its change log showed since the last round, the overrides in `routing_decisions`, then classify with the
# MAGIC repeating tables grouped, appended to `routing_plan` as that round (lfc_routing.rounds.run_round).
# MAGIC
# MAGIC `sources`: JSON, `{"<introspection source>": {"log": "<catalog.schema.table>", "prefix": "<destination prefix>"}}`.

# COMMAND ----------

# ruff: noqa: F821  (a notebook: dbutils and spark are its built-ins)
dbutils.widgets.text("tests_repo_archive", "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip")
import subprocess
import sys

archive = dbutils.widgets.get("tests_repo_archive")
for skill in ("lfc-change-source", "lfc-source-statschema", "lfc-introspection", "lfc-medallion", "lfc-routing"):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", f"{archive}#subdirectory=skills/{skill}/scripts"])

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

from lfc_routing import rounds, tables

dbutils.widgets.text("routing_schema", "")
dbutils.widgets.text("sources", "{}")
q = lambda n: ".".join("`" + p.replace("`", "``") + "`" for p in n.split("."))  # noqa: E731
rs = q(dbutils.widgets.get("routing_schema"))
sources = {k: {**v, "log": q(v["log"])} for k, v in json.loads(dbutils.widgets.get("sources")).items()}
for stmt in tables.ddl(rs) + rounds.ddl(rs):
    spark.sql(stmt)
plan = rounds.run_round(spark, rs, sources)
print(f"round {plan['round']}: {len(plan['tables'])} plan rows from introspection runs {plan['introspection_runs']}")
for t, p in sorted(plan["tables"].items()):
    r = p["route"]
    print(f"  {t}: {p['role']}, {r['class']}, {r['silver']}, {r['snapshot_by']}, {len(plan['groups'][t]['members'])} member(s)")
