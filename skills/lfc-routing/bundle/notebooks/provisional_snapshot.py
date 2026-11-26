# Databricks notebook source
# MAGIC %md
# MAGIC # Provisional snapshot (routing step 3)
# MAGIC
# MAGIC Reads each small table that has statistics (lfc_routing.snapshots.pick, from the source's latest introspection)
# MAGIC through a Unity Catalog JDBC connection with Spark's jdbc source (lfc_change_source.intake.jdbc): the source's
# MAGIC position first, then the table, into `<target_schema>.<table>`. Each is recorded in `snapshot_runs`, and step 7
# MAGIC keeps it as the table's real snapshot while a log the table reads still holds its position. Run it after the
# MAGIC provisional change capture has started (step 2).
# MAGIC
# MAGIC | Parameter | |
# MAGIC |---|---|
# MAGIC | `jdbc_connection` | the UC JDBC connection to read through |
# MAGIC | `source` | the source's name in the introspection tables (its introspection connection) |
# MAGIC | `routing_schema` | catalog.schema of the introspection and routing tables |
# MAGIC | `target_schema` | catalog.schema the snapshot tables go to |
# MAGIC | `position_kind` | the source's position kind (`lsn`: PostgreSQL) |
# MAGIC | `tables` | optional: these tables (comma separated) instead of the picked ones |
# MAGIC | `max_bytes` | the largest table picked |

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

from lfc_change_source import intake
from lfc_introspection import history
from lfc_routing import snapshots

for k, v in {"jdbc_connection": "", "source": "", "routing_schema": "", "target_schema": "", "position_kind": "lsn",
             "tables": "", "max_bytes": str(1 << 30)}.items():
    dbutils.widgets.text(k, v)
arg = {k: dbutils.widgets.get(k) for k in ("jdbc_connection", "source", "routing_schema", "target_schema",
                                         "position_kind", "tables", "max_bytes")}
q = lambda n: ".".join("`" + p.replace("`", "``") + "`" for p in n.split("."))  # noqa: E731
rs, ts = q(arg["routing_schema"]), q(arg["target_schema"])
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ts}")
for stmt in snapshots.ddl(rs):
    spark.sql(stmt)
got = spark.sql(f"SELECT r.run, q.src_schema FROM {rs}.{history.RUNS} r JOIN {rs}.{history.REQUEST} q ON q.n = r.run "
                f"AND q.source = r.source WHERE r.source = '{arg['source']}' AND r.status = 'ok' ORDER BY r.run DESC LIMIT 1").collect()
if not got:
    raise ValueError(f"{arg['source']}: no introspection run yet (introspection_request first)")
run, src_schema = got[0][0], got[0][1]
schema_doc, stats_doc = history.latest(spark, rs, arg["source"], run)
picked, skipped = snapshots.pick(stats_doc, int(arg["max_bytes"]))
names = [t.strip() for t in arg["tables"].split(",") if t.strip()] or [t for t, _ in picked]
for t, why in skipped:
    print(f"skipped {t}: {why}")
lit = lambda v: "NULL" if v is None else "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"  # noqa: E731
out = []
for t in names:
    row = {"tbl": t, "status": "ok"}
    try:
        r = intake.jdbc(spark, arg["jdbc_connection"], f"{src_schema}.{t}", f"{arg['target_schema']}.{t}", arg["position_kind"])
        row.update(snapshot_table=r["table"], position=r["position"]["value"], rows=r["rows"], seconds=r["seconds"])
    except Exception as e:  # noqa: BLE001  recorded as a failed snapshot, with why
        row.update(status="failed", error=f"{type(e).__name__}: {str(e)[:500]}")
    spark.sql(f"INSERT INTO {rs}.{snapshots.RUNS} VALUES ({lit(arg['source'])}, {lit(t)}, current_timestamp(), "
              f"'provisional_jdbc', {lit(row['status'])}, {lit(row.get('error'))}, {lit(row.get('snapshot_table'))}, "
              f"{lit(arg['position_kind'])}, {lit(row.get('position'))}, {row.get('rows') or 'NULL'}, "
              f"{row.get('seconds') or 'NULL'}, {run})")
    out.append(row)
    print(json.dumps(row))
if any(r["status"] != "ok" for r in out):
    raise RuntimeError(f"{sum(r['status'] != 'ok' for r in out)} of {len(out)} snapshots failed (see snapshot_runs)")
