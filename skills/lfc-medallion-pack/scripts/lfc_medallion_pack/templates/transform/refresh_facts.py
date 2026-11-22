# Databricks notebook source
# MAGIC %md
# MAGIC # The facts cycle: only the fact chain of a module's transform pipeline
# MAGIC Its facts land continuously (their own ingestion pipelines); this refreshes their n:1 views and every silver
# MAGIC and gold view that reads them (`selection`, from the marts' SQL: render), not the dimensions' views, so a fact
# MAGIC reaches gold without waiting for the dimension cycle. A pipeline runs one update at a time: this waits for one
# MAGIC already running. Nothing to install: the pipelines API only.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

for name in ("pipeline", "selection"):
    dbutils.widgets.text(name, "")
name, selection = dbutils.widgets.get("pipeline"), [t for t in dbutils.widgets.get("selection").split(",") if t]
w = WorkspaceClient()
pid = next(iter(w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"))).pipeline_id


def busy() -> bool:
    ups = w.pipelines.get(pid).latest_updates or []
    return bool(ups) and str(ups[0].state).split(".")[-1] not in ("COMPLETED", "FAILED", "CANCELED")


while busy():   # the dimension cycle's update, or a facts cycle still running
    time.sleep(5)
update = w.pipelines.start_update(pid, refresh_selection=selection).update_id
while True:
    state = str(w.pipelines.get_update(pid, update).update.state).split(".")[-1]
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(5)
print(f"{name}: {state}, {len(selection)} datasets: {', '.join(selection)}")
if state != "COMPLETED":
    raise RuntimeError(f"the facts cycle's update {update} ended {state}")
