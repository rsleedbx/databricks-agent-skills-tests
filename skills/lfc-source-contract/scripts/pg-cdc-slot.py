# Databricks notebook source
# MAGIC %md
# MAGIC # A PostgreSQL publication and replication slot for a Lakeflow Connect pipeline, through UC JDBC
# MAGIC Writes go through a TYPE JDBC connection (its login must own the tables and have `REPLICATION`), reads through
# MAGIC the POSTGRESQL connection the gateway uses. No password in the notebook. `step` `create` makes
# MAGIC `lfc_<uuid>` and `lfc_pub_<uuid>` (a new uuid when `id` is empty) and prints the pipeline's `slot_config`;
# MAGIC `drop` with the same `id` drops the slot (when not active) and the publication. Safe to run again.
# MAGIC `publication`: an existing one to use instead (it must already have the schemas' tables), for a login that has
# MAGIC `REPLICATION` but doesn't own the tables (a FOR TABLE publication needs their owner); `drop` leaves it.
# MAGIC Run it cell by cell, or as a job with the widgets as parameters.
# ruff: noqa: F821

# COMMAND ----------

# MAGIC %pip install -q "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-contract/scripts"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

for name, default in (("jdbc_connection", ""), ("read_connection", ""), ("database", "postgres"), ("schemas", ""),
                      ("publication", ""), ("id", ""), ("step", "create")):
    dbutils.widgets.text(name, default)
w = {k: dbutils.widgets.get(k).strip() for k in ("jdbc_connection", "read_connection", "database", "schemas",
                                         "publication", "id", "step")}

# COMMAND ----------

import json

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi, guarded, ident, literal

if not w["jdbc_connection"]:
    raise ValueError("jdbc_connection: the TYPE JDBC connection to write with")
if w["step"] == "drop" and not w["id"]:
    raise ValueError("drop: give the id that create printed")
slot_name, pub_name = cdc.names(w["id"] or None)
own_publication = not w["publication"]
pub_name = pub_name if own_publication else w["publication"]
cur = dbapi(spark, w["jdbc_connection"], read_connection=w["read_connection"] or None,
            database=w["database"] if w["read_connection"] else None).cursor()

if w["step"] == "create":
    schemas = w["schemas"].replace(",", " ").split()
    print(cdc.publication(cur, pub_name, schemas))
    print(cdc.slot(cur, slot_name))       # holds WAL from now until the gateway reads it, or drop
    slot_config = {"postgres": {"slot_config": {"slot_name": slot_name, "publication_name": pub_name}}}
    print("id:", slot_name[len("lfc_"):])
    print("In the ingestion pipeline's source_configurations[].catalog (with source_catalog):")
    print(json.dumps(slot_config, indent=2))
elif w["step"] == "drop":                 # after the pipeline is deleted, or WAL keeps growing
    print(cdc.slot(cur, slot_name, drop=True))
    if not own_publication:
        print(f"publication {pub_name} kept (given, not made here)")
    else:
        cur.execute(guarded(f"SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = {literal(pub_name)})",
                            f"DROP PUBLICATION {ident(pub_name)}"))
        print(f"publication {pub_name} dropped")
else:
    raise ValueError(f"step {w['step']}: create or drop")
