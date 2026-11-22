"""source: the source side of a plan (plan.py): the LOBs' schemas in one Lakebase database, with a publication and a
replication slot per ingestion pipeline. Runs in the source bundle's job (a notebook), through Unity Catalog
connections, so no password is in the code:

    from lfc_medallion_pack.source import run
    rows = run(spark, plan, project="febar", database="febar", role="febar_cdc", connection="febar_pg",
               writer="febar_writer", jdbc_connection="febar_jdbc")

  1. the logins and connections (lfc_source_contract.lakebase.setup: new passwords, set in the connections);
  2. the slot budget: the plan's pipelines against the source's max_replication_slots and max_wal_senders;
  3. each module's LOBs, schema <module>_<lob>, built by lfc_source_datagen at the LOB's scale with its tier's signal
     numbers, or empty (data: false; the no-data test);
  4. per ingestion pipeline: its publication over its LOBs' schemas, and its slot (now, so no change is missed: a slot
     holds WAL until its gateway reads it).
Every step is safe to rerun: a finished LOB is kept, a publication or slot that exists is kept. step="batch" adds
`rows` changes to every LOB of the modules with data (lfc_source_datagen.signals.batch: new rows in the tables the
YAML doesn't call dimensions, then the signals); a scheduled run of it is the change stream. step="drop_slots" drops
the plan's slots (when the test is done; a stopped gateway's slot keeps WAL).
"""
from __future__ import annotations


def _connect(spark, jdbc_connection: str, connection: str, database: str):
    from lfc_source_contract.jdbc import dbapi
    return lambda: dbapi(spark, jdbc_connection, read_connection=connection, database=database)


def slot_budget(cur, need: int) -> dict:
    """The source's slot and sender limits and what's used, against `need` more; raises when they don't fit."""
    cur.execute("SELECT current_setting('max_replication_slots')::int, current_setting('max_wal_senders')::int, "
                "(SELECT COUNT(*) FROM pg_replication_slots)")
    slots, senders, used = (int(x) for x in cur.fetchall()[0])
    out = {"max_replication_slots": slots, "max_wal_senders": senders, "slots_used": used, "slots_needed": need}
    return out


def run(spark, plan: dict, project: str, database: str, role: str, connection: str, writer: str,
        jdbc_connection: str, step: str = "source", modules=None, rows: int = 0, control: str = "",
        log=print) -> list[dict]:
    from lfc_source_contract import cdc
    from lfc_source_contract.lakebase import setup
    from lfc_source_datagen.signals import model_sources
    mods = [m for m in plan["modules"] if not modules or m["module"] in modules]
    connect = _connect(spark, jdbc_connection, connection, database)
    if step == "drop_slots":
        cur = connect().cursor()
        return [{"slot": g["slot"], "left": cdc.slot(cur, g["slot"], drop=True)} for m in mods for g in m["groups"]]
    if step == "batch":   # one batch of changes per LOB of the modules with data (signals.batch: the YAML roles)
        from lfc_source_datagen.signals import batch
        out = []
        for m in mods:
            if m["data"]:
                lob = {x["source_schema"]: x["name"] for x in m["lobs"]}
                done = batch(connect, m["model"], list(lob), rows, log=log)
                out += [{"module": m["module"]} | r for r in done]
                if control:   # catalog.schema: the batches the dashboard's latency starts from
                    spark.createDataFrame([(m["module"], lob[r["source"]], int(rows)) for r in done],
                                          "module string, lob string, rows bigint").selectExpr(
                        "*", "current_timestamp() AS at").write.mode("append").saveAsTable(f"{control}.batches")
        return out
    if step != "source":
        raise ValueError(f"step {step}: source, batch or drop_slots")
    log(setup(project, role=role, connection=connection, database=database, writer=writer,
              jdbc_connection=jdbc_connection))
    cur = connect().cursor()
    cur.execute("SELECT slot_name FROM pg_replication_slots")
    have = {r[0] for r in cur.fetchall()}
    need = sum(1 for m in mods for g in m["groups"] if g["slot"] not in have)
    b = slot_budget(cur, need)
    log(f"source: slots {b}")
    if b["slots_used"] + need > min(b["max_replication_slots"], b["max_wal_senders"]):
        raise RuntimeError(f"source: {need} more slots needed, {b['slots_used']} used, the source allows "
                           f"{b['max_replication_slots']} slots and {b['max_wal_senders']} WAL senders: raise both, "
                           "or plan fewer pipelines (limits.tables_per_pipeline)")
    rows = []
    for m in mods:
        srcs = [(x["source_schema"], x["sf"], x["signals"]) for x in m["lobs"]]
        log(f"source: module {m['module']}: {len(srcs)} LOBs of {m['model']}, " + ("filled" if m["data"] else "empty"))
        got = model_sources(connect, m["model"], srcs, login=role,
                            publication=f"{plan['source']['slot_prefix']}_{m['module']}_all", log=log, fill=m["data"])
        tier = {x["source_schema"]: x["tier"] for x in m["lobs"]}
        rows += [{"module": m["module"], "tier": tier.get(r["source"])} | r for r in got]
        for g in m["groups"]:
            schemas = [f"{m['module']}_{lob}" for lob in g["lobs"]]
            log(f"source: {g['publication']}: "
                f"{cdc.publication(cur, g['publication'], schemas, log=log, only=g.get('table_names'))}")
            log(f"source: slot {cdc.slot(cur, g['slot'])}")
    return rows
