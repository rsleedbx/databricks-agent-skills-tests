"""cdc: where each source's change capture stands, against where its gateway stands: three columns, source, target and
the delta between them (PostgreSQL: WAL positions, LSNs, and the bytes between them), per gateway and per table.

  scope slot    source  pg_current_wal_lsn(): the newest WAL position in the source database
                target  the gateway's committed position: its SQLite state (backup/<replicant>/..sqlite on its
                        staging volume, cdc_extractor_metadata_*.committed_cursor.lsn), and the slot's confirmed_flush_lsn
                        (what PostgreSQL knows the gateway has confirmed) beside it
                delta   bytes of WAL the gateway hasn't confirmed: the capture lag
  scope table   source  pg_current_wal_lsn()
                target  the LSN of the last change the gateway published for the table (cdc/ on its staging volume)
                delta   bytes of WAL written since then: on a quiet table it grows though nothing is pending (PostgreSQL
                        keeps no position per table); read it next to the slot's delta
Read from Databricks: the source through the UC connection's remote_query, the staging volume through /Volumes and
read_files. Rows go to <control>.cdc_positions with the time, for the dashboard.
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3


def lsn_bytes(lsn: str | None) -> int | None:
    """A PostgreSQL LSN 'X/Y' (hex) as a byte position."""
    if not lsn or "/" not in lsn:
        return None
    hi, lo = lsn.split("/", 1)
    return (int(hi, 16) << 32) + int(lo.split("-")[0], 16)


def staging(w, gateway_name: str) -> tuple[str, str] | None:
    """(gateway id, its staging folder under /Volumes) for a gateway pipeline by name."""
    p = next(iter(w.pipelines.list_pipelines(filter=f"name LIKE '{gateway_name}'")), None)
    if p is None:
        return None
    gd = w.pipelines.get(p.pipeline_id).spec.gateway_definition
    root = (f"/Volumes/{gd.gateway_storage_catalog}/{gd.gateway_storage_schema}/"
            f"__databricks_ingestion_gateway_staging_data-{p.pipeline_id}/{p.pipeline_id}")
    return p.pipeline_id, root


def committed(root: str) -> str | None:
    """The newest committed LSN in the gateway's SQLite state backups (the CDC replicant's)."""
    best = None
    for f in glob.glob(os.path.join(root, "backup", "*", "*.sqlite")):
        con = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        try:
            for (t,) in con.execute("SELECT name FROM sqlite_master WHERE name LIKE 'cdc_extractor_metadata_%'"):
                for (raw,) in con.execute(f'SELECT committed_cursor FROM "{t}"'):
                    lsn = (json.loads(raw) if raw else {}).get("lsn")
                    if lsn and (best is None or lsn_bytes(lsn) > lsn_bytes(best)):
                        best = lsn
        finally:
            con.close()
    return best


def run(spark, plan: dict, prefix: str, connection: str, database: str, workspace=None) -> list[dict]:
    """Per gateway (its slot) and per table of its LOBs: source, target and delta (the module's docstring)."""
    from databricks.sdk import WorkspaceClient
    w = workspace or WorkspaceClient()
    out = []
    src = spark.sql("SELECT * FROM remote_query(:c, database => :d, query => :q)", args={
        "c": connection, "d": database,
        "q": "SELECT pg_current_wal_lsn()::text AS now_lsn, slot_name, confirmed_flush_lsn::text AS confirmed, "
             "active FROM pg_replication_slots WHERE slot_type = 'logical'"}).collect()
    now = src[0].now_lsn if src else None
    slots = {r.slot_name: r for r in src}
    for m in plan["modules"]:
        for g in m["groups"]:
            name = f"{prefix}_{m['module']}_gw_{g['k']}"
            found = staging(w, name)
            s = slots.get(g["slot"])
            tgt = committed(found[1]) if found else None
            out.append({"module": m["module"], "gateway": f"{m['module']}_gw_{g['k']}", "scope": "slot",
                        "lob": None, "table_name": g["slot"], "source_lsn": now, "target_lsn": tgt,
                        "slot_confirmed_lsn": s.confirmed if s else None,
                        "delta_bytes": None if not (now and tgt) else lsn_bytes(now) - lsn_bytes(tgt),
                        "last_change_at": None, "state": "no slot" if s is None else
                        "active" if s.active else "inactive"})
            if not found:
                continue
            lob = {f"{m['module']}_{x}": x for x in g["lobs"]}
            for r in spark.sql(
                    f"SELECT table_id.schema AS s, table_id.name AS t, MAX(cursor.sequence) AS seq, "
                    f"MAX(cursor.timestampUtc) AS at_ms FROM read_files('{found[1]}/cdc/', format => 'json') "
                    f"GROUP BY 1, 2").collect():
                if r.s not in lob:
                    continue
                pub = r.seq.split("-")[0] if r.seq else None
                pub = pub.replace("00000000/", "0/") if pub else None
                out.append({"module": m["module"], "gateway": f"{m['module']}_gw_{g['k']}", "scope": "table",
                            "lob": lob[r.s], "table_name": r.t, "source_lsn": now, "target_lsn": pub,
                            "slot_confirmed_lsn": s.confirmed if s else None,
                            "delta_bytes": None if not (now and pub) else lsn_bytes(now) - lsn_bytes(pub),
                            "last_change_at": int(r.at_ms) if r.at_ms else None, "state": "published"})
    return out


def save(spark, rows: list[dict], catalog: str, schema: str) -> None:
    if not rows:
        return
    spark.createDataFrame(rows, "module string, gateway string, scope string, lob string, table_name string, "
                                "source_lsn string, target_lsn string, slot_confirmed_lsn string, delta_bytes bigint, "
                                "last_change_at bigint, state string").selectExpr(
        "current_timestamp() AS checked_at", "*", "timestamp_millis(last_change_at) AS last_change_time").drop(
        "last_change_at").write.mode("append").option("mergeSchema", "true").saveAsTable(
        f"`{catalog}`.`{schema}`.cdc_positions")
