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

capture(): where a change's time goes between the gateway and the applier, from what the gateway keeps on its staging
volume (more than its event log has), into two control tables, each by one INSERT ... SELECT FROM read_files per
gateway (only the files not read before):
  gateway_events  telemetry/*.json: CDC_EXTRACTION (about once a minute per gateway: upstream_commit_ and
                  upstream_fetch_timestamp_watermark_ms, the newest source commit read and when it was fetched) and
                  REALTIME_PROGRESS with rows (per table flow, about once a minute: insert, update and delete counts,
                  the window, duration_ms)
  staged_files    cdc/: each change file per LOB, when the gateway wrote it (the file's modification time), its rows
                  and the newest and oldest source commit in it (cursor.timestampUtc)
So a change's path is: source commit -> fetched (the extraction watermark) -> staged (the file's write time; for the
file's newest row that is exact) -> bronze (the applier: v_changes) -> n:1 and gold. v_latency puts the times side by
side per batch and LOB (observe.py).
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
            if not found or not os.path.isdir(f"{found[1]}/cdc"):   # no change published yet: no positions per table
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


def _added(result) -> int | None:
    """The rows an INSERT added, from its result (num_inserted_rows), where the session gives it."""
    try:
        rows = result.collect()
        return int(rows[0][-1]) if rows else 0
    except Exception:  # noqa: BLE001
        return None


def capture(spark, plan: dict, prefix: str, catalog: str, control: str, workspace=None) -> list[dict]:
    """The gateway's own timings into <control>.gateway_events and .staged_files (the module's docstring), one
    INSERT ... SELECT FROM read_files each per gateway, the files read before left out. One row per gateway: found
    (its staging volume, else why not), and the rows each INSERT added, so a gateway skipped or empty shows."""
    from lfc_medallion.stack import spark_string
    if workspace is None:
        from databricks.sdk import WorkspaceClient
        workspace = WorkspaceClient()
    w = workspace
    ctl = f"`{catalog}`.`{control}`"
    done = []
    for m in plan["modules"]:
        mod = m["module"]
        for g in m["groups"]:
            gw = f"{mod}_gw_{g['k']}"
            found = staging(w, f"{prefix}_{gw}")
            row = {"module": mod, "gateway": gw, "found": bool(found), "events": None, "files": None}
            done.append(row)
            if not found:
                row["why"] = f"no gateway pipeline named {prefix}_{gw} visible to this identity"
                continue
            if not os.path.isdir(found[1]):
                row["why"] = f"its staging folder isn't readable here: {found[1]}"
                continue
            root, lit = found[1], spark_string
            lob = f"substr({{col}}, {len(mod) + 2})"
            if os.path.isdir(f"{root}/telemetry"):
                row["events"] = _added(spark.sql(f"INSERT INTO {ctl}.gateway_events SELECT {lit(mod)}, {lit(gw)}, "
                          "CASE event_subtype WHEN 'CDC_EXTRACTION' THEN 'extraction' ELSE 'flow' END, "
                          f"timestamp_seconds(event_timestamp_seconds), {lob.format(col='schema_name')}, table_name, "
                          "timestamp_millis(CAST(operation_details.cdc_extraction_report"
                          ".upstream_commit_timestamp_watermark_ms AS BIGINT)), "
                          "timestamp_millis(CAST(operation_details.cdc_extraction_report"
                          ".upstream_fetch_timestamp_watermark_ms AS BIGINT)), "
                          "to_timestamp(statistics.startTimestamp), to_timestamp(statistics.endTimestamp), "
                          "CAST(statistics.insertCount AS BIGINT), CAST(statistics.updateCount AS BIGINT), "
                          "CAST(statistics.deleteCount AS BIGINT), CAST(statistics.duration_ms AS BIGINT), "
                          f"_metadata.file_path FROM read_files({lit(root + '/telemetry/')}, format => 'json') "
                          "WHERE (event_subtype = 'CDC_EXTRACTION' OR (event_subtype = 'REALTIME_PROGRESS' AND "
                          "CAST(statistics.insertCount AS BIGINT) + CAST(statistics.updateCount AS BIGINT) + "
                          "CAST(statistics.deleteCount AS BIGINT) > 0)) AND _metadata.file_path NOT IN "
                          f"(SELECT file FROM {ctl}.gateway_events WHERE gateway = {lit(gw)})"))
            if os.path.isdir(f"{root}/cdc"):
                row["files"] = _added(spark.sql(f"INSERT INTO {ctl}.staged_files SELECT {lit(mod)}, {lit(gw)}, "
                          f"{lob.format(col='table_id.schema')}, _metadata.file_path, "
                          "_metadata.file_modification_time, COUNT(*), "
                          "MAX(timestamp_millis(CAST(cursor.timestampUtc AS BIGINT))), "
                          "MIN(timestamp_millis(CAST(cursor.timestampUtc AS BIGINT))) "
                          f"FROM read_files({lit(root + '/cdc/')}, format => 'json') "
                          f"WHERE startswith(table_id.schema, {lit(mod + '_')}) AND _metadata.file_path NOT IN "
                          f"(SELECT file FROM {ctl}.staged_files WHERE gateway = {lit(gw)}) GROUP BY 1, 2, 3, 4, 5"))
    return done
