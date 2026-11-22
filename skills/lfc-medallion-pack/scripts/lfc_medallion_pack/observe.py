"""observe: views in the control schema over every pipeline's event log, the merge log, the batches and the checksums,
for a dashboard (and Genie). Nothing names a model: the modules, LOBs, tables and pipelines come from the plan.

    from lfc_medallion_pack.observe import create_views
    create_views(spark, plan, catalog="main", prefix="me_febar", control="me_febar_control")

  v_changes     rows through each stage, per module, pipeline, update, time, LOB, table and kind:
                  1 source capture   the gateway reading the source (snapshot or cdc rows)
                  2 bronze           the ingestion pipeline writing bronze (upserted, deleted; snapshot or cdc)
                  3 n1 merge         merge_all keeping a table of the merge route (inserted, updated, deleted)
                  3 n1 pipeline      the transform's all_<table> views (rows written)
                  4 silver, 5 gold   the transform's views (rows written; no LOB: a view holds every LOB)
  v_updates     each pipeline update: when it started and ended, how, and its seconds in each phase
  v_latency     each batch of changes (the source job's batch step), and when its LOB's rows next reached bronze, the
                n:1 merge, and gold (the transform's next completed update): seconds after the batch
  v_checksums   the latest checksum of each table of each LOB: rows and digest in the source, bronze and n:1
"""
from __future__ import annotations


def _s(x: str) -> str:
    return "'" + str(x).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _metric(name: str) -> str:
    return f"CAST(details:flow_progress.metrics:{name} AS BIGINT)"


def change_sql(plan: dict, catalog: str, prefix: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    parts = []
    for m in plan["modules"]:
        n, tables = m["module"], "|".join(m["tables"])
        for g in m["groups"]:
            gw, ing = f"{n}_gw_{g['k']}", f"{n}_ingest_{g['k']}"
            flow = "origin.flow_name"
            parts.append(
                f"SELECT '1 source capture' AS stage, {_s(n)} AS module, {_s(gw)} AS pipeline, origin.update_id, "
                f"timestamp AS at, regexp_extract({flow}, '\\\\.{n}_(.+)\\\\.({tables})_(cdc|snapshot)_flow$', 1) AS lob, "
                f"regexp_extract({flow}, '\\\\.({tables})_(cdc|snapshot)_flow$', 1) AS table_name, "
                f"regexp_extract({flow}, '_(cdc|snapshot)_flow$', 1) AS kind, CAST(NULL AS BIGINT) AS inserted, "
                f"CAST(NULL AS BIGINT) AS updated, {_metric('num_upserted_rows')} AS upserted, "
                f"{_metric('num_deleted_rows')} AS deleted, {_metric('num_output_rows')} AS output_rows "
                f"FROM {ctl}.`events_{gw}` WHERE event_type = 'flow_progress' "
                f"AND details:flow_progress.metrics:num_output_rows IS NOT NULL")
            parts.append(
                f"SELECT '2 bronze' AS stage, {_s(n)}, {_s(ing)}, origin.update_id, timestamp, "
                f"regexp_extract({flow}, '_{n}_bronze_(.+)_({tables})_(cdc|snapshot)_flow$', 1), "
                f"regexp_extract({flow}, '_({tables})_(cdc|snapshot)_flow$', 1), "
                f"regexp_extract({flow}, '_(cdc|snapshot)_flow$', 1), NULL, NULL, {_metric('num_upserted_rows')}, "
                f"{_metric('num_deleted_rows')}, NULL "
                f"FROM {ctl}.`events_{ing}` WHERE event_type = 'flow_progress' "
                f"AND details:flow_progress.metrics:num_upserted_rows IS NOT NULL")
        tr = f"{n}_transform"
        silver, gold = f"{prefix}_{n}_silver", f"{prefix}_{n}_gold"
        ds = "element_at(split(origin.flow_name, '\\\\.'), -1)"
        parts.append(
            f"SELECT CASE WHEN {ds} LIKE 'all\\\\_%' THEN '3 n1 pipeline' "
            f"WHEN origin.flow_name LIKE {_s(f'%.{gold}.%')} THEN '5 gold' ELSE '4 silver' END, {_s(n)}, {_s(tr)}, "
            f"origin.update_id, timestamp, CAST(NULL AS STRING), {ds}, 'view', NULL, NULL, NULL, NULL, "
            f"{_metric('num_output_rows')} FROM {ctl}.`events_{tr}` WHERE event_type = 'flow_progress' "
            f"AND details:flow_progress.metrics:num_output_rows IS NOT NULL "
            f"AND (origin.flow_name LIKE {_s(f'%.{silver}.%')} OR origin.flow_name LIKE {_s(f'%.{gold}.%')})")
        parts.append(f"SELECT '3 n1 merge', {_s(n)}, 'merge', run_id, at, source, substr(target_table, 5), done, "
                     f"inserted, updated, NULL, deleted, NULL FROM {ctl}.`merge_log_{n}`")
    return f"CREATE OR REPLACE VIEW {ctl}.v_changes AS\n" + "\nUNION ALL\n".join(parts)


def update_sql(plan: dict, catalog: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    logs = [(m["module"], f"{m['module']}_{p}_{g['k']}") for m in plan["modules"] for g in m["groups"]
            for p in ("gw", "ingest")] + [(m["module"], f"{m['module']}_transform") for m in plan["modules"]]
    one = ("SELECT {mod} AS module, {p} AS pipeline, origin.update_id, timestamp, "
           "regexp_extract(message, ' is ([A-Z_]+)\\\\.', 1) AS state FROM {ctl}.`events_{name}` "
           "WHERE event_type = 'update_progress'")
    marks = "\nUNION ALL\n".join(one.format(mod=_s(mod), p=_s(name), ctl=ctl, name=name) for mod, name in logs)
    return (f"CREATE OR REPLACE VIEW {ctl}.v_updates AS\nWITH m AS ({marks}),\n"
            "s AS (SELECT *, LEAD(timestamp) OVER (PARTITION BY pipeline, update_id ORDER BY timestamp) AS next_at "
            "FROM m)\nSELECT module, pipeline, update_id, MIN(timestamp) AS started, MAX(timestamp) AS ended, "
            "MAX_BY(state, timestamp) AS state, "
            "ROUND((unix_millis(MAX(timestamp)) - unix_millis(MIN(timestamp))) / 1000.0, 1) AS seconds, "
            + ", ".join(f"ROUND(SUM(CASE WHEN state = '{p}' THEN unix_millis(next_at) - unix_millis(timestamp) END) "
                        f"/ 1000.0, 1) AS {p.lower()}_s" for p in ("WAITING_FOR_RESOURCES", "INITIALIZING",
                                                                  "SETTING_UP_TABLES", "RUNNING"))
            + " FROM s GROUP BY module, pipeline, update_id")


def latency_sql(catalog: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    return (f"CREATE OR REPLACE VIEW {ctl}.v_latency AS\n"
            f"SELECT b.module, b.lob, b.at AS batch_at, b.rows,\n"
            f"  (SELECT MIN(c.at) FROM {ctl}.v_changes c WHERE c.stage = '2 bronze' AND c.module = b.module "
            f"AND c.lob = b.lob AND c.kind = 'cdc' AND c.upserted > 0 AND c.at > b.at) AS bronze_at,\n"
            f"  (SELECT MIN(c.at) FROM {ctl}.v_changes c WHERE c.stage = '3 n1 merge' AND c.module = b.module "
            f"AND c.lob = b.lob "
            f"AND c.at > b.at) AS merge_at,\n"
            f"  (SELECT MIN(u.ended) FROM {ctl}.v_updates u WHERE u.module = b.module AND u.pipeline LIKE '%_transform' "
            f"AND u.state = 'COMPLETED' AND u.started > b.at) AS gold_at\n"
            f"FROM {ctl}.batches b")


def checksum_sql(catalog: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    return (f"CREATE OR REPLACE VIEW {ctl}.v_checksums AS SELECT * EXCEPT (rn) FROM (SELECT *, ROW_NUMBER() OVER "
            f"(PARTITION BY module, lob, `table` ORDER BY checked_at DESC) AS rn FROM {ctl}.checksums) WHERE rn = 1")


def ensure_tables(spark, plan: dict, catalog: str, control: str) -> None:
    """The control tables the views read, empty until their writers run (a merge, a batch, a checksum)."""
    for sql in table_sql(plan, catalog, control):
        spark.sql(sql)


def table_sql(plan: dict, catalog: str, control: str) -> list[str]:
    ctl = f"`{catalog}`.`{control}`"
    out = [f"CREATE TABLE IF NOT EXISTS {ctl}.`merge_log_{m['module']}` (run_id STRING, target_table STRING, source STRING, "
              "done STRING, version_from BIGINT, version_to BIGINT, inserted BIGINT, updated BIGINT, deleted BIGINT, "
              "seconds DOUBLE, at TIMESTAMP)" for m in plan["modules"]]
    out.append(f"CREATE TABLE IF NOT EXISTS {ctl}.batches (module STRING, lob STRING, rows BIGINT, at TIMESTAMP)")
    out.append(f"CREATE TABLE IF NOT EXISTS {ctl}.checksums (checked_at TIMESTAMP, module STRING, lob STRING, "
               "`table` STRING, source_rows BIGINT, bronze_rows BIGINT, n1_rows BIGINT, source_digest BIGINT, "
               "bronze_digest BIGINT, n1_digest BIGINT, status STRING)")
    return out


def view_sql(plan: dict, catalog: str, prefix: str, control: str) -> list[str]:
    return [change_sql(plan, catalog, prefix, control), update_sql(plan, catalog, control),
            latency_sql(catalog, control), checksum_sql(catalog, control)]


def create_views(spark, plan: dict, catalog: str, prefix: str, control: str) -> list[str]:
    ensure_tables(spark, plan, catalog, control)
    out = []
    for name, sql in (("v_changes", change_sql(plan, catalog, prefix, control)),
                      ("v_updates", update_sql(plan, catalog, control)),
                      ("v_latency", latency_sql(catalog, control)), ("v_checksums", checksum_sql(catalog, control))):
        spark.sql(sql)
        out.append(name)
    return out
