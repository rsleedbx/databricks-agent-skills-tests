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
  v_refresh     each view of each transform update: its layer, the refresh technique chosen (row-based, no-op,
                append-only, an aggregate, complete recompute) and why the cheaper ones weren't
  v_quality     each expectation of each view of each transform update: rows passed and failed
  v_cdc         the latest CDC position per gateway (its slot) and per table: source, target and delta (cdc.py)
  v_latency_table  each batch, and when its changes first reached each table or view of each stage after it: bronze
                and the n:1 merge for the batch's LOB, the n:1 pipeline, silver and gold views (every LOB's): seconds
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
            flow = "element_at(split(origin.flow_name, '\\\\.'), -1)"   # the flow's own name, not its schema
            moved = ("(" + " OR ".join(f"COALESCE({_metric(k)}, 0) > 0" for k in
                                       ("num_upserted_rows", "num_deleted_rows", "num_output_rows")) + ")")
            parts.append(
                f"SELECT '1 source capture' AS stage, {_s(n)} AS module, {_s(gw)} AS pipeline, origin.update_id, "
                f"timestamp AS at, regexp_extract(origin.flow_name, '\\\\.{n}_(.+)\\\\.({tables})_(cdc|snapshot)_flow$', 1) "
                f"AS lob, "
                f"regexp_extract({flow}, '^({tables})_(cdc|snapshot)_flow$', 1) AS table_name, "
                f"regexp_extract({flow}, '_(cdc|snapshot)_flow$', 1) AS kind, CAST(NULL AS BIGINT) AS inserted, "
                f"CAST(NULL AS BIGINT) AS updated, {_metric('num_upserted_rows')} AS upserted, "
                f"{_metric('num_deleted_rows')} AS deleted, {_metric('num_output_rows')} AS output_rows "
                f"FROM {ctl}.`events_{gw}` WHERE event_type = 'flow_progress' AND {moved}")
            parts.append(
                f"SELECT '2 bronze' AS stage, {_s(n)}, {_s(ing)}, origin.update_id, timestamp, "
                f"regexp_extract({flow}, '_{n}_bronze_(.+)_({tables})_(cdc|snapshot)_flow$', 1), "
                f"regexp_extract({flow}, '_({tables})_(cdc|snapshot)_flow$', 1), "
                f"regexp_extract({flow}, '_(cdc|snapshot)_flow$', 1), NULL, NULL, {_metric('num_upserted_rows')}, "
                f"{_metric('num_deleted_rows')}, NULL "
                f"FROM {ctl}.`events_{ing}` WHERE event_type = 'flow_progress' AND {moved} "
                f"AND {flow} RLIKE '_({tables})_(cdc|snapshot)_flow$'")   # not the pipeline's own staging flow
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
    out.append(f"CREATE TABLE IF NOT EXISTS {ctl}.cdc_positions (checked_at TIMESTAMP, module STRING, "
               "gateway STRING, scope STRING, lob STRING, table_name STRING, source_lsn STRING, target_lsn STRING, "
               "slot_confirmed_lsn STRING, delta_bytes BIGINT, state STRING, last_change_time TIMESTAMP)")
    out.append(f"CREATE TABLE IF NOT EXISTS {ctl}.checksums (checked_at TIMESTAMP, module STRING, lob STRING, "
               "`table` STRING, route STRING, source_rows BIGINT, bronze_rows BIGINT, n1_rows BIGINT, source_digest BIGINT, "
               "bronze_digest BIGINT, n1_digest BIGINT, status STRING)")
    return out


def _layer(module: str, prefix: str) -> str:
    gold = f"{prefix}_{module}_gold"
    ds = "element_at(split(origin.flow_name, '\\\\.'), -1)"
    return (f"CASE WHEN {ds} LIKE 'all\\\\_%' THEN '3 n1 pipeline' WHEN origin.flow_name LIKE {_s(f'%.{gold}.%')} "
            f"THEN '5 gold' ELSE '4 silver' END")


def refresh_sql(plan: dict, catalog: str, prefix: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    one = []
    for m in plan["modules"]:
        n = m["module"]
        one.append(
            f"SELECT {_s(n)} AS module, origin.update_id, timestamp AS at, {_layer(n, prefix)} AS layer, "
            f"element_at(split(origin.flow_name, '\\\\.'), -1) AS view_name, "
            f"from_json(details:planning_information.technique_information, "
            f"'array<struct<maintenance_type:string,is_chosen:boolean,"
            f"incrementalization_issues:array<struct<issue_type:string>>>>') AS t "
            f"FROM {ctl}.`events_{n}_transform` WHERE event_type = 'planning_information'")
    return (f"CREATE OR REPLACE VIEW {ctl}.v_refresh AS\nWITH p AS ({' UNION ALL '.join(one)})\n"
            "SELECT module, update_id, at, layer, view_name, "
            "regexp_replace(element_at(filter(t, x -> x.is_chosen), 1).maintenance_type, '^MAINTENANCE_TYPE_', '') "
            "AS technique, array_join(array_distinct(flatten(transform(filter(t, x -> NOT coalesce(x.is_chosen, "
            "false)), x -> transform(coalesce(x.incrementalization_issues, array()), i -> i.issue_type)))), ', ') "
            "AS not_chosen_because FROM p")


def quality_sql(plan: dict, catalog: str, prefix: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    one = []
    for m in plan["modules"]:
        n = m["module"]
        one.append(
            f"SELECT {_s(n)} AS module, origin.update_id, timestamp AS at, {_layer(n, prefix)} AS layer, "
            f"element_at(split(origin.flow_name, '\\\\.'), -1) AS view_name, explode(from_json("
            f"details:flow_progress.data_quality:expectations, "
            f"'array<struct<name:string,passed_records:bigint,failed_records:bigint>>')) AS e "
            f"FROM {ctl}.`events_{n}_transform` WHERE event_type = 'flow_progress' "
            f"AND details:flow_progress.data_quality:expectations IS NOT NULL")
    return (f"CREATE OR REPLACE VIEW {ctl}.v_quality AS SELECT module, update_id, at, layer, view_name, "
            f"e.name AS expectation, e.passed_records AS passed, e.failed_records AS failed FROM "
            f"({' UNION ALL '.join(one)})")


def latency_table_sql(catalog: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    return (f"CREATE OR REPLACE VIEW {ctl}.v_latency_table AS\n"
            f"SELECT b.module, b.lob, b.at AS batch_at, c.stage, c.table_name, MIN(c.at) AS reached_at, "
            f"ROUND((unix_millis(MIN(c.at)) - unix_millis(b.at)) / 1000.0, 1) AS seconds\n"
            f"FROM {ctl}.batches b JOIN {ctl}.v_changes c ON c.module = b.module AND c.at > b.at "
            f"AND (c.lob = b.lob OR c.lob IS NULL) AND c.stage <> '1 source capture' "
            f"AND (c.stage NOT IN ('2 bronze') OR c.kind = 'cdc')\n"
            f"GROUP BY b.module, b.lob, b.at, c.stage, c.table_name")


def cdc_sql(catalog: str, control: str) -> str:
    ctl = f"`{catalog}`.`{control}`"
    return (f"CREATE OR REPLACE VIEW {ctl}.v_cdc AS SELECT * EXCEPT (rn) FROM (SELECT *, ROW_NUMBER() OVER "
            f"(PARTITION BY module, gateway, scope, lob, table_name ORDER BY checked_at DESC) AS rn "
            f"FROM {ctl}.cdc_positions) WHERE rn = 1")


def view_sql(plan: dict, catalog: str, prefix: str, control: str) -> list[str]:
    return [change_sql(plan, catalog, prefix, control), update_sql(plan, catalog, control),
            latency_sql(catalog, control), checksum_sql(catalog, control), refresh_sql(plan, catalog, prefix, control),
            quality_sql(plan, catalog, prefix, control), latency_table_sql(catalog, control), cdc_sql(catalog, control)]


def create_views(spark, plan: dict, catalog: str, prefix: str, control: str) -> list[str]:
    ensure_tables(spark, plan, catalog, control)
    for sql in view_sql(plan, catalog, prefix, control):
        spark.sql(sql)
    return ["v_changes", "v_updates", "v_latency", "v_checksums", "v_refresh", "v_quality", "v_latency_table",
            "v_cdc"]
