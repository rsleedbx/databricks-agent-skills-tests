"""fanin: a fan-in ingestion created the way an integrated CDC pipeline is (name, catalog, schema, an
ingestion_definition with a connector_type and objects), with these connector types:

  fanin_dbz_zbus   Debezium Server through Zerobus, deployed by you: change_log names the append-only log it writes,
                   snapshot_volume the Volume with the snapshot as of T, _sources.yaml and _schema/ (what
                   zerobus-debezium-test writes there), source_type the database. create() makes a view cdc_log over
                   your log in catalog.schema and the silver pipeline <name>-silver (zerobus-debezium-test's
                   silver-from-schema.py, fetched from code_archive); land() runs one silver update
  fanin_crdb_cdf   CockroachDB changefeed files (format='json') in a Volume: changefeed_path holds a folder a table
                   (changefeed_folder, default the table's name); table objects with primary_keys, in key order;
                   land() copies the new files into <catalog>.<schema>.cdc_log (lfc-change-source land.changefeed)

catalog and schema hold this ingestion's own tables: the change log (or the view over yours), its state `_fanin`,
and its silver. Each call is safe to repeat. Other connector types can be added to CONNECTORS: create(w, spark, p,
d) fills p["parts"], land(w, spark, p) returns what it landed.

    from lfc_fanin import fanin
    p = fanin.create(w, spark, name="orders-fanin", catalog="main", schema="orders_fanin",
                     ingestion_definition={"connector_type": "fanin_dbz_zbus", "source_type": "SQLSERVER",
                                           "change_log": "main.dbz.cdc_log", "snapshot_volume": "/Volumes/main/dbz/snapshot"})
    fanin.land(w, spark, p)      # each run: silver catches up from the log
"""
from __future__ import annotations

import json
import time

# silver-from-schema.py's settings for each source_type, as zerobus-debezium-test sets them: cdc.seq, the event field
# that orders a source's changes (ts_ns, the default, for CockroachDB and YugabyteDB); MySQL: binary values as hex
# text, and ON DELETE CASCADE / SET NULL applied by silver (InnoDB's own aren't in the binlog through 9.5);
# YugabyteDB: its log comes through Kafka Connect (the Kafka endpoint's layout)
SOURCE_CONF = {"SQLSERVER": {"cdc.seq": "change_lsn"}, "POSTGRESQL": {"cdc.seq": "lsn"},
               "MYSQL": {"cdc.seq": "binlog", "cdc.binary": "hex", "cdc.fk_actions": "apply"},
               "ORACLE": {"cdc.seq": "scn"}, "DB2": {"cdc.seq": "db2_lsn"}, "INFORMIX": {"cdc.seq": "ifx_lsn"},
               "COCKROACHDB": {}, "YUGABYTEDB": {"cdc.bronze": "kafka"}}
SILVER_FILES = ["skills/zerobus-debezium-test/scripts/" + f for f in
                ("silver-from-schema.py", "canonical_sql.py", "fanout.py", "merge-silver.py")]
MEDALLION = "skills/lfc-medallion/scripts/lfc_medallion/"
CHANGE_SOURCE = "skills/lfc-change-source/scripts/lfc_change_source/"   # Debezium's encodings, which canonical_sql imports
ARCHIVE = "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip"


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq(catalog: str, schema: str, table: str) -> str:
    return f"{_q(catalog)}.{_q(schema)}.{_q(table)}"


def quoted(name: str) -> str:
    """catalog.schema.table quoted part by part (a name already in backticks is kept)."""
    return name if "`" in name else ".".join(_q(x) for x in name.split("."))


def ensure_state(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq(catalog, schema, '_fanin')} (name STRING, connector_type STRING, "
              "spec STRING, parts STRING, updated_at TIMESTAMP) COMMENT 'lfc_fanin: each ingestion and what it created'")


def load(spark, catalog: str, schema: str, name: str) -> dict:
    """The parts an earlier run of this fan-in recorded ({} the first time)."""
    rows = spark.sql(f"SELECT parts FROM {fq(catalog, schema, '_fanin')} WHERE name = :n", args={"n": name}).collect()
    return json.loads(rows[0]["parts"]) if rows and rows[0]["parts"] else {}


def save(spark, p: dict) -> None:
    spark.sql(f"MERGE INTO {fq(p['catalog'], p['schema'], '_fanin')} s USING (SELECT :n AS name, :c AS ct, :s AS spec, "
              ":p AS parts) n ON s.name = n.name WHEN MATCHED THEN UPDATE SET s.connector_type = n.ct, s.spec = n.spec, "
              "s.parts = n.parts, s.updated_at = current_timestamp() WHEN NOT MATCHED THEN INSERT (name, connector_type, "
              "spec, parts, updated_at) VALUES (n.name, n.ct, n.spec, n.parts, current_timestamp())",
              args={"n": p["name"], "c": p["connector_type"], "s": json.dumps(p["ingestion_definition"]),
                    "p": json.dumps(p["parts"])})


def not_yet(e: Exception) -> bool:
    """A Volume folder not written yet (not a missing table, schema or pipeline)."""
    t = str(e)
    return "PATH_NOT_FOUND" in t or (type(e).__name__ in ("NotFound", "ResourceDoesNotExist") and "/Volumes/" in t)


def silver_code(w, folder: str, archive: str) -> None:
    """The silver pipeline's files from the tests repo archive into a workspace folder: silver-from-schema.py and its
    helpers, and the lfc_medallion and lfc_change_source packages they import."""
    import io
    import urllib.request
    import zipfile

    from databricks.sdk.service.workspace import ImportFormat
    with urllib.request.urlopen(archive, timeout=120) as r:   # the tests repo archive (https)
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    top = z.namelist()[0].split("/")[0]
    w.workspace.mkdirs(folder)
    for f in SILVER_FILES:
        w.workspace.upload(f"{folder}/{f.rsplit('/', 1)[1]}", io.BytesIO(z.read(f"{top}/{f}")),
                           format=ImportFormat.RAW, overwrite=True)
    for pkg, prefix in (("lfc_medallion", MEDALLION), ("lfc_change_source", CHANGE_SOURCE)):
        w.workspace.mkdirs(f"{folder}/{pkg}")
        for n in z.namelist():
            if n.startswith(f"{top}/{prefix}") and n.endswith(".py") and n.count("/") == f"{top}/{prefix}".count("/"):
                w.workspace.upload(f"{folder}/{pkg}/{n.rsplit('/', 1)[1]}", io.BytesIO(z.read(n)),
                                   format=ImportFormat.RAW, overwrite=True)


def silver_pipeline(w, name: str, catalog: str, schema: str, log: str, volume: str, conf: dict, archive: str) -> str:
    """The silver pipeline <name>-silver over a change log in its schema and the snapshot Volume (created, or updated
    when it exists): its id."""
    folder = f"/Users/{w.current_user.me().user_name}/.lfc_fanin/{name}"
    silver_code(w, folder, archive)
    spec = {"name": f"{name}-silver", "catalog": catalog, "schema": schema, "serverless": True, "continuous": False,
            "channel": "CURRENT",
            "libraries": [{"file": {"path": f"{folder}/silver-from-schema.py"}}],
            "configuration": {"cdc.volume": volume, "cdc.bronze": "zerobus", "cdc.bronze_table": log,
                              "cdc.code": f"/Workspace{folder}", "cdc.bronze_shards": "1", "cdc.nodedup_table": "",
                              "cdc.history_suffix": "_history", "cdc.on_drift": "record", **conf}}
    flt = "name LIKE '" + spec["name"].replace("'", "''") + "'"
    have = [x.pipeline_id for x in w.pipelines.list_pipelines(filter=flt) if x.name == spec["name"]]
    if have:
        w.api_client.do("PUT", f"/api/2.0/pipelines/{have[0]}", body=spec)
        return have[0]
    return w.api_client.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]


def run_pipeline(w, pid: str, timeout: int = 3600) -> str:
    """One update of a triggered pipeline, waited for: its final state (an update still running from before, e.g.
    on an older spec, is stopped first)."""
    if w.pipelines.get(pid).state.value == "RUNNING":
        w.pipelines.stop_and_wait(pid)
    u = w.pipelines.start_update(pid).update_id
    t0 = time.time()
    while True:
        st = w.pipelines.get_update(pid, u).update.state.value
        if st in ("COMPLETED", "FAILED", "CANCELED") or time.time() - t0 > timeout:
            return st
        time.sleep(15)


def _dbz_create(w, spark, p: dict, d: dict) -> None:
    log, vol = d.get("change_log") or "", d.get("snapshot_volume") or ""
    if not log or not vol.startswith("/Volumes/"):
        raise ValueError("fanin_dbz_zbus: change_log (the append-only log Debezium writes, catalog.schema.table) and "
                         "snapshot_volume (/Volumes/..., its snapshot, _sources.yaml and _schema/)")
    conf = SOURCE_CONF.get((d.get("source_type") or "").upper())
    if conf is None:
        raise ValueError(f"fanin_dbz_zbus: source_type, one of {', '.join(SOURCE_CONF)}")
    log = quoted(log)
    rows = spark.sql(f"SELECT destination, count(*) AS n FROM {log} GROUP BY 1 ORDER BY 1").collect()
    p["log"] = log
    # silver-from-schema.py names its views after the bronze table: give it a plain name in the fan-in's schema
    spark.sql(f"CREATE OR REPLACE VIEW {fq(p['catalog'], p['schema'], 'cdc_log')} COMMENT 'the change log Debezium "
              f"writes (fanin_dbz_zbus)' AS SELECT * FROM {log}")
    pid = silver_pipeline(w, p["name"], p["catalog"], p["schema"], "cdc_log", vol, conf, d.get("code_archive") or ARCHIVE)
    p["parts"] = {"destinations": {r["destination"]: int(r["n"]) for r in rows}, "silver_pipeline": pid}


def _dbz_land(w, spark, p: dict) -> dict:   # Zerobus writes the log; silver catches up from it
    return {"silver": run_pipeline(w, p["parts"]["silver_pipeline"])}


def _crdb_create(w, spark, p: dict, d: dict) -> None:
    from lfc_change_source import land
    path = d.get("changefeed_path") or ""
    if not path.startswith("/Volumes/"):
        raise ValueError("fanin_crdb_cdf: changefeed_path is the Volume folder holding a folder a table")
    objs = d.get("objects", [])
    if any("table" not in o for o in objs):
        raise ValueError("fanin_crdb_cdf: list tables (a changefeed writes a folder a table); no schema objects")
    tables = [o["table"] for o in objs]
    bad = [t["source_table"] for t in tables if not t.get("primary_keys")]
    if bad:
        raise ValueError(f"fanin_crdb_cdf: primary_keys (in key order) for {bad[:5]}: the key is read from the "
                         "event's key array")
    folders = [t.get("changefeed_folder") or t["source_table"] for t in tables]
    if len(set(folders)) != len(folders):
        raise ValueError("fanin_crdb_cdf: two tables share a changefeed folder: set changefeed_folder")
    land.ensure_log(spark, p["log"])
    p["parts"] = {"changefeed_path": path, "tables": tables}


def _crdb_land(w, spark, p: dict) -> dict:
    from lfc_change_source import land
    out = {}
    for t in p["parts"]["tables"]:
        dest = f"{t['source_catalog']}.{t['source_schema']}.{t['source_table']}"
        folder = t.get("changefeed_folder") or t["source_table"]
        try:
            out[dest] = land.changefeed(spark, f"{p['parts']['changefeed_path']}/{folder}", p["log"], dest,
                                        t["primary_keys"], p["name"])
        except Exception as e:  # noqa: BLE001  no files for this table yet
            if not not_yet(e):
                raise
            out[dest] = 0
    return out


# connector type -> (create(w, spark, p, d): fills p["parts"], land(w, spark, p): what it landed)
CONNECTORS = {"fanin_dbz_zbus": (_dbz_create, _dbz_land), "fanin_crdb_cdf": (_crdb_create, _crdb_land)}


def create(w, spark, name: str, catalog: str, schema: str, ingestion_definition: dict) -> dict:
    """A fan-in ingestion (module doc): what it created, also kept in <catalog>.<schema>._fanin."""
    d = dict(ingestion_definition)
    ct = d.get("connector_type")
    if ct not in CONNECTORS:
        raise ValueError(f"connector_type {ct!r}: one of {', '.join(CONNECTORS)}")
    ensure_state(spark, catalog, schema)
    p = {"name": name, "catalog": catalog, "schema": schema, "connector_type": ct, "ingestion_definition": d,
         "log": fq(catalog, schema, d.get("change_log_table", "cdc_log")), "parts": {}}
    CONNECTORS[ct][0](w, spark, p, d)
    save(spark, p)
    return p


def land(w, spark, p: dict) -> dict:
    """One run: what this connector type lands (new changefeed files), or one silver update (fanin_dbz_zbus)."""
    return CONNECTORS[p["connector_type"]][1](w, spark, p)
