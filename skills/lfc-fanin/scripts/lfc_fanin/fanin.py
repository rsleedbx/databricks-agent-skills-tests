"""fanin: a fan-in ingestion created the way an integrated CDC pipeline is (name, catalog, schema, an
ingestion_definition with a connector_type and objects), with these connector types:

  fanin_dbz_zbus   Debezium Server through Zerobus, deployed by you: change_log names the append-only log it writes,
                   snapshot_volume the Volume with _sources.yaml (each source's snapshot table and position) and
                   _schema/ (what zerobus-debezium-test writes there), source_type the database. create() makes a
                   view cdc_log over your log in catalog.schema; land() runs one FlowThrough round (lfc-log-silver
                   flowthrough: routing, then each table's silver), no pipeline
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

# each source_type's position in its Debezium events (lfc-change-source cdc_log SEQUENCES); MySQL: binary values as
# hex text. YugabyteDB isn't here: its log comes through Kafka Connect, another layout
DBZ_SEQ = {"SQLSERVER": "change_lsn", "POSTGRESQL": "lsn", "MYSQL": "binlog", "ORACLE": "scn", "DB2": "db2_lsn",
           "INFORMIX": "ifx_lsn", "COCKROACHDB": "ts_ns"}
DBZ_BINHEX = {"MYSQL"}
DBZ_FK_ACTIONS = {"MYSQL"}   # a log without its foreign keys' ON DELETE actions: silver derives them (lfc-log-silver fk)


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


def _read_yaml(w, path: str):
    import yaml
    return yaml.safe_load(w.files.download(path).contents.read())


def dbz_schema(w, volume: str) -> dict:
    """{table: canonical columns}: <volume>/_schema/schema.yaml, then each _schema/tables/<table>.yaml over it (a
    table's evolved schema)."""
    out = {t["name"]: t["columns"] for t in _read_yaml(w, f"{volume}/_schema/schema.yaml")["tables"]}
    try:
        for e in w.files.list_directory_contents(f"{volume}/_schema/tables"):
            if e.name.endswith(".yaml"):
                out.update({t["name"]: t["columns"] for t in _read_yaml(w, f"{volume}/_schema/tables/{e.name}")["tables"]})
    except Exception as e:  # noqa: BLE001  no evolved tables
        if not not_yet(e):
            raise
    return out


def dbz_sources(w, p: dict) -> list:
    """Each source in _sources.yaml: {label (its destination prefix, its rows' _source), tag (a name for its schema),
    sources: {table: a change source over the customer's log}, extra: {table: its snapshot table and position},
    edges: {child: its foreign keys with an ON DELETE action}}. Columns typed by the canonical schema (lfc_medallion's
    types, Debezium's encodings), a value that doesn't fit NULL and recorded (drift), a missing one its default;
    ordered by the source type's position; foreign key actions where the log lacks them (DBZ_FK_ACTIONS)."""
    import re

    from lfc_change_source.cdc_log import CdcLog
    from lfc_change_source.contract import Position
    from lfc_change_source.debezium import TEMPORAL
    from lfc_medallion.coltypes import ctype, spark_type
    part, st = p["parts"], p["parts"]["source_type"]
    schema, seq = dbz_schema(w, part["volume"]), DBZ_SEQ[st]
    fks = _read_yaml(w, f"{part['volume']}/_schema/schema.yaml")["tables"] if st in DBZ_FK_ACTIONS else []
    out = []
    for s in _read_yaml(w, f"{part['volume']}/_sources.yaml")["sources"]:
        sources, extra = {}, {}
        for t, cols in schema.items():
            tem = {c["name"]: ctype(c) for c in cols if ctype(c) in TEMPORAL}
            sources[t] = CdcLog(s["prefix"], p["log"], t, f"{s['prefix']}.{t}",
                                [c["name"] for c in cols if c.get("primary_key")],
                                {c["name"]: spark_type(c) for c in cols}, seq=seq,
                                precision={c["name"]: c.get("precision") for c in cols if c["name"] in tem},
                                binhex=st in DBZ_BINHEX, kinds=tem, cast="TRY_CAST",
                                defaults={c["name"]: c["default"] for c in cols if c.get("default")})
            if s.get("snapshot_table"):
                extra[t] = {"snapshot_table": f"{quoted(s['snapshot_table'])}.{_q(t)}",
                            "position": Position(seq, str(s.get("t_ns", 0)))}
        edges = {t["name"]: [{"cols": f["columns"], "parent": f["parent_table"], "parent_cols": f["parent_columns"],
                              "on_delete": f["on_delete"]} for f in t.get("fk_constraints") or []
                             if str(f.get("on_delete") or "").upper().replace("_", " ") in ("CASCADE", "SET NULL")]
                 for t in fks if t["name"] in sources}
        out.append({"label": s["prefix"], "tag": re.sub(r"[^a-z0-9_]", "_", s["prefix"].lower()), "sources": sources,
                    "extra": extra, "edges": {k: v for k, v in edges.items() if v}})
    return out


def _dbz_create(w, spark, p: dict, d: dict) -> None:
    log, vol = d.get("change_log") or "", (d.get("snapshot_volume") or "").rstrip("/")
    if not log or not vol.startswith("/Volumes/"):
        raise ValueError("fanin_dbz_zbus: change_log (the append-only log Debezium writes, catalog.schema.table) and "
                         "snapshot_volume (/Volumes/..., _sources.yaml with each source's snapshot table, and _schema/)")
    st = (d.get("source_type") or "").upper()
    if st not in DBZ_SEQ:
        raise ValueError(f"fanin_dbz_zbus: source_type, one of {', '.join(DBZ_SEQ)}")
    log = quoted(log)
    rows = spark.sql(f"SELECT destination, count(*) AS n FROM {log} GROUP BY 1 ORDER BY 1").collect()
    p["log"] = log
    # the log under a plain name in the fan-in's schema: what its dashboard reads (lfc-log-silver dashboard)
    spark.sql(f"CREATE OR REPLACE VIEW {fq(p['catalog'], p['schema'], 'cdc_log')} COMMENT 'the change log Debezium "
              f"writes (fanin_dbz_zbus)' AS SELECT * FROM {log}")
    p["parts"] = {"destinations": {r["destination"]: int(r["n"]) for r in rows}, "volume": vol, "source_type": st}


def _dbz_land(w, spark, p: dict) -> dict:
    """One FlowThrough round a source (lfc-log-silver flowthrough.step): each table routed by its size and measured
    rate (lfc-routing), its silver and its history (every keyed table) built or brought up to the log's newest
    position. One source: in the fan-in's schema. Several: each in <schema>_<source>, then one n:1 view a table in the
    fan-in's schema, with _source (lfc-medallion stack_sql)."""
    from lfc_change_source.cdc_log import SEQUENCES
    from lfc_change_source.contract import Position
    from lfc_log_silver import fk, flowthrough, history
    from lfc_medallion.stack import stack_sql
    from lfc_routing import routing
    seq = DBZ_SEQ[p["parts"]["source_type"]]
    newest = spark.sql(f"SELECT max({SEQUENCES[seq]}) FROM {p['log']} WHERE operation <> 'resolved'").collect()[0][0]
    to = Position(seq, str(newest if newest is not None else 0))
    srcs = dbz_sources(w, p)
    many, out = len(srcs) > 1, {}
    for src in srcs:
        sch = f"{p['schema']}_{src['tag']}" if many else p["schema"]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_q(p['catalog'])}.{_q(sch)}")
        snaps = {t: (f"SELECT * FROM {e['snapshot_table']}", e["position"]) for t, e in src["extra"].items()}
        sources = fk.with_actions(src["sources"], src["edges"], snaps)
        policy = routing.Policy(shared_log=p["log"], history="all",
                                own_table={t: "foreign key actions" for t in src["edges"]})
        got = flowthrough.step(spark, f"{_q(p['catalog'])}.{_q(sch)}", sources, to, policy, extra=src["extra"])
        if got["auto_cdc"]:   # its flows need a continuous pipeline, which this job doesn't run yet
            raise RuntimeError(f"fanin_dbz_zbus: AUTO CDC routes need their pipeline: {sum(map(len, got['auto_cdc']))} tables")
        out[src["label"]] = {k: got[k] for k in ("routes", "moves", "held", "silver")}
    if many:   # n:1: one table a name, every source's rows with its _source
        kind = "STRING" if seq in ("gateway_cursor", "hlc") else "BIGINT"
        for t, s0 in srcs[0]["sources"].items():
            names = [t] + ([t + history.SUFFIX] if s0.keys else [])
            for n in names:
                extra_cols = [("__START_AT", kind), ("__END_AT", kind)] if n != t else []
                cols = {x["tag"]: list(x["sources"][t].columns.items()) + extra_cols for x in srcs if t in x["sources"]}
                sql = stack_sql(p["catalog"], f"{p['schema']}_", n, cols, labels={x["tag"]: x["label"] for x in srcs})
                spark.sql(f"CREATE OR REPLACE VIEW {fq(p['catalog'], p['schema'], n)} AS {sql}")
    return out


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
