"""check_formats.py: every recorded Debezium sample (tests/samples/debezium, lfc_change_source.formats) decoded by
CdcLog on a warehouse and compared with the row the source had: a create's and an update's whole row, a delete's key.
The check that the definitions, the decoding and the producers' real output agree. Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> SCHEMA=<scratch schema> python3 -I check_formats.py
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-change-source", "lfc-medallion"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source import formats  # noqa: E402
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_change_source.debezium import TEMPORAL  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_medallion.coltypes import ctype, spark_type  # noqa: E402


def text(s: str) -> str:
    """A SQL string literal (Spark: a backslash escapes)."""
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def lit(o) -> str:
    return text(json.dumps(o))


def check(wh, log: str, st: str, spec: dict) -> list:
    rows = [json.loads(x) for x in open(os.path.join(HERE, "samples", spec["sample"]))]
    wh.sql(f"CREATE OR REPLACE TABLE {log} (destination STRING, operation STRING, key VARIANT, value VARIANT)").collect()
    wh.sql(f"INSERT INTO {log} VALUES " + ", ".join(
        f"({text(r['destination'])}, '{r['operation']}', parse_json({lit(r['key'])}), "
        f"parse_json({lit(r['value'])}))" for r in rows)).collect()
    bad = []
    for r in rows:
        cols = r["columns"]
        tem = {c["name"]: ctype(c) for c in cols if ctype(c) in TEMPORAL}
        src = CdcLog(st, log, r["table"], r["destination"], [c["name"] for c in cols if c.get("primary_key")],
                     {c["name"]: spark_type(c) for c in cols}, seq=spec["position"], kinds=tem,
                     precision={c["name"]: c.get("precision") for c in cols if c["name"] in tem},
                     binhex=st == "MYSQL")
        vals = src.column_values()
        want = r["expected"] or {}
        names = list(want) if r["operation"] == "delete" else list(vals)
        sel = ", ".join(f"CAST({vals[c]} AS STRING) AS `{c}`" for c in names)
        k = " AND ".join(f"key:{c}::STRING = {text(v)}" for c, v in r["key"].items())
        got = wh.sql(f"SELECT {sel} FROM {log} WHERE destination = '{r['destination']}' AND operation = "
                     f"'{r['operation']}' AND {k}").collect()
        if not got:
            bad.append(f"{st} {r['table']} {r['operation']}: not found")
            continue
        g = dict(zip(names, got[0]))
        diff = {c: (g.get(c), want.get(c)) for c in names if str(g.get(c)) != str(want.get(c))}
        if diff:
            bad.append(f"{st} {r['table']} {r['operation']}: {diff}")
    return bad


def check_gateway(wh, log: str, sample: str, schema: str) -> list:
    """A gateway sample's lines landed as land.gateway lands them (its destination, operation and key expressions),
    read by GatewayLog with the schema file's columns, each event's row (a delete's key) against the line's own values
    by column name."""
    from lfc_change_source import gateway_staging as gs
    from lfc_change_source import land
    from lfc_change_source.gateway_log import from_gateway
    entries = gs.parse_schema(open(os.path.join(HERE, "samples", schema)).read())
    lines = [json.loads(x) for x in open(os.path.join(HERE, "samples", sample)) if x.strip()]
    wh.sql(f"CREATE OR REPLACE TABLE {log} (destination STRING, operation STRING, key VARIANT, value VARIANT)").collect()
    wh.sql(f"INSERT INTO {log} SELECT {land.GATEWAY_DESTINATION}, {land.GATEWAY_OPERATION}, "
           f"{land.gateway_key_sql(land.gateway_keys(entries))}, event FROM (SELECT parse_json(x) AS event FROM VALUES "
           + ", ".join(f"({text(json.dumps(e))})" for e in lines) + " AS t(x))").collect()
    bad = []
    for e in lines:
        k = (e["table_id"]["catalog"], e["table_id"]["schema"], e["table_id"]["name"])
        src = from_gateway("gw", log, entries[k], ".".join(k))
        vals = src.column_values()
        ids = {c: cid for c, _, cid in entries[k]["columns"]}
        names = list(src.keys) if e["operation"] == "DELETE" else list(vals)
        sel = ", ".join(f"CAST({vals[c]} AS STRING) AS `{c}`" for c in names)
        got = wh.sql(f"SELECT {sel} FROM {log} WHERE value:cursor.sequence::STRING = {text(e['cursor']['sequence'])} "
                     f"AND value:operation::STRING = '{e['operation']}' AND value:txnID::STRING = {text(e['txnID'])}"
                     ).collect()
        want = {c: str(e["data"][ids[c]]) for c in names}
        g = dict(zip(names, got[0])) if got else {}
        diff = {c: (g.get(c), want[c]) for c in names if str(g.get(c)) != want[c]}
        if not got or diff:
            bad.append(f"gateway {sample} {e['operation']}: {diff or 'not found'}")
    return bad


if __name__ == "__main__":
    w = WorkspaceClient(profile=os.environ["PROFILE"])
    wh = Warehouse(os.environ["WAREHOUSE"], w)
    log = f"`{os.environ['CATALOG']}`.`{os.environ.get('SCHEMA', 'lfc_change_source_formats')}`.`samples_log`"
    wh.sql(f"CREATE SCHEMA IF NOT EXISTS {log.rsplit('.', 1)[0]}").collect()
    bad = []
    for st, spec in formats.DEBEZIUM.items():
        if spec["sample"]:
            got = check(wh, log, st, spec)
            print(st, "ok" if not got else "\n  ".join(["MISMATCH"] + got), flush=True)
            bad += got
    for sample, schema in (("gateway/sqlserver_ct_t0001.jsonl", "gateway/sqlserver_ct_t0001_schema.json"),
                           ("gateway/cdc_history_update.jsonl", "gateway/target_schema.0.json")):
        got = check_gateway(wh, log, sample, schema)
        print(sample, "ok" if not got else "\n  ".join(["MISMATCH"] + got), flush=True)
        bad += got
    sys.exit(1 if bad else 0)
