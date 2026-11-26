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
    sys.exit(1 if bad else 0)
