"""synth against the recorded samples (tests/samples): each sample's row, written again by synth at the sample's own
position, is the sample: its after image, its key, the position fields cdc_log reads. Offline.

    python3 -m unittest skills/lfc-change-source/tests/test_synth.py
"""
import datetime as dt
import decimal
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_change_source import formats, gateway_staging, synth  # noqa: E402

ENGINES = {"POSTGRESQL": "pg", "SQLSERVER": "mssql", "MYSQL": "mysql", "ORACLE": "oracle", "INFORMIX": "informix",
           "COCKROACHDB": "crdb"}


def lines(*path) -> list:
    with open(os.path.join(HERE, "samples", *path)) as f:
        return [json.loads(x) for x in f if x.strip()]


def text(*path) -> str:
    with open(os.path.join(HERE, "samples", *path)) as f:
        return f.read()


def parsed(c: dict, s):
    """A sample's expected value (the source's row, as text) as the Python value synth takes."""
    if s is None:
        return None
    t = c["type"]
    if t in ("integer", "long", "short", "byte"):
        return int(s)
    if t == "decimal":
        return decimal.Decimal(s)
    if t == "date":
        return dt.date.fromisoformat(s[:10]) if len(s) <= 10 else dt.datetime.fromisoformat(s)
    if t in ("timestamp", "timestamptz"):
        return dt.datetime.fromisoformat(s.replace(" ", "T"))
    return s


def position_of(kind: str, v: dict) -> int:
    """The position cdc_log SEQUENCES reads from an event, in Python."""
    s = v.get("source") or {}
    if kind == "lsn":
        return int(s["lsn"])
    if kind == "change_lsn":
        a, b, c = (int(x, 16) for x in s["change_lsn"].split(":"))
        return (a << 48) + (b << 16) + c
    if kind == "binlog":
        return (int(s["file"].rsplit(".", 1)[1]) << 44) + (int(s["pos"]) << 12) + int(s.get("row") or 0)
    if kind in ("scn",):
        return int(s["scn"])
    if kind == "ifx_lsn":
        return int(s["change_lsn"])
    return int(v["ts_ns"])


def num(x):
    """A JSON value for comparing: a number or numeric text as a Decimal, the rest as it is."""
    try:
        return decimal.Decimal(str(x))
    except decimal.InvalidOperation:
        return x


class AgainstSamples(unittest.TestCase):
    def test_each_debezium_sample_written_again_is_the_sample(self):
        for engine, f in ENGINES.items():
            kind = formats.DEBEZIUM[engine]["position"]
            for r in lines("debezium", f"{f}.jsonl"):
                with self.subTest(engine=engine, table=r["table"], op=r["operation"]):
                    cols = r["columns"]
                    by = {c["name"]: c for c in cols}
                    n = position_of(kind, r["value"])
                    row = {c: parsed(by[c], v) for c, v in r["expected"].items()}   # a delete's: its key
                    key, val = synth.debezium(engine, r["destination"], r["operation"], row, cols, n)
                    self.assertEqual({k: num(v) for k, v in key.items()}, {k: num(v) for k, v in r["key"].items()})
                    if r["operation"] != "delete":
                        want = r["value"]["after"]
                        self.assertEqual({k: num(v) for k, v in val["after"].items()}, {k: num(want[k]) for k in val["after"]})
                    self.assertEqual(val["op"], r["value"]["op"])
                    self.assertEqual(position_of(kind, val), n)                       # its position reads back
                    for k, v in (val.get("source") or {}).items():
                        if k in ("lsn", "change_lsn", "file", "pos", "row", "scn"):
                            self.assertEqual(str(v), str(r["value"]["source"][k]))      # in the producer's form

    def test_each_gateway_sample_written_again_is_the_sample(self):
        for sample, schema in (("sqlserver_ct_t0001.jsonl", "sqlserver_ct_t0001_schema.json"),
                               ("sqlserver_cdc_t1.jsonl", "sqlserver_cdc_t1_schema.json")):
            entries = gateway_staging.parse_schema(text("gateway", schema))
            for e in lines("gateway", sample):
                k = (e["table_id"]["catalog"], e["table_id"]["schema"], e["table_id"]["name"])
                ids = {cid: c for c, _, cid in entries[k]["columns"]}
                row = {ids[cid]: v for cid, v in e["data"].items()}
                got = synth.gateway(entries[k], e["operation"], row, 1, e["table_id"])
                self.assertEqual((got["data"], got["operation"], got["table_id"]["uoid"]),
                                 (e["data"], e["operation"], e["table_id"]["uoid"]))


if __name__ == "__main__":
    unittest.main()
