"""synth: a change source's events written as its producer writes them (formats.py), for tests that inject CDC and
snapshot records at will instead of waiting for a source database to produce an edge case. The inverse of what the
adapters read: a position as each kind's fields (cdc_log SEQUENCES reads them back), a value as Debezium encodes it
(debezium.decode reads it back), a gateway event by column id. tests/test_synth.py checks it against the recorded
samples: each sample's row, written again, is the sample. Plain Python, no Spark.

    from lfc_change_source import synth
    key, value = synth.debezium("POSTGRESQL", "pg1.public.t", "create", {"id": 1, "d": date(2030, 1, 13)}, cols, 1000)
    event = synth.gateway(entry, "INSERT", {"id": 1}, 1000)
"""
from __future__ import annotations

import base64
import datetime as dt
import decimal

from . import formats

EPOCH = dt.date(1970, 1, 1)
OPS = {v: k for k, v in formats.OPS.items()}   # create -> c


def position(kind: str, n: int) -> dict:
    """The event fields a position n of a kind is read from (cdc_log SEQUENCES); "": top level, "source": the source
    block."""
    if kind == "lsn":
        return {"source": {"lsn": n}}
    if kind == "change_lsn":   # SQL Server: vlf:block:slot, hex, 8:8:4
        return {"source": {"change_lsn": f"{n >> 48:08x}:{(n >> 16) & 0xFFFFFFFF:08x}:{n & 0xFFFF:04x}",
                           "commit_lsn": f"{n >> 48:08x}:{(n >> 16) & 0xFFFFFFFF:08x}:{n & 0xFFFF:04x}"}}
    if kind == "binlog":       # MySQL: file number << 44, position << 12, row
        return {"source": {"file": f"binlog.{n >> 44:06d}", "pos": (n >> 12) & 0xFFFFFFFF, "row": n & 0xFFF}}
    if kind == "scn":
        return {"source": {"scn": str(n), "commit_scn": str(n)}}
    if kind == "ifx_lsn":
        return {"source": {"change_lsn": str(n), "commit_lsn": str(n), "begin_lsn": str(n)}}
    if kind == "db2_lsn":      # its last 15 hex digits order it
        return {"source": {"change_lsn": f"00000000:0000:{n:016x}", "commit_lsn": f"00000000:0000:{n:016x}"}}
    if kind == "ts_ns":
        return {"": {"ts_ns": n}}
    raise ValueError(f"synth: no writer for position kind {kind!r}")


def value(c: dict, v, engine: str):
    """A canonical column's value (Python: int, str, Decimal, date, datetime, time, bytes, None) as Debezium sends it."""
    if v is None:
        return None
    t = c["type"]
    p = c.get("precision")
    if t == "date" and engine != "ORACLE":
        return (v - EPOCH).days
    if t in ("date", "timestamp", "timestamptz"):   # Oracle's DATE is a timestamp(0): epoch milliseconds
        if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
            v = dt.datetime(v.year, v.month, v.day)  # noqa: DTZ001  a TIMESTAMP_NTZ value: no time zone
        us = (v.replace(tzinfo=None) - dt.datetime(1970, 1, 1)) // dt.timedelta(microseconds=1)  # noqa: DTZ001
        p = 0 if t == "date" else p
        return us // 1000 if p is not None and int(p) <= 3 else us * 1000 if p is not None and int(p) > 6 else us
    if t in ("time", "timetz"):
        us = (v.hour * 3600 + v.minute * 60 + v.second) * 1_000_000 + v.microsecond
        return us // 1000 if p is not None and int(p) <= 3 else us * 1000 if p is not None and int(p) > 6 else us
    if t == "decimal":
        return str(decimal.Decimal(v))
    if t == "binary":
        return v.hex() if engine == "MYSQL" else base64.b64encode(v).decode()
    return v


def debezium(engine: str, destination: str, operation: str, row: dict, columns: list, n: int) -> tuple:
    """(key, value) of a Debezium event through Zerobus: the key object of the key columns (Oracle: as text), the
    envelope with its after image (None for a delete) and its position n in the engine's kind."""
    spec = formats.DEBEZIUM[engine]
    keys = [c for c in columns if c.get("primary_key")]
    key = {c["name"]: (str(row[c["name"]]) if engine == "ORACLE" else value(c, row[c["name"]], engine)) for c in keys}
    after = None if operation == "delete" else {c["name"]: value(c, row.get(c["name"]), engine) for c in columns}
    pos = position(spec["position"], n)
    out = {"after": after, "op": OPS[operation], **pos.get("", {})}
    if "source" in pos:
        parts = destination.split(".")   # <server>.[<database>.]<schema>.<table> (SQL Server names its database)
        out["source"] = {"connector": engine.lower(), "name": parts[0], "schema": parts[-2], "table": parts[-1],
                         **({"db": parts[1]} if len(parts) == 4 else {}), "snapshot": "false", **pos["source"]}
    out.setdefault("ts_ns", n)
    return key, out


def gateway(entry: dict, operation: str, row: dict, n: int, table_id: dict | None = None) -> dict:
    """A gateway change file's line: table_id, cursor (a 20-digit sequence, ordered as text), operation (INSERT,
    UPDATE, DELETE, REPLACE), data by column id (gateway_staging.parse_schema's entry)."""
    ids = {c: cid for c, _, cid in entry["columns"]}
    data = {ids[c]: (str(v) if isinstance(v, (decimal.Decimal, dt.date, dt.time)) else v) for c, v in row.items()}
    return {"table_id": {**(table_id or {}), "uoid": entry["id"]},
            "cursor": {"sequence": f"{n:020d}", "formatVersion": 0}, "txnID": str(n), "operation": operation,
            "data": data}
