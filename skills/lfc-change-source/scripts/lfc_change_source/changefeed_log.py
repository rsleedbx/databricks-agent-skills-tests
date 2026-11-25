"""changefeed_log: a CockroachDB changefeed's table in the shared change log, after land.changefeed copied its files
there: read as a Debezium change log (cdc_log). The changefeed writes JSON lines into object storage or a Volume
(format='json', updated, resolved; initial_scan for the rows that were there), one folder a table:

  <date>/<time>-<id>-<n>-<table>-<n>.ndjson   {"after": {columns} or null (a delete), "key": [key in order],
                                              "updated": "<wall ns>.<logical>"}
  <date>/<time>.RESOLVED                      {"resolved": "<wall ns>.<logical>"}: every change up to it has arrived

Positions are the HLC (`hlc`, text of fixed width, compared as text). The watermark is the newest resolved position,
not the newest event's: a changefeed's files from several nodes can arrive out of order below it. The changefeed runs with
initial_scan = 'no': the table's snapshot is a table of its own (a snapshot method: qbc, or an initial_scan = 'only'
export through intake.files), given as `snapshot`; without one, a table is its changes from the start.

    from lfc_change_source import land, changefeed_log
    land.changefeed(spark, "/Volumes/c/s/landing/orders", "`c`.`ctl`.`cdc_log`", "shop.public.orders", ["id"], "crdb1")
    src = changefeed_log.from_changefeed("crdb1", "`c`.`ctl`.`cdc_log`", "shop.public.orders", ["id"],
                                         {"id": "BIGINT", "total": "DECIMAL(12,2)", "note": "STRING"})
"""
from __future__ import annotations

import json

from .cdc_log import CdcLog
from .contract import Position, quote

KIND = "hlc"


class ChangefeedLog(CdcLog):
    """A CdcLog of a CockroachDB changefeed: HLC positions, the watermark from RESOLVED."""


def from_changefeed(name: str, log: str, destination: str, keys: list[str], columns: dict,
                    snapshot: str | None = None) -> ChangefeedLog:
    """A changefeed's table in a landed change log (land.changefeed): columns by name from the after image."""
    return ChangefeedLog(name, log, destination.split(".")[-1], destination, list(keys), columns, seq=KIND,
                         snapshot=snapshot)


def hlc(n: int, logical: int = 0) -> str:
    """An HLC of a changefeed's form: 19-digit wall nanoseconds, a point, 10-digit logical."""
    return f"{n:019d}.{logical:010d}"


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
    """The conformance scenario as a changefeed's files in a scratch volume (<schema>.lfc_change_source): the
    snapshot as a Delta table (the changefeed starts after it, initial_scan = 'no'), each step's changes at one HLC a
    step, a RESOLVED file after each, landed into a change log and read from it. Needs a session with a workspace
    client (session.w) for the files."""
    from . import land
    from .gateway_staging import _clear, _put
    cat, sch = schema.split(".", 1)
    session.sql(f"CREATE VOLUME IF NOT EXISTS {quote(cat)}.{quote(sch)}.lfc_change_source")
    root = f"/Volumes/{cat}/{sch}/lfc_change_source/changefeed"
    k, v = columns
    try:
        _clear(session.w, root)   # a rerun starts empty
    except Exception:  # noqa: BLE001  not there yet
        pass
    at = [hlc(1_000_000_000 * (n + 1)) for n in range(len(steps) + 1)]
    snap = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_changefeed_snapshot')}"
    session.sql(f"CREATE OR REPLACE TABLE {snap} ({quote(k)} INT, {quote(v)} STRING)")
    session.sql(f"INSERT INTO {snap} VALUES " + ", ".join(f"({a}, '{b}')" for a, b in snapshot))
    events = [[]] + [[(key, None if op == "delete" else {k: key, v: val}) for op, key, val in step] for step in steps]
    for n, step in enumerate(events):
        lines = [json.dumps({"after": after, "key": [key], "updated": at[n]}) for key, after in step]
        if lines:   # the snapshot's position has no events: only its RESOLVED mark
            _put(session.w, f"{root}/2026-01-01/{at[n].replace('.', '')}-conf-1-1-{n:08d}-t-1.ndjson",
                 "\n".join(lines) + "\n")
        _put(session.w, f"{root}/2026-01-01/{at[n].replace('.', '')}.RESOLVED", json.dumps({"resolved": at[n]}))
    log = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_changefeed')}"
    session.sql(f"DROP TABLE IF EXISTS {log}")
    land.ensure_log(session, log)
    land.changefeed(session, root, log, "conformance.public.t", list(keys), "conformance")
    src = from_changefeed("conformance", log, "conformance.public.t", list(keys), {k: "INT", v: "STRING"},
                          snapshot=f"SELECT * FROM {snap}")
    return src, [Position(KIND, a) for a in at]
