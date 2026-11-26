"""planned: routing's step 8 (docs/design-intelligent.md, "The steps"): silver from the plan. Each routing_plan row of
the latest round becomes a silver.Table: its change source on the log the plan names (source_of(tbl, log): a
GatewayLog, a CdcLog), its cell as the method (silver.method_of), its snapshot table and position from snapshot_runs
(a gateway's snapshot has no position: every change applies over it). Then silver.run keeps them, one round.

A table moved to a log of its own reads that log: a gateway's landing gives the new log the table's whole history (its
filter over every change file). A Debezium split's new log starts at the split: reading the old log up to the cut and
the new one after it isn't built yet (docs/design-audit.md).

    from lfc_log_silver import planned
    tables = planned.tables(plan_rows, snaps, source_of)
    planned.run(session, "`c`.`s_silver`", tables, to)
"""
from __future__ import annotations

from . import silver


def family(cursor: str | None) -> str:
    """A gateway cursor's family: its tables' positions order against each other, not against another family's (SQL
    Server change tracking: a 20-digit version; CDC: ~v<version>-<LSN>-<n>; as text every version sorts below every
    ~v)."""
    return "cdc" if str(cursor or "").startswith("~") else "ct"


def families(session, logs: list, kind_sql: str = "value:cursor.sequence::STRING") -> dict:
    """{destination: its cursor family} from what the logs hold (a table with no change yet: none)."""
    out = {}
    for log in logs:
        for r in session.sql(f"SELECT destination, max({kind_sql}) FROM {log} GROUP BY destination").collect():
            out[r[0]] = family(r[1])
    return out


def tables(plan_rows: list, snaps: dict, source_of, kind: str = "gateway_cursor", history: bool = False,
           fams: dict | None = None) -> dict:
    """{tbl: silver.Table} for the plan's rows (dicts: tbl, cell, log); snaps: {tbl: snapshot_runs row (snapshot_table,
    position)}; source_of(tbl, log) -> a change source."""
    from lfc_change_source.contract import Position
    out = {}
    for r in plan_rows:
        src = source_of(r["tbl"], r.get("log"))
        method, only = silver.method_of(src, r.get("cell") or "view")
        s = snaps.get(r["tbl"]) or {}
        pos = s.get("position")
        out[r["tbl"]] = silver.Table(src, method, only, snapshot_table=s.get("snapshot_table"),
                                     position=Position(kind, str(pos)) if pos not in (None, "") else None,
                                     group=_group(src.fq) + "_" + (fams or {}).get(src.destination, "ct")
                                     if method == "shared" else "",
                                     history=history and bool(src.keys))
    return out


def run(session, schema: str, tables_: dict, to, first: bool | None = None) -> dict:
    """One silver round of the planned tables in `schema` (created if missing), up to position `to`; first: build
    (None: when anything this plan needs isn't there yet). With tables in several logs, `to` is the smallest of
    their newest landed positions (upto()): a table kept past what its log holds would skip the changes landed later."""
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
    if first is None:   # a build when anything this plan needs isn't there (a first run, or one that failed midway)
        have = {r[1].lower() for r in session.sql(f"SHOW TABLES IN {schema}").collect()}
        need = {silver.STATE} | {n.lower() for n in tables_} | {
            silver.SHARED.format(group=t.group or "0").lower() for t in tables_.values() if t.method == "shared"}
        first = not need <= have
    return silver.run(session, tables_, schema, to, first=first)


def _group(log: str) -> str:
    """A shared group a log: a group's members scan one log (silver.shared_changes_sql), so a table in a log of its
    own is in a group of its own; the name from the log's table name."""
    import re
    return re.sub(r"[^A-Za-z0-9_]", "_", log.strip("`").split("`.`")[-1].split(".")[-1].strip("`")) or "0"


def upto(session, logs: list, kind_sql: str = "value:cursor.sequence::STRING", destinations: list | None = None) -> str | None:
    """A round's upper position over several logs: the smallest of their complete positions, of `destinations`'
    events only when given (one cursor family); None when a log holds none of them. A log's newest position isn't
    complete: one position can hold many rows written in several change files (SQL Server change tracking: a
    transaction's version), so its other rows may land later; a round goes up to the one below it, never past a
    position whose rows are still arriving."""
    where = (f"destination IN ({', '.join(repr(d) for d in destinations)})" if destinations else "true")
    tops = [session.sql(f"SELECT max({kind_sql}) FROM {log} WHERE {where} AND {kind_sql} < "
                        f"(SELECT max({kind_sql}) FROM {log} WHERE {where})").collect()[0][0] for log in logs]
    tops = [t for t in tops if t is not None]
    return min(str(t) for t in tops) if tops else None


def run_families(session, schema: str, tables_: dict, fams: dict, kind: str = "gateway_cursor") -> dict:
    """silver rounds, one a cursor family (family()): each with its own upper position, upto() over the logs its
    tables read, so no table is kept past a position its own changes order against. {family: run's result}."""
    from lfc_change_source.contract import Position
    by: dict = {}
    for n, t in tables_.items():
        by.setdefault(fams.get(t.src.destination, "ct"), {})[n] = t
    out = {}
    for fam, ts in sorted(by.items()):
        hi = upto(session, sorted({t.src.fq for t in ts.values()}), destinations=[t.src.destination for t in ts.values()])
        if hi is not None:
            out[fam] = {"to": hi, **run(session, schema, ts, Position(kind, hi))}
    return out
