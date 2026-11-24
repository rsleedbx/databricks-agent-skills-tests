"""gateway_log: a Lakeflow Connect gateway's table in the shared change log, after land.gateway copied its change
files there: read as a Debezium change log (cdc_log), with the columns by id from the gateway's schema files and the
gateway's cursor as the position (gateway_cursor, compared as text). The same log holds Debezium's events, so every
transport is read one way.

    from lfc_change_source import gateway_staging as gs, gateway_log
    entry = gs.schemas(spark, root)[("db", "lob_001", "account")]
    src = gateway_log.from_gateway("lob_001", "`main`.`ctl`.`cdc_log`", entry, "db.lob_001.account",
                                   snapshot=gs.Staging("lob_001", root, entry).snapshot_sql())
"""
from __future__ import annotations

from .cdc_log import CdcLog
from .contract import DELETE, OP, SEQ, Position, last_change_sql, quote


class GatewayLog(CdcLog):
    """A CdcLog whose positions are the gateway's cursor and whose columns come from the event's data by id."""
def from_gateway(name: str, log: str, entry: dict, destination: str, snapshot: str | None = None) -> "GatewayLog":
    """A gateway's table in a landed change log (land.gateway): its columns from the event's data by column id
    (gateway_staging.schemas()'s entry), positions the gateway's cursor, the snapshot (gateway_staging's
    snapshot_sql) as given."""
    cols = {c: t for c, t, _ in entry["columns"]}
    exprs = {c: f"value:data.{quote(cid)}::{t} AS {quote(c)}" for c, t, cid in entry["columns"]}
    return GatewayLog(name, log, destination.split(".")[-1], destination, list(entry["keys"]), cols,
                      seq="gateway_cursor", snapshot=snapshot, exprs=exprs)


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
    """The conformance scenario as a gateway's staging folder (gateway_staging.build), landed into a change log,
    read as a change log."""
    from . import gateway_staging as gs
    from . import land
    staged, positions = gs.build(session, schema, snapshot, steps, columns, keys)
    cat, sch = schema.split(".", 1)
    log = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_landed')}"
    session.sql(f"DROP TABLE IF EXISTS {log}")
    land.ensure_log(session, log)
    land.gateway(session, staged.root, log, "conformance")
    src = from_gateway("conformance", log, staged.entry, "conformance.public.t", snapshot=staged.snapshot_sql())
    return src, [Position("gateway_cursor", p.value) for p in positions]


def current_sql(name: str, log: str, entry: dict, destination: str, root: str, path: str, position: str = "") -> str:
    """The table's current rows (its columns and _op upsert) for either snapshot path (land.snapshot_path): `log`, from
    the change log alone (its landed snapshot and changes); `direct`, its snapshot read in place where the gateway
    wrote it, then each key's last change in the log after the snapshot's position (gateway_staging's
    snapshot_position; '' when the change files start before the snapshot, SQL Server change tracking)."""
    from . import gateway_staging as gs
    src = from_gateway(name, log, entry, destination)
    if path == "log":
        return src.snapshot_sql()
    if path != "direct":
        raise ValueError(f"snapshot path {path!r}: log or direct")
    snap = gs.Staging(name, root, entry).snapshot_sql()
    cols = ", ".join(quote(c) for c in src.columns)
    lit = "'" + position.replace("'", "") + "'"
    both = (f"SELECT {cols}, {OP}, named_struct('v', {lit}, 'o', 0) AS {SEQ} FROM ({snap}) UNION ALL "
            f"SELECT {cols}, {OP}, {SEQ} FROM ({src.changes_sql(Position('gateway_cursor', position), None)})")
    return f"SELECT {cols}, {OP} FROM ({last_change_sql(both, src.keys)}) WHERE {OP} <> '{DELETE}'"
