"""gateway_log: a Lakeflow Connect gateway's table in the shared change log, after land.gateway copied its change
files there: read as a Debezium change log (cdc_log), with the columns by id from the gateway's schema files and the
gateway's cursor as the position (gateway_cursor, compared as text). The same log holds Debezium's events, so every
transport is read one way.

    from lfc_change_source import gateway_staging as gs, gateway_log
    entry = gs.schemas(spark, root)[("db", "lob_001", "account")]
    src = gateway_log.from_gateway("lob_001", "`main`.`ctl`.`cdc_log`", entry, "db.lob_001.account")
"""
from __future__ import annotations

from .cdc_log import CdcLog
from .contract import Position, quote


class GatewayLog(CdcLog):
    """A CdcLog whose positions are the gateway's cursor and whose columns come from the event's data by id."""
    ROW = "value:data"

    def stored_key_values(self, col: str = "k") -> dict:
        """The key land.gateway_key_sql filled in: a JSON array of each key column's value as text, in key order."""
        return {c: f"parse_json({col})[{i}]::{self.columns[c]}" for i, c in enumerate(self.keys)}


def from_gateway(name: str, log: str, entry: dict, destination: str) -> "GatewayLog":
    """A gateway's table in a landed change log (land.gateway): its columns from the event's data by column id
    (gateway_staging.schemas()'s entry), positions the gateway's cursor."""
    cols = {c: t for c, t, _ in entry["columns"]}
    exprs = {c: f"value:data.{quote(cid)}::{t} AS {quote(c)}" for c, t, cid in entry["columns"]}
    return GatewayLog(name, log, destination.split(".")[-1], destination, list(entry["keys"]), cols,
                      seq="gateway_cursor", exprs=exprs)


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
    """The conformance scenario as a gateway's staging folder (gateway_staging.build), landed into a change log,
    read as a change log."""
    from . import gateway_staging as gs
    from . import land
    staged, positions, _ = gs.build(session, schema, snapshot, steps, columns, keys)
    cat, sch = schema.split(".", 1)
    log = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_landed')}"
    session.sql(f"DROP TABLE IF EXISTS {log}")
    land.ensure_log(session, log)
    land.gateway(session, staged.root, log, "conformance")
    src = from_gateway("conformance", log, staged.entry, "conformance.public.t")
    return src, [Position("gateway_cursor", p.value) for p in positions], staged.snapshot_sql()   # its snapshot, in staging
