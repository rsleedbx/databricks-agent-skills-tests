"""lfc_source_contract.cdc: a PostgreSQL source's change capture for Lakeflow Connect, on any DB-API cursor in autocommit
(psycopg on the laptop with autocommit=True, lfc_source_contract.jdbc.dbapi in a notebook or job), each step safe to
run again:

    from lfc_source_contract import cdc
    cdc.publication(cur, "orders_pub", ["express", "grocery"])   # every table in those schemas
    cdc.slot(cur, "orders_slot")                                  # after the gateway and ingestion pipeline exist
    cdc.slot(cur, "orders_slot", drop=True)                       # when the gateway is stopped for good
    slot_name, pub_name = cdc.names()                             # lfc_<uuid>, lfc_pub_<uuid>: a new pair

publication(): FOR TABLE the schemas' tables, as their owner can (FOR TABLES IN SCHEMA needs a superuser on
PostgreSQL 15 and later, Lakebase included): created when missing, the missing tables added to one that exists, and
one that already has them all, by schema or by table, kept (changing another login's publication fails: the error
says what to do). slot(): the pgoutput slot in a statement of its own (a slot can't be made in a transaction that has
written), created when missing; drop=True drops it when not active. A slot holds WAL until a gateway reads it.
names(): a slot and publication pair, lfc_<uuid> and lfc_pub_<uuid>, for a pipeline's slot_config. The gateway's
own names are dbx_<database OID>_<pipeline ID> and dbx_pub_<...>, and a custom name in that form is refused.
"""
from __future__ import annotations

import re
import uuid

from .jdbc import guarded, ident, literal


def _q(table: str) -> str:
    return ".".join(ident(x) for x in table.split(".", 1))


def _autocommit(cur) -> None:
    """Each step commits by itself (and a slot can't be made in a transaction that has written): a cursor of a
    connection that isn't in autocommit would leave writes uncommitted."""
    if getattr(getattr(cur, "connection", None), "autocommit", True) is False:
        raise ValueError("cdc: the cursor's connection must be in autocommit (psycopg: connect(..., autocommit=True))")


def _rows(cur, sql: str, params=None) -> list[tuple]:
    cur.execute(sql, params)
    return [tuple(r) for r in cur.fetchall()]


def _members(cur, name: str) -> set[str]:
    """schema.table for every table publication `name` covers: named in it (pg_publication_rel, where a partitioned
    table is its root; pg_publication_tables lists the root's leaves instead), listed by pg_publication_tables, or in
    one of its schemas (PostgreSQL 15 and later)."""
    q = ("SELECT n.nspname, c.relname FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid "
         "JOIN pg_class c ON c.oid = pr.prrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE p.pubname = %s "
         "UNION SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = %s")
    args = [name, name]
    if int(_rows(cur, "SELECT current_setting('server_version_num')")[0][0]) >= 150000:
        q += (" UNION SELECT n.nspname, c.relname FROM pg_publication_namespace pn JOIN pg_publication p ON p.oid = "
              "pn.pnpubid JOIN pg_namespace n ON n.oid = pn.pnnspid JOIN pg_class c ON c.relnamespace = n.oid "
              "WHERE p.pubname = %s AND c.relkind IN ('r', 'p')")
        args.append(name)
    return {f"{a}.{b}" for a, b in _rows(cur, q, tuple(args))}


def publication(cur, name: str, schemas: list[str], log=print) -> str:
    """Every table in the schemas in publication `name`; returns what it did: created, added N, or kept."""
    if not schemas:
        raise ValueError("publication: no schemas")
    _autocommit(cur)
    # pg_class, not information_schema.tables: that shows only the tables the reading login may use, and in a notebook
    # the reads may go through another login (dbapi's read_connection)
    tables = [f"{a}.{b}" for a, b in _rows(cur, "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON "
                                               "n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition "
                                               "AND n.nspname = ANY(%s::text[]) ORDER BY 1, 2", ("{" + ",".join(schemas) + "}",))]
    if not tables:
        raise ValueError(f"publication: no tables in {', '.join(schemas)}")
    have = _members(cur, name)
    if not _rows(cur, "SELECT 1 FROM pg_publication WHERE pubname = %s", (name,)):
        cur.execute(guarded(f"SELECT 1 FROM pg_publication WHERE pubname = {literal(name)}",
                            f"CREATE PUBLICATION {ident(name)} FOR TABLE {', '.join(_q(t) for t in tables)}"))
        log(f"cdc: publication {name} created ({len(tables)} tables)")
        return "created"
    missing = sorted(set(tables) - have)
    if not missing:
        log(f"cdc: publication {name} already has these tables: kept")
        return "kept"
    for t in missing:
        try:
            cur.execute(guarded(f"SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid WHERE "
                                f"p.pubname = {literal(name)} AND pr.prrelid = {literal(_q(t))}::regclass",
                                f"ALTER PUBLICATION {ident(name)} ADD TABLE {_q(t)}"))
        except Exception as e:  # noqa: BLE001 (any driver error here: say what to do)
            raise RuntimeError(f"publication {name} lacks {t}, and adding it failed ({e.__class__.__name__}: "
                               f"{str(e).strip()[:300]}): it belongs to another login (by schema, it needs a "
                               "superuser); have its owner add the tables, or name another publication") from None
    log(f"cdc: publication {name}: {len(missing)} tables added")
    return f"added {len(missing)}"


def slot(cur, name: str, drop: bool = False) -> dict | None:
    """Create the pgoutput replication slot `name` (kept when it exists), or with drop=True drop it when not active.
    Returns {slot, active, held} (the WAL it holds), or None when there is none."""
    _autocommit(cur)
    if drop:
        cur.execute("DO $s$ BEGIN PERFORM pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = "
                    f"{literal(name)} AND NOT active; END $s$")
    else:
        cur.execute(f"DO $s$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = {literal(name)}) "
                    f"THEN PERFORM pg_create_logical_replication_slot({literal(name)}, 'pgoutput'); END IF; END $s$")
    r = _rows(cur, "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
                   "FROM pg_replication_slots WHERE slot_name = %s", (name,))
    return {"slot": r[0][0], "active": r[0][1], "held": r[0][2]} if r else None


def names(id: str | None = None) -> tuple[str, str]:
    """(slot, publication) for one pipeline: lfc_<id> and lfc_pub_<id>, id a new UUID (its - as _) when not given."""
    id = (id or str(uuid.uuid4())).lower().replace("-", "_")
    if not re.fullmatch(r"[a-z0-9_]{1,50}", id):
        raise ValueError(f"names: {id!r}: letters, digits and _ only, at most 50")
    return f"lfc_{id}", f"lfc_pub_{id}"
