"""contract: what every change source gives a consumer (the fan-in planner, an applier, a check), per source and table,
whatever carries the changes: a Lakeflow Connect bronze table, an append-only change log (Debezium through Zerobus),
the gateway's staging volume, or files in object storage.

  snapshot_sql(at)        the table's full rows as of a position (SQL)
  changes_sql(after, to)  its changes after a position, up to another (SQL): the table's columns, then
                          _op   'upsert' or 'delete'   (AUTO CDC's apply_as_deletes)
                          _seq  sortable within the table: a later change has a larger _seq (AUTO CDC's sequence_by);
                                a delete and an insert of one key at one position sort delete first
  watermark(session, kind)
                          the position up to which every change has arrived (CockroachDB's RESOLVED, the gateway's
                          extraction watermark, a bronze table's latest version); kind: one of the adapter's own,
                          where it has more than one
  stats(session, after)   change rows, bytes and files since a position, from metadata only (no data read): the
                          planner's inputs, as the materialized-view planner's source_table_information
  shared                  whether one physical object carries many tables' or sources' changes (one scan can serve
                          them all), or one table's

A position is a kind and a value; positions of one kind compare, and the consumer keeps them as opaque text. A range's
two positions are one kind; to=None is an open end, the latest change (a time range can't end after the last one).
"""
from __future__ import annotations

from typing import NamedTuple, Protocol

OP, SEQ = "_op", "_seq"     # the standard change columns
UPSERT, DELETE = "upsert", "delete"


class Position(NamedTuple):
    kind: str     # delta_version | lsn | hlc | cdc_seq | ...
    value: str

    @classmethod
    def parse(cls, text: str) -> "Position":
        kind, _, value = text.partition(":")
        return cls(kind, value)

    def __str__(self) -> str:
        return f"{self.kind}:{self.value}"


class Stats(NamedTuple):
    change_rows: int | None = None
    change_bytes: int | None = None
    files: int | None = None


class Source(Protocol):
    name: str          # the source (a LOB, a database): the n:1 table's source column
    table: str
    keys: list[str]
    columns: list[str]
    shared: bool

    def snapshot_sql(self, at: Position | None = None) -> str: ...

    def changes_sql(self, after: Position, to: Position | None) -> str: ...   # None: to the latest

    def watermark(self, session, kind: str | None = None) -> Position: ...   # kind: one of its own

    def stats(self, session, after: Position | None) -> Stats: ...


def quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def last_change_sql(changes: str, keys: list[str]) -> str:
    """Each key's last change from a changes_sql: the consumer's reduction, the same for every source."""
    part = ", ".join(quote(k) for k in keys)
    return (f"SELECT * EXCEPT (`__lfc_n`) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {SEQ} DESC) "
            f"AS `__lfc_n` FROM ({changes})) WHERE `__lfc_n` = 1")
