"""moved: a table moved to a log of its own while its change capture ran (lfc_change_source.producer.split, make before
break), read as one change source: its old log up to the cut, its new log after it. The logs overlap around the cut
(never a gap) and carry the source's own positions, so the two parts join at the cut with nothing twice. Everything
else (columns, keys, positions, the destination) is the new log's source.

A gateway's split doesn't need this: landing gives the new log the table's whole history. A Debezium Server's new log
starts at the split, so silver reads the moved table through Moved.

    from lfc_change_source.moved import Moved
    src = Moved(old=CdcLog(... "`c`.`s`.cdc_log" ...), new=CdcLog(... "`c`.`s`.`cdc_log_orders`" ...),
                cut=Position("lsn", "29814640"))
"""
from __future__ import annotations

from .cdc_log import TEXT
from .contract import Position


def _le(a: Position, b: Position) -> bool:
    return str(a.value) <= str(b.value) if a.kind in TEXT else int(a.value) <= int(b.value)


class Moved:
    def __init__(self, old, new, cut: Position):
        if (old.seq, old.destination, list(old.keys)) != (new.seq, new.destination, list(new.keys)):
            raise ValueError("Moved: the old and new log's sources must be the same table (position kind, destination, keys)")
        if cut.kind != new.seq:
            raise ValueError(f"Moved: the cut is a {new.seq}, not {cut.kind}")
        self.old, self.new, self.cut = old, new, cut
        self.shareable = False   # two logs: a shared group scans one log (lfc-log-silver keeps it by MERGE instead)

    def __getattr__(self, name):   # columns, keys, seq, destination, fq, position_sql, ...: the new log's
        return getattr(self.new, name)

    def changes_sql(self, after: Position | None, to: Position | None) -> str:
        """Its changes after `after` up to `to`: from the old log those at or before the cut, from the new log those
        after it (each part only when the range reaches it)."""
        parts = []
        if after is None or not _le(self.cut, after):            # some of the range is at or before the cut
            hi = to if to is not None and _le(to, self.cut) else self.cut
            parts.append(self.old.changes_sql(after, hi))
        if to is None or not _le(to, self.cut):                  # some of it is after the cut
            lo = after if after is not None and not _le(after, self.cut) else self.cut
            parts.append(self.new.changes_sql(lo, to))
        return " UNION ALL ".join(f"SELECT * FROM ({p})" for p in parts)

    def snapshot_sql(self, at=None) -> str:
        """The table as of `at` from its two logs alone (no snapshot table): each key's last change across both parts,
        deletes dropped (CdcLog.snapshot_sql's shape)."""
        from .contract import DELETE, OP, UPSERT, last_change_sql, quote
        names = ", ".join(quote(c) for c in self.new.columns)
        return (f"SELECT {names}, '{UPSERT}' AS {OP} FROM ({last_change_sql(self.changes_sql(None, at), list(self.new.keys))}) "
                f"WHERE {OP} <> '{DELETE}'")
