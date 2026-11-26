"""combine: silver's one rule (docs/design-no-full-refresh.md): a table's current rows are its snapshot table's rows at
the snapshot's position and each key's last change after it from the change log, deletes dropped. Every reader of a
table's rows (a view, a build, a check) calls this, so every silver cell gives the same table. The change source
(lfc-change-source) gives only its changes; the snapshot is a Delta table of its own (layer 3).

    from lfc_log_silver import combine
    combine.current_sql(src, "SELECT * FROM `main`.`snap`.`orders`", Position("lsn", "29565568"))
"""
from __future__ import annotations


def current_sql(src, snapshot_sql: str | None = None, position=None) -> str:
    """The table's current rows (its columns, _op upsert): with no snapshot, from the log alone (src.snapshot_sql, a
    log that starts with the table); else the snapshot's rows at `position` (None: before every change) and each key's
    last change after it."""
    from lfc_change_source.cdc_log import TEXT
    from lfc_change_source.contract import (
        DELETE,
        OP,
        SEQ,
        UPSERT,
        last_change_sql,
        quote,
    )
    if not snapshot_sql:
        return src.snapshot_sql()
    cols = ", ".join(quote(c) for c in src.columns)
    v = src.position_sql(position) if position is not None else ("''" if src.seq in TEXT else "-1")
    both = (f"SELECT {cols}, '{UPSERT}' AS {OP}, named_struct('v', {v}, 'o', 0) AS {SEQ} FROM ({snapshot_sql}) "
            f"UNION ALL SELECT {cols}, {OP}, {SEQ} FROM ({src.changes_sql(position, None)})")
    return f"SELECT {cols}, {OP} FROM ({last_change_sql(both, src.keys)}) WHERE {OP} <> '{DELETE}'"
