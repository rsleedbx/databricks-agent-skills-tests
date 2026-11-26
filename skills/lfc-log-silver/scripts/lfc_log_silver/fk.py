"""fk: the changes a parent's delete makes through foreign keys (ON DELETE CASCADE, SET NULL) that the change log
lacks (MySQL's binlog through 9.5, MariaDB: InnoDB applies them below it), derived so silver has them. A child's
change source is wrapped: its changes_sql also returns, for each parent delete at position s, a child row whose key
matched just before s as a delete at s (CASCADE) or as an update with the foreign key NULL at s (SET NULL). Deriving
needs the child's state at s, its snapshot and its changes, so it is silver's (combine's rule), not the change log's.

A chain (a cascade into a grandchild) follows from wrapping parents first: a wrapped parent's changes carry its
derived deletes. `depth` bounds it, which also stops a table referencing itself. A wrapped table can't be in a shared
group (a group reads the raw log with one statement for every member): routing keeps it on a table of its own.

    from lfc_log_silver import fk
    srcs = fk.with_actions(sources, edges, snapshots)   # edges: {child: [{cols, parent, parent_cols, on_delete}]}
"""
from __future__ import annotations

ACTIONS = ("CASCADE", "SET NULL")


def _q(n: str) -> str:
    return "`" + n.replace("`", "``") + "`"


class WithFkActions:
    """A change source with its foreign keys' actions: every attribute the wrapped source's, but changes_sql."""

    def __init__(self, src, edges: list, snapshot_sql: str | None = None, position=None):
        self._src, self.edges, self._snapshot_sql, self._position = src, edges, snapshot_sql, position

    def __getattr__(self, name):
        return getattr(self._src, name)

    def changes_sql(self, after, to) -> str:
        return " UNION ALL ".join([self._src.changes_sql(after, to)] + [self._derived(e, after, to) for e in self.edges])

    def _versions(self, to) -> str:
        """The child's rows as they were at each change: its snapshot's at its position, then each change."""
        from lfc_change_source.cdc_log import TEXT
        from lfc_change_source.contract import OP, SEQ, UPSERT
        cols = ", ".join(_q(c) for c in self._src.columns)
        changes = f"SELECT {cols}, {OP}, {SEQ} FROM ({self._src.changes_sql(self._position, to)})"
        if not self._snapshot_sql:
            return changes
        v = (self._src.position_sql(self._position) if self._position is not None
             else ("''" if self._src.seq in TEXT else "-1"))
        typed = ", ".join(f"CAST({_q(c)} AS {t}) AS {_q(c)}" for c, t in self._src.columns.items())
        return (f"SELECT {typed}, '{UPSERT}' AS {OP}, named_struct('v', {v}, 'o', 0) AS {SEQ} FROM "
                f"({self._snapshot_sql}) UNION ALL {changes}")

    def _derived(self, e: dict, after, to) -> str:
        """One foreign key's derived events after `after` up to `to`: the child rows matching each parent delete."""
        from lfc_change_source.contract import DELETE, OP, SEQ, UPSERT
        act = e["on_delete"].upper().replace("_", " ")
        if act not in ACTIONS:
            raise ValueError(f"fk: on_delete {e['on_delete']!r}, one of {', '.join(ACTIONS)}")
        keys = ", ".join(f"v.{_q(k)}" for k in self._src.keys)
        match = " AND ".join(f"v.{_q(c)} = d.{_q(p)}" for c, p in zip(e["cols"], e["parent_cols"]))
        pcols = ", ".join(_q(p) for p in e["parent_cols"])
        deletes = f"SELECT {pcols}, {SEQ} FROM ({e['parent'].changes_sql(after, to)}) WHERE {OP} = '{DELETE}'"
        if act == "CASCADE":
            out = ", ".join(f"v.{_q(c)}" for c in self._src.columns)
            op, o = f"'{DELETE}'", 0
        else:
            out = ", ".join(f"CAST(NULL AS {t}) AS {_q(c)}" if c in e["cols"] else f"v.{_q(c)}"
                            for c, t in self._src.columns.items())
            op, o = f"'{UPSERT}'", 1
        last = (f"SELECT v.*, d.{SEQ}.v AS _at FROM ({self._versions(to)}) v JOIN ({deletes}) d ON {match} AND "
                f"v.{SEQ} < d.{SEQ} QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys}, d.{SEQ} ORDER BY v.{SEQ} DESC) = 1")
        return (f"SELECT {out}, {op} AS {OP}, named_struct('v', v._at, 'o', {o}) AS {SEQ} FROM ({last}) v "
                f"WHERE v.{OP} <> '{DELETE}'")


def with_actions(sources: dict, edges: dict, snapshots: dict | None = None, depth: int = 4) -> dict:
    """{name: source}, each child of `edges` ({child: [{cols, parent (a name in sources), parent_cols, on_delete}]})
    wrapped with its foreign keys' actions, parents wrapped first so a cascade follows a chain, `depth` levels at most.
    snapshots: {name: (snapshot_sql, position)}, each child's state before its changes."""
    snapshots = snapshots or {}
    out = dict(sources)
    for _ in range(depth):   # each level: a child's parents as wrapped so far
        nxt = dict(sources)
        for child, es in edges.items():
            mine = [{**e, "parent": out[e["parent"]]} for e in es
                    if e["on_delete"].upper().replace("_", " ") in ACTIONS]
            if mine:
                sql, pos = snapshots.get(child, (None, None))
                nxt[child] = WithFkActions(sources[child], mine, sql, pos)
        out = nxt
    return out
