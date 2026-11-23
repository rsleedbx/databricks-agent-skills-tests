"""conformance: the checks every adapter passes, on one scenario written as data. An adapter is done when this passes.

The scenario is a snapshot, then steps at increasing positions: inserts, updates, a delete, a delete and an insert of
one key at one position (a replacement), a key changed twice in one range. Each adapter's build() turns it into its
own transport (a bronze table with its change data feed, change-log rows, files) and returns the positions after the
snapshot and after each step. check() then runs the adapter's SQL on a session and compares:

  snapshot       the snapshot's rows at the first position
  last change    last_change_sql over changes_sql(first, last): each key's final state, deletes as deletes
  ranges         every (after, to) of consecutive positions, so a consumer's resumes don't lose or repeat changes
  _seq           increases for each key, with a delete before an insert at one position
  watermark      at or after the last position, never behind it
  stats          change rows since the first position, from metadata (not checked against data: only not negative)

    python -m lfc_change_source.conformance --adapter lfc_bronze --warehouse <id> --schema <catalog>.<schema>
"""
from __future__ import annotations

COLUMNS, KEYS = ["k", "v"], ["k"]
SNAPSHOT = [(1, "a"), (2, "b"), (3, "c")]
STEPS = [                                   # each step: (op, key, value) at one position
    [("insert", 4, "d"), ("update", 1, "a1")],
    [("delete", 2, None), ("update", 1, "a2")],          # key 1 changed twice across the range
    [("delete", 3, None), ("insert", 3, "c2")],          # a replacement at one position: the insert wins
    [("insert", 5, "e")],
    [("delete", 5, None)],                               # a key inserted and deleted within the range: a delete
]


def expected() -> dict:
    """{key: value or None (deleted)} after every step: the scenario's own truth."""
    rows = dict(SNAPSHOT)
    for step in STEPS:
        for op, k, v in step:
            if op == "delete":
                rows[k] = None
            else:
                rows[k] = v
    return rows


def check(session, source, positions: list) -> list[str]:
    """Failures (empty: conforms) of `source` built from the scenario, positions[0] after the snapshot."""
    from .contract import DELETE, OP, SEQ, last_change_sql
    bad = []
    snap = {r[0]: r[1] for r in session.sql(source.snapshot_sql(positions[0])).collect()}
    if snap != dict(SNAPSHOT):
        bad.append(f"snapshot: {snap} != {dict(SNAPSHOT)}")
    last = session.sql(last_change_sql(source.changes_sql(positions[0], positions[-1]), KEYS)).collect()
    got = {r.asDict()["k"]: (None if r.asDict()[OP] == DELETE else r.asDict()["v"]) for r in last}
    want = {k: v for k, v in expected().items() if k not in dict(SNAPSHOT) or v != dict(SNAPSHOT)[k]}
    if got != want:
        bad.append(f"last change: {got} != {want}")
    resumed: dict = {}
    for a, b in zip(positions, positions[1:]):   # resuming range by range ends where one range does
        for r in session.sql(last_change_sql(source.changes_sql(a, b), KEYS)).collect():
            d = r.asDict()
            resumed[d["k"]] = None if d[OP] == DELETE else d["v"]
    if resumed != want:
        bad.append(f"ranges: {resumed} != {want}")
    seqs: dict = {}
    for r in session.sql(source.changes_sql(positions[0], positions[-1])).collect():
        d = r.asDict()
        seqs.setdefault(d["k"], []).append((_order(d[SEQ]), d[OP]))
    for k, s in seqs.items():
        for (s1, o1), (s2, o2) in zip(sorted(s), sorted(s)[1:]):
            if s1 == s2:
                bad.append(f"_seq: key {k} has two changes at {s1}")
        same = [o for (p, o) in sorted(s) if p[:1] == sorted(s)[-1][0][:1]]
        if DELETE in same and same.index(DELETE) != 0:
            bad.append(f"_seq: key {k}: a delete sorts after an insert at one position")
    w = source.watermark(session)
    if w.kind != positions[-1].kind or _order(w.value) < _order(positions[-1].value):
        bad.append(f"watermark: {w} is behind {positions[-1]}")
    st = source.stats(session, positions[0])
    if st.change_rows is not None and st.change_rows < 0:
        bad.append(f"stats: {st}")
    return bad


def _order(v):
    """A position or _seq as something Python orders like the SQL does: a struct by its fields, digits as numbers."""
    if isinstance(v, dict):
        return tuple(_order(x) for x in v.values())
    if isinstance(v, (list, tuple)):
        return tuple(_order(x) for x in v)
    s = str(v)
    return (int(s),) if s.lstrip("-").isdigit() else (s,)


def main(argv=None) -> None:
    import argparse
    from . import ADAPTERS
    ap = argparse.ArgumentParser(description="Check an adapter against the change-source contract on a warehouse")
    ap.add_argument("--adapter", required=True, choices=sorted(ADAPTERS))
    ap.add_argument("--warehouse", required=True, help="a SQL warehouse id")
    ap.add_argument("--schema", required=True, help="catalog.schema for the scenario's scratch objects")
    a = ap.parse_args(argv)
    from .engine import Warehouse   # sql().collect() on a SQL warehouse
    session = Warehouse(a.warehouse)
    source, positions = ADAPTERS[a.adapter].build(session, a.schema, SNAPSHOT, STEPS, COLUMNS, KEYS)
    bad = check(session, source, positions)
    print("\n".join(bad) if bad else f"{a.adapter}: conforms ({len(STEPS)} steps, {len(positions)} positions)")
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
