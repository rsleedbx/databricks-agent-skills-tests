"""routing: which change log each table's changes go to, and which path its snapshot takes, by its class:

  dimension  small and slow    snapshot landed into the log (path log), changes in the shared log
  big_slow   big, few changes  snapshot read in place (path direct), changes in the shared log
  big_fast   many changes      snapshot read in place, changes in a log of its own, sharded when one stream or reader
                               can't keep up

A busy table in the shared log makes every other table's read skip its events (clustering by destination helps only
once OPTIMIZE has rewritten the new files), so a small table's lag and cost follow the busiest table in its log. A table
gets a log of its own once its change rate passes `fast_events_per_s`; a snapshot bigger than `max_log_snapshot_bytes`
is read in place (land.snapshot_path). Both are starting points to measure against, set per deployment; any table's
class can be chosen by name.

Split by transport, not inside a log: a gateway's change files mix all its tables, so a big_fast table goes on a
gateway (or a Debezium target table) of its own, and each landing stays a copy of whole files.

    from lfc_change_source import routing
    p = routing.Policy(shared_log="`main`.`ctl`.`cdc_log`", fast_log="`main`.`ctl`.`cdc_log_{table}`")
    routes = routing.plan({"db.s.orders": (5 << 40, 250_000), "db.s.region": (4096, 0.01)}, p)
    routes["db.s.orders"].log, routes["db.s.orders"].shards   # its own log, 3 shards
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .cdc_log import SEQUENCES, TEXT
from .land import MAX_LOG_SNAPSHOT_BYTES, snapshot_path

CLASSES = ("dimension", "big_slow", "big_fast")
# Zerobus Ingest's documented default per stream (gRPC): 100,000 records a second (benchmarked with 1 KB messages)
STREAM_RECORDS_PER_S = 100_000


@dataclass(frozen=True)
class Route:
    table: str            # <catalog>.<schema>.<table>
    cls: str              # one of CLASSES
    snapshot: str         # land.snapshot_path: log or direct
    log: str              # the change log its changes go to, quoted
    shards: int = 1       # streams (and reader tasks) its changes are split into, by key hash


@dataclass
class Policy:
    shared_log: str                                   # every dimension and big_slow table's changes
    fast_log: str = "{shared}_{table}"                # a big_fast table's own log: {shared} and {table} filled in
    fast_events_per_s: float = 1_000.0                # change events a second at which a table gets its own log
    max_log_snapshot_bytes: int = MAX_LOG_SNAPSHOT_BYTES
    shard_events_per_s: float = STREAM_RECORDS_PER_S  # events a second one stream (and one reader) carries
    chosen: dict = field(default_factory=dict)        # {table: class}: chosen instead of by the numbers

    def log_name(self, table: str) -> str:
        shared = self.shared_log.rstrip("`")
        name = table.split(".")[-1]
        out = self.fast_log.format(shared=shared, table=name)
        return out + "`" if self.shared_log.endswith("`") and not out.endswith("`") else out


def classify(snapshot_bytes: int, events_per_s: float, policy: Policy, table: str = "") -> str:
    """The table's class: chosen by name, else by its change rate, then its snapshot's size."""
    if table in policy.chosen:
        if policy.chosen[table] not in CLASSES:
            raise ValueError(f"{table}: class {policy.chosen[table]!r}, one of {', '.join(CLASSES)}")
        return policy.chosen[table]
    if events_per_s >= policy.fast_events_per_s:
        return "big_fast"
    return "dimension" if snapshot_bytes <= policy.max_log_snapshot_bytes else "big_slow"


def log_for(table: str, snapshot_bytes: int, events_per_s: float, policy: Policy) -> Route:
    """Where one table goes: its class, its snapshot's path, its change log, and its shards."""
    cls = classify(snapshot_bytes, events_per_s, policy, table)
    if cls == "big_fast":
        shards = max(1, math.ceil(events_per_s / policy.shard_events_per_s))
        return Route(table, cls, "direct", policy.log_name(table), shards)
    snap = "log" if cls == "dimension" else "direct"
    return Route(table, cls, snapshot_path(snapshot_bytes, policy.max_log_snapshot_bytes, snap), policy.shared_log)


def plan(tables: dict, policy: Policy) -> dict:
    """{table: Route} for {table: (snapshot bytes, change events a second)}."""
    return {t: log_for(t, b, r, policy) for t, (b, r) in tables.items()}


def suggest(tables: dict, policy: Policy) -> dict:
    """A route to suggest per table from the source's statistics, for the user to check and change: {table: {class,
    snapshot, log (shared or own), shards, why}}. tables: {table: {"bytes": snapshot bytes or None, "events_per_s":
    change rate or None}}. A slow table of unknown size is class and snapshot `auto`: dimension or big_slow, set by its
    real bytes when the snapshot lands (land.snapshot_path). An unknown rate counts as slow, and says so."""
    out = {}
    for t, s in tables.items():
        b, r = s.get("bytes"), s.get("events_per_s")
        route = log_for(t, b if b is not None else 0, r or 0.0, policy)
        why = [f"{_size(b)} snapshot" if b is not None else "size unknown: the snapshot's path is set by its real bytes",
               f"{r:,.1f} changes/s" if r is not None else "change rate unknown: taken as slow"]
        if t in policy.chosen:
            why.append(f"chosen: {policy.chosen[t]}")
        unknown = b is None and route.cls != "big_fast" and t not in policy.chosen   # dimension or big_slow: its bytes say
        out[t] = {"class": "auto" if unknown else route.cls, "snapshot": "auto" if unknown else route.snapshot,
                  "log": "own" if route.log != policy.shared_log else "shared", "shards": route.shards,
                  "why": "; ".join(why)}
    return out


def _size(b: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if b < 1024 or unit == "TiB":
            return f"{b:,.0f} {unit}" if unit == "B" else f"{b:,.1f} {unit}"
        b /= 1024
    return str(b)


def rates(before: dict, after: dict, seconds: float) -> dict:
    """Change events a second per table, from two counts of its events in a log `seconds` apart (a table new in
    `after` counts from 0)."""
    if seconds <= 0:
        raise ValueError("seconds must be positive")
    return {t: max(0, n - before.get(t, 0)) / seconds for t, n in after.items()}


def history_fq(segments: list, kind: str) -> str:
    """A table's change log across its route's history, as a FROM clause for CdcLog (its fq): segments [(log, from)]
    in order, each log holding its events after `from` (a position value of `kind`; None: from the start) up to the
    next segment's. A table moved to its own log at position P reads the shared log up to P and its own after P, with
    no new snapshot: positions of one source compare across logs."""
    seq = SEQUENCES[kind]
    lit = (lambda v: "'" + str(v).replace("'", "") + "'") if kind in TEXT else (lambda v: str(int(v)))
    parts = []
    for i, (log, frm) in enumerate(segments):
        nxt = segments[i + 1][1] if i + 1 < len(segments) else None
        cond = ([f"{seq} > {lit(frm)}"] if frm is not None else []) + ([f"{seq} <= {lit(nxt)}"] if nxt is not None else [])
        # a log's resolved and snapshot rows belong to it whatever their position: only change events are cut
        keep = f" WHERE operation IN ('resolved', 'read') OR ({' AND '.join(cond)})" if cond else ""
        parts.append(f"SELECT * FROM {log}{keep}")
    if len(segments) == 1 and segments[0][1] is None:
        return segments[0][0]   # one log from the start: the log itself
    return "(" + " UNION ALL ".join(parts) + ")"


def transitions(old: dict, new: dict) -> list:
    """What changing a table's route takes, from one plan's route to another's ({class, snapshot, log, shards}):
    [(action, why)]. None of these takes a new snapshot of the source or a rebuild of the pipeline; changing the
    gateway (or Debezium target) a table is on does, and isn't a route change (the applier's task)."""
    out = []
    if old.get("snapshot") != new.get("snapshot") and "auto" not in (old.get("snapshot"), new.get("snapshot")):
        how = ("landed into the log once (land.gateway_snapshots), from the snapshot the producer kept"
               if new.get("snapshot") == "log" else "read where the producer wrote it; its rows in the log are ignored")
        out.append(("snapshot", f"snapshot {old.get('snapshot')} -> {new.get('snapshot')}: {how}; needs the "
                                "producer's snapshot kept (staging cleanup leaves snapshots)"))
    if old.get("log") != new.get("log"):
        out.append(("cutover", f"log {old.get('log')} -> {new.get('log')} at a position P: events after P land in the "
                               f"{new.get('log')} log (land.gateway only=/skip=), readers read the {old.get('log')} log up "
                               "to P and the new one after it (routing.history_fq); no new snapshot"))
    if (old.get("shards") or 1) != (new.get("shards") or 1):
        out.append(("reshard", f"shards {old.get('shards') or 1} -> {new.get('shards') or 1} at a position P: events "
                               "after P split by the new key hash, each key's events in order within a shard (designed; "
                               "sharded landing isn't built yet)"))
    return out
