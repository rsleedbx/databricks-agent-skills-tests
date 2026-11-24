"""routing: each table's class, and from it its snapshot's path, its change log, its shards and how its silver is kept:

  class       size, changes  snapshot  log                    silver
  small_slow  small, few     log       shared                 shared: in a group's table, one MERGE a round for all
  small_fast  small, many    log       shared                 shared, unless it carries much of its group's changes
  big_slow    big, few       direct    shared                 merge_changed: a MERGE only in a round that changed it
  big_fast    big, many      direct    its own, sharded       auto_cdc: flows in a continuous pipeline (setup paid once)

A small table leaves its group (shared) for a table of its own (small_slow: view, small_fast: merge_round) when it:
- needs what only a table of its own gives (Policy.own_table: streaming reads, its own change data feed, time
  travel);
- carries more than shared_max_share of its group's changes (its rows would make every member's MERGE slower);
- is read more than shared_max_reads_per_day (each read casts its row from VARIANT, with no column statistics).
plan() applies these rules (they need the group's numbers); log_for() alone leaves group empty.

The costs behind it (lfc-medallion references/performance-reasoning.md): a small table costs per object (a MERGE has a
fixed floor of seconds whatever it merges), a big one per byte. Most tables are small and slow and hold almost no data,
so they're kept as views; most bytes are in a few big, fast tables, which get their own log and a long-running reader.

A busy table in the shared log makes every other table's read skip its events (clustering by destination helps only
once OPTIMIZE has rewritten the new files), so a table gets a log of its own once it's big and fast, or once any
table's rate passes `own_log_events_per_s`. Split by transport, not inside a log: a gateway's change files mix all its
tables, so such a table goes on a gateway (or a Debezium target table) of its own, and each landing stays a copy of
whole files. The thresholds are starting points to measure against, set per deployment; any table's class (and its
silver) can be chosen by name.

    from lfc_change_source import routing
    p = routing.Policy(shared_log="`main`.`ctl`.`cdc_log`")
    routes = routing.plan({"db.s.orders": (5 << 40, 250_000), "db.s.region": (4096, 0.01)}, p)
    routes["db.s.orders"].log, routes["db.s.orders"].shards, routes["db.s.region"].silver   # own log, 3, view
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .cdc_log import SEQUENCES, TEXT
from .land import MAX_LOG_SNAPSHOT_BYTES, snapshot_path

CLASSES = ("small_slow", "small_fast", "big_slow", "big_fast")
SILVER = {"small_slow": "shared", "small_fast": "shared", "big_slow": "merge_changed", "big_fast": "auto_cdc"}
OWN = {"small_slow": "view", "small_fast": "merge_round"}   # a small table's method out of its group
METHODS = ("shared", "view", "merge_round", "merge_changed", "auto_cdc")
# Zerobus Ingest's documented default per stream (gRPC): 100,000 records a second (benchmarked with 1 KB messages)
STREAM_RECORDS_PER_S = 100_000


@dataclass(frozen=True)
class Route:
    table: str            # <catalog>.<schema>.<table>
    cls: str              # one of CLASSES
    snapshot: str         # land.snapshot_path: log or direct
    log: str              # the change log its changes go to, quoted
    shards: int = 1       # streams (and reader tasks) its changes are split into, by key hash
    silver: str = "view"  # one of METHODS
    group: str = ""       # shared: its group (plan() sets it)
    why: str = ""         # out of its group: the rule that took it out


@dataclass
class Policy:
    shared_log: str                                   # every table's changes but those with a log of their own
    fast_log: str = "{shared}_{table}"                # a table's own log: {shared} and {table} filled in
    fast_events_per_s: float = 1.0                    # change events a second at which a table is fast
    own_log_events_per_s: float = 10_000.0            # ... at which any table gets a log of its own
    max_log_snapshot_bytes: int = MAX_LOG_SNAPSHOT_BYTES   # bigger: big (and its snapshot read in place)
    shard_events_per_s: float = STREAM_RECORDS_PER_S  # events a second one stream (and one reader) carries
    chosen: dict = field(default_factory=dict)        # {table: class}: chosen instead of by the numbers
    silver_chosen: dict = field(default_factory=dict)  # {table: method}: chosen instead of by its class
    own_table: dict = field(default_factory=dict)     # {table: need}: a small table kept out of a group (a need
    #                                                   only its own table meets: streaming reads, change feed, ...)
    shared_group_size: int = 1000                     # tables a group (its MERGE's CASE has a branch a table)
    shared_max_share: float = 0.10                    # of its group's change events: more, and it leaves the group
    shared_max_reads_per_day: float | None = None     # reads a day (query history): more, and it leaves (None: off)

    def log_name(self, table: str) -> str:
        shared = self.shared_log.rstrip("`")
        name = table.split(".")[-1]
        out = self.fast_log.format(shared=shared, table=name)
        return out + "`" if self.shared_log.endswith("`") and not out.endswith("`") else out


def classify(snapshot_bytes: int, events_per_s: float, policy: Policy, table: str = "") -> str:
    """The table's class: chosen by name, else by its snapshot's size and its change rate."""
    if table in policy.chosen:
        if policy.chosen[table] not in CLASSES:
            raise ValueError(f"{table}: class {policy.chosen[table]!r}, one of {', '.join(CLASSES)}")
        return policy.chosen[table]
    size = "small" if snapshot_bytes <= policy.max_log_snapshot_bytes else "big"
    return f"{size}_{'fast' if events_per_s >= policy.fast_events_per_s else 'slow'}"


def log_for(table: str, snapshot_bytes: int, events_per_s: float, policy: Policy) -> Route:
    """Where one table goes: its class, its snapshot's path, its change log, its shards and its silver method."""
    cls = classify(snapshot_bytes, events_per_s, policy, table)
    own = cls == "big_fast" or events_per_s >= policy.own_log_events_per_s
    shards = max(1, math.ceil(events_per_s / policy.shard_events_per_s)) if own else 1
    snap = snapshot_path(snapshot_bytes, policy.max_log_snapshot_bytes, "log" if cls.startswith("small") else "direct")
    default = OWN[cls] if own and SILVER[cls] == "shared" else SILVER[cls]   # a busy table with its own log: no group
    silver = policy.silver_chosen.get(table, default)
    if silver not in METHODS:
        raise ValueError(f"{table}: silver {silver!r}, one of {', '.join(METHODS)}")
    return Route(table, cls, snap, policy.log_name(table) if own else policy.shared_log, shards, silver)


def plan(tables: dict, policy: Policy, reads_per_day: dict | None = None) -> dict:
    """{table: Route} for {table: (snapshot bytes, change events a second)}: each table's route, then the shared
    tables out of their group by the rules above (reads_per_day: {table: reads}, from query history), and the rest in
    groups of shared_group_size by name, each group's tables in one log."""
    from dataclasses import replace
    routes = {t: log_for(t, b, r, policy) for t, (b, r) in tables.items()}
    reads = reads_per_day or {}
    rate = {t: r for t, (_, r) in tables.items()}
    shared = [t for t in sorted(routes) if routes[t].silver == "shared"]
    for log in sorted({routes[t].log for t in shared}):
        members = [t for t in shared if routes[t].log == log]
        total = sum(rate[t] for t in members)
        for t in members:
            why = (f"needs its own table: {policy.own_table[t]}" if t in policy.own_table else
                   f"{rate[t] / total:.0%} of its group's changes (over {policy.shared_max_share:.0%})"
                   if len(members) > 1 and total > 0 and rate[t] / total > policy.shared_max_share
                   and rate[t] >= policy.fast_events_per_s else
                   f"{reads[t]:,.0f} reads a day (over {policy.shared_max_reads_per_day:,.0f})"
                   if policy.shared_max_reads_per_day is not None and reads.get(t, 0) > policy.shared_max_reads_per_day
                   else "")
            if why:
                routes[t] = replace(routes[t], silver=OWN.get(routes[t].cls, "merge_round"),
                                    why=why)
        kept = [t for t in members if routes[t].silver == "shared"]
        tag = "" if log == policy.shared_log else log.strip("`").split(".")[-1] + "_"
        for i, t in enumerate(kept):
            routes[t] = replace(routes[t], group=f"{tag}{i // policy.shared_group_size}")
    return routes


def suggest(tables: dict, policy: Policy) -> dict:
    """A route to suggest per table from the source's statistics, for the user to check and change: {table: {class,
    snapshot, log (shared or own), shards, silver, why}}. tables: {table: {"bytes": snapshot bytes or None,
    "events_per_s": change rate or None}}. A table of unknown size is class `auto` (small or big, set by its real bytes
    when the snapshot lands: land.snapshot_path), its snapshot `auto` too. An unknown rate counts as slow, and says so."""
    out = {}
    routes = plan({t: (s.get("bytes") or 0, s.get("events_per_s") or 0.0) for t, s in tables.items()}, policy,
                  {t: s["reads_per_day"] for t, s in tables.items() if s.get("reads_per_day") is not None})
    for t, s in tables.items():
        b, r = s.get("bytes"), s.get("events_per_s")
        route = routes[t]
        why = [f"{_size(b)} snapshot" if b is not None else "size unknown: the snapshot's path is set by its real bytes",
               f"{r:,.1f} changes/s" if r is not None else "change rate unknown: taken as slow"]
        if t in policy.chosen:
            why.append(f"chosen: {policy.chosen[t]}")
        if route.why:
            why.append(f"out of its group: {route.why}")
        unknown = b is None and t not in policy.chosen   # small or big: its bytes say, when the snapshot lands
        speed = route.cls.split("_")[1]
        out[t] = {"class": f"auto_{speed}" if unknown else route.cls, "snapshot": "auto" if unknown else route.snapshot,
                  "log": "own" if route.log != policy.shared_log else "shared", "shards": route.shards,
                  "silver": route.silver if not unknown or route.silver == "shared" else
                  ("merge_round" if speed == "fast" else "view"), "group": route.group, "why": "; ".join(why)}
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
    a, b = old.get("silver"), new.get("silver")
    if a and b and a != b:
        how = {("shared", "view"): "its view over the log instead of its group's table; its rows left in the group "
                                   "until the group is rebuilt",
               ("shared", "merge_round"): "build its own table from the log once, its view replaced by it; then MERGE "
                                          "every round",
               ("shared", "merge_changed"): "build its own table from the log once; then MERGE in rounds that change it",
               ("view", "shared"): "its rows into its group's table at the group's position (one INSERT from the "
                                   "log), its view over the group",
               ("merge_round", "shared"): "its rows into its group's table at the group's position, its table "
                                          "replaced by a view over the group",
               ("view", "merge_round"): "build the table from the log once, then MERGE every round",
               ("view", "merge_changed"): "build the table from the log once, then MERGE in rounds that change it",
               ("merge_round", "view"): "drop the table, create the view: no work a round from then on",
               ("merge_changed", "view"): "drop the table, create the view",
               ("merge_round", "merge_changed"): "a schedule change: MERGE only in rounds that change it",
               ("merge_changed", "merge_round"): "a schedule change: MERGE every round"}.get(
            (a, b), "the table built again from the log by the new method (a full read of its events)")
        out.append(("silver", f"silver {a} -> {b}: {how}; no new snapshot"))
    if (old.get("shards") or 1) != (new.get("shards") or 1):
        out.append(("reshard", f"shards {old.get('shards') or 1} -> {new.get('shards') or 1} at a position P: events "
                               "after P split by the new key hash, each key's events in order within a shard (designed; "
                               "sharded landing isn't built yet)"))
    return out
