"""producer: restarting what writes a change log, the same way for every transport (docs/design-intelligent.md, "A log
can be split while it runs"). A producer (a Debezium Server, a Lakeflow Connect gateway) carries some tables into one
log; an adapter gives it three operations:

  position()   where it has got to (the source's position kind: an LSN, an SCN, a binlog file and position)
  stop()       stops it; its position stays where it stopped
  start()      starts it with its tables and its log (a new producer: from where its source lets it start)

A split moves some tables to a new log make before break: the new producer first, until its first change has landed,
then the old one restarted without them. The logs overlap for the moved tables (never a gap); silver applies an event
at a position once, so a moved table reads its old log up to the cut (the old producer's position when it stopped
carrying the table) and its new log after it. split() plans the steps as plain data; carry_out() runs them.

    steps = producer.split(old, ["db.s.orders"], new_log="`c`.`s`.`cdc_log_orders`", make=debezium_producer)
    cuts = producer.carry_out(steps, landed=lambda p: ...)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Callable, Protocol


class Producer(Protocol):
    name: str
    log: str
    tables: tuple

    def position(self) -> str: ...
    def stop(self) -> None: ...
    def start(self) -> None: ...


@dataclass(frozen=True)
class Step:
    op: str              # start, wait_landed, stop, start_again
    producer: object     # the Producer it acts on
    why: str = ""


@dataclass
class Spec:
    """What a producer carries: a name, a log, its tables. An adapter's make(spec) gives the Producer."""
    name: str
    log: str
    tables: tuple = field(default_factory=tuple)


def split(old: Spec, moved: list, new_log: str, make: Callable, new_name: str | None = None) -> list:
    """The steps that move `moved` from old's log to new_log, make before break (module doc). make(Spec) -> Producer."""
    missing = sorted(set(moved) - set(old.tables))
    moved = tuple(t for t in old.tables if t in set(moved))
    if not moved:
        raise ValueError(f"split: none of the tables is carried by {old.name}")
    if new_log == old.log:
        raise ValueError("split: the new log is the old one")
    if missing:
        raise ValueError(f"split: not carried by {old.name}: {missing}")
    rest = tuple(t for t in old.tables if t not in moved)
    new = make(Spec(new_name or f"{old.name}-{len(moved)}t", new_log, moved))
    kept = make(replace(old, tables=rest))
    steps = [Step("start", new, f"{len(moved)} tables into {new_log}"),
             Step("wait_landed", new, "its first change landed: the new log carries them from here"),
             Step("stop", make(old), "its position now is the moved tables' cut")]
    steps.append(Step("start_again", kept, f"without the {len(moved)} moved tables") if rest else
                 Step("retire", kept, "every table moved: the old producer stays stopped"))
    return steps


def carry_out(steps: list, landed: Callable, wait: Callable | None = None) -> dict:
    """Runs split()'s steps. landed(producer) -> bool: its first change is in its log; wait(seconds) between checks.
    Returns {"cut": the old producer's position when it stopped, "moved": its tables, "log": the new log}."""
    import time
    wait = wait or time.sleep
    out: dict = {}
    for s in steps:
        if s.op == "start":
            s.producer.start()
            out.update(moved=tuple(s.producer.tables), log=s.producer.log)
        elif s.op == "wait_landed":
            for _ in range(360):
                if landed(s.producer):
                    break
                wait(5)
            else:
                raise TimeoutError(f"{s.producer.name}: no change landed in its log; the old producer still runs")
        elif s.op == "stop":
            s.producer.stop()
            out["cut"] = s.producer.position()
        elif s.op == "start_again":
            s.producer.start()
        elif s.op != "retire":
            raise ValueError(f"carry_out: step {s.op!r}")
    return out


# Debezium Server: what a producer's application.properties says about its tables, its log and its position
DEBEZIUM_KEYS = {"tables": "debezium.source.table.include.list", "log": "debezium.sink.zerobus.table",
                 "slot": "debezium.source.slot.name", "offsets": "debezium.source.offset.storage.file.filename"}


def debezium_properties(text: str, tables=None, log: str | None = None, slot: str | None = None,
                        offsets: str | None = None) -> str:
    """A Debezium Server's application.properties with its tables (schema.table, as the connector names them), its
    Zerobus target table (the log, unquoted catalog.schema.table), its PostgreSQL slot and its offsets file replaced;
    each key must be there already (an empty include list would capture every table, so tables can't be empty)."""
    if tables is not None and not list(tables):
        raise ValueError("debezium_properties: no tables (an empty include list captures every table)")
    new = {"tables": ",".join(tables) if tables is not None else None, "log": log.replace("`", "") if log else None,
           "slot": slot, "offsets": offsets}
    for k, v in new.items():
        if v is None:
            continue
        key = DEBEZIUM_KEYS[k]
        pat = re.compile(rf"^{re.escape(key)}=.*$", re.M)
        if not pat.search(text):
            raise ValueError(f"debezium_properties: no {key} line to replace")
        text = pat.sub(lambda _m, key=key, v=v: f"{key}={v}", text, count=1)
    return text
