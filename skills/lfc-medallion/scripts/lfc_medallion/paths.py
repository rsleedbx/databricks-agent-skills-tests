"""paths: how one table's MERGE of its changed sources runs, chosen by cost, as a SQL planner picks a plan by
cardinality. A path is an engine and a way to split the sources:

  engine  spark      the merge job's own Spark session
          warehouse  a SQL warehouse, through the Statement Execution API (engine.Warehouse)
  split   k          the n changed sources in k groups, each one MERGE of a UNION ALL of its sources' change feeds,
                     the k MERGEs at once on threads: k = 1 is one MERGE of everything, k = n one MERGE per source

A UNION ALL plans its sources' change feeds one after another (their logs and version ranges), so one MERGE grows with
its sources; the k MERGEs plan at once, but each commits to the same target in turn. So a path costs

  fixed + per_source * ceil(n / k) + per_commit * k + per_row * rows   (+ start, a stopped warehouse's first use)

and the best k is about sqrt(per_source * n / per_commit). Each engine's line starts from what was measured
(DEFAULTS) and is refit from the paths log (every MERGE's engine, k, sources, rows and seconds) once it has runs at
two or more shapes; an engine never measured is taken only when forced. choose() gives the cheapest path and the
predictions it was chosen over, for the log.
"""
from __future__ import annotations

import math

ENGINES = ("spark", "warehouse")

# (fixed s, per source s, per commit s, per row s, measured): scale1, TPC-B, 3 merge-route tables at once, 20 rows a
# source, 2026-11-22. spark from the merge job's log, k = 1: 14 s at 10 sources, 78 at 62, 141 at 124 (its per-commit
# cost not measured yet: the warehouse's taken). warehouse, a serverless SQL warehouse: k = 1, 6.6, 17.6 and 31.1 s
# (33 to 35 three tables at once); k = n (a MERGE a source, 16 threads), 9.1, 41.6 and 70.3 s.
DEFAULTS = {"spark": (3.0, 1.11, 0.54, 0.0, True), "warehouse": (4.5, 0.23, 0.54, 0.0, True)}
START_S = 15.0    # a stopped serverless warehouse's start before its first statement (an estimate until measured)
MAX_K = 32        # MERGEs of one table at once (a warehouse runs about 10 queries a cluster at once; more queue)


def cost(line: tuple, sources: int, k: int, rows: int = 0) -> float:
    a, b, c, d, _ = line
    return a + b * math.ceil(sources / k) + c * k + d * (rows or 0)


class Model:
    def __init__(self, lines: dict | None = None, start_s: float = START_S, max_k: int = MAX_K):
        self.lines = dict(DEFAULTS if lines is None else lines)
        self.start_s, self.max_k = start_s, max_k

    def best(self, engine: str, sources: int, rows: int = 0, cold: bool = False) -> tuple[int, float]:
        """(k, seconds): the cheapest split for this engine."""
        line = self.lines[engine]
        ks = range(1, max(1, min(self.max_k, sources)) + 1)
        k = min(ks, key=lambda k: (cost(line, sources, k, rows), k))
        return k, cost(line, sources, k, rows) + (self.start_s if cold and engine == "warehouse" else 0.0)

    def fit(self, runs: list[tuple]) -> "Model":
        """Refit each engine's line from its runs [(engine, k, sources, rows, seconds)], by least squares on
        ceil(n/k), k and rows; an engine whose runs don't vary in both keeps the parts they can't tell apart."""
        by: dict[str, list] = {}
        for e, k, n, r, s in runs:
            if e in self.lines and s is not None and n and k:
                by.setdefault(e, []).append((math.ceil(n / k), float(k), float(r or 0), float(s)))
        for e, pts in by.items():
            a0, b0, c0, d0, _ = self.lines[e]
            if len({p[0] for p in pts}) < 2:
                continue
            if len({p[1] for p in pts}) >= 2 and len(pts) >= 4:   # both planning and commits vary: fit both
                got = _lsq([(1.0, p[0], p[1]) for p in pts], [p[3] - d0 * p[2] for p in pts])
                if got and got[1] >= 0 and got[2] >= 0:
                    self.lines[e] = (got[0], got[1], got[2], d0, True)
                    continue
            got = _lsq([(1.0, p[0]) for p in pts], [p[3] - c0 * p[1] - d0 * p[2] for p in pts])   # planning only
            if got and got[1] >= 0:
                self.lines[e] = (got[0], got[1], c0, d0, True)
        return self


def _lsq(xs: list[tuple], ys: list[float]) -> tuple | None:
    """Least squares by the normal equations; None when they are singular."""
    n = len(xs[0])
    m = [[sum(x[i] * x[j] for x in xs) for j in range(n)] + [sum(x[i] * y for x, y in zip(xs, ys))] for i in range(n)]
    for i in range(n):
        p = max(range(i, n), key=lambda r: abs(m[r][i]))
        m[i], m[p] = m[p], m[i]
        if abs(m[i][i]) < 1e-9:
            return None
        for r in range(n):
            if r != i:
                f = m[r][i] / m[i][i]
                m[r] = [a - f * b for a, b in zip(m[r], m[i])]
    return tuple(m[i][n] / m[i][i] for i in range(n))


def parse(path: str) -> tuple[str, int | None]:
    """'warehouse' or 'warehouse:8' -> (engine, k or None: the model's)."""
    engine, _, k = path.partition(":")
    if engine not in ENGINES or (k and not k.isdigit()):
        raise ValueError(f"path {path!r}: <{'|'.join(ENGINES)}>[:k]")
    return engine, int(k) if k else None


def choose(model: Model, sources: int, rows: int = 0, engines=("spark",), forced: str | None = None,
           cold: frozenset = frozenset()) -> tuple[str, int, dict]:
    """(engine, k, {engine: [k, predicted seconds]}) for one table's MERGE of `sources` sources: `forced` when given
    (an engine, with or without its k), else the cheapest measured engine at its best k."""
    preds = {e: list(model.best(e, sources, rows, e in cold)) for e in engines if e in model.lines}
    if forced:
        e, k = parse(forced)
        if e not in preds:
            raise ValueError(f"path {forced}: engine {e} isn't available (a warehouse needs --warehouse)")
        k = min(k or preds[e][0], max(1, sources))
        return e, k, preds | {e: [k, round(cost(model.lines[e], sources, k, rows), 1)]}
    known = [e for e in preds if model.lines[e][4]] or list(preds)
    e = min(known, key=lambda e: (preds[e][1], e))
    return e, preds[e][0], {x: [k, round(s, 1)] for x, (k, s) in preds.items()}


def groups(items: list, k: int) -> list[list]:
    """items in k groups as even as can be, in order."""
    k = max(1, min(k, len(items)))
    q, r = divmod(len(items), k)
    out, i = [], 0
    for g in range(k):
        n = q + (1 if g < r else 0)
        out.append(items[i:i + n])
        i += n
    return out


class Planner:
    """One merge run's choices: the engines there are ({name: a session with sql()}), the model, a forced path, each
    (table, source)'s change rows (from the event logs) and the engines not started yet. pick() per table's MERGE,
    record() what it took; records are the paths log's rows."""

    def __init__(self, engines: dict, model: Model | None = None, forced: str | None = None, rows: dict | None = None,
                 cold=()):
        import threading
        self.engines, self.model, self.forced = engines, model or Model(), forced
        self.rows, self.cold, self.records, self._lock = dict(rows or {}), set(cold), [], threading.Lock()

    def pick(self, table: str, sources: list[str]):
        """(engine name, its session, k, change rows, predictions) for one MERGE of `sources` of `table`."""
        rows = sum(int(self.rows.get((table, s)) or 0) for s in sources)
        with self._lock:
            e, k, preds = choose(self.model, len(sources), rows, tuple(self.engines), self.forced,
                                 frozenset(self.cold))
            self.cold.discard(e)   # started by this MERGE: warm for the next
        return e, self.engines[e], k, rows, preds

    def record(self, target_table: str, engine: str, k: int, sources: int, rows: int, preds: dict, seconds: float,
               retries: int, done: str = "merged") -> None:
        with self._lock:
            self.records.append({"target_table": target_table, "engine": engine, "k": k, "sources": sources,
                                 "change_rows": rows, "predicted_s": (preds.get(engine) or [None, None])[1],
                                 "seconds": round(seconds, 2), "retries": retries, "done": done,
                                 "forced": bool(self.forced), "predictions": preds})
