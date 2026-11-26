"""paths: the fan-in planner, the merge route's planner. Many sources fan in to one n:1 table; for each table and run
it chooses how that table's MERGE of its changed sources runs, by cost, as a SQL planner picks a plan by cardinality.
(The pipeline route's materialized views are planned by Databricks' own refresh planner, Enzyme; the fan-in planner
takes its decisions where they apply: references/merge-paths.md.) A path is an engine and a way to split the sources:

  engine  spark      the merge job's own Spark session
          warehouse  a SQL warehouse, through the Statement Execution API (engine.Warehouse)
  split   k          the n changed sources in k groups, each one MERGE of a UNION ALL of its sources' change feeds,
                     the k MERGEs at once on threads: k = 1 is one MERGE of everything, k = n one MERGE per source

A path takes the longer of two times (a roofline):

  latency     fixed + per_source * ceil(n / k) + per_commit * k
              one MERGE alone: it plans its sources' change feeds one after another (a UNION ALL) and reads them, and
              the k MERGEs of one table commit to it in turn
  throughput  (w_source * n * at_once + w_statement * k * at_once + w_row * rows * at_once) / units
              the engine's work for every MERGE in flight (`at_once` tables, each k MERGEs), over its capacity
              (`units`: a warehouse's running clusters, a size up 1.25 times, 2X-Small = 1; the job's session = 1)

plus a stopped warehouse's start on its first use. Few sources: one MERGE, its latency binds. Many: split until the
engine's throughput binds; more units (clusters, a larger size) move that point. Each engine's line starts from what
was measured (DEFAULTS) and is refit from the paths log (every MERGE's engine, k, sources, rows, tables at once, units
and seconds): each run counts toward the side that binds it. An engine never measured is taken only when forced.
choose() gives the cheapest path and the predictions it was chosen over, for the log.
"""
from __future__ import annotations

import math
from typing import NamedTuple

ENGINES = ("spark", "warehouse")


class Line(NamedTuple):
    """An engine's costs, in seconds."""
    fixed: float          # a MERGE's own start
    per_source: float     # each source in one MERGE (planned and read in turn)
    per_commit: float     # each MERGE of one table (they commit to it in turn)
    w_source: float       # the engine's work per source, over its units
    w_statement: float    # the engine's work per MERGE, over its units
    w_row: float = 0.0    # the engine's work per change row, over its units
    measured: bool = True


# Starting lines, fit from runs of TPC-B at many LOBs on a serverless warehouse and a serverless job; every run's
# paths log refits them (Model.fit), so a deployment's own runs replace them.
DEFAULTS = {"spark": Line(3.0, 1.05, 0.54, 0.30, 1.40), "warehouse": Line(14.6, 0.73, 2.09, 0.204, 0.345)}
START_S = 15.0    # a stopped serverless warehouse's start before its first statement (an estimate until measured)
MAX_K = 32        # MERGEs of one table at once
SIZES = ("2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large")


SIZE_STEP = 1.25   # a size up: a larger cluster runs each MERGE a little faster, it admits no more of them


def units(cluster_size: str | None, clusters: int | None) -> float:
    """A warehouse's capacity for many small MERGEs, in 2X-Small clusters: the clusters running (each admits about
    10 at once), times SIZE_STEP a size up (a larger cluster runs each
    MERGE a little faster but admits no more of them)."""
    i = SIZES.index(cluster_size) if cluster_size in SIZES else 0
    return float(SIZE_STEP ** i * max(1, clusters or 1))


def latency(line: Line, sources: int, k: int) -> float:
    return line.fixed + line.per_source * math.ceil(sources / k) + line.per_commit * k


def throughput(line: Line, sources: int, k: int, rows: int = 0, at_once: int = 1, u: float = 1.0) -> float:
    return (line.w_source * sources + line.w_statement * k + line.w_row * (rows or 0)) * max(1, at_once) / max(u, 1e-9)


def cost(line: Line, sources: int, k: int, rows: int = 0, at_once: int = 1, u: float = 1.0) -> float:
    return max(latency(line, sources, k), throughput(line, sources, k, rows, at_once, u))


class Model:
    def __init__(self, lines: dict | None = None, start_s: float = START_S, max_k: int = MAX_K):
        self.lines = {e: Line(*v) for e, v in (DEFAULTS if lines is None else lines).items()}
        self.start_s, self.max_k = start_s, max_k

    def best(self, engine: str, sources: int, rows: int = 0, cold: bool = False, at_once: int = 1,
             u: float = 1.0) -> tuple[int, float]:
        """(k, seconds): the cheapest split for this engine."""
        line = self.lines[engine]
        ks = range(1, max(1, min(self.max_k, sources)) + 1)
        k = min(ks, key=lambda k: (round(cost(line, sources, k, rows, at_once, u), 3), k))
        return k, cost(line, sources, k, rows, at_once, u) + (self.start_s if cold and engine == "warehouse" else 0.0)

    def fit(self, runs: list[tuple], rounds: int = 3) -> "Model":
        """Refit each engine's line from its runs [(engine, k, sources, rows, seconds, at_once, units)]: each run goes
        to the side that binds it under the current line, and each side is refit by least squares from its runs when
        they vary enough; a side whose runs can't tell its parts apart keeps them."""
        by: dict[str, list] = {}
        for r in runs:
            e, k, n, rows, s = r[:5]
            at_once, u = (list(r[5:7]) + [1, 1.0])[:2]
            if e in self.lines and s is not None and n and k:
                by.setdefault(e, []).append((int(k), int(n), float(rows or 0), float(s), int(at_once or 1),
                                             float(u or 1.0)))
        for e, pts in by.items():
            for _ in range(rounds):
                ln = self.lines[e]
                lat = [p for p in pts if latency(ln, p[1], p[0]) >= throughput(ln, p[1], p[0], p[2], p[4], p[5])]
                thr = [p for p in pts if p not in lat]
                ln = _fit_latency(ln, lat)
                self.lines[e] = _fit_throughput(ln, thr)
        return self


def _fit_latency(ln: Line, pts: list) -> Line:
    if len({math.ceil(n / k) for k, n, *_ in pts}) < 2:
        return ln
    if len({k for k, *_ in pts}) >= 2 and len(pts) >= 4:   # planning and commits both vary: fit both
        got = _lsq([(1.0, math.ceil(n / k), float(k)) for k, n, *_ in pts], [p[3] for p in pts])
        if got and min(got) >= 0:
            return ln._replace(fixed=got[0], per_source=got[1], per_commit=got[2])
    got = _lsq([(1.0, math.ceil(n / k)) for k, n, *_ in pts], [p[3] - ln.per_commit * p[0] for p in pts])
    return ln._replace(fixed=got[0], per_source=got[1]) if got and min(got) >= 0 else ln


def _fit_throughput(ln: Line, pts: list) -> Line:
    """seconds * units / at_once = w_source * n + w_statement * k: both when the runs vary in n/k, else one scale."""
    if not pts:
        return ln
    xs, ys = [(float(n), float(k)) for k, n, *_ in pts], [s * u / a for k, n, r, s, a, u in pts]
    if len({round(n / k, 3) for k, n, *_ in pts}) >= 2:
        got = _lsq(xs, ys)
        if got and min(got) >= 0:
            return ln._replace(w_source=got[0], w_statement=got[1])
    now = [ln.w_source * x[0] + ln.w_statement * x[1] for x in xs]
    f = sum(a * b for a, b in zip(now, ys)) / (sum(a * a for a in now) or 1.0)
    return ln._replace(w_source=ln.w_source * f, w_statement=ln.w_statement * f) if f > 0 else ln


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


def fit_r2(xs: list[tuple], ys: list[float]) -> tuple[tuple | None, float | None]:
    """Least squares with an intercept: (coefficients, intercept first; R²), or (None, None) when singular."""
    w = _lsq([(1.0, *x) for x in xs], ys) if xs else None
    if not w:
        return None, None
    pred = [w[0] + sum(a * b for a, b in zip(w[1:], x)) for x in xs]
    mean = sum(ys) / len(ys)
    total = sum((y - mean) ** 2 for y in ys)
    return w, (1 - sum((y - p) ** 2 for y, p in zip(ys, pred)) / total) if total else None


def parse(path: str) -> tuple[str, int | None]:
    """'warehouse' or 'warehouse:8' -> (engine, k or None: the model's)."""
    engine, _, k = path.partition(":")
    if engine not in ENGINES or (k and not k.isdigit()):
        raise ValueError(f"path {path!r}: <{'|'.join(ENGINES)}>[:k]")
    return engine, int(k) if k else None


def choose(model: Model, sources: int, rows: int = 0, engines=("spark",), forced: str | None = None,
           cold: frozenset = frozenset(), at_once: int = 1, units: dict | None = None) -> tuple[str, int, dict]:
    """(engine, k, {engine: [k, predicted seconds]}) for one table's MERGE of `sources` sources, `at_once` tables'
    MERGEs in flight on engines of `units` ({engine: units}, 1 by default): `forced` when given (an engine, with or
    without its k), else the cheapest measured engine at its best k."""
    u = units or {}
    preds = {e: list(model.best(e, sources, rows, e in cold, at_once, u.get(e, 1.0)))
             for e in engines if e in model.lines}
    if forced:
        e, k = parse(forced)
        if e not in preds:
            raise ValueError(f"path {forced}: engine {e} isn't available (a warehouse needs --warehouse)")
        k = min(k or preds[e][0], max(1, sources))
        return e, k, preds | {e: [k, round(cost(model.lines[e], sources, k, rows, at_once, u.get(e, 1.0)), 1)]}
    known = [e for e in preds if model.lines[e].measured] or list(preds)
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


COPY_SHARE = 0.5      # a source whose change rows reach this share of its rows is copied whole, not merged
COPY_MIN_ROWS = 10000  # below this many change rows a source is merged: the target's row counts aren't read
TABLE_RUNS = 6        # a target table's own runs (same fingerprint) before its own model replaces the pooled one


def technique(change_rows: int | None, source_rows: int | None, share: float = COPY_SHARE) -> tuple[str, str]:
    """(merge | copy, why) for one source of one table, as the materialized-view planner chooses between incremental
    and recompute: a small share of changes is merged, a large one copied whole (a partition overwrite by source)."""
    if not change_rows or change_rows < COPY_MIN_ROWS:
        return "merge", "THRESHOLD_SMALL_CHANGES_INC"
    if not source_rows:
        return "merge", "SOURCE_ROWS_UNKNOWN_INC"
    if change_rows >= share * source_rows:
        return "copy", "THRESHOLD_LARGE_CHANGES_RECOMPUTE"
    return "merge", "THRESHOLD_SMALL_CHANGES_INC"


def fingerprint(key: list[str], cols: list[str]) -> str:
    """A target table's MERGE shape: its key and columns. History is kept per table and fingerprint, and a new one
    (a column added, another key) starts it again, as a query fingerprint does for a materialized view."""
    import hashlib
    return hashlib.sha1(("|".join(key) + "#" + "|".join(cols)).encode()).hexdigest()[:12]


def table_models(runs: list[tuple], pooled: "Model", min_runs: int = TABLE_RUNS) -> dict:
    """{(target_table, fingerprint): Model} refit from that table's own runs when it has `min_runs` or more; others
    use the pooled model. runs: (target_table, fingerprint, engine, k, sources, rows, seconds, at_once, units)."""
    by: dict = {}
    for r in runs:
        if r[1]:
            by.setdefault((r[0], r[1]), []).append(r[2:])
    return {t: Model({e: tuple(v) for e, v in pooled.lines.items()}, pooled.start_s, pooled.max_k).fit(rs)
            for t, rs in by.items() if len(rs) >= min_runs}


class Planner:
    """One merge run's choices: the engines there are ({name: a session with sql()}), the model, a forced path, each
    (table, source)'s change rows (from the event logs), the engines not started yet, each engine's units and the
    tables merged at once (`at_once`, set once the run knows them), and per (target table, fingerprint) a model of
    its own (table_models). pick() per table's MERGE, techniques() per table's sources, record() what it took;
    records are the paths log's rows."""

    def __init__(self, engines: dict, model: Model | None = None, forced: str | None = None, rows: dict | None = None,
                 cold=(), units: dict | None = None, at_once: int = 1, tables: dict | None = None):
        import threading
        self.engines, self.model, self.forced = engines, model or Model(), forced
        self.rows, self.cold, self.records, self._lock = dict(rows or {}), set(cold), [], threading.Lock()
        self.units, self.at_once, self.tables = dict(units or {}), at_once, dict(tables or {})

    def needs_counts(self, table: str, sources: list[str]) -> bool:
        """Whether any source has enough change rows that its share of the target decides merge or copy."""
        return any(int(self.rows.get((table, s)) or 0) >= COPY_MIN_ROWS for s in sources)

    def techniques(self, table: str, sources: list[str], counts: dict | None = None) -> dict:
        """{source: (merge | copy, why)}: counts are each source's rows in the target ({source: rows})."""
        return {s: technique(self.rows.get((table, s)), (counts or {}).get(s)) for s in sources}

    def pick(self, table: str, sources: list[str], out_name: str | None = None, fp: str | None = None):
        """(engine name, its session, k, change rows, predictions) for one MERGE of `sources` of `table`, by the
        target's own model when it has one (out_name, fp), else the pooled one."""
        rows = sum(int(self.rows.get((table, s)) or 0) for s in sources)
        model = self.tables.get((out_name, fp), self.model)
        with self._lock:
            e, k, preds = choose(model, len(sources), rows, tuple(self.engines), self.forced,
                                 frozenset(self.cold), self.at_once, self.units)
            self.cold.discard(e)   # started by this MERGE: warm for the next
        return e, self.engines[e], k, rows, preds

    def record(self, target_table: str, engine: str, k: int, sources: int, rows: int, preds: dict, seconds: float,
               retries: int, done: str = "merged", fp: str | None = None, insights: list | None = None,
               statements: list | None = None, planning_s: float | None = None) -> None:
        with self._lock:
            self.records.append({"target_table": target_table, "engine": engine, "k": k, "sources": sources,
                                 "change_rows": rows, "predicted_s": (preds.get(engine) or [None, None])[1],
                                 "seconds": round(seconds, 2), "retries": retries, "done": done,
                                 "forced": bool(self.forced), "predictions": preds, "at_once": self.at_once,
                                 "units": self.units.get(engine, 1.0), "fingerprint": fp,
                                 "own_model": (target_table, fp) in self.tables, "insights": insights or [],
                                 "statements": statements or [],
                                 "planning_s": None if planning_s is None else round(planning_s, 2)})
