"""route: the merge route as one job task, and the pipeline's side of it.

merge_route() is what runs before the pipeline each refresh: it plans every source's bronze (table_plan, with hints
where the data can't tell), agrees one route per table (`merge` only when every source's plan says so), applies the
routes a table keeps or an operator's tag asks for (routes_to_apply, with last run's routes so an interrupted switch
finishes), hands tables back to the pipeline (release), merges the rest one table at a time (merge_all, taking over a
view the pipeline left), recording the plan in <target>._lfc_medallion_plan before and after.
merged_tables() is what the pipeline reads, in its source file, to leave those tables out (stack_all(skip=...)).

A job runs it with the package's entry point (python_wheel_task, package lfc-medallion, entry point
lfc-medallion-merge) or a spark_python_task of two lines; arguments: --catalog --prefix --target [--source-column]
[--hints '{"nation": "lookup"}']. Every source <prefix>* takes part: a subset would make merge_all drop the others'
rows.
"""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor

from . import discover, evidence, merge
from .stack import spark_string

PLAN = "_lfc_medallion_plan"


def _has(spark, catalog: str, target: str, table: str) -> bool:
    """SHOW TABLES ... LIKE, which a pipeline's source file may run (spark.catalog.tableExists is not allowed there)."""
    return bool(spark.sql(f"SHOW TABLES IN `{catalog}`.`{target}` LIKE {spark_string(table)}").collect())


def merged_tables(spark, catalog: str, target: str) -> list[str]:
    """The tables on the `merge` route in the last plan merge_route wrote; none before it first ran."""
    if not _has(spark, catalog, target, PLAN):
        return []
    return [r.table_name for r in spark.sql(f"SELECT DISTINCT table_name FROM `{catalog}`.`{target}`.{PLAN} "
                                            "WHERE route = 'merge'").collect()]


def write_plan(spark, catalog: str, target: str, rows: list, apply: dict, proposed: dict) -> None:
    """<target>._lfc_medallion_plan: each source's plan of each table, the route applied, and any proposal."""
    from .stack import spark_string
    cols = ("source", "table_name", "role", "planned", "decided_by", "why", "route", "proposed")
    vals = [(*r, apply[r[1]], json.dumps(proposed[r[1]]) if r[1] in proposed else "") for r in rows]
    body = (" UNION ALL ".join("SELECT " + ", ".join(f"{spark_string('' if v is None else str(v))} AS {c}"
                                                    for c, v in zip(cols, x)) for x in vals) if vals else
            "SELECT " + ", ".join(f"CAST(NULL AS STRING) AS {c}" for c in cols) + " WHERE false")
    spark.sql(f"CREATE OR REPLACE TABLE `{catalog}`.`{target}`.{PLAN} AS {body}")   # SQL: either engine runs it


class _Workspace:
    """One databricks.sdk WorkspaceClient for the whole run, made on first use (the lookups that need it are few),
    under a lock: the key reads run on several threads, and two making the client at once broke its config
    ("dictionary changed size during iteration", 2026-11-22)."""

    def __init__(self):
        import threading
        self._w = None
        self._lock = threading.Lock()

    def __getattr__(self, attr):
        if attr.startswith("_"):
            raise AttributeError(attr)
        if self._w is None:
            with self._lock:
                if self._w is None:
                    from databricks.sdk import WorkspaceClient
                    self._w = WorkspaceClient()
        return getattr(self._w, attr)


def _kinds(spark, catalog: str, target: str) -> dict:
    return {r.table_name: r.table_type for r in spark.sql(
        f"SELECT table_name, table_type FROM `{catalog}`.information_schema.tables "
        f"WHERE table_schema = {spark_string(target)}").collect()}


def merge_route(spark, catalog: str, prefix: str, target: str, source_column: str = "_source", hints=None,
                name: str = "all_{table}", workspace=None, log: str | None = None, replan: bool = False,
                parallel: int = 8, events: list[str] | None = None, history_target: str | None = None,
                warehouse: str | None = None, path: str | None = None, read_by: str = "version") -> dict:
    """Plan, release, merge, then record, for every source <prefix><source> into <target>. Returns what it did.
    A table bound for `merge` with no primary key in some source (or whose key can't be read) stays on the pipeline;
    one merged before keeps its table and route, with no merge this run. A merge that fails keeps a table merged
    before (retried next run) and sends a first take-over back to the pipeline (dropping only the table this run
    created); all are noted in `proposed`. The plan is written before any release or merge, with first take-overs
    still on the pipeline (a table isn't skipped before it arrives), and again at the end, from what was done.
    log: the merge log's table (merge_all), e.g. a control schema's; each run's rows carry one run id.
    The last plan is kept for the sources it has (a table's route changes only by a tag, routes_to_apply): only new
    sources are planned, in parallel; replan=True plans every source again (its evidence: keys, rows, history).
    Keys are read at once, and up to `parallel` tables are merged at once (merge_all, each on its own thread).
    events: the ingestion pipelines' event logs (tables): one query over them says which sources' tables got rows
    or a snapshot since their state, and only those are read (changed_since); without it, every source is read.
    history_target: a schema where merge_all also keeps the tables left on the pipeline whose bronze is SCD type 2
    (a fact, say: its current rows stay the pipeline's view, fast), so their history outlives a full refresh of
    bronze, as the merge route's tables' does; never released or switched by a route change."""
    import uuid
    run_id = str(uuid.uuid4())
    workspace = workspace or _Workspace()
    timings, t = {}, time.time()   # the route's own seconds, part by part: the fan-in planner's planning time
    ctx = merge.load_context(spark, catalog, prefix, target)   # read once, for every table
    sources, held = ctx["names"], ctx["held"]
    timings["context_s"], t = round(time.time() - t, 1), time.time()
    changed = changed_since(spark, catalog, prefix, ctx, events, name) if events else None
    timings["changed_since_s"], t = round(time.time() - t, 1), time.time()
    planner = make_planner(spark, catalog, target, log, warehouse, path, changed)
    timings["planner_s"], t = round(time.time() - t, 1), time.time()
    rows, planned = [], {}
    kept: dict[str, list] = {}   # the last plan's rows, per source
    if not replan and _has(spark, catalog, target, PLAN):
        for r in spark.sql(f"SELECT source, table_name, role, planned, decided_by, why "
                           f"FROM `{catalog}`.`{target}`.{PLAN}").collect():
            kept.setdefault(r.source, []).append((r.source, r.table_name, r.role, r.planned, r.decided_by, r.why))
    new = [x for x in sources if x not in kept or {t for _, t, *_ in kept[x]} != set(held[x])
           or _hints_changed(kept[x], hints)]

    def plan_one(source):
        return [(source, table, t["role"], t["route"], t["decided_by"], t["why"]) for table, t in
                evidence.table_plan(spark, catalog, prefix + source, hints=hints, workspace=workspace)["tables"].items()]
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(new)))) as ex:
        fresh = dict(zip(new, ex.map(plan_one, new)))
    for source in sources:
        for row in fresh.get(source) or kept.get(source, []):
            table, route = row[1], row[3]
            planned[table] = "merge" if route == "merge" and planned.get(table, "merge") == "merge" else "pipeline"
            rows.append(row)
    previous = {r.table_name: r.route for r in spark.sql(
        f"SELECT DISTINCT table_name, route FROM `{catalog}`.`{target}`.{PLAN}").collect()} \
        if _has(spark, catalog, target, PLAN) else {}
    apply, proposed = merge.routes_to_apply(spark, catalog, target, planned, name=name, previous=previous)
    # merged before: merge_all's state has the table (a merge finished), not just a plan that said so
    established = {t for t in planned if name.format(table=t) in merge.merged_targets(spark, catalog, target)}
    skip = set()

    def key_missing(pair):   # merge_all needs every source's key: kept in the state, else read (threads)
        table, s = pair
        saved = ctx["state"].get((name.format(table=table), s))
        if saved and saved[2]:
            return None
        try:
            return None if evidence.recorded_keys(spark, f"`{catalog}`.`{prefix}{s}`.`{table}`",
                                                  f"{catalog}.{prefix}{s}.{table}", workspace) else s
        except Exception as e:  # noqa: BLE001  a key that can't be read counts as missing, this run only
            return f"{s} ({str(e).splitlines()[0][:80]})"
    pairs = [(t, x) for t, r in apply.items() if r == "merge" for x in sources if t in held[x]]
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(pairs)))) as ex:
        missing = list(ex.map(key_missing, pairs))
    for table in [t for t, r in apply.items() if r == "merge"]:
        keyless = [m for (t, _), m in zip(pairs, missing) if t == table and m]
        if keyless and table in established:   # keep the table and route; no merge this run
            skip.add(table)
            proposed[table] = {**proposed.get(table, {}), "refused": f"no primary key in {', '.join(keyless)}",
                               "kept": "merge, retried next run"}
        elif keyless:
            apply[table] = "pipeline"
            proposed[table] = {**proposed.get(table, {}), "refused": f"no primary key in {', '.join(keyless)}"}
    # first: first take-overs stay `pipeline` until their merge succeeds, so the pipeline doesn't skip a table before
    # it arrives; switches back to the pipeline are recorded, so a run that dies after release finishes them
    first = {t for t, r in apply.items() if r == "merge" and t not in established}
    write_plan(spark, catalog, target, rows, {t: "pipeline" if t in first else r for t, r in apply.items()}, proposed)
    released = merge.release(spark, catalog, target, sorted(t for t, r in apply.items() if r == "pipeline"), name=name)
    before = _kinds(spark, catalog, target)
    done = {}

    def merge_one(table):   # each table's merge_all on its own thread: (table, report, error)
        try:
            return table, merge.merge_all(spark, catalog, prefix, target, [table], source_column=source_column,
                                          name=name, workspace=workspace, take_over=True, run_id=run_id, log=log,
                                          changed=changed, context=ctx, planner=planner, read_by=read_by), None
        except Exception as e:  # noqa: BLE001  one table's failure doesn't stop the others
            return table, None, e
    todo = sorted(t for t, r in apply.items() if r == "merge" and t not in skip)
    planner.at_once = max(1, min(parallel, len(todo)))   # the tables whose MERGEs share an engine at once
    with ThreadPoolExecutor(max_workers=max(1, min(parallel, len(todo)))) as ex:
        results = list(ex.map(merge_one, todo))
    timings["tables_s"], t = round(time.time() - t, 1), time.time()
    try:
        add_phases(planner, planner.records)   # each warehouse MERGE's compile, execute, queue and bytes
    except Exception as e:  # noqa: BLE001  query history unreadable: the log without phases
        print(f"phases not read: {str(e).splitlines()[0][:200]}")
    try:
        write_paths(spark, catalog, target, log, run_id, planner.records)   # the optimizer's choices and costs
    except Exception as e:  # noqa: BLE001  a log write doesn't undo the merges
        print(f"paths log not written: {str(e).splitlines()[0][:200]}")
    for table, report, e in results:   # what each did, one at a time, in table order
        if e is None:
            done.update({t: {s: x["done"] for s, x in v.items()} for t, v in report.items()})
        else:
            error = str(e).splitlines()[0][:200]
            if table in established:   # merged before: keep the table and its route, retry next run
                proposed[table] = {**proposed.get(table, {}), "failed": error, "kept": "merge, retried next run"}
                continue
            apply[table] = "pipeline"   # a first take-over that failed: back to the pipeline
            out = name.format(table=table)
            # drop only a table this run created: not a table before it, and a plain managed table now (never a
            # pipeline's view or streaming table, never one that was there already)
            if before.get(out) not in ("MANAGED", "EXTERNAL") and _kinds(spark, catalog, target).get(out) == "MANAGED":
                spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{target}`.`{out}`")
                released.append(out)
            released += merge.release(spark, catalog, target, [table], name=name)   # its state, if any was saved
            proposed[table] = {**proposed.get(table, {}), "failed": error}
    write_plan(spark, catalog, target, rows, apply, proposed)   # again: what was done
    history = keep_history(spark, catalog, prefix, history_target, ctx,
                           sorted(t for t, r in apply.items() if r == "pipeline"), source_column, name, workspace,
                           run_id, log, events, parallel) if history_target else {}
    timings["after_s"] = round(time.time() - t, 1)
    return {"run_id": run_id, "timings": timings, "planned_sources": sorted(new), "history": history,
            "changed": None if changed is None else len(changed), "merge": sorted(t for t, r in apply.items() if r == "merge"),
            "pipeline": sorted(t for t, r in apply.items() if r == "pipeline"), "released": sorted(set(released)),
            "proposed": proposed, "done": done}


def scd2_tables(spark, catalog: str, prefix: str, ctx: dict, tables: list[str]) -> list[str]:
    """The tables whose bronze is SCD type 2 (__START_AT) in some source: from the catalog's columns read once, else
    one DESCRIBE per table (its first source)."""
    cols = ctx.get("cols") or {}
    out = []
    for t in tables:
        srcs = [x for x in ctx["names"] if t in ctx["held"].get(x, ())]
        known = [cols[x][t] for x in srcs if x in cols and t in cols[x]]
        names = {c for cs in known for c, _ in cs} if known else (
            {c for c, _ in merge.columns(spark, f"`{catalog}`.`{prefix}{srcs[0]}`.`{t}`")} if srcs else set())
        if merge.START in names:
            out.append(t)
    return out


def keep_history(spark, catalog: str, prefix: str, history_target: str, ctx: dict, tables: list[str],
                 source_column: str, name: str, workspace, run_id: str, log: str | None, events, parallel: int) -> dict:
    """merge_all into history_target for the SCD type 2 tables among `tables`: {table: {source: done}}, or
    {table: {"error": ...}} for one that failed (the others go on)."""
    todo = scd2_tables(spark, catalog, prefix, ctx, tables)
    if not todo:
        return {}
    hctx = merge.load_context(spark, catalog, prefix, history_target)
    changed = changed_since(spark, catalog, prefix, hctx, events, name) if events else None

    def one(table):
        try:
            report = merge.merge_all(spark, catalog, prefix, history_target, [table], source_column=source_column,
                                     name=name, workspace=workspace, run_id=run_id, log=log, changed=changed,
                                     context=hctx, keep_gone=True)   # a source gone keeps its history
            return {t: {s: x["done"] for s, x in v.items()} for t, v in report.items()}
        except Exception as e:  # noqa: BLE001
            return {name.format(table=table): {"error": str(e).splitlines()[0][:200]}}
    out = {}
    with ThreadPoolExecutor(max_workers=max(1, min(parallel, len(todo)))) as ex:
        for r in ex.map(one, todo):
            out.update(r)
    return out


def _hints_changed(rows: list, hints: dict | None) -> bool:
    """A kept plan made under other hints: a hinted table planned otherwise, or a table planned by a hint since
    dropped. Its source is planned again, so a role changed in the schema YAML (rendered into --hints) takes effect."""
    hints = hints or {}
    for _, table, role, _, decided_by, _ in rows:
        h = hints.get(table)
        want = h.get("role") if isinstance(h, dict) else h
        if (want is not None and (role != want or decided_by != "hint")) or (want is None and decided_by == "hint"):
            return True
    return False


def paths_log(catalog: str, target: str, log: str | None) -> str:
    """The paths log beside the merge log: <log>_paths (or <target>._lfc_medallion_merge_paths)."""
    if not log:
        return f"`{catalog}`.`{target}`.`_lfc_medallion_merge_paths`"
    return log[:-1] + "_paths`" if log.endswith("`") else log + "_paths"


def make_planner(spark, catalog: str, target: str, log: str | None, warehouse: str | None, path: str | None,
                 changed: dict | None):
    """The run's paths.Planner: spark, and the warehouse when given (cold unless it is running), the model refit from
    the paths log's merged runs, a forced path unless `path` is auto (or none), and each pair's change rows."""
    from . import paths
    engines, cold, units = {"spark": spark}, set(), {"spark": 1.0}
    if warehouse:
        from .engine import Warehouse
        wh = Warehouse(warehouse)
        engines["warehouse"] = wh
        try:
            if wh.state() != "RUNNING":
                cold.add("warehouse")
            units["warehouse"] = wh.units()
        except Exception:  # noqa: BLE001
            cold.add("warehouse")
    got = []
    for cols in ("target_table, fingerprint, ", "target_table, NULL AS fingerprint, "):   # a log from before fingerprints
        try:
            got = spark.sql(f"SELECT {cols}engine, k, sources, change_rows, seconds, at_once, units FROM "
                            f"{paths_log(catalog, target, log)} WHERE done = 'merged' ORDER BY at DESC "
                            "LIMIT 2000").collect()
            break
        except Exception:  # noqa: BLE001  no log yet (or one from before at_once and units): the measured defaults
            continue
    runs = [(r.target_table, r.fingerprint, r.engine, int(r.k), int(r.sources), int(r.change_rows or 0),
             float(r.seconds), int(r.at_once or 1), float(r.units or 1.0)) for r in got]
    pooled = paths.Model().fit([r[2:] for r in runs])
    forced = None if path in (None, "", "auto") else path
    return paths.Planner(engines, pooled, forced, changed if isinstance(changed, dict) else {}, cold, units,
                         tables=paths.table_models(runs, pooled))


def add_phases(planner, records: list[dict]) -> None:
    """Each record's warehouse statements' phases and bytes from query history (engine.Warehouse.phases), summed
    into it: compile, execute, queue, bytes and files read. Nothing without a warehouse or statements."""
    wh = planner.engines.get("warehouse")
    ids = [i for r in records for i in r.get("statements") or []]
    if wh is None or not ids or not hasattr(wh, "phases"):
        return
    got = wh.phases(ids)
    for r in records:
        ph = [got[i] for i in r.get("statements") or [] if i in got]
        for k in ("compile_s", "execute_s", "queue_s", "read_bytes", "read_files"):
            vals = [p[k] for p in ph if p.get(k) is not None]
            r[k] = round(sum(vals), 2) if vals else None


NEWER = ("fingerprint STRING, own_model BOOLEAN, insights STRING, statements STRING, compile_s DOUBLE, "
         "execute_s DOUBLE, queue_s DOUBLE, read_bytes BIGINT, read_files BIGINT, planning_s DOUBLE")


def write_paths(spark, catalog: str, target: str, log: str | None, run_id: str, records: list[dict]) -> None:
    """The planner's records into the paths log: each table's MERGE, its path, predicted and taken seconds."""
    if not records:
        return
    from .merge import _shared
    from .stack import spark_string
    fq = paths_log(catalog, target, log)
    _shared(spark, f"CREATE TABLE IF NOT EXISTS {fq} (run_id STRING, target_table STRING, engine STRING, k INT, "
                   "sources INT, change_rows BIGINT, predicted_s DOUBLE, seconds DOUBLE, retries INT, done STRING, "
                   f"forced BOOLEAN, predictions STRING, at TIMESTAMP, at_once INT, units DOUBLE, {NEWER}) COMMENT "
                   "'lfc_medallion: each MERGE, its path (engine, k at once), its predicted and taken seconds, the "
                   "tables merged at once and the engine units (paths.py)'")
    have = {r[0] for r in spark.sql(f"DESCRIBE TABLE {fq}").collect()}   # a log from before some columns
    missing = [c for c in ("at_once INT, units DOUBLE, " + NEWER).split(", ") if c.split()[0] not in have]
    if missing:   # only those: one ALTER of columns some of which exist fails as a whole
        spark.sql(f"ALTER TABLE {fq} ADD COLUMNS ({', '.join(missing)})")
    num = lambda v: "NULL" if v is None else str(v)  # noqa: E731
    vals = ", ".join(f"({spark_string(run_id)}, {spark_string(r['target_table'])}, {spark_string(r['engine'])}, "
                     f"{int(r['k'])}, {int(r['sources'])}, {int(r['change_rows'] or 0)}, {num(r['predicted_s'])}, "
                     f"{r['seconds']}, {int(r['retries'])}, {spark_string(r['done'])}, {str(r['forced']).lower()}, "
                     f"{spark_string(json.dumps(r['predictions']))}, current_timestamp(), "
                     f"{int(r.get('at_once') or 1)}, {float(r.get('units') or 1.0)}, "
                     f"{spark_string(r['fingerprint']) if r.get('fingerprint') else 'NULL'}, "
                     f"{str(bool(r.get('own_model'))).lower()}, {spark_string(json.dumps(r.get('insights') or []))}, "
                     f"{spark_string(json.dumps(r.get('statements') or []))}, {num(r.get('compile_s'))}, "
                     f"{num(r.get('execute_s'))}, {num(r.get('queue_s'))}, {num(r.get('read_bytes'))}, "
                     f"{num(r.get('read_files'))}, {num(r.get('planning_s'))})" for r in records)
    _shared(spark, f"INSERT INTO {fq} (run_id, target_table, engine, k, sources, change_rows, predicted_s, seconds, "
                   f"retries, done, forced, predictions, at, at_once, units, fingerprint, own_model, insights, "
                   f"statements, compile_s, execute_s, queue_s, read_bytes, read_files, planning_s) VALUES {vals}")


MARGIN_S = 120   # event-log times against the state's: a run's own rows count as changed; reading by time starts this
                 # far before the state's time, so every source flagged as changed has a commit after its start


def changed_since(spark, catalog: str, prefix: str, ctx: dict, events: list[str], name: str = "all_{table}",
                  margin_s: int = MARGIN_S) -> dict | None:
    """The (table, source) pairs whose bronze table got rows (upserted or deleted), or any completed snapshot (a full
    refresh, even of an empty table; snapshots run only in the first update and a refresh),
    after its state was saved, from ONE query over the ingestion pipelines' event logs (flow_progress per flow: a
    destination table's flow is named <catalog>_<prefix><source>_<table>_<cdc|snapshot>_flow); a pair with no state
    is changed. None when the logs can't be read: then every source is read."""
    flows = {}
    for x, tabs in ctx["held"].items():
        for t in tabs:
            for kind in ("cdc", "snapshot"):
                flows[f"{catalog}_{prefix}{x}_{t}_{kind}_flow"] = (t, x)
    times = [v[3] for v in ctx["state"].values() if len(v) > 3 and v[3] is not None]
    if not times:
        return None
    union = " UNION ALL ".join(
        f"SELECT origin.flow_name AS f, timestamp, details:flow_progress.metrics AS m, "
        f"details:flow_progress.status AS status FROM {e} "
        f"WHERE event_type = 'flow_progress'" for e in events)
    oldest = min(times)
    try:
        got = spark.sql(f"SELECT element_at(split(f, '\\\\.'), -1) AS flow, MAX(timestamp) AS at, "
                        "SUM(COALESCE(CAST(m:num_upserted_rows AS BIGINT), 0) + "
                        f"COALESCE(CAST(m:num_deleted_rows AS BIGINT), 0)) AS n FROM ({union}) "
                        f"WHERE timestamp > timestamp'{oldest}' - INTERVAL {int(margin_s)} SECONDS "
                        "AND (COALESCE(CAST(m:num_upserted_rows AS BIGINT), 0) > 0 "
                        "OR COALESCE(CAST(m:num_deleted_rows AS BIGINT), 0) > 0 "
                        "OR (endswith(f, '_snapshot_flow') AND status = 'COMPLETED')) GROUP BY 1").collect()
    except Exception:  # noqa: BLE001  an event log not there yet, no access: read every source
        return None
    out: dict = {}   # (table, source) -> its change rows since about its state (the planner's cardinality)
    ctx["refreshed"] = set()   # pairs whose snapshot flow ran: read by version (it detects the reset), not by time
    for r in got:
        pair = flows.get(r.flow)
        if not pair:
            continue
        if r.flow.endswith("_snapshot_flow"):
            ctx["refreshed"].add(pair)
        saved = ctx["state"].get((name.format(table=pair[0]), pair[1]))
        at = saved[3] if saved and len(saved) > 3 else None
        if at is None or (r.at.timestamp() if hasattr(r.at, "timestamp") else 0) > at.timestamp() - margin_s:
            out[pair] = out.get(pair, 0) + int(getattr(r, "n", 0) or 0)
    for x, tabs in ctx["held"].items():   # never merged: read it
        for t in tabs:
            if (name.format(table=t), x) not in ctx["state"]:
                out.setdefault((t, x), None)
    return out


def main(argv=None) -> None:
    """The job task: python_wheel_task (entry point lfc-medallion-merge) or spark_python_task."""
    ap = argparse.ArgumentParser(prog="lfc-medallion-merge")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--prefix", required=True, help="the sources' schemas: <prefix><source>")
    ap.add_argument("--target", required=True, help="the schema of the n:1 tables (the pipeline's)")
    ap.add_argument("--source-column", default="_source")
    ap.add_argument("--hints", default="", help='JSON: {"table": "role"}')
    ap.add_argument("--log", default="", help="the merge log's table, catalog.schema.table (default in --target)")
    ap.add_argument("--replan", action="store_true", help="plan every source again, not only new ones")
    ap.add_argument("--warehouse", default="", help="a SQL warehouse id: a path the optimizer may take (paths.py)")
    ap.add_argument("--path", default="auto", help="auto (by cost), or force one: spark, warehouse, warehouse:8 ...")
    ap.add_argument("--read-by", default="version", choices=("version", "time"),
                    help="time: an SCD type 1 source the event logs flag as changed is read from its saved time "
                         "(no history read per source); version: by Delta versions")
    ap.add_argument("--events", default="", help="the ingestion pipelines' event log tables, comma-separated: only the "
                                                  "sources whose bronze got rows since their state are read")
    ap.add_argument("--history-target", default="", help="a schema that keeps the SCD type 2 history of the tables "
                                                          "left on the pipeline (merge_all there)")
    a = ap.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(json.dumps(merge_route(spark, a.catalog, a.prefix, a.target, a.source_column,
                                 json.loads(a.hints) if a.hints else None, log=a.log or None,
                                 replan=a.replan, events=[e for e in a.events.split(",") if e] or None,
                                 history_target=a.history_target or None, warehouse=a.warehouse or None,
                                 path=a.path or None, read_by=a.read_by)))
