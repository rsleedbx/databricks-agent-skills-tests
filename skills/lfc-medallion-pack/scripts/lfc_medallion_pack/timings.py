"""timings: how long each step of a module took, at its scale, recorded so a bigger deployment can be predicted.

    lfc-medallion-pack timings --plan source/plan.json --module scale --run <refresh job run id> \\
        [--source-run <setup_source run id>] [--deploy-seconds 28] [--note "..."] --out runs.jsonl
    lfc-medallion-pack predict runs.jsonl --tables 4000

One JSON line per step: the module's scale (lobs, tables, pipelines), the step (source, deploy, ingest_<k>,
transform, verify, gateway_explore), its seconds, and for a pipeline update its phases (seconds in each state:
WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING) and how many flows it defined. A record names no
workspace, host, id or user: it can go in a public repo. Read from the Jobs and Pipelines REST APIs through the
Databricks CLI (`databricks api get`, your profile), so no token is handled, after the run.

predict fits each step's seconds to a line in tables (least squares over the records of that step) and gives the
seconds at the tables asked; with one record of a step it scales that one in proportion. A step whose time doesn't
grow with tables (deploy, verify's install) comes out flat.
"""
from __future__ import annotations

import datetime as dt
import json
import re

PHASES = ("WAITING_FOR_RESOURCES", "INITIALIZING", "SETTING_UP_TABLES", "RUNNING", "COMPLETED", "FAILED", "CANCELED")
EXPLORED = re.compile(r"Schema Exploration has COMPLETED in ([\d.]+) seconds\. Found (\d+) table")


def _ts(s: str) -> float:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def scale(plan: dict, module: str, lobs: int | None = None) -> dict:
    """The module's size: LOBs, tables, pipelines; lobs: the size a past run had (the plan grew since), its
    pipelines as plan.py would have grouped them."""
    import math
    m = next(x for x in plan["modules"] if x["module"] == module)
    n, per_lob = lobs or len(m["lobs"]), len(m["tables"])
    per = max(1, int(m.get("tables_per_pipeline") or plan["limits"]["tables_per_pipeline"]) // per_lob)
    groups = [min(per, n - k * per) * per_lob for k in range(math.ceil(n / per))]
    return {"module": module, "pack": m["pack"], "data": m["data"], "lobs": n, "tables": n * per_lob,
            "pipelines": len(groups), "tables_per_pipeline": groups[0], "group_tables": groups}


class Api:
    """GET through the Databricks CLI: databricks api get <path> [--profile p], JSON back."""

    def __init__(self, profile: str | None = None):
        self.profile = profile

    def get(self, path: str, **query) -> dict:
        import subprocess
        import urllib.parse
        q = ("?" + urllib.parse.urlencode(query)) if query else ""
        cmd = ["databricks", "api", "get", path + q] + (["--profile", self.profile] if self.profile else [])
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(f"{' '.join(cmd[:4])}: {r.stderr.strip()[:300]}")
        return json.loads(r.stdout or "{}")

    def events(self, pipeline_id: str):
        """A pipeline's events, newest first, page by page."""
        token = None
        while True:
            q = {"max_results": 250} | ({"page_token": token} if token else {})
            d = self.get(f"/api/2.0/pipelines/{pipeline_id}/events", **q)
            yield from d.get("events", [])
            token = d.get("next_page_token")
            if not token:
                return


def update_phases(api: Api, pipeline_id: str, start_ms: int, end_ms: int) -> dict:
    """The pipeline update a job task ran (created in the task's window): seconds per state, flows defined."""
    ups = [u for u in api.get(f"/api/2.0/pipelines/{pipeline_id}/updates", max_results=25).get("updates", [])
           if start_ms - 60_000 <= int(u.get("creation_time", 0)) <= end_ms]
    if not ups:
        return {}
    u = min(ups, key=lambda x: int(x["creation_time"]))
    ev = []
    for e in api.events(pipeline_id):   # newest first: stop past its start
        if _ts(e["timestamp"]) * 1000 < int(u["creation_time"]) - 60_000:
            break
        if (e.get("origin") or {}).get("update_id") == u["update_id"]:
            ev.append(e)
    marks, flows = [], 0
    for e in ev:
        if e.get("event_type") == "update_progress":
            st = next((p for p in PHASES if f"is {p}" in (e.get("message") or "")), None)
            if st:
                marks.append((_ts(e["timestamp"]), st))
        elif e.get("event_type") == "flow_definition":
            flows += 1
    marks.sort()
    phases = {}
    for (t0, st), (t1, _) in zip(marks, marks[1:]):
        phases[st] = round(phases.get(st, 0) + t1 - t0, 1)
    return {"phases": phases, "flows": flows, "state": marks[-1][1] if marks else None}


def explored(api: Api, pipeline_id: str, until_ms: int) -> dict | None:
    """A gateway's last schema exploration before until_ms (the run's end): its seconds and the tables it found."""
    for e in api.events(pipeline_id):
        if _ts(e["timestamp"]) * 1000 > until_ms:
            continue
        m = EXPLORED.search(e.get("message") or "")
        if m:
            return {"seconds": float(m.group(1)), "found_tables": int(m.group(2)), "at": e["timestamp"][:19]}
    return None


def _task_row(t: dict) -> dict:
    return {"step": t["task_key"], "seconds": round((t.get("end_time", 0) - t.get("start_time", 0)) / 1000),
            "result": (t.get("state") or {}).get("result_state", "")}


def record(api: Api, plan: dict, module: str, run_id: int, source_run: int | None = None,
           deploy_seconds: float | None = None, note: str = "", lobs: int | None = None,
           kind: str = "", task_kinds: dict[str, str] | None = None) -> list[dict]:
    """kind tags the run: first (each pipeline's first snapshot), steady (no new tables), grown (tables added);
    task_kinds overrides it per task ({"ingest_1": "grown"})."""
    sc = scale(plan, module, lobs)
    groups = sc.pop("group_tables")
    base = sc | {"date": dt.date.today().isoformat(), "kind": kind, "note": note}
    out = []
    if source_run:
        t = api.get("/api/2.2/jobs/runs/get", run_id=source_run)["tasks"][0]
        out.append(base | _task_row(t) | {"step": "source"})
    if deploy_seconds is not None:
        out.append(base | {"step": "deploy", "seconds": deploy_seconds})
    run = api.get("/api/2.2/jobs/runs/get", run_id=run_id)
    gateways = {}
    for t in sorted(run["tasks"], key=lambda x: x.get("start_time", 0)):
        row = base | _task_row(t)
        if task_kinds and t["task_key"] in task_kinds:
            row["kind"] = task_kinds[t["task_key"]]
        k = re.fullmatch(r"ingest_(\d+)", t["task_key"])
        if k and int(k.group(1)) <= len(groups):
            row["pipeline_tables"] = groups[int(k.group(1)) - 1]
        pid = (t.get("pipeline_task") or {}).get("pipeline_id")
        if pid:
            row |= update_phases(api, pid, t["start_time"], t["end_time"])
            gw = ((api.get(f"/api/2.0/pipelines/{pid}").get("spec") or {}).get("ingestion_definition") or {})
            if gw.get("ingestion_gateway_id") and k:
                gateways[int(k.group(1))] = gw["ingestion_gateway_id"]
        out.append(row)
    for k, g in sorted(gateways.items()):
        e = explored(api, g, run.get("end_time", 0))
        if e:
            out.append(base | {"step": f"gateway_explore_{k}", "pipeline_tables": groups[k - 1]} | e)
    return out


def predict(rows: list[dict], tables: int) -> dict[str, dict]:
    """Each step's seconds at `tables` (a module's tables; for an ingest step, one pipeline's): a least-squares line
    over that step's successful records at two or more sizes (an ingest step: of the same kind; data and no-data
    apart), else the one size's own seconds, unscaled (`fit: none`)."""
    by: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        if r.get("result") not in (None, "", "SUCCESS"):
            continue
        step = re.sub(r"_\d+$", "", r["step"])
        step += (f" {r['kind']}" if r.get("kind") and step == "ingest" else "") + (" data" if r.get("data") else "")
        x = r.get("pipeline_tables") or r.get("tables")   # an ingestion pipeline: its own tables
        if "seconds" in r and x:
            by.setdefault(step, []).append((float(x), float(r["seconds"])))
    out = {}
    for step, pts in sorted(by.items()):
        sizes = sorted({int(x) for x, _ in pts})
        if len(sizes) < 2:
            out[step] = {"seconds": round(sum(y for _, y in pts) / len(pts), 1), "fit": "none", "from_tables": sizes}
            continue
        n = len(pts)
        mx, my = sum(x for x, _ in pts) / n, sum(y for _, y in pts) / n
        b = sum((x - mx) * (y - my) for x, y in pts) / sum((x - mx) ** 2 for x, _ in pts)
        out[step] = {"seconds": round(max(my + b * (tables - mx), 0), 1), "fit": "line",
                     "per_table": round(b, 3), "from_tables": sizes, "records": n}
    return out


def report(rows: list[dict]) -> str:
    """The records as markdown: one table row per step of each run (in the file's order), with its scale."""
    def ph(r: dict) -> str:
        p = r.get("phases") or {}
        short = {"WAITING_FOR_RESOURCES": "wait", "INITIALIZING": "init", "SETTING_UP_TABLES": "setup",
                 "RUNNING": "run"}
        return ", ".join(f"{short.get(k, k.lower())} {round(v)}" for k, v in p.items() if k in short)
    lines = ["| date | module | data | LOBs | tables | pipelines (tables each) | step | seconds | phases (s) | flows "
             "| note |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    last = None
    for r in rows:
        key = (r["date"], r["module"], r["lobs"], r.get("note"))
        note = r.get("note", "") if key != last else ""
        last = key
        found = f" ({r['found_tables']} tables found)" if "found_tables" in r else ""
        own = f", its {r['pipeline_tables']}" if "pipeline_tables" in r else ""
        lines.append(f"| {r['date']} | {r['module']} | {'yes' if r.get('data') else 'no'} | {r['lobs']} | "
                     f"{r['tables']} | {r['pipelines']} ({r['tables_per_pipeline']}) | {r['step']}{own}{found} | "
                     f"{r.get('seconds', '')} | {ph(r)} | {r.get('flows', '')} | {note} |")
    return "\n".join(lines) + "\n"
