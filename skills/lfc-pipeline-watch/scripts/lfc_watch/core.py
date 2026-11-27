"""check and watch (package doc). The pieces are separate so a caller can take one: flows (the events), staging (the
volume), check (both, one reading), watch (checks until ready, failed or stalled)."""
from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest, not_found

DONE = ("COMPLETED",)                       # a snapshot flow is done
UP = ("RUNNING", "COMPLETED")               # a cdc flow captures; an update runs
BAD = ("FAILED", "CANCELED", "STOPPED")
FLOW = re.compile(r"(?:^|[._])(?P<t>[^._]+(?:_[^._]+)*?)_(?P<k>snapshot|cdc)_flow$")


def ms(ts) -> float:
    """An event timestamp (ISO text, or a number of ms) as epoch ms."""
    if isinstance(ts, (int, float)):
        return float(ts)
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000


def events(api, pipeline_id: str, since_ms: float = 0, max_events: int = 1000) -> list:
    """The pipeline's events since since_ms, newest first, with their details (the REST answer)."""
    out = []
    for e in Rest.of(api).pages(f"/api/2.0/pipelines/{pipeline_id}/events", "events", max_events, max_results=250):
        if since_ms and e.get("timestamp") and ms(e["timestamp"]) < since_ms:
            break
        out.append(e)
    return out


def table_of(flow: str, tables=None):
    """(table, "snapshot" | "cdc") of a flow name, or None: `<c>.<s>.<t>_cdc_flow`, `extractor_<...>_<t>_snapshot_flow`,
    `[applier_]<c>_<s>_<t>_{snapshot|cdc}_flow`. With tables, the longest one the name ends with (a name with _ in it)."""
    m = re.search(r"_(snapshot|cdc)_flow$", flow or "")
    if not m:
        return None
    head = flow[:m.start()]
    if tables:
        hit = [t for t in tables if head.lower() == t.lower() or re.search(rf"[._]{re.escape(t.lower())}$", head.lower())]
        return (max(hit, key=len), m.group(1)) if hit else None
    return re.split(r"[._]", head)[-1], m.group(1)


def flows(evs: list, tables=None) -> tuple:
    """({table: {"snapshot": state, "cdc": state}}, [(table or None, error)]) from events newest first: each flow's
    newest state; the ERROR events, the pipeline's own with table None."""
    state: dict = {}
    errs = []
    for e in evs:
        fl = (e.get("origin") or {}).get("flow_name") or ""
        tk = table_of(fl, tables)
        if e.get("event_type") == "flow_progress" and tk:
            st = ((e.get("details") or {}).get("flow_progress") or {}).get("status")
            state.setdefault(tk[0], {}).setdefault(tk[1], st)
        if e.get("level") == "ERROR":
            ex = ((e.get("error") or {}).get("exceptions") or [{}])[0]
            msg = (ex.get("message") or e.get("message") or "").strip().split("\n")[0][:400]
            if not tk:   # a flow's error named only in the message
                m = re.search(r"[\w.-]+_(?:cdc|snapshot)_flow", e.get("message") or "")
                tk = table_of(m.group(0), tables) if m else None
            errs.append((tk[0] if tk else None, msg))
    return state, errs


def staging(api, root: str, cap: int = 5000) -> dict:
    """{"files", "bytes"} under a staging folder (a gateway's), up to `cap` entries; {} when it isn't there yet."""
    api, files, size, seen, todo = Rest.of(api), 0, 0, 0, [root]
    try:
        while todo and seen < cap:
            for d in api.ls(todo.pop()):
                seen += 1
                if d.get("is_directory"):
                    todo.append(d["path"].rstrip("/"))
                else:
                    files += 1
                    size += int(d.get("file_size") or 0)
    except Exception as e:
        if not_found(e):   # not written yet
            return {}
        raise
    return {"files": files, "bytes": size, **({"capped": True} if seen >= cap else {})}


def staging_root(api, pipeline_id: str):
    """A gateway's staging folder under /Volumes, or None for a pipeline that isn't a gateway."""
    gd = (Rest.of(api).get(f"/api/2.0/pipelines/{pipeline_id}").get("spec") or {}).get("gateway_definition")
    if not gd:
        return None
    return (f"/Volumes/{gd['gateway_storage_catalog']}/{gd['gateway_storage_schema']}/"
            f"__databricks_ingestion_gateway_staging_data-{pipeline_id}/{pipeline_id}")


@dataclass
class Status:
    update: str | None
    tables: dict = field(default_factory=dict)       # {table: {"snapshot": state, "cdc": state}}
    errors: list = field(default_factory=list)       # [(table or None, message)]
    staging: dict = field(default_factory=dict)      # {"files", "bytes"}
    wanted: tuple = ()

    def own_error(self):
        """The pipeline's own error (no table): its update can't run, so waiting won't help."""
        return next((m for t, m in self.errors if t is None), None)

    def failed(self) -> dict:
        """{table or "_pipeline": error}."""
        out = {(t or "_pipeline"): m for t, m in reversed(self.errors)}
        for t, f in self.tables.items():
            if any(s in BAD for s in f.values()):
                out.setdefault(t, f"flow {f}")
        return out

    def waiting(self) -> list:
        """The tables not ready: snapshot not COMPLETED or cdc not RUNNING (of `wanted`, else of those seen)."""
        names = self.wanted or tuple(self.tables)
        return [t for t in names if not (self.tables.get(t, {}).get("snapshot") in DONE
                                         and self.tables.get(t, {}).get("cdc") in UP)]

    def ready(self) -> bool:
        return bool(self.wanted or self.tables) and not self.waiting() and not self.failed()

    def line(self) -> str:
        st = self.staging
        stg = f" staging {st['files']} files {st['bytes']:,} B" if st else ""
        names = self.wanted or tuple(self.tables)
        f = self.failed()
        return (f"update {self.update}; ready {len(names) - len(self.waiting())}/{len(names)}"
                f"{'; waiting ' + ','.join(self.waiting()) if self.waiting() else ''}"
                f"{'; failed ' + '; '.join(f'{k}: {v[:120]}' for k, v in f.items()) if f else ''}{stg}")


def check(api, pipeline_id: str, since_ms: float = 0, tables=None, root: str | None = "auto") -> Status:
    """One reading (package doc). api: a Rest (lfc_rest) or a WorkspaceClient. root: the staging folder, "auto" (a
    gateway's, from its definition) or None (skip)."""
    api = Rest.of(api)
    upd = ((api.get(f"/api/2.0/pipelines/{pipeline_id}/updates", max_results=1).get("updates") or [{}])[0]).get("state")
    state, errs = flows(events(api, pipeline_id, since_ms), tables)
    s = Status(upd, state, errs, {}, tuple(tables or ()))
    if root == "auto":   # staging growth is the "still working" sign while a table waits: listing it after is wasted
        root = staging_root(api, pipeline_id) if s.waiting() else None
    s.staging = staging(api, root) if root else {}
    return s


def watch(api, pipeline_id: str, since_ms: float = 0, tables=None, every: float = 15, stall_s: float = 900,
          max_s: float = 3600, until: str = "ready", out=None, sleep=time.sleep, clock=time.time) -> Status:
    """Checks every `every` s (15: a small table is ready within a poll or two), one line each, until: "ready" (every table snapshotted and capturing), "up" (the update
    runs), the pipeline's own error (raises RuntimeError), stall_s with nothing changed (no flow state, no staging
    growth: TimeoutError), or max_s. A table's error doesn't stop it (the gateway retries); it shows in the line and in
    the Status returned."""
    out, api = out or sys.stdout, Rest.of(api)
    t0, last, changed = clock(), None, clock()
    while True:
        s = check(api, pipeline_id, since_ms, tables)
        print(f"[{int(clock() - t0):>5}s] {s.line()}", file=out, flush=True)
        if s.own_error():
            raise RuntimeError(f"pipeline {pipeline_id}: {s.own_error()}")
        if (until == "up" and s.update in UP) or (until == "ready" and s.ready()):
            return s
        sig = (s.update, repr(sorted(s.tables.items())), s.staging.get("files"), s.staging.get("bytes"))
        if sig != last:
            last, changed = sig, clock()
        elif clock() - changed > stall_s:
            raise TimeoutError(f"pipeline {pipeline_id}: nothing changed for {int(stall_s)} s: {s.line()}")
        if clock() - t0 > max_s:
            raise TimeoutError(f"pipeline {pipeline_id}: not {until} after {int(max_s)} s: {s.line()}")
        sleep(every)
