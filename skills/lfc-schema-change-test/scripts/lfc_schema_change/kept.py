"""kept: a finished run's pipelines and schemas, kept KEEP_DAYS days (stopped: no cost) or deleted now.

Each kept run is one JSON line in LFC_KEPT (~/.local/state/lfc-tests/kept.jsonl), in the record lib.sh writes, so
kept-sweep.sh deletes either kind after its delete_after date. One writer at a time: the same mkdir lock as lib.sh
(flock isn't on macOS); a lock older than 10 minutes is taken over (a writer that died).
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import shutil
import time
from pathlib import Path

from lfc_change_source import pipeline

KEPT = os.environ.get("LFC_KEPT") or str(Path.home() / ".local/state/lfc-tests/kept.jsonl")


@contextlib.contextmanager
def lock(path: str = KEPT, take_over_s: float = 600, sleep=time.sleep):
    d = Path(f"{path}.lock")
    d.parent.mkdir(parents=True, exist_ok=True)
    waited = 0.0
    while True:
        try:
            d.mkdir()
            break
        except FileExistsError:
            if waited >= take_over_s:
                shutil.rmtree(d, ignore_errors=True)
                waited = 0.0
            sleep(1)
            waited += 1
    try:
        yield
    finally:
        shutil.rmtree(d, ignore_errors=True)


def record(run, stopped: bool, slot_dropped: str = "", days: int = 14, today: dt.date | None = None) -> dict:
    """The line kept-sweep.sh reads (the same keys and kinds as lib.sh's keep_run)."""
    after = (today or dt.datetime.now(dt.timezone.utc).date()) + dt.timedelta(days=days)
    return {"id": run.schema, "kept": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "delete_after": after.isoformat(), "host": run.host, "profile": run.profile, "catalog": run.catalog,
            "schema": run.schema, "staging": run.staging, "pipelines": [p for p in (run.ap, run.gw) if p],
            "stopped": stopped, "engine": run.engine, "db": run.db.name if run.src_owned else "", "src_db": run.db.name,
            "slot": run.slot, "slot_dropped": slot_dropped, "connection": run.connection, "conn_user": run.conn_user,
            "src_sql": run.src.cmd, "source_json": run.source_json, "keep_source": "1" if run.keep_source else "0",
            "extra_pipelines": list(run.extra_pipelines), "extra_schemas": list(run.extra_schemas),
            "extra_sql": run.extra_sql}


def keep(run, days: int, path: str = KEPT, log=print) -> dict:
    """Stop the pipelines (a continuous gateway too) and record the run."""
    stopped = True
    for p in [run.ap, run.gw, *run.extra_pipelines]:
        if not p:
            continue
        try:
            pipeline.stop(run.api, p)
        except Exception:  # noqa: BLE001  said, and recorded as stopped: false
            stopped = False
            log(f"WARNING: could not stop pipeline {p}: it may still be running")
    rec = record(run, stopped, run.drop_slot_for_keep(), days)
    with lock(path), open(path, "a") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    extra = f" {' '.join(run.extra_schemas)}" if run.extra_schemas else ""
    log(f"kept until {rec['delete_after']} ({'stopped' if stopped else 'STOP FAILED'}; delete earlier: "
        f"kept-sweep.sh --now {run.schema}): {run.host}/pipelines/{run.ap} {f'gateway {run.gw}, ' if run.gw else ''}"
        f"schemas {run.catalog}.{run.schema}{f' and {run.staging}' if run.staging else ''}{extra}, source database {run.db.name}")
    return rec


def delete(run, wait: float = 15, log=print) -> dict:
    """Delete the pipelines, schemas and extras, and the source database when this run made it. What's left."""
    for p in [run.ap, run.gw]:
        if p:
            with contextlib.suppress(Exception):
                pipeline.delete(run.api, p)
    time.sleep(wait)
    for p in run.extra_pipelines:
        with contextlib.suppress(Exception):
            pipeline.delete(run.api, p)
    for x in [f"{run.catalog}.{run.schema}", f"{run.catalog}.{run.staging}" if run.staging else "", *run.extra_schemas]:
        if x:
            run.delete_schema(x)
    left = run.drop_source()
    log(f"cleanup: pipelines and schemas deleted; source database "
        f"{'kept (KEEP_SOURCE=1)' if run.keep_source else f'left: {left}'}")
    return {"db_left": left}
