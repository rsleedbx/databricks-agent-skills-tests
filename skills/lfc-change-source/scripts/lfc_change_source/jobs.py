"""jobs: a notebook run once as a serverless job (jobs/runs/submit), its state, and its wait. api: an lfc_rest.Rest.
The workspace's Debezium Server (producer.WorkspaceDebeziumServer) and the JDBC snapshot (notebooks/jdbc_snapshot.py)
run this way.

    rid = jobs.submit(api, "jdbc-snapshot", "/Users/me/jdbc_snapshot", {"tables": "DBZ.CUSTOMERS", ...})
    jobs.wait(api, rid)            # {"life_cycle", "result", "url", "task_run_id"} once it has ended
"""
from __future__ import annotations

import time

ENDED = ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")


def submit(api, name: str, notebook: str, params: dict) -> int:
    """One run of the notebook with these widgets (strings), serverless (no cluster given). Its run id."""
    body = {"run_name": name, "tasks": [{"task_key": "notebook", "notebook_task": {
        "notebook_path": notebook, "base_parameters": {k: str(v) for k, v in params.items()}}}]}
    return api.post("/api/2.1/jobs/runs/submit", body)["run_id"]


def state(api, run_id: int) -> dict:
    r = api.get("/api/2.1/jobs/runs/get", run_id=run_id)
    return {"life_cycle": r["state"].get("life_cycle_state"), "result": r["state"].get("result_state"),
            "url": r.get("run_page_url"), "task_run_id": (r.get("tasks") or [{}])[0].get("run_id")}


def wait(api, run_id: int, timeout_s: int = 3600, sleep=time.sleep) -> dict:
    """The run's state once it has ended; TimeoutError after timeout_s."""
    end = time.time() + timeout_s
    while True:
        st = state(api, run_id)
        if st["life_cycle"] in ENDED:
            return st
        if time.time() >= end:
            raise TimeoutError(f"run {run_id} not ended after {timeout_s} s ({st['url']})")
        sleep(10)


def output(api, task_run_id: int) -> dict:
    """The notebook's exit value and error, if any: {"notebook_output", "error", "error_trace"}."""
    o = api.get("/api/2.1/jobs/runs/get-output", run_id=task_run_id)
    return {k: o.get(k) for k in ("notebook_output", "error", "error_trace") if o.get(k)}


def upload_notebook(api, path: str, source: str) -> str:
    """A Python notebook's source (Databricks format) imported at path, the folder made. path."""
    import base64
    import posixpath
    api.post("/api/2.0/workspace/mkdirs", {"path": posixpath.dirname(path)})
    api.post("/api/2.0/workspace/import", {"path": path, "format": "SOURCE", "language": "PYTHON", "overwrite": True,
                                           "content": base64.b64encode(source.encode()).decode()})
    return path
