"""pipeline: a Lakeflow pipeline created or updated by name, and one update of it run and waited for (the workspace
client given). Used by the snapshot methods (qbc) and lfc-fanin's silver."""
from __future__ import annotations

import time


def upsert_pipeline(w, spec: dict) -> str:
    """The pipeline named spec["name"]: created, or its spec put when one has that name. Its id."""
    flt = "name LIKE '" + spec["name"].replace("'", "''") + "'"
    have = [x.pipeline_id for x in w.pipelines.list_pipelines(filter=flt) if x.name == spec["name"]]
    if have:
        w.api_client.do("PUT", f"/api/2.0/pipelines/{have[0]}", body=spec)
        return have[0]
    return w.api_client.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]


def run_pipeline(w, pid: str, timeout: int = 3600) -> str:
    """One update of a triggered pipeline, waited for: its final state (an update still running from before, e.g.
    on an older spec, is stopped first)."""
    if w.pipelines.get(pid).state.value == "RUNNING":
        w.pipelines.stop_and_wait(pid)
    u = w.pipelines.start_update(pid).update_id
    t0 = time.time()
    while True:
        st = w.pipelines.get_update(pid, u).update.state.value
        if st in ("COMPLETED", "FAILED", "CANCELED") or time.time() - t0 > timeout:
            return st
        time.sleep(15)
