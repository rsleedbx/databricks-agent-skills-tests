"""pipeline: a Lakeflow pipeline created or updated by name, and one update of it run and waited for (the workspace
client given). Used by the snapshot methods (qbc) and lfc-fanin's silver."""
from __future__ import annotations

import time


def find(api, name: str) -> list:
    """The ids of the pipelines named exactly `name`."""
    flt = "name LIKE '" + name.replace("'", "''") + "'"
    return [x["pipeline_id"] for x in api.pages("/api/2.0/pipelines", "statuses", filter=flt) if x.get("name") == name]


def upsert_pipeline(w, spec: dict) -> str:
    """The pipeline named spec["name"]: created, or its spec put when one has that name. Its id. w: an lfc_rest.Rest
    or a WorkspaceClient."""
    from ._rest import Rest
    api = Rest.of(w)
    have = find(api, spec["name"])
    if have:
        api.put(f"/api/2.0/pipelines/{have[0]}", spec)
        return have[0]
    pid = api.post("/api/2.0/pipelines", spec)["pipeline_id"]
    grant_view(api, pid)
    return pid


def grant_view(api, pipeline_id: str, group: str = "users") -> None:
    """CAN_VIEW for the workspace's users on a new pipeline, so anyone can follow its runs and events (a PATCH: adds,
    replaces nothing)."""
    api.patch(f"/api/2.0/permissions/pipelines/{pipeline_id}",
              {"access_control_list": [{"group_name": group, "permission_level": "CAN_VIEW"}]})


def run_pipeline(w, pid: str, timeout: int = 3600, full_refresh: bool = False) -> str:
    """One update of a triggered pipeline, waited for: its final state (an update still running from before, e.g.
    on an older spec, is stopped first). full_refresh: every table rebuilt (a snapshot taken again from scratch)."""
    from ._rest import Rest
    api = Rest.of(w)
    if api.get(f"/api/2.0/pipelines/{pid}").get("state") == "RUNNING":   # no state: created and never run
        api.post(f"/api/2.0/pipelines/{pid}/stop")
        api.poll_until(lambda: api.get(f"/api/2.0/pipelines/{pid}"), lambda p: p.get("state") in ("IDLE", "FAILED"),
                       every=10, timeout=900)
    u = api.post(f"/api/2.0/pipelines/{pid}/updates", {"full_refresh": True} if full_refresh else {})["update_id"]
    t0 = time.time()
    while True:
        st = ((api.get(f"/api/2.0/pipelines/{pid}/updates/{u}").get("update")) or {}).get("state")
        if st in ("COMPLETED", "FAILED", "CANCELED") or time.time() - t0 > timeout:
            return st
        time.sleep(15)


# one pipeline's lifecycle, as REST calls (api: an lfc_rest.Rest); the callers above and the scenario harnesses use these
def create(api, spec: dict, view_group: str | None = "users") -> str:
    """A new pipeline (spec: a /api/2.0/pipelines body); its id. view_group gets CAN_VIEW (None: no grant)."""
    pid = api.post("/api/2.0/pipelines", spec)["pipeline_id"]
    if view_group:
        try:
            grant_view(api, pid, view_group)
        except Exception as e:  # noqa: BLE001  the pipeline exists: its id must reach the caller (to clean it up)
            import sys
            print(f"pipeline {pid}: CAN_VIEW for {view_group} not granted: {str(e)[:200]}", file=sys.stderr)
    return pid


def start_update(api, pid: str, refresh: list | None = None, full_refresh: bool = False) -> str:
    """An update started (refresh: the tables to fully refresh); its id."""
    body = {"full_refresh_selection": list(refresh)} if refresh else ({"full_refresh": True} if full_refresh else {})
    return api.post(f"/api/2.0/pipelines/{pid}/updates", body)["update_id"]


def get_update(api, pid: str, update_id: str) -> dict:
    """The update's state, cause, ... ({} when it has none yet)."""
    return api.get(f"/api/2.0/pipelines/{pid}/updates/{update_id}").get("update") or {}


def newest_update(api, pid: str) -> str:
    """The id of the pipeline's latest update ("" when it has none)."""
    ups = api.get(f"/api/2.0/pipelines/{pid}/updates", max_results=1).get("updates") or []
    return ups[0].get("update_id", "") if ups else ""


def stop(api, pid: str) -> None:
    api.post(f"/api/2.0/pipelines/{pid}/stop")


def delete(api, pid: str) -> None:
    api.delete(f"/api/2.0/pipelines/{pid}")
