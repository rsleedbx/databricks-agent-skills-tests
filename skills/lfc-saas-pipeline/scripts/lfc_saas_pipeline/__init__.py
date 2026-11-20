"""The helper cells of the Lakeflow Connect SaaS pipeline notebooks, as a package: install it in the notebook and
keep only the spec cell.

The docs' notebooks (for example docs.databricks.com/aws/en/ingestion/lakeflow-connect/1password-event-logs-pipeline)
define these functions in two cells marked "Do NOT modify". Same names, same arguments:

    create_pipeline(pipeline_definition)            POST   /api/2.0/pipelines
    edit_pipeline(id, pipeline_definition)          PUT    /api/2.0/pipelines/{id}
    delete_pipeline(id)                             DELETE /api/2.0/pipelines/{id}
    list_pipeline(filter)                           GET    /api/2.0/pipelines
    get_pipeline(id)                                GET    /api/2.0/pipelines/{id}
    start_pipeline(id, full_refresh=False)          POST   /api/2.0/pipelines/{id}/updates

What differs: a pipeline definition may be a JSON string (as in the docs) or a dict; each call prints the response
as the docs' cells do and also returns it (a dict), and raises on an error instead of printing it; create_pipeline
gives the workspace group `users` CAN_VIEW on the new pipeline (viewers=() for none); start_pipeline takes
validate_only and refresh_selection too, and wait_update() waits for an update's end. Authentication is the
Databricks SDK's: in a notebook it signs in by itself, elsewhere it uses DATABRICKS_CONFIG_PROFILE or the
environment, so no token is handled here. arg(name, default) reads a job parameter (a notebook widget) when one is
set, so one notebook runs from the UI and as a job.
"""
from __future__ import annotations

import json
import time
from typing import Any

API = "/api/2.0/pipelines"
DONE = ("COMPLETED", "FAILED", "CANCELED")
_client = None


def client():
    """The workspace client (created once): the notebook's own login, or the SDK's default sign-in elsewhere."""
    global _client
    if _client is None:
        from databricks.sdk import WorkspaceClient

        _client = WorkspaceClient()
    return _client


def _call(method: str, path: str, body: dict | None = None, query: dict | None = None) -> dict:
    return client().api_client.do(method, path, body=body, query=query) or {}


def _definition(pipeline_definition: str | dict) -> dict:
    return json.loads(pipeline_definition) if isinstance(pipeline_definition, str) else dict(pipeline_definition)


def _show(response: dict) -> dict:
    print("Response from API:\n{}".format(json.dumps(response, indent=2, sort_keys=False)))
    return response


def create_pipeline(pipeline_definition: str | dict, viewers: tuple[str, ...] = ("users",)) -> dict:
    """Create the pipeline; give each group in viewers CAN_VIEW on it. Returns {pipeline_id, ...}."""
    response = _call("POST", API, _definition(pipeline_definition))
    if viewers and response.get("pipeline_id"):
        grant_view(response["pipeline_id"], viewers)
    return _show(response)


def grant_view(pipeline_id: str, groups: tuple[str, ...] = ("users",)) -> dict:
    """CAN_VIEW for each group on a pipeline, added to its permissions (none replaced)."""
    acl = [{"group_name": g, "permission_level": "CAN_VIEW"} for g in groups]
    return _call("PATCH", f"/api/2.0/permissions/pipelines/{pipeline_id}", {"access_control_list": acl})


def edit_pipeline(id: str, pipeline_definition: str | dict) -> dict:  # noqa: A002  (the docs' argument name)
    return _show(_call("PUT", f"{API}/{id}", _definition(pipeline_definition)))


def delete_pipeline(id: str) -> dict:  # noqa: A002
    return _show(_call("DELETE", f"{API}/{id}"))


def list_pipeline(filter: str = "") -> dict:  # noqa: A002
    return _show(_call("GET", API, query={"filter": filter} if filter else None))


def get_pipeline(id: str) -> dict:  # noqa: A002
    return _show(_call("GET", f"{API}/{id}"))


def start_pipeline(id: str, full_refresh: bool = False, validate_only: bool = False,  # noqa: A002
                   refresh_selection: list[str] | None = None) -> dict:
    """Start an update. Returns {update_id}. refresh_selection: only these tables (destination names)."""
    body: dict[str, Any] = {"full_refresh": full_refresh, "validate_only": validate_only, "cause": "API_CALL"}
    if refresh_selection:
        body["refresh_selection"] = list(refresh_selection)
    return _show(_call("POST", f"{API}/{id}/updates", body))


def wait_update(id: str, update_id: str, timeout_s: int = 3600, poll_s: int = 15,  # noqa: A002
                sleep=time.sleep) -> str:
    """Wait for an update to end; its final state (COMPLETED, FAILED, CANCELED), or the last one seen at timeout."""
    state, waited = "", 0
    while waited <= timeout_s:
        state = (_call("GET", f"{API}/{id}/updates/{update_id}").get("update") or {}).get("state", "")
        if state in DONE:
            return state
        sleep(poll_s)
        waited += poll_s
    return state


def arg(name: str, default: str) -> str:
    """A job parameter or notebook widget of this name when set (and not empty), else default."""
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils.widgets.get(name) or default
    except Exception:  # noqa: BLE001  no widget of that name, or not in a notebook
        return default
