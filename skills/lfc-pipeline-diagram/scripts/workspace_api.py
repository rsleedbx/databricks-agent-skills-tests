"""workspace_api.py: read the Databricks REST API, the JSON back as the API returns it, for pipeline-diagram.py and
medallion.py.

Through the Databricks Python SDK when it is installed: in a notebook (and Genie Code) it uses the notebook's own
login, where the Databricks CLI doesn't run; on a laptop, the CLI profile given. Without the SDK (a stock python3), the
same paths through `databricks api get`.
"""
import json
import subprocess
from urllib.parse import urlencode


class ApiError(Exception):
    """A request the workspace refused or couldn't answer; str() is its first line."""


_clients = {}


def _sdk(profile):
    """The SDK's client for profile, None without the SDK; a bad profile or no login is an ApiError."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        return None
    if profile not in _clients:
        try:
            _clients[profile] = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        except Exception as e:                              # the SDK resolves the login here (ValueError)
            raise ApiError((str(e).strip().splitlines() or ["no Databricks login"])[0]) from e
    return _clients[profile]


def get(path, profile=None, query=None):
    """GET path (with query, a dict) and return the decoded JSON ({} for an empty body)."""
    url = path + ("?" + urlencode(query) if query else "")
    w = _sdk(profile)
    if w is None:
        cmd = ["databricks", "api", "get", url, "-o", "json"] + (["--profile", profile] if profile else [])
        try:
            r = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise ApiError(((e.stderr or "").strip().splitlines() or [f"databricks api get {path} failed"])[0]) from e
        return json.loads(r.stdout or "null") or {}
    try:
        return w.api_client.do("GET", url) or {}
    except Exception as e:                                  # the SDK's DatabricksError and its subclasses
        raise ApiError((str(e).strip().splitlines() or [f"GET {path} failed"])[0]) from e


def host(profile=None):
    """The workspace URL."""
    w = _sdk(profile)
    if w is not None:
        return w.config.host or ""
    cmd = ["databricks", "auth", "describe", "-o", "json"] + (["--profile", profile] if profile else [])
    r = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return (json.loads(r.stdout or "{}").get("details") or {}).get("host", "") if r.returncode == 0 else ""


def tables(catalog, schema, profile=None):
    """Every table in catalog.schema, all pages."""
    out, token = [], None
    while True:
        q = {"catalog_name": catalog, "schema_name": schema, "max_results": 1000}
        if token:
            q["page_token"] = token
        page = get("/api/2.1/unity-catalog/tables", profile, q)
        out += page.get("tables") or []
        token = page.get("next_page_token")
        if not token:
            return out
