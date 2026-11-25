"""dashboard: the FlowThrough dashboard (dashboard/flowthrough.lvdash.json) on one schema: CDC coming in, each table's
snapshot and route, with links to its raw CDC, raw snapshot and silver. A query can't know the workspace's address,
so the file holds __HOST__, __WSID__ and __DASHBOARD__ (its own id, for the Raw CDC page's link), filled in here.

    python3 -I dashboard.py --profile <p> --warehouse <id> --catalog <c> --schema <s> [--host https://<vanity host>]
"""
from __future__ import annotations

import json
import os

FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "dashboard", "flowthrough.lvdash.json")


def filled(text: str, host: str, wsid: str, dashboard: str) -> str:
    return text.replace("__HOST__", host.rstrip("/")).replace("__WSID__", wsid).replace("__DASHBOARD__", dashboard)


def deploy(w, warehouse: str, catalog: str, schema: str, host: str | None = None, folder: str | None = None) -> str:
    """Created (or updated, by its name) and published: its published URL."""
    from databricks.sdk.service.dashboards import Dashboard
    host = (host or w.config.host).rstrip("/")
    wsid = str(w.get_workspace_id())
    name = f"FlowThrough {catalog}.{schema}"
    folder = folder or f"/Workspace/Users/{w.current_user.me().user_name}/flowthrough"
    w.workspace.mkdirs(folder)
    with open(FILE) as f:
        text = f.read()
    bare = lambda f: (f or "").removeprefix("/Workspace").rstrip("/")   # with or without the /Workspace prefix
    # the list gives no folder: each one of that name is read for its own
    have = [d for d in w.lakeview.list() if d.display_name == name
            and bare(w.lakeview.get(d.dashboard_id).parent_path) == bare(folder)]

    def put(did: str | None, body: str) -> str:
        d = Dashboard(display_name=name, warehouse_id=warehouse, parent_path=folder, serialized_dashboard=body)
        # the queries name tables bare: catalog and schema come from the dashboard's defaults
        q = {"dataset_catalog": catalog, "dataset_schema": schema}
        if did:
            return w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{did}", query=q,
                                   body={"display_name": name, "warehouse_id": warehouse, "serialized_dashboard": body})["dashboard_id"]
        return w.api_client.do("POST", "/api/2.0/lakeview/dashboards", query=q, body=d.as_dict())["dashboard_id"]

    did = put(have[0].dashboard_id if have else None, filled(text, host, wsid, "__DASHBOARD__"))
    put(did, filled(text, host, wsid, did))   # its own id, now known, in the Raw CDC link
    w.lakeview.publish(did, warehouse_id=warehouse, embed_credentials=False)
    return f"{host}/dashboardsv3/{did}/published?o={wsid}"


def main(argv=None) -> None:
    import argparse
    ap = argparse.ArgumentParser(description="The FlowThrough dashboard on one schema")
    for a in ("profile", "warehouse", "catalog", "schema"):
        ap.add_argument(f"--{a}", required=True)
    ap.add_argument("--host", help="the address its links use (default: the profile's host)")
    a = ap.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    print(json.dumps({"url": deploy(WorkspaceClient(profile=a.profile), a.warehouse, a.catalog, a.schema, a.host)}))


if __name__ == "__main__":
    main()
