"""pipeline_times.py <profile> <pipeline name>: each completed update of a pipeline, in order: its total seconds
(created to completed) and the seconds its flows ran (RUNNING to completed), from the pipeline's event log, as JSON."""
import json
import sys
from datetime import datetime

from databricks.sdk import WorkspaceClient


def times(w, name: str) -> list:
    flt = "name LIKE '" + name.replace("'", "''") + "'"
    ps = [p for p in w.pipelines.list_pipelines(filter=flt) if p.name == name]
    if not ps:
        return []
    ev, tok = [], None
    while True:
        r = w.api_client.do("GET", f"/api/2.0/pipelines/{ps[0].pipeline_id}/events",
                            query={"max_results": 250, **({"page_token": tok} if tok else {})})
        ev += r.get("events", [])
        tok = r.get("next_page_token")
        if not tok or len(ev) > 5000:
            break
    by: dict = {}
    for e in ev:
        u = (e.get("origin") or {}).get("update_id")
        st = ((e.get("details") or {}).get("update_progress") or {}).get("state")
        if u and e.get("event_type") == "update_progress" and st:
            by.setdefault(u, {})[st] = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
    out = []
    for d in by.values():
        if "COMPLETED" in d:
            start = d.get("CREATED") or d.get("WAITING_FOR_RESOURCES") or d.get("INITIALIZING")
            out.append({"completed": d["COMPLETED"].isoformat()[:19],
                        "total_s": round((d["COMPLETED"] - start).total_seconds()) if start else None,
                        "running_s": round((d["COMPLETED"] - d["RUNNING"]).total_seconds()) if "RUNNING" in d else None})
    return sorted(out, key=lambda x: x["completed"])


if __name__ == "__main__":
    print(json.dumps(times(WorkspaceClient(profile=sys.argv[1]), sys.argv[2])))
