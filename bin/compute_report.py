#!/usr/bin/env python3
"""compute_report.py <profile> <name-prefix> [<since>]: for each pipeline whose name starts with <name-prefix> (and,
with <since>, an ISO UTC time such as 2026-10-18T02:47:00Z, whose latest update started after it), what the run set
serverless to, what its latest update ran on, whether that compute started, and whether it then reached the source,
for the records' "compute" field (lfc-test-results). Read-only.
  Prints TSV:
    name, kind      gateway | pipeline
    serverless      unset | true | false: spec.serverless, which echoes what was sent
    ran_on          serverless | classic: the latest update's config.serverless; not-started without an update
    started         yes: the update got past WAITING_FOR_RESOURCES (INITIALIZING, or Extractor STARTED); no; unknown
    reached_source  no: a connection error from the source (with the compute up, usually the firewall); yes: the
                    update reached RUNNING or COMPLETED with no connection error; unknown otherwise
    update_state, error (the first ERROR event's error code, if any)

  bin/compute_report.py <profile> janedoe-mssql-azvm
REST through lfc_rest (bin/pipeline_api.py, the events through lfc_watch): no CLI.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "skills" / "lfc-rest" / "scripts"))
sys.path.insert(0, str(R / "skills" / "lfc-pipeline-watch" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lfc_rest import Rest  # noqa: E402
from lfc_watch.core import events as pipeline_events, ms as epoch_ms  # noqa: E402
import pipeline_api  # noqa: E402

HEADER = ["name", "kind", "serverless", "ran_on", "started", "reached_source", "update_state", "error"]
STARTED = re.compile(r"is INITIALIZING|Extractor STARTED|is RUNNING|is COMPLETED")
RUNNING = re.compile(r"is RUNNING|is COMPLETED")
CONNECTION = re.compile(r"CONNECTION_ERROR|Could not connect to the source")
ERROR_CODE = re.compile(r"[A-Z_]+_ERROR(\.[A-Z_]+)?|You must use serverless compute|not supported in your workspace")


def first_exception_message(e: dict) -> str:
    exc = (e.get("error") or {}).get("exceptions") or []
    return str((exc[0].get("message") if exc else None) or "")


def error_text(e: dict) -> str:
    """The event's first exception's message, else its message (jq: .error.exceptions[0].message // .message)."""
    return first_exception_message(e) or str(e.get("message") or "")


def tostring(v) -> str:
    """jq's tostring for the values read here: true, false, null, or the text."""
    if v is None:
        return "null"
    return str(v).lower() if isinstance(v, bool) else str(v)


def created_ms_and_iso(created) -> tuple[float, str]:
    """An update's creation_time as epoch ms and as ISO UTC text: the API's value is either (the shell read ms)."""
    if isinstance(created, (int, float)):
        return float(created), datetime.fromtimestamp(created / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = str(created or "")
    return (epoch_ms(text) if text else 0.0), text


def update_facts(api: Rest, pipe: str, upd: str, st: str, created_ms: float) -> tuple[str, str, str, str]:
    """(ran_on, started, reached, error) of one update, from its config and the events since it was created (its
    own events come after its creation, and the newest 1,000 of the pipeline are read at most)."""
    ran, started, reached, err = "not-started", "unknown", "unknown", ""
    try:
        v = tostring((pipeline_api.get_update(api, pipe, upd).get("update") or {}).get("config", {}).get("serverless"))
    except Exception:  # noqa: BLE001  an unreadable update: what ran is unknown
        v = ""
    ran = {"true": "serverless", "false": "classic"}.get(v, ran)
    try:
        evs = [e for e in pipeline_events(api, pipe, since_ms=created_ms, max_events=1000)
               if (e.get("origin") or {}).get("update_id") == upd]
    except Exception:  # noqa: BLE001
        evs = []
    messages = [str(e.get("message") or "") for e in evs]
    if any(STARTED.search(m) for m in messages):
        started = "yes"
    if started == "unknown" and st not in ("RUNNING", ""):
        started = "no"
    for e in evs:
        m = ERROR_CODE.search(error_text(e)) if e.get("level") == "ERROR" else None
        if m:
            err = m.group(0)
            break
    if any(CONNECTION.search(first_exception_message(e)) for e in evs):
        reached = "no"
    elif any(RUNNING.search(m) for m in messages):
        reached = "yes"
    return ran, started, reached, err


def rows(api: Rest, prefix: str, since: str) -> list[list[str]]:
    out = []
    for pid in pipeline_api.list_named(api, prefix):
        try:
            p = pipeline_api.get(api, pid)
        except Exception:  # noqa: BLE001  a pipeline that can't be read is left out, as before
            continue
        spec = p.get("spec") or {}
        latest = (p.get("latest_updates") or [{}])[0]
        upd, st = latest.get("update_id") or "", latest.get("state") or ""
        created_ms, created = created_ms_and_iso(latest.get("creation_time"))
        if since and created < since:           # ISO UTC times compare as strings
            continue
        kind = "gateway" if (spec.get("gateway_definition") or {}).get("connection_name") else "pipeline"
        serverless = tostring(spec.get("serverless")) if spec.get("serverless") is not None else "unset"
        ran, started, reached, err = "not-started", "unknown", "unknown", ""
        if upd:
            ran, started, reached, err = update_facts(api, pid, upd, st, created_ms)
        out.append([p.get("name") or "", kind, serverless, ran, started, reached, st, err])
    return out


def aligned(table: list[list[str]]) -> str:
    """The rows as columns padded to their widest cell, two spaces apart (column -t)."""
    widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in table)


def report(profile: str, prefix: str, since: str = "") -> list[list[str]]:
    """The header and the rows, as the TSV's cells."""
    return [HEADER] + rows(Rest.connect(profile), prefix, since)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    for r in report(argv[0], argv[1], argv[2] if len(argv) > 2 else ""):
        print("\t".join(r))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
