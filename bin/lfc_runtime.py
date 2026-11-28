#!/usr/bin/env python3
"""lfc_runtime.py [--summary] <profile> <pipeline-id> [update-id]: which Lakeflow runtime a pipeline update ran on, and
which of its settings changed since the last time you looked at this workspace.

Settings appear, disappear and change from release to release, and differ between workspaces on different releases,
so record them with every test run. Reads the update's runtime_details event (the latest update if none is given) and
(a full update ID or its first characters) prints the runtime: DBR version, channel, image build, cloud and region.
Then it saves every pipelines.* setting the update ran with and compares them with the previous snapshot for this
profile and pipeline kind (gateway, standard-applier, integrated, query-based: their settings differ by kind, so
comparing across kinds is noise): added (+), removed (-), changed (~).

--summary prints only the runtime line (for a test's result file). Snapshots stay on this machine, in
${LFC_RUNTIME_DIR:-~/.lfc-test/runtime}/<profile>/, never in a repository: they carry workspace details.
bin/lfc-runtime.sh is a shim that runs this file: the skills' scripts call that path.
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "skills" / "lfc-rest" / "scripts"))
sys.path.insert(0, str(R / "skills" / "lfc-pipeline-watch" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lfc_rest import Rest  # noqa: E402
from lfc_watch.core import events as pipeline_events  # noqa: E402
import pipeline_api  # noqa: E402

EVENTS_CAP = 40 * 250   # the API's pages, as far as a pipeline's whole history is read
BUILD = re.compile(r"dp-[0-9]{8}")


def kind_of(spec: dict) -> str:
    ingestion = spec.get("ingestion_definition") or {}
    if spec.get("gateway_definition"):
        return "gateway"
    if ingestion.get("ingestion_gateway_id"):
        return "standard-applier"
    if ingestion.get("connector_type") == "CDC":
        return "integrated"
    if ingestion:
        return "query-based"
    return "other"


def runtime_event(events: list[dict], upd: str) -> dict:
    """The latest runtime_details event of the update (any update when upd is empty), or {}."""
    rts = [e for e in events if e.get("event_type") == "runtime_details"
           and (not upd or (e.get("origin") or {}).get("update_id", "").startswith(upd))]
    return sorted(rts, key=lambda e: e.get("timestamp") or "")[-1] if rts else {}


def settings_text(rt: dict, dbr, channel, image) -> str:
    """The settings as key TAB value lines, sorted by key; the runtime's three first."""
    cfg = ((rt.get("details") or {}).get("runtime_details") or {}).get("runtime_configuration") or {}
    cfg = cfg.get("configuration") or {}
    rows = {"_runtime.dbr": str(dbr), "_runtime.channel": str(channel), "_runtime.image": str(image)}
    for k, v in cfg.items():
        rows[k] = v if isinstance(v, str) else json.dumps(v)
    return "".join(f"{k}\t{rows[k]}\n" for k in sorted(rows))


def diff_lines(prev: dict[str, str], now: dict[str, str]) -> list[str]:
    """added (+), removed (-), changed (~) between two settings snapshots, by key; "no changes" when none."""
    out = []
    for k in sorted(set(prev) | set(now)):
        if k not in prev:
            out.append(f"  + {k} = {now[k]}")
        elif k not in now:
            out.append(f"  - {k} (was {prev[k]})")
        elif prev[k] != now[k]:
            out.append(f"  ~ {k}: {prev[k]} -> {now[k]}")
    return out or ["  no changes"]


def read_snapshot(path: str) -> dict[str, str]:
    out = {}
    for line in Path(path).read_text().splitlines():
        k, _, v = line.partition("\t")
        out[k] = v
    return out


def main(argv: list[str]) -> int:
    summary = bool(argv) and argv[0] == "--summary"
    if summary:
        argv = argv[1:]
    if len(argv) < 2:
        print(__doc__)
        return 2
    profile, pipe = argv[0], argv[1]
    upd = argv[2] if len(argv) > 2 else ""
    api = Rest.connect(profile)

    events = pipeline_events(api, pipe, 0, EVENTS_CAP)
    rt = runtime_event(events, upd)
    if rt.get("event_type") != "runtime_details":
        print(f"no runtime_details event for {upd or 'any update'} of {pipe}")
        return 1
    det = ((rt.get("details") or {}).get("runtime_details") or {}).get("runtime_version") or {}
    origin = rt.get("origin") or {}
    dbr, channel, image = det.get("dbr_version") or "", det.get("channel") or "", det.get("image_key") or ""
    cloud, region, update = origin.get("cloud") or "", origin.get("region") or "", origin.get("update_id") or ""
    ts, source = rt.get("timestamp") or "", origin.get("ingestion_source_type") or ""
    m = BUILD.search(image)
    build = m.group(0) if m else ""          # dp-20261015
    print(f"Lakeflow runtime: DBR {dbr}, channel {channel}, image {image.split('-', 1)[0]} build {build[3:]} "
          f"({image}), {cloud} {region}" + (f", source {source}" if source else ""))
    if summary:
        return 0
    print(f"Update {update} at {ts}")

    kind = kind_of(pipeline_api.get(api, pipe).get("spec") or {})
    d = os.path.join(os.environ.get("LFC_RUNTIME_DIR") or os.path.expanduser("~/.lfc-test/runtime"), profile)
    os.makedirs(d, mode=0o700, exist_ok=True)
    os.chmod(d, 0o700)
    snap = os.path.join(d, f"{ts.split('T', 1)[0]}_dbr{dbr}_{build or 'nobuild'}_{kind}_{update[:8]}.txt")
    text = settings_text(rt, dbr, channel, image)
    prevs = [p for p in sorted(glob.glob(os.path.join(d, f"*_{kind}_*.txt")), key=os.path.getmtime, reverse=True)
             if p != snap]
    Path(snap).write_text(text)
    print(f"{len(text.splitlines())} settings saved to {snap}")
    if not prevs:
        print(f"no earlier {kind} snapshot for {profile} to compare with")
        return 0
    print(f"Compared with {os.path.basename(prevs[0])}:")
    for line in diff_lines(read_snapshot(prevs[0]), read_snapshot(snap)):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
