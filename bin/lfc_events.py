#!/usr/bin/env python3
"""lfc_events.py <profile> <pipeline-id> [--update <id>] [--since <hours>] [--source table|api|auto] [--warehouse <id>]
               [--types <t1,t2>] [--levels <L1,L2>] [--summary | --json [--max <n>] [--details-chars <n>]]:
               a pipeline's events, from the pipeline events system table or the API.

Two sources, both public:
  table  system.lakeflow_pipeline_events_preview.pipeline_events (Beta; system.lakeflow.pipeline_events at GA): every
         event of every pipeline in the account for 13 months, including METRICS-level events (flow_progress and
         cluster_resources every minute while a pipeline runs) that the API never returns. Needs a SQL warehouse and
         SELECT on the system schema. Rows arrive with a delay: seconds when measured, no stated bound.
  api    /api/2.0/pipelines/<id>/events: INFO, WARN and ERROR events only, but current to the second. Use it while
         debugging a pipeline that is running now.
  auto   (default) the table, unless it can't be read or it is behind the API's newest event: then the API.

Prints one line per event: time, type, level, update, flow or dataset, message. --types keeps only those event types,
--levels only those levels (INFO, WARN, ERROR; with the API the filter goes to the API, so fewer pages are read).
--json prints instead one JSON array, newest first: timestamp, level, event_type, update_id, flow, message, error_class
(API only), details (JSON text, cut to --details-chars, default 800); --max keeps the newest n (and with the API stops
reading pages once it has them). --since 0: no time window (the API's 40 pages at most).
--summary prints instead: events by type and level; warnings (gc_pressure, abnormal_termination, disk_space, WARN
and ERROR); outbound API calls (remote_api_usage, SaaS connectors); the newest cluster_resources (autoscaling, task
slots); each update's start, full refresh and last state; rows and backlog per table (flow_progress); the latest
snapshot or CDC operation per table and why (operation_progress); and streaming batch counts and durations per flow
(stream_progress). The worked queries in the docs' pipeline events page and its observability guide cover the same
ground in SQL. LFC_EVENTS_TABLE overrides the table path.

REST through lfc_rest: the API's events through lfc_watch's events(), the table through lfc_change_source's Warehouse.
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "skills" / "lfc-rest" / "scripts"))
sys.path.insert(0, str(R / "skills" / "lfc-pipeline-watch" / "scripts"))
sys.path.insert(0, str(R / "skills" / "lfc-change-source" / "scripts"))
from lfc_rest import Rest  # noqa: E402
from lfc_watch.core import events as pipeline_events  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402

TABLE_DEFAULT = "system.lakeflow_pipeline_events_preview.pipeline_events"
LEVELS_RE = re.compile(r"^(INFO|WARN|ERROR)(,(INFO|WARN|ERROR))*$")
UUID_RE = re.compile(r"^[0-9a-f-]{36}$")
HEX_RE = re.compile(r"^[0-9a-f-]+$")
ALERT_TYPES = ("gc_pressure", "abnormal_termination", "disk_space")
API_CAP = 40 * 250   # the API's 40 pages of 250 events


def first(*vals):
    """jq's //: the first value that is neither null nor false (an empty string counts)."""
    for v in vals:
        if v is not None and v is not False:
            return v
    return vals[-1] if vals else None


def to_num(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def fmt_num(n: float):
    return int(n) if float(n).is_integer() else n


def details_of(raw) -> dict:
    """An event's details from its base64 JSON (the table); {} when none or unreadable."""
    if not raw:
        return {}
    try:
        v = json.loads(base64.b64decode(raw).decode())
    except (ValueError, TypeError):
        return {}
    return v if isinstance(v, dict) else {}


def event_from_api(e: dict) -> dict:
    origin = e.get("origin") or {}
    return {"time": e.get("timestamp") or "", "type": e.get("event_type") or "", "level": e.get("level") or "",
            "update": origin.get("update_id") or "",
            "flow": first(origin.get("flow_name"), origin.get("dataset_name"), ""),
            "message": re.sub(r"[\t\n]+", " ", e.get("message") or ""),
            "details": e.get("details") or {},
            "error_class": first(((e.get("error") or {}).get("exceptions") or [{}])[0].get("class_name"), "")}


def event_from_row(row) -> dict:
    """One row of the table: time, type, level, update, flow, message, details (base64 JSON)."""
    return {"time": row[0] or "", "type": row[1] or "", "level": row[2] or "", "update": row[3] or "",
            "flow": row[4] or "", "message": row[5] or "", "details": details_of(row[6]), "error_class": ""}


class Source:
    """One pipeline's events from one source."""

    def __init__(self, api: Rest, pipe: str, since_h: int, warehouse: str):
        self.api, self.pipe, self.since_h, self.warehouse = api, pipe, since_h, warehouse
        self.table = os.environ.get("LFC_EVENTS_TABLE") or TABLE_DEFAULT

    def cutoff_ms(self) -> float:
        if self.since_h == 0:
            return 0
        return (datetime.now(timezone.utc) - timedelta(hours=self.since_h)).timestamp() * 1000

    def from_api(self, update: str, levels: str, max_n: int) -> list[dict]:
        """The API's events in the window (newest first from the API, cut at max_n when that is safe), then the
        update's and the levels'. Oldest first."""
        cap = max_n if max_n and not update and not levels else API_CAP
        raw = pipeline_events(self.api, self.pipe, since_ms=self.cutoff_ms(), max_events=cap)
        evs = [event_from_api(e) for e in raw]
        return _shape(evs, update, levels)

    def newest_api(self) -> str:
        raw = pipeline_events(self.api, self.pipe, max_events=1)
        return raw[0].get("timestamp") or "" if raw else ""

    def from_table(self, update: str, levels: str) -> list[dict]:
        """Raises RuntimeError (no warehouse, the statement's error) when the table can't be read."""
        wh = Warehouse(self.warehouse, self.api) if self.warehouse else Warehouse.pick(self.api)
        upd = f"AND update_id LIKE '{update}%'" if update else ""
        stmt = f"""SELECT date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), event_type, level, update_id,
         coalesce(origin.flow_name, origin.dataset_name, ''), regexp_replace(coalesce(message, ''), '[\\t\\n]+', ' '),
         regexp_replace(base64(to_json(details)), '[\\r\\n]', '')
       FROM {self.table} WHERE pipeline_id = '{self.pipe}'
         AND event_time > current_timestamp() - INTERVAL {self.since_h} HOURS {upd} ORDER BY event_time"""
        return _shape([event_from_row(r) for r in wh.sql(stmt).collect()], update, levels)


def _shape(evs: list[dict], update: str, levels: str) -> list[dict]:
    if update:
        evs = [e for e in evs if e["update"].startswith(update)]
    if levels:
        evs = [e for e in evs if e["level"] in levels.split(",")]
    return sorted(evs, key=lambda e: e["time"])


def alert(e: dict) -> bool:
    return e["level"] in ("WARN", "ERROR") or e["type"] in ALERT_TYPES


def json_out(events: list[dict], max_n: int, dch: int) -> str:
    rows = [{"timestamp": e["time"], "event_type": e["type"], "level": e["level"], "update_id": e["update"],
             "flow": e["flow"], "message": e["message"][:500], "error_class": e["error_class"],
             "details": json.dumps(e["details"] or {}, separators=(",", ":"))[:dch]} for e in reversed(events)]
    if max_n > 0:
        rows = rows[:max_n]
    return json.dumps(rows, indent=2, ensure_ascii=False)


def summary(events: list[dict], used: str) -> list[str]:
    out = [f"Events by type and level ({used}):"]
    counts = Counter((e["type"], e["level"]) for e in events)
    for (typ, lvl), n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        out.append(f"  {n:6d}  {typ:<28} {lvl}")
    out.append("Warnings and errors:")
    alerts = [f"  {e['time']}  {e['type']}  {e['level']}  {e['message'][:160]}" for e in events if alert(e)]
    out += alerts[-20:] if alerts else ["  none"]
    out.append("Outbound API calls (remote_api_usage):")
    out += _api_calls(events, used)
    out += _cluster(events, used)
    out += _updates(events)
    out += _rows(events)
    out += _operations(events)
    out += _streams(events)
    return out


def _api_calls(events: list[dict], used: str) -> list[str]:
    items = [e["details"] for e in events if e["type"] == "remote_api_usage"]
    if not items:
        why = "the API may not return them; try --source table" if used == "api" else \
            "not a SaaS pipeline, or none in the window"
        return [f"  none ({why})"]
    groups: dict[str, list[dict]] = {}
    for d in items:
        inner = first(d.get("remote_api_usage"), d)
        groups.setdefault(str(first(inner.get("api_name"), inner.get("endpoint"), inner.get("source"), "?")),
                          []).append(inner)
    lines = []
    for key in sorted(groups):
        g = groups[key]
        reqs = sum(to_num(first(x.get("num_requests"), x.get("request_count"), 1)) for x in g)
        lines.append(f"  {key}: {len(g)} events, {fmt_num(reqs)} requests")
    return lines


def _cluster(events: list[dict], used: str) -> list[str]:
    cr = [e for e in events if e["type"] == "cluster_resources"]
    out = ["Newest cluster_resources:"]
    if not cr:
        why = "METRICS-level: only in the table" if used == "api" else "none in the window"
        return out + [f"  none ({why})"]
    last = cr[-1]
    inner = first(last["details"].get("cluster_resources"), last["details"])
    autoscale = inner.get("autoscale_info")
    obj = {"autoscale": autoscale.get("state") if isinstance(autoscale, dict) else None}
    if isinstance(inner.get("task_slot_metrics"), dict):
        obj.update(inner["task_slot_metrics"])
    text = json.dumps({k: v for k, v in obj.items() if v is not None}, separators=(",", ":"))[:300]
    return out + [f"  {last['time']}  {text}"]


def _updates(events: list[dict]) -> list[str]:
    groups: dict[str, list[dict]] = {}
    for e in events:
        if e["type"] in ("create_update", "update_progress"):
            groups.setdefault(e["update"], []).append(e)
    rows = []
    for u, g in groups.items():
        creates = [e for e in g if e["type"] == "create_update"]
        progress = sorted((e for e in g if e["type"] == "update_progress"), key=lambda e: e["time"])
        start = creates[0]["time"] if creates else "?"
        full = first((creates[0]["details"].get("create_update") or {}).get("full_refresh") if creates else None, False)
        state = first((progress[-1]["details"].get("update_progress") or {}).get("state") if progress else None, "?")
        rows.append((start, u, full, state))
    rows.sort(key=lambda r: r[0])
    body = [f"  {start}  {u[:8]}  full_refresh={str(full).lower()}  {state}" for start, u, full, state in rows]
    return ["Updates (start, full refresh, last state):"] + body[-10:]


def _rows(events: list[dict]) -> list[str]:
    groups: dict[str, dict] = {}
    for e in events:
        if e["type"] != "flow_progress" or not e["flow"]:
            continue
        m = (e["details"].get("flow_progress") or {}).get("metrics") or {}
        g = groups.setdefault(e["flow"], {"up": 0.0, "del": 0.0, "out": 0.0, "backlog": 0.0})
        g["up"] += to_num(first(m.get("num_upserted_rows"), 0))
        g["del"] += to_num(first(m.get("num_deleted_rows"), 0))
        g["out"] += to_num(first(m.get("num_output_rows"), 0))
        g["backlog"] = max(g["backlog"], to_num(first(m.get("backlog_bytes"), 0)))
    lines = []
    for flow in sorted(groups):
        g = groups[flow]
        if g["up"] + g["del"] + g["out"] + g["backlog"] > 0:
            lines.append(f"  {flow.split('.')[-1]}  upserted={fmt_num(g['up'])} deleted={fmt_num(g['del'])} "
                         f"output={fmt_num(g['out'])} max_backlog_bytes={fmt_num(g['backlog'])}")
    return ["Rows per table (flow_progress: upserted, deleted, output; max backlog bytes):"] + \
        (lines[:25] if lines else ["  none with rows or backlog"])


def _operations(events: list[dict]) -> list[str]:
    latest: dict[str, dict] = {}
    for e in sorted(events, key=lambda e: e["time"]):
        d = e["details"].get("operation_progress") if e["type"] == "operation_progress" else None
        if not d:
            continue
        keys = sorted(x for x in d if x != "status")
        k = keys[0] if keys else "?"
        vals = [v for x, v in d.items() if x != "status"]
        x = first(vals[0] if vals else None, {})
        latest[first(x.get("target_table_name"), k)] = {"time": e["time"], "k": k, "status": d.get("status"), "x": x}
    out = ["Snapshots and CDC operations (operation_progress, latest per table):"]
    if not latest:
        return out + ["  none"]
    lines = []
    for target in sorted(latest):
        r = latest[target]
        short = str(first(r["x"].get("target_table_name"), "")).split(".")[-1]
        lines.append(f"  {r['time']}  {r['k']} {short}  {r['status']}  {first(r['x'].get('snapshot_reason'), '')}")
    return out + lines[:25]


def _streams(events: list[dict]) -> list[str]:
    stats: dict[str, list[float]] = {}
    for e in events:
        if e["type"] != "stream_progress":
            continue
        try:
            pj = json.loads((e["details"].get("stream_progress") or {}).get("progress_json") or "")
        except (ValueError, TypeError):
            continue
        if not isinstance(pj, dict):
            continue
        dur = to_num(first(pj.get("batchDuration"), 0))
        s = stats.setdefault(str(first(pj.get("name"), "?")), [0, 0.0, 0.0])
        s[0] += 1
        s[1] += dur
        s[2] = max(s[2], dur)
    lines = [f"  {name.split('.')[-1]}  batches={n} avg_ms={int(total / n)} max_ms={int(mx)}"
             for name, (n, total, mx) in stats.items()]
    return ["Streaming batches (stream_progress: batches, avg and max duration ms):"] + \
        (sorted(lines)[:25] if lines else ["  none"])


def parse(argv: list[str]) -> dict | None:
    """The arguments as a dict, or None for a usage error."""
    if len(argv) < 2:
        return None
    o = {"profile": argv[0], "pipe": argv[1], "update": "", "since": "24", "source": "auto", "warehouse": "",
         "types": "", "summary": False, "levels": "", "json": False, "max": "0", "dch": "800"}
    flags = {"--update": "update", "--since": "since", "--source": "source", "--warehouse": "warehouse",
             "--types": "types", "--levels": "levels", "--max": "max", "--details-chars": "dch"}
    rest = argv[2:]
    while rest:
        a = rest.pop(0)
        if a in flags and rest:
            o[flags[a]] = rest.pop(0)
        elif a == "--summary":
            o["summary"] = True
        elif a == "--json":
            o["json"] = True
        else:
            return None
    return o


def check_args(o: dict) -> str:
    """The message for a bad argument, or ""."""
    if not UUID_RE.match(o["pipe"]):
        return f"pipeline id should be a UUID: {o['pipe']}"
    if o["update"] and not HEX_RE.match(o["update"]):
        return f"update id should be hex: {o['update']}"
    if not o["since"].isdigit():
        return f"--since takes hours: {o['since']}"
    if o["levels"] and not LEVELS_RE.match(o["levels"]):
        return "--levels: INFO, WARN, ERROR, comma separated"
    if not (o["max"].isdigit() and o["dch"].isdigit()):
        return "--max and --details-chars take numbers"
    if o["source"] not in ("table", "api", "auto"):
        return "--source is table, api or auto"
    return ""


def read_events(src: Source, o: dict) -> tuple[list[dict], str]:
    """(events, used) from the source the options name; auto falls back to the API as described above."""
    def from_api() -> list[dict]:
        return src.from_api(o["update"], o["levels"], int(o["max"]))

    if o["source"] == "api":
        return from_api(), "api"
    try:
        events = src.from_table(o["update"], o["levels"])
    except Exception as e:  # noqa: BLE001  the table's reason is what the user needs
        if o["source"] == "table":
            raise _Unreadable(f"can't read {src.table}: {' '.join(str(e).split())[:300]}")
        _note(f"can't read {src.table} ({' '.join(str(e).split())[:160]}): using the API (INFO, WARN, ERROR only)")
        return from_api(), "api"
    if o["source"] == "table":
        return events, "table"
    api_last = src.newest_api()
    tbl_last = max((e["time"] for e in events), default="")
    if api_last and tbl_last < api_last:
        _note(f"the table ends at {tbl_last or 'nothing'}, the API at {api_last}: using the API "
              "(INFO, WARN, ERROR only)")
        return from_api(), "api"
    return events, "table"


class _Unreadable(Exception):
    """The table was asked for and can't be read: the message is the reason."""


def _note(s: str) -> None:
    print(s, file=sys.stderr)


def run(argv: list[str]) -> int:
    o = parse(argv)
    if o is None:
        print(__doc__)
        return 2
    bad = check_args(o)
    if bad:
        _note(bad)
        return 2
    api = Rest.connect(o["profile"])
    src = Source(api, o["pipe"], int(o["since"]), o["warehouse"])
    try:
        events, used = read_events(src, o)
    except _Unreadable as e:
        _note(str(e))
        return 1
    since = int(o["since"])
    win = "no time window" if since == 0 else f"last {since} h"
    _note(f"{len(events)} events from the {used}, {win}{', update ' + o['update'] if o['update'] else ''}")
    types = o["types"].split(",") if o["types"] else None
    if types:
        events = [e for e in events if e["type"] in types]
    if o["json"]:
        print(json_out(events, int(o["max"]), int(o["dch"])))
    elif not o["summary"]:
        for e in events:
            print("\t".join([e["time"], e["type"], e["level"], e["update"], e["flow"], e["message"]]))
    else:
        print("\n".join(summary(events, used)))
    return 0


if __name__ == "__main__":
    sys.exit(run(sys.argv[1:]))
