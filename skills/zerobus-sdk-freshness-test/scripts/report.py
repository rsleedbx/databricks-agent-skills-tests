#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.40"]
# ///
"""report.py <profile> --warehouse <id> --table <catalog.schema.table> (--run run.json | --window <start>,<end>) [--probe probe.json] [--sent N] [--column producer_time] [--json]:
what one producer run landed and how fresh it was: rows sent vs landed, the per-second rate and burst seen in the data, the table's commits, and freshness by phase.

  --run        producer.py's output: the run window, the burst window, and the rows sent
  --window     instead of --run: the run window as ISO UTC times (2026-10-22T20:51:00,2026-10-22T21:07:00); the burst
               is then found in the data (seconds above --burst-above events/s)
  --probe      freshness_probe.py's output; summarized per path for the whole run and before, during and after the burst
               (the first --warmup 15 seconds of the run are left out while the streams open)
  --sent N     rows sent, when there's no --run file (the producer's last line)
  --column     the event time column (default producer_time); an expression works for a VARIANT column
  --json       print one JSON object (metrics for a record.py record) instead of the text report

Commits come from DESCRIBE HISTORY within the run window. Its timestamps are whole seconds, so each interval is
+/- 1 s. Landed counts every row, duplicates included, as the producer counts what it sent.
"""
import argparse
import calendar
import json
import statistics
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def summary(xs):
    return {"n": len(xs), "p50_ms": round(statistics.median(xs)), "p90_ms": round(pct(xs, 90)), "max_ms": round(max(xs))} if xs else None


def iso(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t))


def epoch(s):
    return calendar.timegm(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S"))


def main():
    if len(sys.argv) == 1:
        print(__doc__)
        return
    p = argparse.ArgumentParser(usage=__doc__.split("\n")[0])
    p.add_argument("profile")
    p.add_argument("--warehouse", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--run")
    p.add_argument("--window")
    p.add_argument("--probe")
    p.add_argument("--sent", type=int)
    p.add_argument("--column", default="producer_time")
    p.add_argument("--burst-above", type=float, default=0)
    p.add_argument("--warmup", type=float, default=15)
    p.add_argument("--json", action="store_true")
    a = p.parse_args()
    w = WorkspaceClient(profile=a.profile)

    def sql(stmt):
        r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=a.warehouse, wait_timeout="50s",
                                                    disposition=Disposition.INLINE, format=Format.JSON_ARRAY)
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            r = w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            sys.exit(f"query failed: {r.status.error.message if r.status.error else r.status.state}")
        return r.result.data_array or [] if r.result else []

    run = json.loads(Path(a.run).read_text()) if a.run else {}
    if run:
        start, end, burst, sent = run["start"], run["end"], run.get("burst"), run.get("sent")
    elif a.window:
        start, end = (epoch(x) for x in a.window.split(","))
        burst, sent = None, a.sent
    else:
        sys.exit("--run or --window is required")
    sent = a.sent if a.sent is not None else sent
    col = a.column

    # Per-second arrivals by event time over the window (a minute of margin on each side)
    per_s = [(int(s), int(n)) for s, n in sql(
        f"SELECT unix_timestamp(date_trunc('SECOND', {col})), count(*) FROM {a.table} "
        f"WHERE {col} >= timestamp_seconds({int(start) - 60}) AND {col} < timestamp_seconds({int(end) + 60}) GROUP BY 1 ORDER BY 1")]
    landed = sum(n for _, n in per_s)
    base = statistics.median(n for _, n in per_s) if per_s else 0
    hot = [s for s, n in per_s if n > (a.burst_above or 3 * base)]
    seen_burst = (min(hot), max(hot) + 1) if hot else None
    burst = burst or seen_burst

    # Commits within the window
    hist = sql(f"SELECT version, unix_timestamp(timestamp), operation, NULL "
               f"FROM (DESCRIBE HISTORY {a.table}) WHERE timestamp >= timestamp_seconds({int(start)}) "
               f"AND timestamp <= timestamp_seconds({int(end) + 30}) ORDER BY version")
    ops = {}
    for _, _, op, _ in hist:
        ops[op] = ops.get(op, 0) + 1
    writes = [(int(v), int(t)) for v, t, op, _ in hist if op in ("WRITE", "STREAMING UPDATE")]
    gaps = [writes[i + 1][1] - writes[i][1] for i in range(len(writes) - 1)]

    out = {"window": [iso(start), iso(end)], "burst": [iso(burst[0]), iso(burst[1])] if burst else None,
           "sent": sent, "landed": landed, "missing": (sent - landed) if sent is not None else None,
           "rate_per_s": {"median": round(base), "peak": max((n for _, n in per_s), default=0),
                          "burst_mean": round(statistics.mean(n for s, n in per_s if seen_burst and seen_burst[0] <= s < seen_burst[1])) if seen_burst else None},
           "commits": {"operations": ops, "write_commits": len(writes),
                       "interval_s": {"min": min(gaps), "p50": statistics.median(gaps), "max": max(gaps)} if gaps else None}}

    if a.probe:
        probe = json.loads(Path(a.probe).read_text())
        lo, hi = start + a.warmup, end + 1
        fresh = {}
        for label in probe["paths"]:
            xs = [(s["t"], s["ms"]) for s in probe["samples"] if s["path"] == label and s["ms"] is not None and lo <= s["t"] <= hi]
            f = {"run": summary([m for _, m in xs])}
            if burst:
                b0, b1 = burst[0], burst[1] + 1
                f.update(before=summary([m for t, m in xs if t < b0]), during=summary([m for t, m in xs if b0 <= t <= b1]),
                         after=summary([m for t, m in xs if t > b1]))
            f["failed_or_empty"] = sum(1 for s in probe["samples"] if s["path"] == label and s["ms"] is None and lo <= s["t"] <= hi)
            fresh[label] = f
        out["freshness"] = fresh

    if a.json:
        print(json.dumps(out))
        return
    print(f"window   {out['window'][0]} .. {out['window'][1]} UTC" + (f"; burst {out['burst'][0]} .. {out['burst'][1]}" if burst else ""))
    print(f"rows     sent {sent if sent is not None else '?'}, landed {landed}" + (f", missing {out['missing']}" if sent is not None else ""))
    r = out["rate_per_s"]
    print(f"rate     median {r['median']:,}/s, peak {r['peak']:,}/s" + (f", burst mean {r['burst_mean']:,}/s" if r["burst_mean"] else ""))
    c = out["commits"]
    print(f"commits  {c['operations']}; write interval {c['interval_s']} s")
    for label, f in out.get("freshness", {}).items():
        print(f"fresh    {label}: " + "  ".join(f"{k} p50 {v['p50_ms']:,} / p90 {v['p90_ms']:,} / max {v['max_ms']:,} ms (n={v['n']})"
                                              for k, v in f.items() if isinstance(v, dict)) + f"  failed/empty {f['failed_or_empty']}")


if __name__ == "__main__":
    main()
