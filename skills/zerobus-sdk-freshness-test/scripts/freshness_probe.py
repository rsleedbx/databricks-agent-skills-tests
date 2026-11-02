#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.40"]
# ///
"""freshness_probe.py <profile> --path <label>|<warehouse-id>|<table>[|<column>] [--path ...] [--seconds 1200] [--interval 2] [--out probe.json]:
sample how far behind each path is (the warehouse's clock minus the newest event time it can see, in ms), all paths at the same moments.

  --path       label|warehouse id|catalog.schema.table|time column (default producer_time). Repeat it to sample several
               tables or warehouses side by side, e.g. the same table through two warehouses, or two ingest paths.
               For a column inside a VARIANT, give an expression: timestamp_micros(value:producer_time::bigint)
  --seconds    how long to sample; start it before the producer and let it run past the producer's end
  --interval   seconds between samples on each path (each path runs in its own thread)
  --out        every sample, {"paths": [...], "samples": [{"t": epoch, "path": label, "ms": float|null}]}; report.py
               summarizes it by the producer's run and burst windows

The query reads only the last 2 minutes and uses current_timestamp(), so the result cache never answers it. A sample
is null when the query failed or saw no event in the last 2 minutes (before the producer starts, after it stops).
"""
import argparse
import json
import statistics
import sys
import threading
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState


def sql_for(table, col):
    return (f"SELECT (unix_micros(current_timestamp()) - unix_micros(max({col}))) / 1000.0 FROM {table} "
            f"WHERE {col} > current_timestamp() - INTERVAL 2 MINUTES")


def main():
    if len(sys.argv) == 1:
        print(__doc__)
        return
    p = argparse.ArgumentParser(usage=__doc__.split("\n")[0])
    p.add_argument("profile")
    p.add_argument("--path", action="append", required=True)
    p.add_argument("--seconds", type=int, default=1200)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--out", default="probe.json")
    a = p.parse_args()
    w = WorkspaceClient(profile=a.profile)
    paths = []
    for x in a.path:
        parts = x.split("|")
        if len(parts) not in (3, 4):
            sys.exit(f"--path {x!r}: want label|warehouse-id|table[|column]")
        paths.append((parts[0], parts[1], sql_for(parts[2], parts[3] if len(parts) == 4 else "producer_time")))
    samples, lock, stop = [], threading.Lock(), time.time() + a.seconds

    def loop(label, wh, sql):
        while time.time() < stop:
            t = time.time()
            try:
                r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="30s",
                                                            disposition=Disposition.INLINE, format=Format.JSON_ARRAY)
                while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
                    time.sleep(0.1)
                    r = w.statement_execution.get_statement(r.statement_id)
                if r.status.state != StatementState.SUCCEEDED:                # a bad --path or a stopped warehouse: say so
                    err = r.status.error.message if r.status.error else r.status.state.value
                    raise RuntimeError(f"statement {r.status.state.value}: {err}")
                v = r.result.data_array[0][0] if r.result and r.result.data_array else None
                ms = float(v) if v is not None else None
            except Exception as e:
                ms = None
                print(f"{label}: {str(e)[:160]}", file=sys.stderr, flush=True)
            with lock:
                samples.append({"t": t, "path": label, "ms": ms})
            time.sleep(max(0, a.interval - (time.time() - t)))

    threads = [threading.Thread(target=loop, args=pth, daemon=True) for pth in paths]
    for th in threads:
        th.start()
    start = time.time()
    while any(th.is_alive() for th in threads):
        time.sleep(min(60, max(1, stop - time.time())))
        with lock:
            recent = [s for s in samples if s["t"] > time.time() - 60 and s["ms"] is not None]
        line = "  ".join(f"{lbl}: p50={statistics.median(xs):,.0f} max={max(xs):,.0f}" for lbl, _, _ in paths
                         for xs in [[s["ms"] for s in recent if s["path"] == lbl]] if xs)
        print(f"[{time.time() - start:5.0f}s] {line or 'no events in the last 2 minutes'}", flush=True)
    Path(a.out).write_text(json.dumps({"paths": [x[0] for x in paths], "samples": samples}, indent=1))
    print(f"wrote {a.out} ({len(samples)} samples)")


if __name__ == "__main__":
    main()
