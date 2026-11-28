#!/usr/bin/env python3
"""stop_after.py <minutes> <pipeline-id>...: stop each pipeline's latest update once it has run longer than a limit.

Integrated CDC updates can run a long time on an idle source (smart closure waits for the lag to converge), so tests
stop them by hand. Usage:
  source env.sh
  bin/stop_after.py <minutes> <pipeline-id>...
The profile is PIPELINE_WS (default DEFAULT). Polls once a minute until every pipeline is done or stopped.
REST through lfc_rest (bin/pipeline_api.py): no CLI.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "skills" / "lfc-rest" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lfc_rest import Rest  # noqa: E402
import pipeline_api  # noqa: E402

DONE = ("COMPLETED", "FAILED", "CANCELED")
POLL_S = 60


def age_and_state(api: Rest, pipe: str) -> tuple[str, int]:
    """The latest update's state and its age in whole minutes (0 when it has none)."""
    latest = pipeline_api.latest_updates(api, pipe, 1)
    if not latest:
        return "", 0
    u = latest[0]
    age_ms = time.time() * 1000 - float(u.get("creation_time") or 0)
    return u.get("state") or "", int(age_ms // 60000)


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: stop_after.py <minutes> <pipeline-id>...")
        return 1
    try:
        limit = int(argv[0])
    except ValueError:
        print(f"minutes must be a whole number: {argv[0]}")
        return 1
    pending = argv[1:]
    if not pending:
        print("usage: stop_after.py <minutes> <pipeline-id>...")
        return 1
    api = Rest.connect(os.environ.get("PIPELINE_WS") or "DEFAULT")
    while pending:
        still = []
        for p in pending:
            st, age = age_and_state(api, p)
            hhmm = time.strftime("%H:%M", time.gmtime())
            if st in DONE:
                print(f"{hhmm} {p} {st} after {age}m")
            elif age >= limit:
                try:
                    pipeline_api.stop(api, p)
                    print(f"{hhmm} {p} STOPPED at {age}m (was {st})")
                except Exception:  # noqa: BLE001  as before: a stop that failed prints nothing and isn't kept
                    pass
            else:
                still.append(p)
        pending = still
        if pending:
            time.sleep(POLL_S)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
