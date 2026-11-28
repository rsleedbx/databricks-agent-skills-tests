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
    """The latest update's state and its age in whole minutes ("", 0 when it has none)."""
    latest = pipeline_api.latest_updates(api, pipe, 1)
    if not latest:
        return "", 0
    u = latest[0]
    age_ms = time.time() * 1000 - float(u.get("creation_time") or 0)
    return u.get("state") or "", int(age_ms // 60000)


def step(api: Rest, pipe: str, limit: int, hhmm: str) -> tuple[bool, str]:
    """One look at a pipeline: (still pending, the line to print or ""). A failed lookup keeps it pending (as the shell
    did); one with no update has nothing to stop, so it is done; a stop that fails is dropped without a line."""
    try:
        st, age = age_and_state(api, pipe)
    except Exception:  # noqa: BLE001  a failed lookup (the network, a pipeline gone): try again next round
        return True, ""
    if not st:
        return False, f"{hhmm} {pipe} no update"
    if st in DONE:
        return False, f"{hhmm} {pipe} {st} after {age}m"
    if age >= limit:
        try:
            pipeline_api.stop(api, pipe)
        except Exception:  # noqa: BLE001  as before: a stop that failed prints nothing and is not kept
            return False, ""
        return False, f"{hhmm} {pipe} STOPPED at {age}m (was {st})"
    return True, ""


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: stop_after.py <minutes> <pipeline-id>...")
        return 1
    try:
        limit = int(argv[0])
    except ValueError:
        print(f"minutes must be a whole number: {argv[0]}")
        return 1
    pending = argv[1:]
    api = Rest.connect(os.environ.get("PIPELINE_WS") or "DEFAULT")
    while pending:
        still = []
        for p in pending:
            wait, line = step(api, p, limit, time.strftime("%H:%M", time.gmtime()))
            if line:
                print(line)
            if wait:
                still.append(p)
        pending = still
        if pending:
            time.sleep(POLL_S)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
