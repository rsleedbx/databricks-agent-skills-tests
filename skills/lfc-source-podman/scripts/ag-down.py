#!/usr/bin/env python3
"""ag-down.py [--keep-data]: remove the ag-up.py containers <AG_NAME>-p/-s and their network; without --keep-data also
the state directory AG_DIR (data, certificates, passwords). Environment: AG_NAME, AG_DIR, PODMAN as in ag-up.py.
Removes with the container CLI (podman unshare for the data, as the container's uid) and falls back to a plain delete.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ag import Ag, valid_name
from podman_host import Fail


def main(argv: list[str]) -> int:
    ag = Ag()
    if not valid_name(ag.name):
        raise Fail("bad AG_NAME", 2)
    for r in ("p", "s"):
        if ag.podman(["rm", "-f", f"{ag.name}-{r}"], quiet=True).returncode == 0:
            print(f"removed {ag.name}-{r}")
    ag.podman(["network", "rm", f"{ag.name}-net"], quiet=True)
    if argv[:1] != ["--keep-data"]:
        d = ag.dir
        if d in ("/", ag.home, "/root", "/home", "/var", "/tmp", ""):
            raise Fail(f"refusing to remove {d}", 2)
        if ag.podman(["unshare", "rm", "-rf", d], quiet=True).returncode != 0 and os.path.isdir(d):
            shutil.rmtree(d)
        print(f"removed {d}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
