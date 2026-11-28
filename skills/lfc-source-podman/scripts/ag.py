"""ag: the Always On test's names and state, shared by ag-up.py, ag-sql.py and ag-down.py (lfc-source-podman).

    ag = Ag()                  # AG_NAME (default lfc-ag), AG_DIR (default $HOME/<AG_NAME>), PODMAN (default podman)
    ag.podman("ps")            # the container CLI (podman or docker), a CompletedProcess
    ag.env()                   # {"SA_PW": ..., "LFC_PW": ..., "CERT_PW": ...} from <AG_DIR>/ag.env, never printed

Standard library only; ag-up.py makes ag.env, the others read it.
"""
from __future__ import annotations

import os
import re
import subprocess

from podman_host import Fail

NAME = re.compile(r"[A-Za-z0-9_-]+")


def valid_name(s: str) -> bool:
    """Letters, digits, _ and -: a name that's safe in a container, network, login or SQL identifier."""
    return NAME.fullmatch(s or "") is not None


class Ag:
    def __init__(self, env: dict | None = None):
        env = os.environ if env is None else env
        home = env.get("HOME") or os.path.expanduser("~")
        self.name = env.get("AG_NAME") or "lfc-ag"
        self.dir = env.get("AG_DIR") or os.path.join(home, self.name)
        self.pm = env.get("PODMAN") or "podman"
        self.home = home

    def podman(self, args: list[str], quiet: bool = False, check: bool = False, **kw) -> subprocess.CompletedProcess:
        """The container CLI's answer; check: a nonzero exit raises CalledProcessError. quiet: its stdout and stderr
        dropped (`>/dev/null 2>&1`)."""
        if quiet:
            kw.update(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return subprocess.run([self.pm, *args], check=check, **kw)

    def env(self) -> dict:
        """ag.env's KEY=VALUE lines. Missing: the same failure a shell's `. ag.env` gives."""
        path = os.path.join(self.dir, "ag.env")
        try:
            with open(path) as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            raise Fail(f"{path}: No such file or directory", 1)
        out = {}
        for line in lines:
            k, eq, v = line.partition("=")
            if eq:
                out[k] = v
        return out
