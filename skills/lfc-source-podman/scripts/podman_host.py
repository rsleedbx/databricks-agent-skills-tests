"""podman_host: the host side of the lfc-source-podman checks, shared by the ensure-* scripts. Standard library only.

Report: one line per step, "PASS" / "FIXED" / "FAIL" / "WARN" and what it was (`row`); a FAIL counts, and the script
exits 1 when any did.
Host: commands on one host: this machine (no prefix), over ssh ("ssh <user>@<vm>"), or through another command prefix
("limactl shell <instance> --"). Commands are the host's own (sudo, apt-get, dnf, loginctl, podman, systemctl).
Fail: the end of a script with a message and an exit code (2: a usage or setting refused before anything ran).
"""
from __future__ import annotations

import shlex
import subprocess


class Fail(Exception):
    def __init__(self, msg: str | None, rc: int = 1):
        super().__init__(msg or "")
        self.msg, self.rc = msg, rc


def call(argv: list[str], quiet: bool = True) -> int:
    """The command's exit code (not checked). quiet: its stdout and stderr dropped (`>/dev/null 2>&1`)."""
    kw = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL} if quiet else {}
    return subprocess.run(argv, check=False, **kw).returncode


class Report:
    def __init__(self, width: int = 44):
        self.width, self.fails = width, 0

    def row(self, status: str, step: str, detail: str = "") -> None:
        print(f"{status:<5} {step:<{self.width}} {detail}")
        if status == "FAIL":
            self.fails += 1


class Host:
    def __init__(self, on: str = ""):
        """on: "" (this machine), "ssh <user>@<vm>", or a prefix such as "limactl shell <instance> --"."""
        self.on = on or ""

    def argv(self, cmd: str) -> list[str]:
        if not self.on:
            return ["bash", "-c", cmd]
        if self.on.startswith("ssh "):   # ssh joins its words into one remote command: the command goes quoted
            return shlex.split(self.on) + [f"bash -c {shlex.quote(cmd)}"]
        return shlex.split(self.on) + ["bash", "-c", cmd]

    def run(self, cmd: str, quiet: bool = False) -> subprocess.CompletedProcess:
        """Runs cmd; its stdout is returned (text). quiet: its stderr is dropped too."""
        return subprocess.run(self.argv(cmd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL if quiet else None,
                              text=True, check=False)

    def ok(self, cmd: str, quiet: bool = False) -> bool:
        return self.run(cmd, quiet).returncode == 0

    def out(self, cmd: str, quiet: bool = False) -> str:
        """stdout without trailing newlines (the shell's $(...))."""
        return self.run(cmd, quiet).stdout.rstrip("\n")
