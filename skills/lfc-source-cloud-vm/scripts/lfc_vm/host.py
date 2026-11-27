"""The VM's side: commands (podman, juicefs, ss, findmnt ...), the one-command-at-a-time lock and the cloud's instance
metadata. Every command has a clear failure; a wait has a bound. Python opens files non-inheritable, so a container or a
`podman exec` (and its conmon) never keeps the lock after the command that took it has ended."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
import urllib.request
from collections.abc import Sequence

META = "http://169.254.169.254"


class Fail(Exception):
    """A command's end: the message goes to stderr as `slot: <message>` (none: already said), then exit rc."""

    def __init__(self, msg: str | None, rc: int = 1):
        super().__init__(msg or "")
        self.msg, self.rc = msg, rc


class Host:
    """Runs the VM's commands. A test replaces run() (or puts fakes on PATH)."""

    def run(self, argv: Sequence[str], input: str | None = None, timeout: float | None = None,
            quiet: bool = False, capture: bool = True, merge: bool = False) -> subprocess.CompletedProcess:
        """argv's result: stdout captured (capture), stderr shown unless quiet. A timeout gives rc 124, like timeout(1);
        a missing command rc 127. merge: stderr into stdout (podman logs replays the container's stderr there)."""
        try:
            return subprocess.run(list(argv), input=input, text=True, timeout=timeout,
                                  stdout=subprocess.PIPE if capture else None,
                                  stderr=subprocess.STDOUT if merge else subprocess.DEVNULL if quiet else None)
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(argv, 124, "", "")
        except FileNotFoundError:
            if not quiet:
                print(f"slot: {argv[0]}: not found", file=sys.stderr)
            return subprocess.CompletedProcess(argv, 127, "", "")

    def out(self, argv: Sequence[str], **kw) -> str:
        """stdout of argv, "" when it fails (quiet)."""
        r = self.run(argv, quiet=True, **kw)
        return r.stdout if r.returncode == 0 else ""

    def ok(self, argv: Sequence[str], **kw) -> bool:
        return self.run(argv, quiet=True, **kw).returncode == 0

    # ---- podman
    def inspect(self, container: str, fmt: str) -> str:
        return self.out(["podman", "inspect", "-f", fmt, container]).strip()

    def running(self, container: str) -> bool:
        return self.inspect(container, "{{.State.Running}}") == "true"

    def running_names(self) -> list[str]:
        return [n for n in self.out(["podman", "ps", "--format", "{{.Names}}"]).split() if n.startswith("lfc-slot-")]

    def ip(self, container: str) -> str:
        return self.inspect(container, "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}")

    def exec(self, container: str, words: Sequence[str], input: str | None = None, opts: Sequence[str] = (),
             timeout: float | None = None, quiet: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
        """`podman exec [-i] <opts> <container> <words>`: -i only with input, so it never waits on our stdin."""
        return self.run(["podman", "exec", *(["-i"] if input is not None else []), *opts, container, *words],
                        input=input, timeout=timeout, quiet=quiet, capture=capture)

    def remove(self, container: str) -> None:
        self.run(["podman", "rm", "-f", container], quiet=True)

    def stop(self, container: str) -> None:
        if self.ok(["podman", "container", "exists", container]):
            self.run(["podman", "stop", "-t", "120", container], quiet=True)
            self.remove(container)

    def logs(self, container: str) -> str:
        r = self.run(["podman", "logs", container], merge=True)
        return r.stdout or ""

    # ---- the VM
    def fstype(self, path: str) -> str:
        return self.out(["findmnt", "-n", "-o", "FSTYPE", "--target", path]).strip()

    def listening_ports(self) -> list[int]:
        ports = []
        for line in self.out(["ss", "-Htln"]).splitlines():
            f = line.split()
            if len(f) >= 4 and f[3].rsplit(":", 1)[-1].isdigit():
                ports.append(int(f[3].rsplit(":", 1)[-1]))
        return ports

    def metadata(self, path: str, headers: dict | None = None, method: str = "GET") -> str:
        """The instance metadata service's answer (5 s), "" when there's none (another cloud, or a laptop)."""
        try:
            req = urllib.request.Request(META + path, headers=headers or {}, method=method)
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.read().decode()
        except Exception:  # noqa: BLE001  no metadata service: the caller's fallback
            return ""

    def azure_compute(self) -> dict:
        t = self.metadata("/metadata/instance/compute?api-version=2021-12-13", {"Metadata": "true"})
        try:
            return json.loads(t) if t else {}
        except ValueError:
            return {}

    def aws(self, what: str) -> str:
        tok = self.metadata("/latest/api/token", {"X-aws-ec2-metadata-token-ttl-seconds": "60"}, "PUT")
        return self.metadata(f"/latest/meta-data/{what}", {"X-aws-ec2-metadata-token": tok} if tok else {}).strip()

    def public_ip(self) -> str:
        """The cloud metadata's public IP (Azure leaves it empty for a Standard SKU IP: set HOST), else the first
        local IP."""
        h = self.metadata("/metadata/instance/network/interface/0/ipv4/ipAddress/0/publicIpAddress"
                          "?api-version=2021-02-01&format=text", {"Metadata": "true"}).strip()
        h = h or self.metadata("/latest/meta-data/public-ipv4").strip()
        return h or (self.out(["hostname", "-I"]).split() or [""])[0]


class Lock:
    """One command at a time (flock on path); wait bounds the wait behind another command instead of hanging with it."""

    def __init__(self, path: str, wait: float):
        self.path, self.wait, self.fd = path, wait, None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT, 0o600)
        end = time.monotonic() + self.wait
        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= end:
                    os.close(self.fd)
                    self.fd = None
                    raise Fail(f"another slot command still holds {self.path} after {self.wait:g} s: see it with "
                               "pgrep -af lfc-slot")
                time.sleep(0.5)

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __exit__(self, *exc):
        self.release()
