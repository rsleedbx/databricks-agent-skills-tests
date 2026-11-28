#!/usr/bin/env python3
"""ensure-podman-socket.py [--on "<command prefix>"] [--mac-connection <name>]
  Rootful Podman without sudo: give the login user the root Podman socket through a `podman` group, the way the
  `docker` group works. After it, `podman --remote --url unix:///run/podman/podman.sock ...` runs rootful containers
  (storage in /var/lib/containers, --privileged, raised ulimits) as the login user, with no sudo and so no password.
  Members of the group can do anything root can through the socket: give it only to users who already have sudo.
  --on runs it on a host: "ssh <user>@<vm>" or "limactl shell <instance> --" (default: this machine). Uses sudo once.
  Steps (PASS / FIXED / FAIL): the podman group with the user in it; the socket unit's group and mode
  (/etc/systemd/system/podman.socket.d/group.conf); /run/podman readable by the group at boot (tmpfiles.d);
  podman.socket active; then, in a new session, the socket answers and a rootful container runs as uid 0.
  --mac-connection <name> (with --on "limactl shell <instance> --"): also register `podman -c <name>` on the Mac, over
  Lima's ssh, to that root socket, so the Mac's podman uses the VM's one rootful store (a VM made without Lima's
  podman template forwards no socket to the Mac).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from podman_host import Fail, Host, Report, call

SOCK = "unix:///run/podman/podman.sock"
LIMA_PREFIX = "limactl shell "


def ssh_settings(cfg: str) -> tuple[str, str, str]:
    """(port, user, identity file) from a Lima ssh.config: the first line of each keyword, the file's quotes off."""
    got = {}
    try:
        with open(cfg) as f:
            for line in f:
                parts = line.split()
                if len(parts) > 1 and parts[0] in ("Port", "User", "IdentityFile") and parts[0] not in got:
                    got[parts[0]] = parts[1].replace('"', "")
    except FileNotFoundError:
        pass
    return got.get("Port", ""), got.get("User", ""), got.get("IdentityFile", "")


def main(argv: list[str]) -> int:
    on, macconn = "", ""
    args = list(argv)
    while args:
        a = args.pop(0)
        if a not in ("--on", "--mac-connection") or not args:
            raise Fail(f"unknown {a}", 2)
        if a == "--on":
            on = args.pop(0)
        else:
            macconn = args.pop(0)
    rep, h = Report(width=44), Host(on)
    instance = on.split()[2] if on.startswith(LIMA_PREFIX) else ""

    u = h.out("id -un")
    if not h.ok("command -v podman >/dev/null"):
        rep.row("FAIL", "Podman installed", "run ensure-podman-rootless.py first (it installs Podman)")
        return 1
    if h.ok(f"id -nG {u} | tr ' ' '\\n' | grep -qx podman"):
        rep.row("PASS", f"{u} in the podman group", "")
    elif h.ok(f"sudo groupadd -f podman && sudo usermod -aG podman {u}"):
        rep.row("FIXED", f"{u} in the podman group", "added (new sessions get it)")
        # limactl shell reuses one persistent ssh connection, so its sessions never pick up a new group: close it, and the
        # next limactl shell logs in again. That interrupts other limactl commands running then; it's done only this once.
        if instance and call(["ssh", "-F", os.path.expanduser(f"~/.lima/{instance}/ssh.config"), "-O", "exit",
                              f"lima-{instance}"]) == 0:
            rep.row("FIXED", "Lima login refreshed", "closed the persistent ssh connection")
    else:
        rep.row("FAIL", f"{u} in the podman group", "groupadd/usermod failed")

    if h.ok('grep -q "SocketGroup=podman" /etc/systemd/system/podman.socket.d/group.conf 2>/dev/null'):
        rep.row("PASS", "socket group and mode", "podman, 0660")
    elif h.ok('sudo mkdir -p /etc/systemd/system/podman.socket.d && '
              r'printf "[Socket]\nSocketMode=0660\nSocketGroup=podman\n" | sudo tee /etc/systemd/system/podman.socket.d/group.conf >/dev/null '
              '&& sudo systemctl daemon-reload'):
        rep.row("FIXED", "socket group and mode", "podman, 0660")
    else:
        rep.row("FAIL", "socket group and mode", "couldn't write the unit override")

    if h.ok('grep -q "root podman" /etc/tmpfiles.d/podman-socket-dir.conf 2>/dev/null'):
        rep.row("PASS", "/run/podman readable by the group", "")
    elif h.ok('echo "d /run/podman 0750 root podman -" | sudo tee /etc/tmpfiles.d/podman-socket-dir.conf >/dev/null '
              '&& sudo systemd-tmpfiles --create /etc/tmpfiles.d/podman-socket-dir.conf'):
        rep.row("FIXED", "/run/podman readable by the group", "tmpfiles.d")
    else:
        rep.row("FAIL", "/run/podman readable by the group", "tmpfiles failed")

    if (h.out('stat -c "%G %a" /run/podman/podman.sock 2>/dev/null || sudo stat -c "%G %a" /run/podman/podman.sock')
            == "podman 660" and h.out("systemctl is-active podman.socket") == "active"):
        rep.row("PASS", "podman.socket active, group podman", "")
    elif h.ok("sudo systemctl enable podman.socket >/dev/null 2>&1; sudo systemctl restart podman.socket"):
        rep.row("FIXED", "podman.socket active, group podman", "restarted")
    else:
        rep.row("FAIL", "podman.socket active, group podman", "systemctl failed")

    # a new session picks up the group: the check must not run in this one if the group was just added
    lines = h.out(f"podman --remote --url {SOCK} run --rm docker.io/library/alpine:3 id -u", quiet=True).splitlines()
    if (lines[-1] if lines else "") == "0":
        rep.row("PASS", "rootful container without sudo", "uid 0 in the container")
    else:
        rep.row("FAIL", "rootful container without sudo",
                f"no answer from {SOCK} (a session opened before the group was added doesn't have it: run again)")

    if macconn:
        if instance:
            port, user, key = ssh_settings(os.path.expanduser(f"~/.lima/{instance}/ssh.config"))
            call(["podman", "system", "connection", "rm", macconn])
            added = call(["podman", "system", "connection", "add", macconn, "--identity", key,
                          f"ssh://{user}@127.0.0.1:{port}/run/podman/podman.sock"]) == 0
            info = subprocess.run(["podman", "-c", macconn, "info", "--format", "{{.Host.Security.Rootless}}"],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False).stdout.strip()
            if added and info == "false":
                rep.row("PASS", f"Mac connection {macconn}", f"podman -c {macconn}: rootful, the VM's store")
            else:
                rep.row("FAIL", f"Mac connection {macconn}", "podman system connection add or info failed")
        else:
            rep.row("FAIL", f"Mac connection {macconn}", 'only with --on "limactl shell <instance> --"')
    return 1 if rep.fails else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
