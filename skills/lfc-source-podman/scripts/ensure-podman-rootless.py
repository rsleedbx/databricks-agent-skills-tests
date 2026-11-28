#!/usr/bin/env python3
"""ensure-podman-rootless.py [--on "<command prefix>"]
  Make a Linux host ready to run test containers as the login user, with plain `podman` (rootless): the only place
  that needs sudo, and only once per host. --on runs it there: "ssh <user>@<vm>" or "limactl shell <instance> --"
  (default: this machine). Steps, one line each (PASS / FIXED / FAIL):
    Podman installed (apt-get or dnf);
    subordinate user and group IDs for the user (/etc/subuid, /etc/subgid), which rootless containers map users to;
    linger on (loginctl enable-linger): without it systemd stops a rootless user's containers when the user's last
      session ends, and scripts that drive a host over many short ssh or limactl sessions lose their containers;
    ~/.cache owned by the user (earlier rootful runs can leave root-owned files there);
    a rootless container runs, and a container's name resolves from another on a Podman network.
  Exit 1 if anything FAILs. Runs the host's own commands (sudo, apt-get, dnf, loginctl, podman) on the host.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from podman_host import Fail, Host, Report


def main(argv: list[str]) -> int:
    on = argv[1] if argv[:1] == ["--on"] and len(argv) > 1 else ""
    if argv[:1] == ["--on"] and len(argv) < 2:
        raise Fail("--on needs a command prefix", 2)
    rep, h = Report(width=44), Host(on)

    if h.ok("command -v podman >/dev/null"):
        rep.row("PASS", "Podman installed", h.out("podman --version"))
    elif h.ok('if command -v apt-get >/dev/null; then sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq && '
              'sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq podman; '
              'elif command -v dnf >/dev/null; then sudo dnf install -y -q podman; else exit 1; fi', quiet=True):
        rep.row("FIXED", "Podman installed", h.out("podman --version"))
    else:
        rep.row("FAIL", "Podman installed", "no apt-get or dnf, or the install failed")

    u = h.out("id -un")
    if h.ok(f"grep -q '^{u}:' /etc/subuid && grep -q '^{u}:' /etc/subgid"):
        rep.row("PASS", f"subordinate IDs for {u}", h.out(f"grep '^{u}:' /etc/subuid"))
    elif h.ok(f"sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 {u} && podman system migrate",
              quiet=True):
        rep.row("FIXED", f"subordinate IDs for {u}", "100000-165535")
    else:
        rep.row("FAIL", f"subordinate IDs for {u}", "usermod failed")

    if h.out(f"loginctl show-user {u} -p Linger --value", quiet=True) == "yes":
        rep.row("PASS", f"linger for {u}", "on")
    elif h.ok(f"sudo loginctl enable-linger {u}", quiet=True):
        rep.row("FIXED", f"linger for {u}", "turned on")
    else:
        rep.row("FAIL", f"linger for {u}", "loginctl enable-linger failed")

    n = h.out('find "$HOME/.cache" -xdev ! -user "$(id -un)" 2>/dev/null | wc -l | tr -d " "') or "0"
    if n == "0":
        rep.row("PASS", f"~/.cache owned by {u}", "")
    elif h.ok('sudo chown -R "$(id -un):$(id -gn)" "$HOME/.cache"'):
        rep.row("FIXED", f"~/.cache owned by {u}", f"{n} files were someone else's")
    else:
        rep.row("FAIL", f"~/.cache owned by {u}", "chown failed")

    r = h.out('podman info --format "{{.Host.Security.Rootless}} {{.Host.Arch}}"', quiet=True)
    if r.startswith("true "):
        rep.row("PASS", "rootless Podman", f"arch {r[5:]}")
    else:
        rep.row("FAIL", "rootless Podman", f"podman info: {r or 'failed'}")

    d = h.out('N=ensure-rootless-$$; podman network create $N >/dev/null && podman run -d --rm --name $N-a --network $N '
              '--network-alias peer docker.io/library/alpine:3 sleep 30 >/dev/null '
              '&& podman run --rm --network $N docker.io/library/alpine:3 sh -c "ping -c1 -W3 peer >/dev/null && echo ok"; '
              'podman rm -f $N-a >/dev/null 2>&1; podman network rm $N >/dev/null 2>&1', quiet=True)
    if d == "ok":
        rep.row("PASS", "container names resolve on a network", "")
    else:
        rep.row("FAIL", "container names resolve on a network", "aardvark-dns missing? (apt-get install aardvark-dns)")
    return 1 if rep.fails else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
