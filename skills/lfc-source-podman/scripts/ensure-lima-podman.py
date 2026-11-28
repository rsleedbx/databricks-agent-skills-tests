#!/usr/bin/env python3
"""ensure-lima-podman.py <instance> [--create] [--fix-rosetta]
  Make a Lima VM ready to run test containers with `limactl shell <instance> -- podman --remote ...` (rootful, no sudo):
  the instance exists (--create makes it with this skill's command), it's running, and ensure-podman-rootless.py
  passes inside it (Podman installed, which an existing VM can lack: an existing Ubuntu 25.10 Lima VM,
  had none; subordinate IDs; linger).
  Rosetta: an existing VM can lack it (one with "rosetta" in its name had it off, 2026-10-28), and x86_64-only images
  (SQL Server, Db2, Informix) then fail at start with "Exec format error". --fix-rosetta turns it on for a vz VM:
  limactl stop, limactl edit --rosetta, limactl start (the VM restarts, so its containers stop).
  Prints one line per step, PASS / FIXED / FAIL / WARN; exit 1 if anything FAILs.
  Runs limactl (Lima's CLI: no workspace equivalent) and the two ensure scripts beside this one, through limactl shell.
  MODE=rootless prepares rootless Podman instead of the rootful socket.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from podman_host import Fail, Host, Report, call


def instances(listing: str) -> dict:
    """limactl list --format '{{.Name}} {{.Status}}' (or VMType): name -> the second column, first line per name."""
    out = {}
    for line in listing.splitlines():
        parts = line.split()
        if parts and parts[0] not in out:
            out[parts[0]] = parts[1] if len(parts) > 1 else ""
    return out


def limactl_list(fmt: str) -> dict:
    p = subprocess.run(["limactl", "list", "--format", fmt], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                       text=True, check=False)
    return instances(p.stdout)


def run_child(script: str, on: str) -> int:
    """An ensure script beside this one, on the instance; its own lines go to this output."""
    return subprocess.run([sys.executable, "-I", str(HERE / script), "--on", on], check=False).returncode


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: ensure-lima-podman.py <instance> [--create] [--fix-rosetta]", file=sys.stderr)
        return 1
    inst, create, fix_rosetta = argv[0], False, False
    for a in argv[1:]:
        if a == "--create":
            create = True
        elif a == "--fix-rosetta":
            fix_rosetta = True
        else:
            print(f"unknown option {a}", file=sys.stderr)
            return 2
    rep = Report(width=40)
    if shutil.which("limactl") is None:
        rep.row("FAIL", "limactl installed", "brew install lima")
        return 1
    st = limactl_list("{{.Name}} {{.Status}}").get(inst, "")
    if not st:
        if not create:
            rep.row("FAIL", f"instance {inst}", "not found: run again with --create, or pick one from limactl list")
            return 1
        if call(["limactl", "start", "--tty=false", "--name", inst, "--arch", "aarch64", "--vm-type", "vz", "--rosetta",
                 "--cpus", "4", "--memory", "10", "--disk", "60", "template:podman-rootful"]) == 0:
            rep.row("FIXED", f"instance {inst}", "created (template:podman-rootful, Rosetta)")
        else:
            rep.row("FAIL", f"instance {inst}", "limactl start failed")
            return 1
    elif st != "Running":
        if call(["limactl", "start", "--tty=false", inst]) == 0:
            rep.row("FIXED", f"instance {inst} running", f"was {st}")
        else:
            rep.row("FAIL", f"instance {inst} running", f"limactl start failed (was {st})")
            return 1
    else:
        rep.row("PASS", f"instance {inst} running", "")

    on = f"limactl shell {inst} --"
    here = Host(on)
    # Podman inside the VM, then rootful Podman without sudo through the root socket (ensure-podman-socket.py): one
    # store, for databases that need rootful and for tests that could run either way. MODE=rootless: rootless instead.
    if os.environ.get("MODE", "rootful") == "rootless":
        if run_child("ensure-podman-rootless.py", on) != 0:
            rep.fails += 1
    else:
        if not here.ok("command -v podman >/dev/null"):
            call([sys.executable, "-I", str(HERE / "ensure-podman-rootless.py"), "--on", on])
        if run_child("ensure-podman-socket.py", on) != 0:
            rep.fails += 1
    if here.ok("test -e /proc/sys/fs/binfmt_misc/rosetta"):
        rep.row("PASS", "Rosetta for x86_64 images", "")
    elif fix_rosetta:
        vt = limactl_list("{{.Name}} {{.VMType}}").get(inst, "")
        if vt != "vz":
            rep.row("FAIL", "Rosetta for x86_64 images", f"needs a vz VM (this one is {vt}): create one with --create")
        elif (call(["limactl", "stop", inst]) == 0 and call(["limactl", "edit", "--tty=false", "--rosetta", inst]) == 0
              and call(["limactl", "start", "--tty=false", inst]) == 0 and here.ok("test -e /proc/sys/fs/binfmt_misc/rosetta")):
            rep.row("FIXED", "Rosetta for x86_64 images", "turned on (limactl edit --rosetta) and registered; the VM restarted")
        else:
            rep.row("FAIL", "Rosetta for x86_64 images", "limactl stop / edit --rosetta / start didn't register it")
    else:
        rep.row("WARN", "Rosetta for x86_64 images",
                "not registered: x86_64 images fail with 'Exec format error'; run again with --fix-rosetta (restarts the VM)")
    return 1 if rep.fails else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Fail as e:
        print(e.msg, file=sys.stderr)
        sys.exit(e.rc)
