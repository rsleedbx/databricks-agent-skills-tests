#!/usr/bin/env python3
"""remove_podman_desktop.py [--yes] [--machines]: remove Podman Desktop from a Mac so it stops starting its own podman
machine VM (a second VM next to Lima, whose disk image grows and doesn't shrink).
Without --yes it only reports: the app, its Homebrew cask, its files, podman machines and their disk use, and VM disk
files that were deleted while still open. --yes quits the app, uninstalls the cask (--zap) and deletes its files.
--machines also stops and removes every `podman machine` VM (their databases are lost) and the downloaded VM images.
Kept: the podman CLI, Lima and its VMs, ~/.config/containers (system connections).
"""
from __future__ import annotations

import glob
import os
import platform
import re
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import have, run

ID = "io.podmandesktop.PodmanDesktop"
HOME = os.path.expanduser("~")
MD = os.path.join(HOME, ".local", "share", "containers", "podman", "machine")
FILES = [
    "/Applications/Podman Desktop.app", os.path.join(HOME, "Applications", "Podman Desktop.app"),
    os.path.join(HOME, ".local", "share", "containers", "podman-desktop"),
    os.path.join(HOME, "Library", "Application Support", "Podman Desktop"),
    os.path.join(HOME, "Library", "Preferences", ID + ".plist"),
    os.path.join(HOME, "Library", "Saved Application State", ID + ".savedState"),
    os.path.join(HOME, "Library", "Caches", ID), os.path.join(HOME, "Library", "Caches", ID + ".ShipIt"),
    os.path.join(HOME, "Library", "HTTPStorages", ID),
    os.path.join(HOME, "Library", "Logs", "Podman Desktop"),
    os.path.join(HOME, "Library", "LaunchAgents", "io.podman_desktop.PodmanDesktop.plist"),
]
VM_IMAGE = re.compile(r"\.(raw|img|qcow2|zst|xz)$")
CHANGING = {"yes": False}


def do(argv: list[str]) -> None:
    """Run argv when --yes was given; otherwise say what would run."""
    if CHANGING["yes"]:
        print("+ " + " ".join(argv))
        run(argv, 300)
    else:
        print("would run: " + " ".join(argv))


def free() -> str:
    st = os.statvfs("/")
    return f"{st.f_bavail * st.f_frsize / 1073741824:.1f} GB free"


def du(path: str) -> str:
    _, out, _ = run(["du", "-sh", path], 60)
    return out.split("\t")[0].strip() if out else "?"


def main(argv: list[str]) -> int:
    yes, machines_flag = "--yes" in argv, "--machines" in argv
    if any(a not in ("--yes", "--machines") for a in argv):
        print(__doc__.split("\n\n")[0] + "\n\nusage: remove_podman_desktop.py [--yes] [--machines]", file=sys.stderr)
        return 2
    if platform.system() != "Darwin":
        print("macOS only", file=sys.stderr)
        return 2
    CHANGING["yes"] = yes
    print(f"disk: {free()}")

    print("== Podman Desktop")
    if run(["pgrep", "-f", "Podman Desktop.app"], 15)[0] == 0:
        print("running")
    cask = have("brew") and run(["brew", "list", "--cask", "podman-desktop"], 60)[0] == 0
    if cask:
        print("Homebrew cask: podman-desktop (auto-updates, and 'brew bundle' reinstalls it from a Brewfile that lists it)")
    for b in (os.path.join(HOME, "Brewfile"), os.path.join(HOME, ".Brewfile"),
              os.path.join(HOME, ".config", "homebrew", "Brewfile")):
        if not os.path.isfile(b):
            continue
        with open(b) as fh:
            if '"podman-desktop"' in fh.read():
                print(f"listed in {b}: remove that line too")
    for f in FILES:
        if os.path.exists(f):
            print(f"  {du(f)}\t{f}")

    print("== podman machines (Podman Desktop starts podman-machine-default)")
    _, listed, _ = run(["podman", "machine", "list", "--format", "{{.Name}}"], 60)
    machines = [m.strip().rstrip("*") for m in listed.splitlines() if m.strip()]
    for m in machines:
        print(f"  {m}")
    if os.path.isdir(MD):
        for root, _, files in os.walk(MD):
            for name in files:
                if VM_IMAGE.search(name):
                    p = os.path.join(root, name)
                    print(f"  {du(p)}  {p}")
    if not machines:
        _, procs, _ = run(["pgrep", "-fl", "vfkit|krunkit"], 15)
        for line in procs.splitlines():
            print(f"  running with no machine: {line}")
    _, opened, _ = run(["lsof", "+L1", "-nP"], 60)
    deleted = set()
    for ln in opened.splitlines():
        x = ln.split()
        if len(x) > 1 and re.search(r"\.raw|\.img|podman", ln, re.IGNORECASE):
            deleted.add(f"{x[0]} pid {x[1]}: {x[-1]}")
    deleted = sorted(deleted)
    if deleted:
        print("deleted while open (still taking space; stop these processes):")
        for d in deleted:
            print(f"  {d}")

    if not yes:
        print()
        print("nothing changed: rerun with --yes to remove Podman Desktop" + (", and --machines to remove the machines too"
                                                                         if machines else ""))
        return 0

    print("== removing")
    # quit first: a running app restarts its machine and rewrites its files
    run(["osascript", "-e", f'quit app id "{ID}"'], 30)
    time.sleep(2)
    run(["pkill", "-f", "Podman Desktop.app"], 15)
    if machines_flag:
        # remove machines before their files: a running VM keeps writing to a deleted disk image
        for m in machines:
            do(["podman", "machine", "stop", m])
            do(["podman", "machine", "rm", "-f", m])
        for c in glob.glob(os.path.join(MD, "*", "cache")):
            if os.path.isdir(c):
                do_remove(c)
    if cask:
        do(["brew", "uninstall", "--cask", "--zap", "podman-desktop"])
    for f in FILES:
        if os.path.exists(f):
            do_remove(f)
    time.sleep(3)
    print(f"disk: {free()} (the virtualization service can take a minute to give the space back)")
    return 0


def do_remove(path: str) -> None:
    """rm -rf of one path, when --yes (the path is never empty: it's built from HOME or the fixed list)."""
    if not path or path in ("/", HOME):
        raise ValueError(f"refusing to remove {path!r}")
    if CHANGING["yes"]:
        print(f"+ rm -rf {path}")
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path, ignore_errors=True)
        else:
            os.remove(path)
    else:
        print(f"would run: rm -rf {path}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
