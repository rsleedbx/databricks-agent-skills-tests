---
name: podman-desktop-remove
description: "Remove Podman Desktop from a Mac so it stops starting its own podman machine VM, whose disk image fills the laptop, while keeping the podman CLI and the Lima VMs the tests use. Reports first (app, Homebrew cask, files, machines and their disk use, deleted-but-open VM disks), removes only when asked. Use when the laptop's disk keeps filling, when Podman Desktop is installed next to Lima, or before setting up lfc-source-podman."
parent: lfc-source-podman
---

# Remove Podman Desktop, keep Podman and Lima

The tests run their databases in a Lima VM (**lfc-source-podman**) and drive it with the `podman` CLI. Podman Desktop isn't needed for that, and it gets in the way:

- **It starts a second VM.** On launch it starts (or offers to create) `podman-machine-default`, with a disk image under `~/.local/share/containers/podman/machine/<provider>/` that grows as containers write and never shrinks. Each provider (`applehv`, `libkrun`) also keeps its own downloaded VM image in `cache/`.
- **It comes back.** The Homebrew cask auto-updates, a `brew bundle` reinstalls it from a Brewfile that lists it, and the app recreates its folders when started. Dragging the app to the Trash leaves the cask record and the files behind.
- **Deleting its folders while the VM runs frees nothing**: the VM keeps writing to the deleted image (see **lfc-source-podman**, "Remove the VM, and get the laptop's disk back").

## Run it

```bash
S=skills/podman-desktop-remove/scripts
python3 $S/remove_podman_desktop.py                     # report only: changes nothing
python3 $S/remove_podman_desktop.py --yes               # quit the app, brew uninstall --cask --zap, delete its files
python3 $S/remove_podman_desktop.py --yes --machines    # also stop and remove every podman machine, and the VM image caches
```

The report lists the app if it's running, the cask, any Brewfile that lists it, each leftover file with its size, the podman machines and their disk images, `vfkit`/`krunkit` processes running with no machine, and VM disk files that were deleted while still open (`lsof +L1`). It prints free space before and after.

**`--machines` deletes the databases in those VMs.** Ask the user before passing it, and check `podman machine list` first: a machine created on purpose (for example for **lfc-source-datagen** calibration) goes too. Without it, the machines stay and the report shows what they take.

Kept: the `podman` CLI (`brew install podman`), Lima and its VMs, and `~/.config/containers` (the system connections to Lima and remote hosts). Not touched: `podman-mac-helper` (`/Library/LaunchDaemons/com.github.containers.podman.helper-<user>.plist`), which `podman machine` installs, not Podman Desktop; removing it needs `sudo podman-mac-helper uninstall`.

## After

- `df -h /` a minute later: the space comes back as the virtualization service exits.
- If the report still shows deleted-but-open disk files, stop those processes (`kill <pid>`; for Lima, `limactl stop --force <name>`).
- Remove `podman-desktop` from any Brewfile the report named, or the next `brew bundle` reinstalls it.

## Tested

2026-10-18, macOS 26 on Apple Silicon: the app had been dragged out of `/Applications`, but Homebrew still listed the `podman-desktop` cask, and starting it earlier that day had recreated `~/.local/share/containers/podman-desktop` and a `libkrun` machine folder with a `machine-start.lock`. The report found the cask and six leftover paths (about 200 KB), no machines, and no deleted-but-open disks, with a Lima VM running alongside.
