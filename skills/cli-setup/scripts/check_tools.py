#!/usr/bin/env python3
"""check_tools.py [all|base|local|aws|azure|gcp|clients]: which of the lfc-source-* skills' tools are installed, their
versions, and the install command for the missing ones (Homebrew on macOS; on Linux the distribution's package or the
vendor's installer, as in lfc-source-setup "Tools"). Lima is checked on macOS only: Linux runs Podman natively.
Exits 1 if anything asked for is missing. Default: base. It installs nothing.
"""
from __future__ import annotations

import os
import platform
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from laptop import have, run

# (group, command, Homebrew install args, Linux install, version args, macOS only)
CHECKS = [
    ("base", "databricks", "databricks/tap/databricks",
     "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh", ["-v"], False),
    ("base", "uv", "uv", "curl -LsSf https://astral.sh/uv/install.sh | sh", ["--version"], False),
    ("base", "jq", "jq", "apt install jq (or dnf install jq)", ["--version"], False),
    # the Go yq (mikefarah), not the Python wrapper
    ("base", "yq", "yq", "the binary from https://github.com/mikefarah/yq/releases", ["--version"], False),
    ("base", "duckdb", "duckdb", "the binary from https://github.com/duckdb/duckdb/releases", ["--version"], False),
    ("local", "podman", "podman", "apt install podman (or dnf install podman)", ["--version"], False),
    ("local", "limactl", "lima", "", ["--version"], True),
    # keg-only: add $(brew --prefix libpq)/bin to PATH
    ("clients", "psql", "libpq", "apt install postgresql-client", ["--version"], False),
    # keg-only: add $(brew --prefix mysql-client)/bin to PATH
    ("clients", "mysql", "mysql-client", "apt install mysql-client", ["--version"], False),
    # Microsoft's Go sqlcmd (the older ODBC one works too)
    ("clients", "sqlcmd", "sqlcmd", "the binary from https://github.com/microsoft/go-sqlcmd/releases", ["-?"], False),
    ("clients", "mongosh", "mongosh", "https://www.mongodb.com/try/download/shell", ["--version"], False),
    ("aws", "aws", "awscli",
     "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html", ["--version"], False),
    ("azure", "az", "azure-cli", "https://learn.microsoft.com/cli/azure/install-azure-cli-linux", ["--version"], False),
    ("gcp", "gcloud", "--cask gcloud-cli", "https://cloud.google.com/sdk/docs/install", ["--version"], False),
]


def version_line(cmd: str, args: list[str]) -> str:
    """The first line with a version number in the tool's output (warnings left out), cut to 70 characters."""
    _, out, err = run([cmd, *args], timeout=30)
    for line in (out + err).splitlines():
        if "warning" in line.lower():
            continue
        if re.search(r"[0-9]+\.[0-9]+", line):
            return line[:70]
    return ""


def main(argv: list[str]) -> int:
    want = argv[0] if argv else "base"
    darwin = platform.system() == "Darwin"
    missing: list[str] = []
    other: list[str] = []
    for group, cmd, pkg, lin, ver, mac_only in CHECKS:
        if not (want in ("all", group) or (want == "local" and group == "base")):
            continue
        if mac_only and not darwin:
            continue
        if have(cmd):
            print(f"{cmd:<11} {'ok':<8} {version_line(cmd, ver)}")
        elif darwin:
            print(f"{cmd:<11} {'MISSING':<8} brew install {pkg}")
            missing.append(pkg)
        else:
            print(f"{cmd:<11} {'MISSING':<8} {lin}")
            other.append(cmd)
    if missing:
        print()
        print(("install: brew install " + " ".join(missing)).replace(" --cask gcloud-cli", ""))
        if any("cask" in m for m in missing):
            print("         brew install --cask gcloud-cli")
        return 1
    return 0 if not other else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
