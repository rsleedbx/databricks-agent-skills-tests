#!/usr/bin/env python3
"""inventory.py <profile>...: every UC connection in each workspace, one tab-separated line each:
  profile  name  connection_type  host  port  owner  created_at (epoch ms)  (passwords are never returned by the API)
A profile whose list fails prints one line saying so (check its login). Connections are read through lfc_rest.Rest.
"""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest

CONNECTIONS = "/api/2.1/unity-catalog/connections"


def text(value) -> str:
    return "" if value is None else str(value)


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.split("\n\n")[0] + "\n\nusage: inventory.py <profile>...", file=sys.stderr)
        return 2
    for profile in argv:
        try:
            api = Rest.connect(profile)
            conns = list(api.pages(CONNECTIONS, "connections"))
        except Exception:  # noqa: BLE001
            print(f"{profile}\t(failed to list: check the profile's login)")
            continue
        for c in conns:
            opts = c.get("options") or {}
            print("\t".join([profile, text(c.get("name")), text(c.get("connection_type")), text(opts.get("host")),
                             text(opts.get("port")), text(c.get("owner")), text(c.get("created_at"))]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
