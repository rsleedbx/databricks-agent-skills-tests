#!/usr/bin/env python3
"""serverless-egress.py <cloud> <region> [--state FILE]: the serverless egress ranges a source's firewall must allow,
   from Databricks' published ip-ranges.json (outbound, IPv4, for one cloud and region: aws us-east-2, azure centralus,
   gcp us-central1). Prints when the file was published, then one CIDR per line.
   The list changes as often as every 30 days, and new ranges become active as soon as 60 days after they're published,
   so refresh at least monthly. With --state FILE: compare with the list saved there by the last run (lines starting
   "+" were added, "-" removed), then save the current list in FILE for next time. Exit 0; 1 if the file can't be read
   or has no ranges for that region; 3 if the list changed since FILE (update the firewall).
   Downloads the file with urllib (the shell script used curl); no Databricks call, no cloud CLI.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

URL = "https://www.databricks.com/networking/v1/ip-ranges.json"


def ranges(doc: dict, cloud: str, region: str) -> list[str]:
    """The outbound IPv4 ranges of one cloud and region, unique, sorted."""
    return sorted({c for x in doc.get("prefixes") or []
                   if x.get("type") == "outbound" and x.get("platform") == cloud and x.get("region") == region
                   for c in x.get("ipv4Prefixes") or []})


def saved(path: str) -> tuple[str, list[str]]:
    """(the saved file's first line, its CIDRs sorted), or ("", []) when it isn't there."""
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return "", []
    return (lines[0] if lines else ""), sorted(x for x in lines if x and not x.startswith("#"))


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    cloud, region = argv[0], argv[1]
    state = ""
    rest = argv[2:]
    while rest:
        if rest[0] == "--state" and len(rest) > 1:
            state = rest[1]
            rest = rest[2:]
        else:
            print(f"unknown {rest[0]}", file=sys.stderr)
            return 2
    try:
        with urllib.request.urlopen(URL, timeout=60) as r:
            doc = json.loads(r.read().decode())
    except (OSError, ValueError):
        print(f"can't download {URL}", file=sys.stderr)
        return 1
    ts = doc.get("timestampSeconds")
    now = ranges(doc, cloud, region)
    if not now:
        print(f"no outbound ranges for {cloud} {region} in {URL}", file=sys.stderr)
        return 1
    when = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d") if ts else "?"
    print(f"# {cloud} {region}: {len(now)} serverless egress ranges, ip-ranges.json published {when} "
          f"(timestampSeconds {ts if ts is not None else ''})")
    rc = 0
    if state and os.path.isfile(state):
        head, old = saved(state)
        if old == now:
            print(f"# unchanged since the list saved in {state} ({head.removeprefix('# ')})")
        else:
            rc = 3
            print(f"# CHANGED since {state}: add the + lines to the firewall now; remove the - lines once nothing uses them")
            for x in sorted(set(now) - set(old)):
                print(f"+ {x}")
            for x in sorted(set(old) - set(now)):
                print(f"- {x}")
    for x in now:
        print(x)
    if state:
        with open(state, "w") as f:
            f.write(f"# {cloud} {region} timestampSeconds={ts if ts is not None else ''} "
                    f"fetched={datetime.now(timezone.utc):%Y-%m-%d}\n")
            f.write("".join(x + "\n" for x in now))
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
