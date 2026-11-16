#!/usr/bin/env python3
"""placement.py: where each table goes, from a placement file (composite topology), as one TSV row a table.

usage: placement.py <placement.yaml> <tables file, one name a line> > placement.tsv

placement.yaml: groups, tried in order; a table goes to the first that names it (tables) or matches it (match, a
regular expression over the whole name). A table no group takes is an error.

  groups:
    - name: busy
      tables: [customers, orders]
      producer: {servers: 1}            # Debezium Servers for this group's tables (crc32(name) % servers)
      bronze: own                       # own: cdc_log_<table>, written by the fan-out from cdc_log; shared: cdc_log
      silver: {mode: pipeline}          # pipeline (AUTO CDC) or merge (merge-silver.py jobs)
    - name: quiet
      match: ".*"
      producer: {servers: 2}
      bronze: shared
      silver: {mode: merge, jobs: 2}    # jobs: merge-silver.py jobs for the group (crc32(name) % jobs)

Output columns: table, group, bronze (own | shared), silver (pipeline | merge), server (a global index over every
group's servers, in group order), silver shard, silver shards (the group's jobs; 1 for a pipeline).
"""
import os
import re
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # canonical_sql.py sits next to this file
from canonical_sql import shard_of


def main():
    with open(sys.argv[1]) as f:
        groups = yaml.safe_load(f)["groups"]
    with open(sys.argv[2]) as f:
        tables = [t.strip() for t in f if t.strip()]
    base, starts = 0, []
    for g in groups:
        starts.append(base)
        base += int((g.get("producer") or {}).get("servers", 1))
    out, missing = [], []
    for t in tables:
        for gi, g in enumerate(groups):
            if t in (g.get("tables") or []) or (g.get("match") and re.fullmatch(g["match"], t)):
                servers = int((g.get("producer") or {}).get("servers", 1))
                silver = g.get("silver") or {}
                mode = silver.get("mode", "merge")
                jobs = int(silver.get("jobs", 1)) if mode == "merge" else 1
                out.append([t, g["name"], g.get("bronze", "shared"), mode, str(starts[gi] + shard_of(t, servers)),
                            str(shard_of(t, jobs)), str(jobs)])
                break
        else:
            missing.append(t)
    if missing:
        sys.exit(f"placement: no group takes {', '.join(missing[:5])}{' ...' if len(missing) > 5 else ''}")
    for r in out:
        print("\t".join(r))
    counts = {}
    for r in out:
        counts[r[1]] = counts.get(r[1], 0) + 1
    print(f"-- {len(out)} tables, {base} producer server(s): " + ", ".join(f"{g} {n}" for g, n in counts.items()), file=sys.stderr)


if __name__ == "__main__":
    main()
