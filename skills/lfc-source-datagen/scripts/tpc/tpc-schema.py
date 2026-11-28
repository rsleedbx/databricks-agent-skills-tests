#!/usr/bin/env python3
"""tpc-schema.py <tpcb|tpcc|tpch|tpcds|tpce|tpcdi> [SF]: prints the workload's tables as MySQL DDL, with the lfc tags in the
comments (cast.sql): lfc:rows= target, lfc:v={...} a column's value as an integer formula in x.n, lfc:n={...} n from a row
(for composite keys, key formulas, and tables without a key), lfc:by={...} the column n rises with. Keys, foreign keys, and
checks are the spec's; a table the spec gives no primary key has none. lfc-source-statschema's ddl-to-schema.py turns
it into schema.yaml, and its ddl.py into any engine's DDL. HISTORY (env): TPC-B's history rows.
    tpc-schema.py tpch 0.1 | python3 ../../../lfc-source-statschema/scripts/ddl-to-schema.py --dialect mysql > schema.yaml
  tpcb and tpch are written from the spec in Python (lfc_source_datagen/tpc_schema.py, which a notebook imports too).
  tpcc, tpcds, tpce and tpcdi come from statschema's canonical schemas (schemas/<workload>.yaml): its shape.py sets each
  table's rows at SF (row_count_per_sf x SF; fixed tables stay as they are) and writes the tags, its ddl.py writes MySQL.
  A text column's word-list tag (lfc:e) longer than MySQL's 1,024-character comment goes without its tag: the planner's
  default text cast, as for TPC-H's text columns (TPC-DS: 11 columns, TPC-E: 7), so the DDL has no side table (ddl.py's
  lfc_seed.tags) and every tag sits in its comment. Timestamps stay LFC_EPOCH(col) and LFC_DAYS(col), which each
  engine's ddl.py transpiles. Formulas use only integer arithmetic, DIV, %, and CASE, which transpile exactly.
Runs statschema's scripts with this Python (they need pyyaml and sqlglot, as their own headers say); no uv.
From Python: schema_text(workload, sf) -> the DDL text (raises ValueError).
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))              # lfc_source_datagen, beside the tpc/ folder
STATSCHEMA = os.path.join(HERE, "..", "..", "..", "lfc-source-statschema")
USAGE = "usage: tpc-schema.py <tpcb|tpcc|tpch|tpcds|tpce|tpcdi> [SF]"


def _statschema(w, sf):
    """statschema's shape then ddl for a canonical TPC schema, as MySQL DDL with the portable forms put back."""
    ss = os.path.join(STATSCHEMA, "scripts")
    schema = os.path.join(STATSCHEMA, "schemas", f"{w}.yaml")
    with tempfile.TemporaryDirectory(prefix="tpc-schema-") as tmp:
        yml = os.path.join(tmp, "shaped.yaml")
        p = subprocess.run([sys.executable, os.path.join(ss, "shape.py"), schema, "--sf", str(sf), "--expected", os.devnull],
                           capture_output=True, text=True, check=False)
        if p.returncode != 0:
            raise ValueError(f"{w} from statschema failed")
        with open(yml, "w") as f:
            f.write(p.stdout)
        p = subprocess.run([sys.executable, os.path.join(ss, "ddl.py"), yml, "--dialect", "mysql"], capture_output=True,
                           text=True, check=False)
        if p.returncode != 0:
            raise ValueError(f"{w} from statschema failed")
        ddl = p.stdout
    # ddl.py writes LFC_EPOCH and LFC_DAYS as MySQL's TIMESTAMPDIFF and DATEDIFF, which no other engine's planner reads back:
    # put the portable forms back (each engine's ddl.py writes its own); drop the side table's statements and the 'lfc:ref'
    # stubs that stand in for the long tags
    out = []
    for line in ddl.splitlines():
        line = re.sub(r"TIMESTAMPDIFF\(SECOND, ''1970-01-01 00:00:00'', ([A-Za-z_][A-Za-z_0-9]*)\)", r"LFC_EPOCH(\1)", line)
        line = re.sub(r"DATEDIFF\(([A-Za-z_][A-Za-z_0-9]*), ''1970-01-01''\)", r"LFC_DAYS(\1)", line)
        if "lfc_seed" in line:
            continue
        line = line.replace(" COMMENT 'lfc:ref'", "", 1)
        line = line.replace(" lfc:ref'", "'", 1)
        out.append(line)
    return "\n".join(out) + "\n"


def schema_text(w, sf=1):
    """The workload's MySQL DDL with the lfc tags at scale factor sf (raises ValueError when it can't be written)."""
    if w in ("tpcb", "tpch"):
        from lfc_source_datagen.tpc_schema import tpc_schema
        return tpc_schema(w, float(sf), os.environ.get("HISTORY"))
    if w in ("tpcc", "tpcds", "tpce", "tpcdi"):
        return _statschema(w, sf)
    raise ValueError(f"unknown workload: {w}")


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    if not a:
        print(USAGE, file=sys.stderr)
        return 2
    try:
        sys.stdout.write(schema_text(a[0], a[1] if len(a) > 1 else 1))
    except ValueError as e:
        print(f"tpc-schema.py: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
