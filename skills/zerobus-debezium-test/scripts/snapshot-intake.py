"""snapshot-intake.py: each source's snapshot into the snapshot contract (lfc-change-source intake.py), for silver.

  table  the tables a query-based pipeline wrote (qbc.py) in <catalog>.<schema>: used as they are, rows counted
  files  the export in the run's Volume, <volume>/<table>/<prefix>/, copied once into <catalog>.<schema>.<table>,
         each column typed as the canonical schema says (canonical_sql: lfc_medallion's types, Debezium's encodings)

One --source a source: <prefix>=<catalog>.<schema>=<its canonical schema.yaml>. Every table prints a line as it lands
(one at a time), and --out gets _snapshot.tsv: table, prefix, "table", rows (merge-silver.py loads the tables with rows).

    python3 -I snapshot-intake.py --profile <p> --warehouse <id> --mode files --volume /Volumes/c/s/snapshot \\
      --format csv --csv '{"nullValue": "\\\\N"}' --source pg1=c.s_snap1=schema-1.yaml --out _snapshot.tsv
"""
from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from canonical_sql import TEMPORAL, ctype, spark_type  # noqa: E402  (also puts lfc-change-source on sys.path)
from lfc_change_source import intake  # noqa: E402


def columns(cols: list) -> list:
    """Canonical columns as intake's: name, Databricks type, temporal kind, precision, default."""
    return [{"name": c["name"], "type": spark_type(c), "kind": ctype(c) if ctype(c) in TEMPORAL else None,
             "precision": c.get("precision"), "default": c.get("default")} for c in cols]


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Each source's snapshot into the snapshot contract's tables")
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--mode", choices=["table", "files"], required=True)
    ap.add_argument("--volume", help="files: the Volume with <table>/<prefix>/")
    ap.add_argument("--format", default="parquet", help="files: parquet or csv")
    ap.add_argument("--csv", default="{}", help="files: read_files options for CSV, JSON")
    ap.add_argument("--binhex", action="store_true", help="files: binary as hex text")
    ap.add_argument("--source", action="append", required=True, help="<prefix>=<catalog>.<schema>=<schema.yaml>")
    ap.add_argument("--out", required=True, help="_snapshot.tsv to write")
    a = ap.parse_args(argv)
    import yaml
    from databricks.sdk import WorkspaceClient
    from lfc_change_source.engine import Warehouse
    wh = Warehouse(a.warehouse, WorkspaceClient(profile=a.profile))
    opts = {"header": "true", **json.loads(a.csv)} if a.format == "csv" else json.loads(a.csv)
    lines = []
    for spec in a.source:
        prefix, target, schema_yaml = spec.split("=", 2)
        with open(schema_yaml) as f:
            tables = {t["name"]: t["columns"] for t in yaml.safe_load(f)["tables"]}
        for t, cols in tables.items():
            name = f"{target}.{t}"
            got = (intake.table(wh, name) if a.mode == "table" else
                   intake.files(wh, f"{a.volume}/{t}/{prefix}", a.format, name, columns(cols), opts, a.binhex))
            print(json.dumps({"source": prefix, **got}), flush=True)
            lines.append(f"{t}\t{prefix}\ttable\t{got['rows']}\n")
    with open(a.out, "w") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()
