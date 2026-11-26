#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml", "sqlglot", "databricks-sdk"]
# ///
"""introspect.py: a source database's schema.yaml and stats.yaml (statschema's format), read through a Unity Catalog
connection on a SQL warehouse: the catalog only (lfc_introspection.introspect).

usage: introspect.py --profile P --warehouse ID --connection C --engine E [--database D | --service S] --schema S --out DIR
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))   # the warehouse session
from lfc_introspection.introspect import introspect  # noqa: E402
from statschema.tools.canonical import write  # noqa: E402  (introspect put statschema on the path)


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for f in ("--profile", "--warehouse", "--connection", "--engine", "--schema", "--out"):
        a.add_argument(f, required=True)
    a.add_argument("--database"); a.add_argument("--service")
    x = a.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from lfc_change_source.engine import Warehouse
    wh = Warehouse(x.warehouse, WorkspaceClient(profile=x.profile))
    schema, stats = introspect(wh, x.connection, x.engine, x.database, x.schema, x.service)
    os.makedirs(x.out, exist_ok=True)
    write(schema, stats, os.path.join(x.out, "schema.yaml"), os.path.join(x.out, "stats.yaml"))
    print(f"{len(schema['tables'])} tables -> {x.out}/schema.yaml, stats.yaml", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
