#!/usr/bin/env python3
"""dashboard.py: the routing dashboard (../dashboard/routing.lvdash.json) on one schema's routing tables: the latest
plan, the overrides, every round, introspection runs, and the change capture, snapshot and ingestion runs. Deployed
by lfc-log-silver's dashboard.deploy (created or updated by name, published).

    python3 -I dashboard.py --profile <p> --warehouse <id> --catalog <c> --schema <s>
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-log-silver", "scripts"))
from lfc_log_silver import dashboard  # noqa: E402

FILE = os.path.join(HERE, "..", "dashboard", "routing.lvdash.json")


def main(argv=None) -> None:
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for f in ("--profile", "--warehouse", "--catalog", "--schema"):
        a.add_argument(f, required=True)
    x = a.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    print(dashboard.deploy(WorkspaceClient(profile=x.profile), x.warehouse, x.catalog, x.schema, file=FILE,
                           title="FlowThrough routing"))


if __name__ == "__main__":
    main()
