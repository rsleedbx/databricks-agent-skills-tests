#!/usr/bin/env python3
"""size_query.py: the sizing query for one engine, as one statement: the engine's catalog SQL (<engine>.sql beside
this file) run through the Unity Catalog connection with remote_query. size.sh runs it with the Databricks CLI; in a
notebook or Genie Code, run it with spark.sql (the CLI doesn't run there):

    import sys; sys.path.insert(0, "<skills folder>/lfc-source-sizing/scripts")
    from size_query import remote_query
    display(spark.sql(remote_query("<connection>", "postgres", database="<database>")))

usage: size_query.py <connection> <engine> [--database D | --service S] [--file NAME]   (prints the statement)
"""
import argparse
import os
import re
import sys

# the statement and the per-engine queries live in lfc-introspection (lfc_introspection.catalog), which data
# generation and routing read too; this file is sizing's command line over them
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-introspection", "scripts"))
from lfc_introspection.catalog import ENGINES, HERE, NEEDS_DATABASE, SQL_FILE, SizeQueryError, remote_query  # noqa: E402,F401


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    a.add_argument("connection"); a.add_argument("engine")
    a.add_argument("--database"); a.add_argument("--service"); a.add_argument("--file")
    x = a.parse_args(argv)
    try:
        print(remote_query(x.connection, x.engine, x.database, x.service, x.file))
    except SizeQueryError as e:
        print(e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
