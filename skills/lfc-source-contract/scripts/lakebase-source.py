#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.81", "psycopg[binary]>=3.2"]
# ///
"""lakebase-source.py: a Lakebase project as Lakeflow Connect's PostgreSQL source, in one step, from a checkout.

  uv run lakebase-source.py --project P --role R --connection C [--database D] [--branch B] [--profile X]
                            [--create-project [--pg-version 17] [--suspend-seconds 300]] [--contract source.json]

The code is lfc_source_contract/lakebase.py (its docstring has every step); a workspace job runs the same code as the
installed package's entry point lfc-lakebase-source (SKILL.md, "Lakebase as the source").
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lfc_source_contract.lakebase import main

sys.exit(main())
