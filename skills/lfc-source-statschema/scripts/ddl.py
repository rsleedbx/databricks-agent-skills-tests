#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml", "sqlglot"]
# ///
# uv run ddl.py schema.yaml --dialect postgres > schema.sql
# A wrapper: the code is statschema/tools/ddl.py (its header says what it does); from Python, statschema.tools.
import os
import runpy
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
runpy.run_path(os.path.join(HERE, "statschema", "tools", "ddl.py"), run_name="__main__")
