#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml", "sqlglot"]
# ///
# uv run ddl-to-schema.py --dialect mysql < schema.sql > schema.yaml
# A wrapper: the code is statschema/tools/ddl_to_schema.py (its header says what it does); from Python, statschema.tools.
import os
import runpy
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
runpy.run_path(os.path.join(HERE, "statschema", "tools", "ddl_to_schema.py"), run_name="__main__")
