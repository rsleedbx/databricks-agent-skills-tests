#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml"]
# ///
# uv run shape.py schema.yaml [--stats stats.yaml] [--sf 1] [--scale 1] [--keep-small 1000]
# A wrapper: the code is statschema/tools/shape.py (its header says what it does); from Python, statschema.tools.
import os
import runpy
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
runpy.run_path(os.path.join(HERE, "statschema", "tools", "shape.py"), run_name="__main__")
