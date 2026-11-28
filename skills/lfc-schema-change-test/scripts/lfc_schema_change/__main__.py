"""python3 -m lfc_schema_change [--profile <name>] <scenario> (scenarios/<scenario with _>.py); exit 0, 1 (a claim failed), 2 (set-up)."""
import importlib
import sys
from pathlib import Path

if not __package__:                               # run as a folder: the package's parent on the path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lfc_schema_change import scenario

args = sys.argv[1:]
if args[:1] == ["--profile"] and len(args) > 1:   # the laptop's workspace (in a workspace: leave it out)
    import os
    os.environ["PROFILE"], args = args[1], args[2:]
names = sorted(p.stem.replace("_", "-") for p in (Path(__file__).parent / "scenarios").glob("[a-z]*.py"))
if len(args) != 1 or args[0] not in names:
    print(f"python3 -m lfc_schema_change [--profile <name>] <{'|'.join(names)}>", file=sys.stderr)
    sys.exit(2)
sys.exit(scenario.main(importlib.import_module(f"lfc_schema_change.scenarios.{args[0].replace('-', '_')}")))
