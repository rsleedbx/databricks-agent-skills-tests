"""python3 -m lfc_schema_change <scenario> (scenarios/<scenario with _>.py); exit 0, 1 (a claim failed), 2 (set-up)."""
import importlib
import sys
from pathlib import Path

if not __package__:                               # run as a folder: the package's parent on the path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lfc_schema_change import scenario

names = sorted(p.stem.replace("_", "-") for p in (Path(__file__).parent / "scenarios").glob("[a-z]*.py"))
if len(sys.argv) != 2 or sys.argv[1] not in names:
    print(f"python3 -m lfc_schema_change <{'|'.join(names)}>", file=sys.stderr)
    sys.exit(2)
sys.exit(scenario.main(importlib.import_module(f"lfc_schema_change.scenarios.{sys.argv[1].replace('-', '_')}")))
