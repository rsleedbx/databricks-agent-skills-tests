#!/usr/bin/env python3
"""bin/check_model_counts.py: the table counts lfc-pipeline-diagram draws for each statschema model match the models.

pipeline-diagram.py can't read statschema (the lakeflow layer doesn't reference the shape layer), so it keeps its own
count per model. This compares each with the number of entries under tables: in lfc-source-statschema
schemas/<model>.yaml, and checks every schema has a count. Exit 0 when they match, 1 with one line per mismatch.
--count <model>: print that model's table count.
Stdlib only.
"""
import importlib.util
import re
import sys
from pathlib import Path

R = Path(__file__).resolve().parent.parent
SCHEMAS = R / "skills/lfc-source-statschema/schemas"
spec = importlib.util.spec_from_file_location("diagram", R / "skills/lfc-pipeline-diagram/scripts/pipeline-diagram.py")
diagram = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagram)


def tables_in(path: Path) -> int:
    """Entries of the top-level tables: list (their indent is the first "- name:" after it)."""
    body = path.read_text().split("\ntables:", 1)[1]
    indent = re.search(r"^( *)- name:", body, re.M).group(1)
    return len(re.findall(rf"^{indent}- name:", body, re.M))


if sys.argv[1:2] == ["--count"]:   # one model's table count, for the checks that need it (bin/test_scripts.sh)
    print(tables_in(SCHEMAS / f"{sys.argv[2]}.yaml"))
    sys.exit(0)
problems = []
for path in sorted(SCHEMAS.glob("*.yaml")):
    model, n = path.stem, tables_in(path)
    drawn = diagram.MODELS.get(model, (None,))[0]
    if drawn != n:
        problems.append(f"{model}: schemas/{path.name} has {n} tables, pipeline-diagram.py MODELS says {drawn}")
for p in problems:
    print(p)
sys.exit(1 if problems else 0)
