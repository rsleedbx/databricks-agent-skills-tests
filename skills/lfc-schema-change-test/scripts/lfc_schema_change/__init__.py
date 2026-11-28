"""lfc_schema_change: Lakeflow Connect scenario tests on a SQL Server source, in Python (scenario.Run: the shared steps;
kept: a finished run kept or deleted; scenarios/: one module per scenario). python3 -m lfc_schema_change <scenario>."""

import sys
from pathlib import Path

SKILLS = Path(__file__).resolve().parents[3]
REPO = SKILLS.parent
for _dep in ("lfc-rest", "lfc-change-source", "lfc-source-contract", "lfc-pipeline-watch"):   # a checkout's siblings
    if str(SKILLS / _dep / "scripts") not in sys.path:
        sys.path.insert(0, str(SKILLS / _dep / "scripts"))
