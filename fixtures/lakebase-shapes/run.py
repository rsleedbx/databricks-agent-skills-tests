#!/usr/bin/env python3
"""fixtures/lakebase-shapes/run.py: product-level shape test (no agent): create the shape pipelines from
pipelines/*.json, run them, apply mutate.sql, run again, and print verify.sql. See README.md for the expected results.

Usage (after `LFC_ENV=<name> source env.sh` and seeding, see README.md):
  fixtures/lakebase-shapes/run.py

Needs: LFC_CATALOG, LFC_SHAPES_CONNECTION, LFC_LAKEBASE_PROJECT, LFC_NAME_PREFIX, PIPELINE_WS (the profile).
Workspace calls through lfc_rest (the pipelines and schemas REST API, the SQL statement API for verify.sql); the
Lakebase step runs mutate.sql as the project owner (lfc_source_contract.lakebase.owner_login, psycopg), the way psql
did. Results go to ${RESULTS_ROOT:-/tmp}/shapes.
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path

D = Path(__file__).resolve().parent
ROOT = D.parent.parent
sys.path.insert(0, str(ROOT / "skills" / "lfc-rest" / "scripts"))
sys.path.insert(0, str(ROOT / "skills" / "lfc-change-source" / "scripts"))
sys.path.insert(0, str(ROOT / "skills" / "lfc-source-contract" / "scripts"))
sys.path.insert(0, str(ROOT))
import psycopg  # noqa: E402
from lfc_rest import Rest  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_source_contract import lakebase  # noqa: E402
from run import fill  # noqa: E402  (the runner's ${VAR} fill: an unset variable stops it)

SCHEMAS = ["shapes_scd1", "shapes_scd2", "shapes_append", "shapes_probe"]
PIPES = ["scd1", "scd2", "append", "probe_scd1_nokey", "probe_scd2_nokey_dups", "probe_nokey_nocursor"]
TERMINAL = ("COMPLETED", "FAILED", "CANCELED")


def run_all(api: Rest, out: Path) -> None:
    """Start an update of each pipeline in ids.txt, then print each one's state every 20 s until all are done."""
    lines = []
    for line in (out / "ids.txt").read_text().splitlines():
        k, pid = line.split()
        lines.append((k, pid, api.post(f"/api/2.0/pipelines/{pid}/updates", {}).get("update_id", "")))
    (out / "updates.txt").write_text("".join(f"{k} {pid} {u}\n" for k, pid, u in lines))
    while True:
        left, parts = False, []
        for k, pid, u in lines:
            st = api.get(f"/api/2.0/pipelines/{pid}/updates/{u}").get("update", {}).get("state", "")
            parts.append(f" {k}={st}")
            left = left or st not in TERMINAL
        if not left:
            print(time.strftime("%H:%M:%S", time.gmtime()) + "".join(parts))
            return
        time.sleep(20)


def main(argv: list[str]) -> int:
    profile = os.environ["PIPELINE_WS"]
    catalog = os.environ["LFC_CATALOG"]
    out = Path(os.environ.get("RESULTS_ROOT") or "/tmp") / "shapes"
    out.mkdir(parents=True, exist_ok=True)
    api = Rest.connect(profile)

    for s in SCHEMAS:
        try:
            api.post("/api/2.1/unity-catalog/schemas", {"name": s, "catalog_name": catalog})
        except Exception:  # noqa: BLE001  an existing schema is fine (the command's output was discarded before)
            pass
    ids = []
    for k in PIPES:
        body = json.loads(fill((D / "pipelines" / f"{k}.json").read_text()))
        (out / f"{k}.json").write_text(json.dumps(body, indent=2))
        try:
            pid = api.post("/api/2.0/pipelines", body).get("pipeline_id", "")
            ids.append(f"{k} {pid}")
            print(f"created {k} {pid}")
        except Exception as e:  # noqa: BLE001  a failed create is reported and the rest go on
            print(f"create FAILED {k}: {' '.join(str(e).splitlines()[:2])}")
    (out / "ids.txt").write_text("".join(f"{i}\n" for i in ids))

    print("### run 1")
    run_all(api, out)
    print("### mutate")
    lb = lakebase.owner_login(os.environ["LFC_LAKEBASE_PROJECT"], profile=profile)
    with psycopg.connect(host=lb["host"], port=5432, dbname="databricks_postgres", user=lb["user"],
                         password=lb["password"], sslmode="require", autocommit=True) as pg:
        pg.execute((D / "mutate.sql").read_text())
    print("### run 2")
    run_all(api, out)
    verify = fill((D / "verify.sql").read_text())
    (out / "verify.sql").write_text(verify)
    rows = Warehouse.pick(api).sql(verify).collect()
    csv.writer(sys.stdout).writerows(list(r) for r in rows)
    print(f"pipeline ids: {out / 'ids.txt'} (delete them when done)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
