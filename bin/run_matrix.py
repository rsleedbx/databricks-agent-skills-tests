#!/usr/bin/env python3
"""run_matrix.py [batch ...]: run the full Lakeflow Connect matrix (see MATRIX.md) against one workspace.
LFC_SKIP_LAKEBASE=1 leaves the Lakebase tests out (a workspace with no Lakebase project for them).

Usage:
  LFC_ENV=<name> source env.sh
  bin/run_matrix.py                 # all batches, in order
  bin/run_matrix.py qbc fcqbc       # only some batches

Engines: MySQL, PostgreSQL, SQL Server and Oracle (the -mysql/-pg/-sqlserver/-oracle prompts). No MariaDB batch:
the MySQL connector's source-setup page lists MySQL 5.7 and later on RDS, Aurora, Azure, EC2 and Cloud SQL, and no
MariaDB (Databricks docs, lakeflow-connect/mysql-source-setup, checked 2026-10-27). Db2, Informix, CockroachDB and
YugabyteDB have no Lakeflow Connect connector: the CDC overview (lakeflow-connect/cdc-overview) lists four database
connectors. Test those engines with lfc-source-sizing and zerobus-debezium-test instead.
Batches run one after another so tests on the same connection never race for the same default pipeline name. Order
matters: qbc creates the Lakebase project and connection that fcqbc-lakebase and dml-lakebase use.

Read back, right away, what compute each pipeline of this run asked for and got: whether it started and reached the
source (the records' "compute" field). Sandbox workspaces can disappear within hours, and a classic update takes
about 8 minutes before it fails, so a pipeline still WAITING_FOR_RESOURCES here shows started=unknown: rerun the same
command (bin/compute_report.py).
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import compute_report  # noqa: E402
import run_batch  # noqa: E402

MATRIX = ROOT / "prompts" / "lfc-matrix"
ALL_BATCHES = ["qbc", "fcqbc", "icdc", "scdc", "dml"]


def batch_files(b: str) -> list[Path]:
    """The prompt files of one batch; LFC_SKIP_LAKEBASE drops the Lakebase ones; unknown batches raise."""
    if b in ("qbc", "fcqbc", "icdc"):
        files = [MATRIX / f"{b}-{x}.txt" for x in ("lakebase", "pg", "mysql", "sqlserver")]
        if os.environ.get("LFC_ORACLE_CONNECTION"):   # Oracle: when the env names its connections
            files.append(MATRIX / f"{b}-oracle.txt")
    elif b == "scdc":
        files = [MATRIX / f"scdc-{x}.txt" for x in ("lakebase", "pg", "mysql", "sqlserver")]
    elif b == "dml":
        files = [MATRIX / "dml-lakebase.txt"]
    else:
        raise ValueError(b)
    if os.environ.get("LFC_SKIP_LAKEBASE"):
        files = [f for f in files if not f.name.endswith("-lakebase.txt")]
    return files


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def readback(start: str) -> None:
    """What compute each pipeline of this run asked for and got: the table, saved beside the results."""
    out = Path(os.environ.get("RESULTS_ROOT") or "/tmp") / "compute.tsv"
    pws = os.environ.get("PIPELINE_WS") or "DEFAULT"
    prefix = os.environ["LFC_NAME_PREFIX"]
    print(f"### compute readback  {stamp()}  (serverless asked: {os.environ.get('LFC_SERVERLESS', '?')})")
    # the login may have expired by now: an account-token config for the workspace, when there is one
    ws = subprocess.run(["bash", str(ROOT / "skills/account-token-fallback/scripts/ws-token.sh"), pws],
                        stdout=subprocess.PIPE, text=True).stdout.strip()
    saved = os.environ.get("DATABRICKS_CONFIG_FILE")
    if ws:
        os.environ["DATABRICKS_CONFIG_FILE"] = ws
    try:
        table = compute_report.report(pws, f"{prefix}-", start)
    finally:
        if ws:
            if saved is None:
                os.environ.pop("DATABRICKS_CONFIG_FILE", None)
            else:
                os.environ["DATABRICKS_CONFIG_FILE"] = saved
    out.write_text("".join("\t".join(r) + "\n" for r in table))
    print(compute_report.aligned(table))
    print(f"saved {out}; again later: bin/compute_report.py {pws} {prefix}- {start}")


def main(argv: list[str]) -> int:
    batches = argv or ALL_BATCHES
    start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")   # for the compute readback at the end
    for b in batches:
        if b not in ALL_BATCHES:
            print(f"unknown batch: {b} (qbc, fcqbc, icdc, scdc, dml)")
            return 1
        files = batch_files(b)
        print(f"### batch {b}  {stamp()}")
        run_batch.main([str(f) for f in files])
    print(f"### done  {stamp()}  results: {os.environ.get('RESULTS_ROOT') or '/tmp'}")
    if os.environ.get("LFC_NAME_PREFIX"):
        readback(start)
    else:
        print("### compute readback skipped: LFC_NAME_PREFIX is unset")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
