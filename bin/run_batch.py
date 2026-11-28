#!/usr/bin/env python3
"""run_batch.py <prompts-file>...: run several prompt files in parallel, each in its own scratch WORK_DIR.

Usage:
  source env.sh
  bin/run_batch.py prompts/lfc-matrix/qbc-*.txt

Run tests that create pipelines on the same connection in separate batches, so two sessions don't pick the same
default pipeline name at once.

Results: $RESULTS_ROOT/<name>-results/<tag>.json (summarize with ./summarize.py), logs: $RESULTS_ROOT/<name>.log
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALLOWED_BASE = "Read,Write,Edit,Glob,Grep,WebFetch,Bash(databricks:*),Bash(ls:*),Bash(mkdir:*),Bash(jq:*)"
# conn.json holds a generated password (Lakebase demo), so allow deleting it by every path a session might use
CONN_PATHS = ("conn.json", "{w}/conn.json", "/private{w}/conn.json", "scratch/conn.json", "{w}/scratch/conn.json",
              "/private{w}/scratch/conn.json")


def allowed_tools(work: str) -> str:
    rules = [ALLOWED_BASE]
    for p in CONN_PATHS:
        p = p.format(w=work)
        rules += [f"Bash(rm {p})", f"Bash(rm -f {p})"]
    return ",".join(rules)


def connection_args() -> list[str]:
    """--connection N for each LFC_*_CONNECTION that is set: keep those and their foreign catalogs for a month."""
    out = []
    for name in sorted(os.environ):
        if name.startswith("LFC_") and name.endswith("_CONNECTION") and os.environ[name]:
            out += ["--connection", os.environ[name]]
    return out


def last_result_line(log: Path) -> str:
    lines = [ln for ln in log.read_text(errors="replace").splitlines() if "passed" in ln or "FAILED" in ln]
    return lines[-1] if lines else ""


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: run_batch.py <prompts-file>...")
        return 1
    out_root = Path(os.environ.get("RESULTS_ROOT") or "/tmp")
    out_root.mkdir(parents=True, exist_ok=True)
    # keep the target catalog and the foreign catalogs on the env's connections for another month (RemoveAfter tag)
    catalog = [os.environ["LFC_CATALOG"]] if os.environ.get("LFC_CATALOG") else []
    subprocess.run([sys.executable, "-I", str(ROOT / "skills/lfc-workspace-setup/scripts/keep_catalogs.py"),
                    os.environ.get("PIPELINE_WS") or "DEFAULT", *connection_args(), *catalog])

    jobs = []
    names = []
    for f in argv:
        path = Path(f).resolve()
        name = path.name[:-4] if path.name.endswith(".txt") else path.name
        names.append(name)
        work = out_root / f"{name}-work"
        if work.exists():
            shutil.rmtree(work)
        shutil.rmtree(out_root / f"{name}-results", ignore_errors=True)
        env = {**os.environ, "WORK_DIR": str(work), "CLAUDE_ALLOWED_TOOLS": allowed_tools(str(work))}
        log = (out_root / f"{name}.log").open("w")
        jobs.append(subprocess.Popen([sys.executable, str(ROOT / "run.py"), os.environ.get("SKILLS_REPO", ""),
                                      str(path)],
                                     stdout=log, stderr=subprocess.STDOUT, env=env))
    for j in jobs:
        j.wait()
    for name in names:
        print(f"== {name}: {last_result_line(out_root / f'{name}.log')}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
