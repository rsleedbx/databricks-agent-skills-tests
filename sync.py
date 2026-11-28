#!/usr/bin/env python3
"""sync.py: sync skill files and test files via a Databricks workspace file bridge (for sandboxes without git push).

Uploads and downloads individual files (not patches) to avoid notebook format corruption. The workspace notebook
round-trip prepends a header line to every file; pull strips it automatically.

Usage:
  source env.sh
  ./sync.py push [file ...]    # upload skills-repo files to the workspace
  ./sync.py pull [file ...]    # download skills-repo files from the workspace
  ./sync.py push-tests         # upload this repo's test files
  ./sync.py pull-tests         # download this repo's test files

With no file arguments, push/pull use the default skill file list. File arguments are paths relative to the skills repo.

Environment variables (set via env.sh / env.local.sh):
  SKILLS_REPO        Path to the databricks-agent-skills checkout (default: ../databricks-agent-skills)
  OMNIGENT_WS        Databricks CLI profile for the workspace used as the file bridge
  DATABRICKS_WS_USER Workspace user (default: the profile's current user)
"""
from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "bin"))
sys.path.insert(0, str(HERE / "skills" / "lfc-rest" / "scripts"))
from lfc_rest import Rest  # noqa: E402
import workspace_files as wf  # noqa: E402

USAGE = """Usage: sync.py {push|pull|push-tests|pull-tests} [file ...]

  push [file ...]       Upload skills-repo files to the workspace (default: LFC skill + CLAUDE.md + manifest.json)
  pull [file ...]       Download skills-repo files from the workspace
  push-tests            Upload this repo's test files
  pull-tests            Download this repo's test files"""


def default_files(skills_repo: Path) -> list[str]:
    """The source files that matter (no generated plugin bundles): CLAUDE.md, manifest.json and the LFC skill's
    SKILL.md and references (those that exist)."""
    lfc = skills_repo / "skills" / "databricks-lakeflow-connect"
    skill = ["skills/databricks-lakeflow-connect/SKILL.md"] if (lfc / "SKILL.md").is_file() else []
    refs = sorted(glob.glob(str(lfc / "references" / "*.md")))
    return ["CLAUDE.md", "manifest.json"] + skill + [os.path.relpath(p, skills_repo) for p in refs]


def test_files(here: Path) -> list[str]:
    """This repo's test files, relative to it (those that exist, sorted)."""
    pats = ["README.md", "env.sh", "env.example.sh", "run.py", "summarize.py", "sync.py", "bin/*.py", "bin/*.sh",
            "prompts/*.txt", "prompts/*/*.txt"]
    found = {os.path.relpath(p, here) for pat in pats for p in glob.glob(str(here / pat))}
    return sorted(found)


def ws_base(api: Rest) -> str:
    user = os.environ.get("DATABRICKS_WS_USER") or api.get("/api/2.0/preview/scim/v2/Me").get("userName", "")
    return f"/Users/{user}/skill-files"


def do_push(api: Rest, base: Path, ws: str, files: list[str]) -> None:
    print(f"Pushing {len(files)} file(s) to {os.environ['OMNIGENT_WS']}:{ws} ...")
    for d in sorted({os.path.dirname(f"{ws}/{f}") for f in files}):
        try:
            wf.mkdirs(api, d)
        except Exception:  # noqa: BLE001  as before: a folder that can't be made shows in the import
            pass
    for f in files:
        if not (base / f).is_file():
            print(f"  SKIP (not found): {f}")
            continue
        try:
            wf.put(api, f"{ws}/{f}", (base / f).read_bytes(), fmt="SOURCE", language="PYTHON", overwrite=True)
            print(f"  {f}")
        except Exception:  # noqa: BLE001
            print(f"  FAIL: {f}")
    print("Done.")


def strip_header(data: bytes) -> bytes:
    """tail -n +2: the notebook round-trip's first line goes."""
    return data.split(b"\n", 1)[1] if b"\n" in data else b""


def do_pull(api: Rest, base: Path, ws: str, files: list[str]) -> None:
    print(f"Pulling {len(files)} file(s) from {os.environ['OMNIGENT_WS']}:{ws} ...")
    for f in files:
        (base / f).parent.mkdir(parents=True, exist_ok=True)
        try:
            data = wf.get(api, f"{ws}/{f}", fmt="SOURCE")
        except Exception:  # noqa: BLE001  a file the workspace doesn't have is skipped, as before
            print(f"  SKIP (not in workspace): {f}")
            continue
        (base / f).write_bytes(strip_header(data))
        print(f"  {f}")
    print("Done.")


def main(argv: list[str]) -> int:
    skills_repo = Path(os.environ.get("SKILLS_REPO") or HERE.parent / "databricks-agent-skills").resolve()
    if not os.environ.get("OMNIGENT_WS"):
        print("set OMNIGENT_WS (see env.example.sh)", file=sys.stderr)
        return 1
    cmd, files = (argv[0] if argv else "push"), argv[1:]
    if cmd not in ("push", "pull", "push-tests", "pull-tests"):
        print(USAGE)
        return 1
    api = Rest.connect(os.environ["OMNIGENT_WS"])
    base_ws = ws_base(api)
    if cmd == "push":
        do_push(api, skills_repo, base_ws, files or default_files(skills_repo))
    elif cmd == "pull":
        do_pull(api, skills_repo, base_ws, files or default_files(skills_repo))
        print("")
        print(f"Next steps (in {skills_repo}):")
        print("  python3 scripts/skills.py generate")
        print("  python3 scripts/skills.py validate")
    elif cmd == "push-tests":
        do_push(api, HERE, f"{base_ws}/tests", files or test_files(HERE))
    else:
        do_pull(api, HERE, f"{base_ws}/tests", files or test_files(HERE))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
