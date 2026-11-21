"""lfc-medallion-pack: start a repo of n:1 modules, plan and render it, check a pack, list LOBs.

    lfc-medallion-pack init <dir> --name febar --archive <tests repo archive URL>   # the bundles, notebooks, project
    lfc-medallion-pack render <dir>/febar.yaml        # resources/<module>.yml, marts/<pack>/, source/plan.json
    lfc-medallion-pack check <pack name or folder>    # the marts against the model, offline
    lfc-medallion-pack lobs 10 --seed 7               # the LOBs and their 20-70-10 tiers
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import check as check_mod
from . import lobs as lobs_mod
from . import pack as pack_mod
from . import plan as plan_mod

TEMPLATES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
ARCHIVE = "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip"


def init(folder: str, name: str, archive: str) -> list[str]:
    """Write the templates into folder (a file already there is kept): both bundles, the source and verify notebooks,
    the n:1 pipeline file and <name>.yaml."""
    lobs_mod.NAME.match(name) or sys.exit(f"--name {name}: lower-case letters, digits and _")
    out = []
    for root, _, files in os.walk(TEMPLATES):
        for f in files:
            src = os.path.join(root, f)
            rel = os.path.relpath(src, TEMPLATES)
            dst = os.path.join(folder, f"{name}.yaml" if rel == "project.yaml" else rel)
            if os.path.exists(dst):
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(src) as fh:
                text = fh.read().replace("@@NAME@@", name).replace("@@ARCHIVE@@", archive)
            with open(dst, "w") as fh:
                fh.write(text)
            out.append(dst)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lfc-medallion-pack", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("init")
    i.add_argument("dir")
    i.add_argument("--name", required=True)
    i.add_argument("--archive", default=ARCHIVE)
    r = sub.add_parser("render")
    r.add_argument("project")
    c = sub.add_parser("check")
    c.add_argument("pack")
    lo = sub.add_parser("lobs")
    lo.add_argument("n", type=int)
    lo.add_argument("--seed", type=int, default=1)
    a = ap.parse_args(argv)
    if a.cmd == "init":
        for f in init(a.dir, a.name, a.archive):
            print(f)
        return 0
    if a.cmd == "render":
        archive = plan_mod.load_project(a.project).get("archive") or ARCHIVE
        print(json.dumps(plan_mod.render(a.project, archive), indent=1))
        return 0
    if a.cmd == "check":
        problems = check_mod.check(pack_mod.load(a.pack))
        for p in problems:
            print(p)
        print(f"{a.pack}: {'ok' if not problems else f'{len(problems)} problems'}")
        return 1 if problems else 0
    for x in lobs_mod.lobs(a.n, a.seed):
        print(f"{x['name']}\t{x['tier']}\t{x['score']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
