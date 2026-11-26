"""lfc-medallion-pack: start a repo of n:1 modules, plan and render it, check a pack, list LOBs.

    lfc-medallion-pack init <dir> --name febar --archive <tests repo archive URL>   # the bundles, notebooks, project
    lfc-medallion-pack render <dir>/febar.yaml        # resources/<module>.yml, marts/<pack>/, source/plan.json
    lfc-medallion-pack check <pack name or folder>    # the marts against the model, offline
    lfc-medallion-pack lobs 10 --seed 7               # the LOBs and their 20-70-10 tiers
    lfc-medallion-pack timings --plan source/plan.json --module m --run <id> --out runs.jsonl   # each step's seconds
    lfc-medallion-pack predict runs.jsonl --tables 4000   # each step's seconds at that many tables
    lfc-medallion-pack report runs.jsonl                 # the records as a markdown table
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


CODE = {"checks/verify.py", "source/setup_source.py", "transform/n1.py", "transform/refresh.py"}   # the pack's own


def init(folder: str, name: str, archive: str, refresh: bool = False) -> list[str]:
    """Write the templates into folder (a file already there is kept): both bundles, the source and verify notebooks,
    the n:1 pipeline file and <name>.yaml. refresh: also rewrite the pack's own notebooks (CODE), which a project
    doesn't edit, so their fixes reach a project made before them; the project's YAML and bundles stay."""
    lobs_mod.NAME.match(name) or sys.exit(f"--name {name}: lower-case letters, digits and _")
    out = []
    for root, _, files in os.walk(TEMPLATES):
        for f in files:
            src = os.path.join(root, f)
            rel = os.path.relpath(src, TEMPLATES)
            dst = os.path.join(folder, f"{name}.yaml" if rel == "project.yaml" else rel)
            if os.path.exists(dst) and not (refresh and rel in CODE):
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
    i.add_argument("--refresh", action="store_true", help="also rewrite the pack's own notebooks (not the project's "
                                                          "YAML or bundles)")
    r = sub.add_parser("render")
    r.add_argument("project")
    c = sub.add_parser("check")
    c.add_argument("pack")
    lo = sub.add_parser("lobs")
    lo.add_argument("n", type=int)
    lo.add_argument("--seed", type=int, default=1)
    tm = sub.add_parser("timings")
    tm.add_argument("--plan", required=True)
    tm.add_argument("--module", required=True)
    tm.add_argument("--run", type=int, required=True)
    tm.add_argument("--source-run", type=int)
    tm.add_argument("--deploy-seconds", type=float)
    tm.add_argument("--note", default="")
    tm.add_argument("--kind", default="", choices=["", "first", "steady", "grown"],
                    help="first: each pipeline's first snapshot; steady: no new tables; grown: tables added")
    tm.add_argument("--task-kind", action="append", default=[], help="a task's own kind: ingest_1=grown")
    tm.add_argument("--lobs", type=int, help="the module's LOBs at the time of the run, when the plan grew since")
    tm.add_argument("--profile")
    tm.add_argument("--out", required=True)
    pr = sub.add_parser("predict")
    pr.add_argument("records")
    pr.add_argument("--tables", type=int, required=True)
    rp = sub.add_parser("report")
    rp.add_argument("records")
    a = ap.parse_args(argv)
    if a.cmd == "report":
        from . import timings
        with open(a.records) as fh:
            print(timings.report([json.loads(ln) for ln in fh if ln.strip()]), end="")
        return 0
    if a.cmd == "timings":
        from . import timings
        rows = timings.record(timings.Api(a.profile), plan_mod.read_plan(a.plan), a.module, a.run, a.source_run, a.deploy_seconds, a.note,
                                a.lobs, a.kind,
                                dict(x.split("=", 1) for x in a.task_kind))
        with open(a.out, "a") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
                print(json.dumps(r))
        return 0
    if a.cmd == "predict":
        from . import timings
        with open(a.records) as fh:
            rows = [json.loads(ln) for ln in fh if ln.strip()]
        print(json.dumps(timings.predict(rows, a.tables), indent=1))
        return 0
    if a.cmd == "init":
        for f in init(a.dir, a.name, a.archive, a.refresh):
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
