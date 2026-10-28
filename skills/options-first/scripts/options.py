# /// script
# dependencies = ["pyyaml"]
# ///
"""options.py: the choices for one question, ranked by how simple they are to set up and operate.

    uv run options.py render <options.yaml>     # the ranked table (markdown) and the recommendation
    uv run options.py check <options.yaml>...   # validate (exit 1 on a problem)
    uv run options.py index [<dir>]             # every options.yaml under <dir> (default: skills/): topic, choices, pick

An options.yaml sits next to the skill that answers the question:

    topic: History downstream of a Lakeflow Connect table
    question: Keep each row's history when the connector writes SCD type 1 only
    options:
      - name: AUTO CDC FROM SNAPSHOT
        setup: {effort: 1, what: one pipeline, 5 lines}
        operate: {effort: 2, what: full table scan per run}
        breaks: [run during a full refresh]          # events that can break it
        recovery: none needed                         # what the operator does after each break
        status: tested                                # tested | partly-tested | untested | docs | not-supported
        evidence: scripts/sqlserver-history-snapshot.sh

effort is 1 (nothing to build or watch) to 5 (custom code you own). Ranking: usable status first (tested, then
partly-tested, docs, untested; not-supported last), then setup + operate effort, then fewer breaks. The pick is the
first option that is tested or documented.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

STATUS = ["tested", "partly-tested", "docs", "untested", "not-supported"]
USABLE = {"tested", "docs"}


def load(path: Path) -> tuple[dict, list[str]]:
    d = yaml.safe_load(path.read_text()) or {}
    bad = [
        f"{path}: missing {k}" for k in ("topic", "question", "options") if not d.get(k)
    ]
    for i, o in enumerate(d.get("options") or []):
        where = f"{path}: option {i + 1} ({o.get('name', '?')})"
        bad += [
            f"{where}: missing {k}"
            for k in ("name", "setup", "operate", "status")
            if k not in o
        ]
        for k in ("setup", "operate"):
            e = (o.get(k) or {}).get("effort")
            if not isinstance(e, int) or not 1 <= e <= 5:
                bad.append(f"{where}: {k}.effort must be 1 to 5")
        if o.get("status") not in STATUS:
            bad.append(f"{where}: status must be one of {', '.join(STATUS)}")
    return d, bad


def ranked(d: dict) -> list[dict]:
    def key(o: dict) -> tuple:
        return (
            STATUS.index(o["status"]),
            o["setup"]["effort"] + o["operate"]["effort"],
            len(o.get("breaks") or []),
        )

    return sorted(d["options"], key=key)


def pick(d: dict) -> dict | None:
    return next((o for o in ranked(d) if o["status"] in USABLE), None)


def cell(s: object) -> str:
    return str(s).replace("|", "\\|").replace("\n", " ")


def render(d: dict) -> str:
    out = [
        f"## {d['topic']}",
        "",
        d["question"],
        "",
        "| # | Option | Setup | Operate | Breaks on | Recovery | Status |",
        "|---|---|---|---|---|---|---|",
    ]
    for i, o in enumerate(ranked(d), 1):
        out.append(
            f"| {i} | {cell(o['name'])} | {o['setup']['effort']}: {cell(o['setup'].get('what', ''))} "
            f"| {o['operate']['effort']}: {cell(o['operate'].get('what', ''))} "
            f"| {cell('; '.join(o.get('breaks') or []) or 'none known')} | {cell(o.get('recovery', ''))} "
            f"| {o['status']} |"
        )
    p = pick(d)
    out += [
        "",
        f"Pick: **{p['name']}** ({p['status']})."
        if p
        else "Pick: none tested or documented yet: test the first row.",
    ]
    return "\n".join(out)


def main(argv: list[str]) -> int:
    if not argv or argv[0] not in {"render", "check", "index"}:
        print(__doc__.split("\n\n")[0], file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    if cmd == "index":
        root = Path(args[0]) if args else Path(__file__).resolve().parents[2]
        rows, bad = [], []
        for f in sorted(root.rglob("options.yaml")):
            d, b = load(f)
            bad += b
            if not b:
                p = pick(d)
                rows.append(
                    f"| {d['topic']} | {len(d['options'])} | {p['name'] if p else '(none tested)'} | {f.relative_to(root)} |"
                )
        print(
            "| Topic | Choices | Pick | File |\n|---|---|---|---|\n" + "\n".join(rows)
        )
        for b in bad:
            print("problem:", b, file=sys.stderr)
        return 1 if bad else 0
    if not args:
        print(f"usage: options.py {cmd} <options.yaml>", file=sys.stderr)
        return 2
    rc = 0
    for a in args:
        d, bad = load(Path(a))
        for b in bad:
            print("problem:", b, file=sys.stderr)
        rc |= bool(bad)
        if cmd == "render" and not bad:
            print(render(d))
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
