#!/usr/bin/env python3
"""bin/test_gate.py: should a check of bin/test_scripts.sh run, given what changed since it last passed?

  bin/test_gate.py decide <name> [--value V] [--cache F] -- <the check's command>...
        prints "skip <when>" (exit 0) when the check last passed with these inputs unchanged, else "run <fp>" (exit 1)
  bin/test_gate.py record <name> <fp> pass|fail
  bin/test_gate.py temp-group <script>
        the repo paths the script's checks that pass files through $T use (one group: they run or skip together)

A check's inputs are the repo paths in its command, widened so nothing it reads is missed:
  - a path under skills/<x> or .claude/skills/<x>: that skill, and every skill and bin file its code references,
    transitively (bin/check_layers.py's edges from code files, not docs; cached per run in --cache);
  - a path under bin/ (a repo-wide tool such as check_layers.py or lint_scripts.py): the whole repo;
  - none: no inputs, so the check always runs.
--value adds what isn't a file (the database settings of a run). Passes are kept by bin/test_skip (runner
test_scripts, local, never committed); TEST_SKIP_AGAIN=1 runs every check.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "bin"))
sys.path.insert(0, str(R / "bin/test_skip/scripts"))
import test_skip  # noqa: E402

RUNNER = "test_scripts"
SKILL = re.compile(r"^(\.claude/skills|skills)/([^/]+)")
DOCS = {".md", ".txt", ".html"}


def edges(cache: Path | None) -> dict[str, set[str]]:
    """node -> nodes it references (skills by name, bin files as bin/<file>), from check_layers, cached per run."""
    if cache and cache.exists():
        return {k: set(v) for k, v in json.loads(cache.read_text()).items()}
    import check_layers

    model = check_layers.load_model(R)
    out: dict[str, set[str]] = {}
    for src, dst, rel, *_ in check_layers.find_edges(model, R):
        if Path(rel).suffix.lower() not in DOCS:   # a doc's mention doesn't make one skill's tests depend on another
            out.setdefault(src, set()).add(dst)
    if cache:
        cache.write_text(json.dumps({k: sorted(v) for k, v in out.items()}))
    return out


def node_path(node: str) -> Path | None:
    for base in ("skills", ".claude/skills"):
        if (R / base / node).is_dir():
            return R / base / node
    p = R / node
    return p if p.exists() else None


def inputs(args: list[str], graph_of) -> list[Path]:
    """The paths a check depends on (see the module text); [] when its command names no repo path."""
    found = {Path(m) for a in args for m in re.findall(re.escape(str(R)) + r"/[\w./-]+", a)}
    rel = [p.relative_to(R) for p in found if p.is_relative_to(R)]
    if not rel:
        return []
    if any(str(p).startswith("bin/") for p in rel):
        return [R]
    todo = {SKILL.match(str(p)).group(2) for p in rel if SKILL.match(str(p))}
    loose = [R / p for p in rel if not SKILL.match(str(p))]
    graph, seen = graph_of(), set()
    while todo:
        n = todo.pop()
        if n not in seen:
            seen.add(n)
            todo |= graph.get(n, set()) - seen
    return sorted({p for p in (node_path(n) for n in seen) if p} | set(loose))


def temp_group(script: Path) -> list[str]:
    """Skill folders named by the script's check lines that use $T, through its X=$SK/<skill>/... variables."""
    text = script.read_text()
    var = {m.group(1): m.group(2) for m in re.finditer(r"\b([A-Z]{2})=\$SK/([\w.-]+)", text)}
    skills = set()
    for line in text.splitlines():
        if line.lstrip().startswith("check ") and "$T/" in line:
            skills |= {var[v] for v in re.findall(r"\$([A-Z]{2})\b", line) if v in var}
            skills |= set(re.findall(r"\$SK/([\w.-]+)", line))
    return [str(R / "skills" / s) for s in sorted(skills)]


def main(argv: list[str]) -> int:
    if argv[:1] == ["temp-group"]:
        print("\n".join(temp_group(Path(argv[1]))))
        return 0
    if argv[:1] == ["record"]:
        name, fp, result = argv[1:4]
        test_skip.record(RUNNER, name, fp, result == "pass")
        return 0
    if argv[:1] != ["decide"] or "--" not in argv:
        sys.exit(__doc__)
    head, cmd = argv[1:argv.index("--")], argv[argv.index("--") + 1:]
    name, values, cache = head[0], [], None
    rest = head[1:]
    while rest:
        flag, val, rest = rest[0], rest[1], rest[2:]
        if flag == "--value":
            values.append(val)
        elif flag == "--cache":
            cache = Path(val)
    ins = inputs(cmd, lambda: edges(cache))
    if not ins:
        print("run -")   # nothing to compare: always run, never recorded
        return 1
    fp = test_skip.fingerprint(ins, values)
    skip, last = test_skip.should_skip(RUNNER, name, fp)
    print(f"skip {last['when']}" if skip else f"run {fp}")
    return 0 if skip else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
