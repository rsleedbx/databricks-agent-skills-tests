#!/usr/bin/env python3
"""bin/test_gate.py: should a check of bin/test_scripts.sh run, given what changed since it last passed?

  bin/test_gate.py decide <name> [--value V] [--cache F] -- <the check's command>...
        prints "skip <when>" (exit 0) when the check last passed with these inputs unchanged, else "run <fp>" (exit 1)
  bin/test_gate.py record <name> <fp> pass|fail
  bin/test_gate.py group-decide <name> <script> [--value V]        the checks that pass files through $T: one group
  bin/test_gate.py group-add <list> [--cache F] -- <a $T check's command>   while the group runs: add its inputs
  bin/test_gate.py group-record <name> <script> <list> pass|fail [--value V]
        The group skips when the inputs its checks had at its last pass, and the script, are unchanged.

A check's inputs are the repo paths in its command, widened so nothing it reads is missed:
  - a path under skills/<x> or .claude/skills/<x>: that skill, and every skill and bin file its code references,
    transitively (bin/check_layers.py's edges from code files, not docs; cached per run in --cache), the repo-root
    config and data files (defaults.yaml, layers.yaml, env.sh, ...), fixtures/, and results/ when its code reads it;
  - a path under bin/ (a repo-wide tool such as check_layers.py or lint_scripts.py): the whole repo;
  - none: no inputs, so the check always runs.
--value adds what isn't a file (the database settings of a run). Passes are kept by bin/test_skip, one ledger per
checkout (runner test_scripts.<hash of the repo root>; local, never committed); TEST_SKIP_AGAIN=1 runs every check.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

R = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(R / "bin"))
sys.path.insert(0, str(R / "bin/test_skip/scripts"))
import test_skip

# one ledger (and group file) per checkout: fingerprints hash absolute paths, so passes never carry across worktrees,
# and worktrees never overwrite each other's
RUNNER = "test_scripts." + hashlib.sha256(str(R).encode()).hexdigest()[:8]
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
    found_paths = {p for p in (node_path(n) for n in seen) if p} | set(loose)
    found_paths |= {b for p in list(found_paths) for b in bin_refs(p)}   # bin/<dir>/... the layer edges don't follow
    if any(reads_results(p) for p in found_paths):   # its code reads results/ (runs, ddl-matrix): scanned before
        found_paths.add(R / "results")               # the root files are added (their text mentions results/ too)
    return sorted(found_paths | set(root_inputs()))


ROOT_KINDS = {".yaml", ".yml", ".sh", ".txt", ".json", ".py"}   # repo-root config and data, not prose


def root_inputs() -> list[Path]:
    """Files at the repo root that skill code may read (defaults.yaml, layers.yaml, env.sh, ...) and fixtures/."""
    return sorted([p for p in R.iterdir() if p.is_file() and p.suffix in ROOT_KINDS] +
                  ([R / "fixtures"] if (R / "fixtures").is_dir() else []))


BIN_REF = re.compile(r"\bbin/[\w.-]+(?:/[\w.-]+)*")


def _code_files(path: Path):
    if path.is_file():
        yield path
    elif path.is_dir():
        yield from (f for f in path.rglob("*") if f.is_file() and f.suffix.lower() not in DOCS)


def bin_refs(path: Path) -> set[Path]:
    """bin/ files and folders a skill's code names, below subfolders too (bin/test_skip/scripts/...)."""
    out = set()
    for f in _code_files(path):
        try:
            text = f.read_text(errors="ignore")
        except OSError:
            continue
        for m in BIN_REF.findall(text):
            p = R / m
            while not p.exists() and p != R / "bin":
                p = p.parent
            if p != R / "bin":
                out.add(p)
    return out


def reads_results(path: Path) -> bool:
    """Does a skill's code read the repo's results/ (its runs records, coverage page, ddl-matrix)?"""
    files = [path] if path.is_file() else [f for f in path.rglob("*") if f.is_file() and f.suffix.lower() not in DOCS]
    for f in files:
        try:
            if "results/" in f.read_text(errors="ignore"):
                return True
        except OSError:
            pass
    return False


GROUP_SUFFIX = ".temp-group.json"   # beside the ledger: the inputs the group's checks had at its last pass


def group_file() -> Path:
    return test_skip.ledger_path(RUNNER).with_suffix(GROUP_SUFFIX)


def group_decide(name: str, script: Path, values: list[str]) -> tuple[bool, str]:
    """(skip, line) for the checks that pass files through $T: skip when the inputs they had at their last pass
    (every check's own command, so continuation lines and heredocs count) and this script are unchanged."""
    f = group_file()
    if not f.exists():
        return False, "run -new"
    paths = [str(R / p) for p in json.loads(f.read_text())["paths"]] + [str(script)]
    fp = test_skip.fingerprint(paths, values)
    skip, last = test_skip.should_skip(RUNNER, name, fp)
    return skip, (f"skip {last['when']}" if skip else f"run {fp}")


def group_add(cmd: list[str], cache: Path | None, acc: Path) -> None:
    """Add one $T check's inputs (from its full command) to this run's list."""
    with acc.open("a") as out:
        for p in inputs(cmd, lambda: edges(cache)):
            out.write(f"{p}\n")


def group_record(name: str, script: Path, values: list[str], acc: Path, ok: bool) -> None:
    paths = sorted({ln.strip() for ln in acc.read_text().splitlines() if ln.strip()}) if acc.exists() else []
    if not paths:   # nothing collected (the list is gone, or no check of the group ran): never a pass to skip on
        ok = False
    if ok:
        rel = sorted({str(Path(p).relative_to(R)) if Path(p).is_relative_to(R) else p for p in paths})
        group_file().parent.mkdir(parents=True, exist_ok=True)
        group_file().write_text(json.dumps({"paths": rel}, indent=1))
    test_skip.record(RUNNER, name, test_skip.fingerprint(paths + [str(script)], values), ok)


def opts(head: list[str]) -> tuple[list[str], list[str], Path | None]:
    """(positionals, --value values, --cache path) from the arguments before --."""
    pos, values, cache = [], [], None
    while head:
        x = head.pop(0)
        if x == "--value":
            values.append(head.pop(0))
        elif x == "--cache":
            cache = Path(head.pop(0))
        else:
            pos.append(x)
    return pos, values, cache


def group_main(argv: list[str]) -> int:
    """group-decide <name> <script> [--value V]; group-add <acc> [--cache F] -- <command>;
    group-record <name> <script> <acc> pass|fail [--value V]."""
    cut = argv.index("--") if "--" in argv else len(argv)
    pos, values, cache = opts(argv[1:cut])
    if argv[0] == "group-decide":
        skip, line = group_decide(pos[0], Path(pos[1]), values)
        print(line)
        return 0 if skip else 1
    if argv[0] == "group-add":
        group_add(argv[cut + 1:], cache, Path(pos[0]))
        return 0
    group_record(pos[0], Path(pos[1]), values, Path(pos[2]), pos[3] == "pass")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] in (["group-decide"], ["group-add"], ["group-record"]):
        return group_main(argv)
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
