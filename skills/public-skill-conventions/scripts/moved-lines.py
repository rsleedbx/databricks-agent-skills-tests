#!/usr/bin/env python3
"""moved-lines.py <skills-dir> <skill> --base <git-ref> [--lines] [--links]: after content moves (files merged, renamed,
deleted, or a skill split in two), what of skills/<skill>/**/*.md at <base> is in no skill now (the working tree, so
content moved to a sibling skill counts as kept). Each item is either carried over or named in the PR as dropped on
purpose. Exits 1 if any term is gone.
  default  terms gone everywhere: `code` spans, ALL_CAPS names (error classes, states), docs URLs, numbers with a unit.
           Kept where a .md line has the term and 2 other words of the old line (a URL: also anywhere in a .json, .py,
           .sh, .sql or .yaml file). Prints the term and the first base line that had it: a dropped option, state, role
           or limit.
  --lines  also every line (25+ characters; link targets, markdown marks, spacing ignored) with no close match, with
           its best match ratio. Long after a rewrite; read it to find dropped explanations.
  --links  also the lines in other skills that name <skill>: after a split, check each still points to the skill
           that now owns its topic.

  python3 skills/public-skill-conventions/scripts/moved-lines.py ../databricks-agent-skills/skills databricks-lakeflow-connect --base upstream/main
"""
from __future__ import annotations

import collections
import difflib
import re
import subprocess
import sys
from pathlib import Path

MIN, CUTOFF, CONTEXT = 25, 0.75, 2


def norm(line: str) -> str:
    s = re.sub(r"\]\([^)]*\)", "]", line)  # link targets change when a section moves
    s = re.sub(r"[*_`]", "", s)
    return re.sub(r"\s+", " ", s).strip(" -|#>:").lower()


TERM = re.compile(r"`([^`\n]{2,60})`|\b([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+|[A-Z]{4,})\b|(https?://[^\s)>\]]+)"
                  r"|\b(\d[\d,.]*\s?(?:%|ms|s|seconds|minutes|hours|days|GB|MB|TB|tables|rows|requests))\b")
COMMON = {"TODO", "NOTE", "JSON", "YAML", "HTTP", "HTTPS", "OAUTH", "CREATE", "SELECT", "TABLE", "WHERE", "FROM"}


def terms(text: str) -> list[str]:
    out = []
    for m in TERM.finditer(text):
        t = next(g for g in m.groups() if g).strip().rstrip(".,;:")
        if t.upper() not in COMMON and len(t) > 2:
            out.append(t)
    return out


def fix(s: str) -> str:
    """Lowercase, with cloud-specific docs paths made cloud-neutral."""
    return s.lower().replace("/aws/en/", "/").replace("/azure/", "/")


def words(s: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]{4,}", s))


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, check=True).stdout


def main() -> int:
    args = sys.argv[1:]
    if len(args) < 4 or "--base" not in args:
        print(__doc__)
        return 2
    skills, name = Path(args[0]).resolve(), args[1]
    base = args[args.index("--base") + 1]
    root = Path(git(skills, "rev-parse", "--show-toplevel").strip())
    rel = f"{skills.relative_to(root)}/{name}"
    before = [f for f in git(root, "ls-tree", "-r", "--name-only", base, "--", rel).split() if f.endswith(".md")]
    if not before:
        print(f"no .md files under {rel} at {base}")
        return 2
    now = [norm(x) for p in sorted(skills.glob("*/**/*.md")) for x in p.read_text().splitlines()]
    have, blob = set(now), "\n".join(now)
    index = collections.defaultdict(set)
    for i, line in enumerate(now):
        for w in words(line):
            index[w].add(i)
    texts = {f: git(root, "show", f"{base}:{f}") for f in before}
    files = [p for p in sorted(skills.glob("*/**/*")) if p.is_file()]
    lines_now = [fix(x) for p in files if p.suffix == ".md" for x in p.read_text().splitlines()]
    # a catalog (.json) or script holds a docs URL without prose around it: there, the URL alone is enough
    data_now = fix("\n".join(p.read_text(errors="ignore") for p in files
                              if p.suffix in {".json", ".py", ".sh", ".sql", ".yaml", ".yml"}))
    gone: dict[str, str] = {}
    for f, t in texts.items():
        for n, raw in enumerate(t.splitlines(), 1):
            context = words(raw.lower())
            for term in terms(raw):
                key = fix(term)
                # kept if a line now has the term and shares context with the old line (PENDING elsewhere isn't it)
                need = min(CONTEXT, len(context - words(key)))
                if term not in gone and not (key.startswith("http") and key in data_now) and not any(key in x and len(words(x) & context - words(key)) >= need
                                                for x in lines_now):
                    gone[term] = f"{f[len(rel) + 1:]}:{n}\t{raw.strip()[:140]}"
    for term, where in gone.items():
        print(f"{term}\t{where}")
    missing = 0
    if "--lines" in args:
        print("\nlines with no close match:")
    for f, t in texts.items() if "--lines" in args else ():
        for n, raw in enumerate(t.splitlines(), 1):
            line = norm(raw)
            if len(line) < MIN or line.startswith("```") or re.fullmatch(r"[|:\- ]+", raw.strip()):
                continue
            if line in have or line in blob:
                continue
            hits = collections.Counter(i for w in words(line) for i in index.get(w, ()))
            best = max((difflib.SequenceMatcher(None, line, now[i]).ratio() for i, _ in hits.most_common(30)), default=0)
            if best < CUTOFF:
                missing += 1
                print(f"{f[len(rel) + 1:]}:{n}\t{best:.2f}\t{raw.strip()[:160]}")
    if "--links" in args:
        print(f"\nother skills naming {name} (check each points to the skill that owns its topic now):")
        for p in sorted(skills.glob("*/**/*.md")):
            if p.relative_to(skills).parts[0] == name:
                continue
            for n, raw in enumerate(p.read_text().splitlines(), 1):
                if re.search(rf"\b{re.escape(name)}\b(?!-)", raw):
                    print(f"  {p.relative_to(skills)}:{n}\t{raw.strip()[:160]}")
    lines = f", {missing} line(s) with no close match" if "--lines" in args else ""
    print(f"{name}: {len(gone)} term(s) from {base} gone everywhere{lines}", file=sys.stderr)
    return 1 if gone else 0


if __name__ == "__main__":
    sys.exit(main())
