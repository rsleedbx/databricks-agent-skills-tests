#!/usr/bin/env python3
"""conventions-check.py <skills-dir> <skill> [--base <git-ref>]: does one skill in a checkout of the public skills repo
follow what the other skills there do? Compares skills/<skill>/ (SKILL.md and references/*.md) with every other skill
in <skills-dir>. With --base, also the skill's own SKILL.md section names at that ref (run inside the checkout).
Prints FAIL and WARN lines and exits 1 on any FAIL:
  FAIL  a link to a host other than Databricks'; a docs link with a cloud in the path (/aws/, /azure/, /gcp/, or
        learn.microsoft.com/azure/databricks) where docs.databricks.com/<path> works; test-report wording ("tested",
        "seen on", "last seen", "in our tests", "we ran"); a reference file name with a number prefix or not kebab-case;
        a frontmatter key most other skills have
  WARN  an error table header no other skill uses and not in the skill at --base; a SKILL.md section (##) no other
        skill uses and not in --base. Each is reported once, with the files that use it. Also: a reference file
        SKILL.md doesn't link; a pipeline example with the deprecated `target:` (use `schema:`); agents/openai.yaml
        text cut off or in the generator's "Use $<skill> for ..." form (hand-write it); a skill script with no test
        in the repo's tests/ that names the skill

  python3 skills/public-skill-conventions/scripts/conventions-check.py ../databricks-agent-skills/skills databricks-lakeflow-connect --base upstream/main
"""
from __future__ import annotations

import collections
import re
import subprocess
import sys
from pathlib import Path

HOSTS = re.compile(r"^https?://([^/]*\.)?(databricks\.com|databricks\.net|azuredatabricks\.net)(/|$)")
CLOUD_DOCS = re.compile(r"docs\.databricks\.com/(aws|azure|gcp)/|learn\.microsoft\.com/[^)\s]*azure/databricks")
WORDING = re.compile(r"(?i)\b(tested|seen on|last seen|in our tests|we ran)\b")
ERR_HEAD = re.compile(r"(?i)\b(error|symptom|issue|problem|trap)\b")
TABLE = re.compile(r"^(\|[^\n]*\|)\n\|[ :|-]+\|$", re.M)
FENCE = re.compile(r"```[^\n]*\n(.*?)```", re.S)
TARGET = re.compile(r'^\s*(target:\s*\S|"target"\s*:)', re.M)  # pipeline `target` is deprecated for `schema`


def norm(head: str) -> str:
    return " | ".join(c.strip().lower() for c in head.strip().strip("|").split("|"))


def frontmatter(md: str) -> set[str]:
    m = re.match(r"^---\n(.*?)\n---\n", md, re.S)
    return {k for k in re.findall(r"^([a-z_]+):", m.group(1), re.M)} if m else set()


def texts(d: Path) -> dict[Path, str]:
    out = {d / "SKILL.md": (d / "SKILL.md").read_text()}
    for p in sorted((d / "references").glob("*.md")) if (d / "references").is_dir() else []:
        out[p] = p.read_text()
    return out


def prose(md: str) -> str:
    """Markdown without fenced code: code samples may hold dates, hosts and words that aren't claims."""
    return re.sub(r"```.*?```", "", md, flags=re.S)


def main() -> int:
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__)
        return 2
    root, name = Path(args[0]), args[1]
    base = args[args.index("--base") + 1] if "--base" in args else ""
    me = root / name
    if not (me / "SKILL.md").is_file():
        print(f"no {me}/SKILL.md")
        return 2
    others = [d for d in sorted(root.iterdir()) if d.is_dir() and d.name != name and (d / "SKILL.md").is_file()]
    fm, heads, h2 = collections.Counter(), collections.Counter(), collections.Counter()
    for d in others:
        md = (d / "SKILL.md").read_text()
        fm.update(frontmatter(md))
        h2.update({h.strip().lower() for h in re.findall(r"^## (.+)$", md, re.M)})
        for t in texts(d).values():
            heads.update(norm(h) for h in TABLE.findall(t))
    fails = warns = 0

    def say(kind: str, where: str, text: str) -> None:
        nonlocal fails, warns
        print(f"{kind}  {where}: {text}")
        fails += kind == "FAIL"
        warns += kind == "WARN"

    md = (me / "SKILL.md").read_text()
    mine: dict[str, list[str]] = collections.defaultdict(list)
    for k in sorted(k for k, n in fm.items() if n > len(others) / 2 and k not in frontmatter(md)):
        say("FAIL", "SKILL.md", f"frontmatter has no `{k}` ({fm[k]} of {len(others)} skills have it)")
    for p in sorted((me / "references").glob("*")) if (me / "references").is_dir() else []:
        if re.match(r"^\d", p.name) or not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*\.md", p.name):
            say("FAIL", str(p.relative_to(root)), "reference names are descriptive kebab-case, no number prefix")
    for p, t in texts(me).items():
        rel = str(p.relative_to(root))
        for i, line in enumerate(prose(t).split("\n"), 1):
            for url in re.findall(r"\]\((https?://[^)\s]+)\)|<(https?://[^>\s]+)>", line):
                u = url[0] or url[1]
                if not HOSTS.match(u):
                    say("FAIL", f"{rel}:{i}", f"link to a non-Databricks host: {u}")
                elif CLOUD_DOCS.search(u):
                    say("FAIL", f"{rel}:{i}", f"cloud-specific docs link (use docs.databricks.com/<path>): {u}")
            for w in WORDING.findall(line):
                say("FAIL", f"{rel}:{i}", f"test-report wording '{w}': state the behavior")
        for h in TABLE.findall(t):
            if ERR_HEAD.search(h):
                mine[norm(h)].append(p.name)
    for p in sorted((me / "references").glob("*.md")) if (me / "references").is_dir() else []:
        if f"references/{p.name}" not in md:
            say("WARN", str(p.relative_to(root)), "not linked from SKILL.md (list every reference file there)")
    for p, t in texts(me).items():
        if any(TARGET.search(c) and ("pipelines" in c or "ingestion_definition" in c) for c in FENCE.findall(t)):
            say("WARN", str(p.relative_to(root)), "pipeline example sets `target:`, deprecated: use `schema:`")
    oa = me / "agents" / "openai.yaml"
    if oa.is_file():
        y = oa.read_text()
        short = re.search(r'short_description:\s*"(.*)"', y)
        if (short and short.group(1).rstrip().endswith(("...", "\u2026"))) or f"Use ${name} for " in y:
            say("WARN", str(oa.relative_to(root)), "generated text (cut off, or 'Use $<skill> for ...'): hand-write "
                "short_description and default_prompt so the skill reads apart from its siblings")
    tests = root.parent / "tests"
    scripts = list((me / "scripts").glob("*.py")) if (me / "scripts").is_dir() else []
    if scripts and tests.is_dir() and not any(name in t.read_text() for t in tests.glob("*_test.py")):
        say("WARN", f"{name}/scripts", "no test in tests/ names this skill: checks that need the network "
                "(a catalog build) also need an offline test")
    old, old_heads = set(), set()
    if base:
        git = ["git", "-C", str(root.parent)]
        rel = f"{me.relative_to(root.parent)}"
        r = subprocess.run([*git, "show", f"{base}:{rel}/SKILL.md"], capture_output=True, text=True)
        old = {h.strip().lower() for h in re.findall(r"^## (.+)$", r.stdout, re.M)}
        ls = subprocess.run([*git, "ls-tree", "-r", "--name-only", base, rel], capture_output=True, text=True).stdout
        for f in (x for x in ls.split() if x.endswith(".md")):
            t = subprocess.run([*git, "show", f"{base}:{f}"], capture_output=True, text=True).stdout
            old_heads.update(norm(h) for h in TABLE.findall(t))
    common = ", ".join(f"'{k}' ({n})" for k, n in heads.most_common(60) if ERR_HEAD.search(k))[:160]
    for h, where in mine.items():
        if heads[h] == 0 and h not in old_heads:
            say("WARN", ", ".join(sorted(set(where))), f"error table header '{h}' is in no other skill; others: {common}")
    for h in re.findall(r"^## (.+)$", md, re.M):
        if h2[h.strip().lower()] == 0 and h.strip().lower() not in old:
            say("WARN", "SKILL.md", f"section '## {h}' is in no other skill{' and not in ' + base if base else ''}")
    print(f"{name}: {fails} FAIL, {warns} WARN against {len(others)} skills")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
