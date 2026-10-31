#!/usr/bin/env python3
"""bin/lint_scripts.py [--sizes-only]: rules for the skills' scripts and SKILL.md files, checked from the syntax tree.

Prints one line per problem (path:line rule: text) and exits 1 if there is any. Checks:
  header     every runnable script (a #! line) names itself in its first 15 lines: that's its usage, so an agent runs
             it with no arguments instead of reading it
  size       every SKILL.md is at most 20,000 characters and 500 lines (the Agent Skills spec: under 5,000 tokens);
             detail goes to references/
  rm-guard   rm -r on a variable path needs ${VAR:?} (an empty variable must stop the command, not remove from /)
  first-arg  a script that reads a flag only as its first argument ([ "${1:-}" = --flag ]) is called with it first
  jq-one     one value from JSON is `jq -r '<path>'`, not `yq -p json -oy` (yq + -o shell is for several values)
  bash32     .sh files run on stock macOS bash 3.2 and on bash 5: no bash-4 builtins or expansions (declare -A,
             mapfile, ${x,,}, ...), no date -d/-v/-j (date math in jq: now, fromdate, strftime, strflocaltime), no
             bare timeout (not on macOS), and an array that can be empty (X=(), read -a X) expands as
             ${X[@]+"${X[@]}"} under set -u. A script that runs only on Linux says `# bash32: exempt` in its
             first 15 lines.
The shell rules run on every .sh file and on every ```bash / ```sh block in the skills' markdown, parsed with
ast-grep (tree-sitter bash): AST_GREP names the binary, else `uvx --from ast-grep-cli==0.51.0 ast-grep`.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

R = Path(__file__).resolve().parent.parent
MAX_CHARS, MAX_LINES = 20_000, 500
RULE = "id: cmd\nlanguage: bash\nrule: {kind: command}\n"
PLAIN_PATH = re.compile(r"""^\.[\w.\[\]"-]*( // "[^"]*")?$""")
BASH4 = [   # (regex, what to do instead): bash 4+ only, or GNU/BSD-only commands
    (r"\b(declare|local|typeset) -[a-zA-Z]*A", "declare -A is bash 4: one variable per key, or a file"),
    (r"\b(declare|local|typeset) -[a-zA-Z]*n\b", "namerefs are bash 4.3: pass the name and use ${!name}"),
    (r"\b(mapfile|readarray)\b", "mapfile is bash 4: while IFS= read -r l; do A+=(\"$l\"); done"),
    (r"\$\{[A-Za-z_]\w*(\[[^]]*\])?(,,?|\^\^?)\}", "case conversion is bash 4: tr '[:upper:]' '[:lower:]'"),
    (r"\$\{[A-Za-z_]\w*(\[[^]]*\])?@[QEPAKa]\}", "${x@Q} is bash 4.4"),
    (r"\[-[0-9]+\]\}", "negative indexes are bash 4.3: ${a[${#a[@]}-1]}"),
    (r"&>>|\|&", "&>> and |& are bash 4: >>f 2>&1, 2>&1 |"),
    (r"\bwait -n\b|\bcoproc\b|\bglobstar\b|\bEPOCH(SECONDS|REALTIME)\b", "bash 4+ only"),
    (r"\bdate\b[^|;&#]*\s-[a-zA-Z]*[dvj]\b", "date -d is GNU, -v/-j BSD: do the date math in jq"),
]
SINGLE_QUOTED = re.compile(r"'[^'\n]*'")
EMPTYABLE = re.compile(r"(?:^|[\s;(])(?:local\s+)?([A-Za-z_]\w*)=\(\s*\)|read\s+-[a-zA-Z]*a\s+([A-Za-z_]\w*)")


def files(*globs: str) -> list[Path]:
    out = subprocess.run(["git", "-C", str(R), "ls-files", *globs], capture_output=True, text=True, check=True).stdout
    return [R / p for p in out.split()]


def ast_grep() -> list[str]:
    if os.environ.get("AST_GREP"):
        return [os.environ["AST_GREP"]]
    if shutil.which("uvx"):
        return ["uvx", "-q", "--from", "ast-grep-cli==0.51.0", "ast-grep"]
    sys.exit("lint_scripts.py: set AST_GREP or install uv (uvx) for ast-grep")


def md_blocks(md: Path, out: Path) -> dict[Path, tuple[Path, int]]:
    """Write each ```bash / ```sh block of a markdown file to its own .sh file; {block file: (markdown, first line)}."""
    blocks, lines, i = {}, md.read_text().splitlines(), 0
    while i < len(lines):
        m = re.match(r"\s*```(bash|sh|shell)\s*$", lines[i])
        if not m:
            i += 1
            continue
        start, body, i = i + 2, [], i + 1
        while i < len(lines) and not lines[i].strip().startswith("```"):
            body.append(lines[i])
            i += 1
        f = out / f"{len(blocks)}-{md.stem}.sh"
        # blocks indented under list items; <placeholder> words would parse as redirections, so they become words
        text = "\n".join(re.sub(r"^\s{0,4}", "", b) for b in body) + "\n"
        f.write_text(re.sub(r"<([A-Za-z][\w.-]*)>", lambda m: "PH_" + re.sub(r"\W", "_", m.group(1)), text))
        blocks[f] = (md, start)
        i += 1
    # inline code that runs a script (`bin/x.sh <a> --flag`), one per line of its own file, mapped to its line
    for n, line in enumerate(lines, 1):
        for m in re.finditer(r"`((?:\.?/|\$\w+/)?(?:bin|skills|scripts)/[^`]*?\.(?:sh|py)\b[^`]*)`", line):
            f = out / f"inline-{n}-{m.start()}-{md.stem}.sh"
            f.write_text(re.sub(r"<([A-Za-z][\w.-]*)>", lambda p: "PH_" + re.sub(r"\W", "_", p.group(1)), m.group(1)) + "\n")
            blocks[f] = (md, n)
    return blocks


def commands(paths: list[Path], where: dict[Path, tuple[Path, int]]) -> list[tuple[Path, int, str]]:
    """(source file, line, command text) for every command ast-grep finds in the given files."""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as r:
        r.write(RULE)
    found = []
    for i in range(0, len(paths), 200):
        p = subprocess.run([*ast_grep(), "scan", "-r", r.name, "--json=stream", *map(str, paths[i:i + 200])],
                           stdin=subprocess.DEVNULL, capture_output=True, text=True, check=False)
        for line in p.stdout.splitlines():
            m = json.loads(line)
            f, n = Path(m["file"]), m["range"]["start"]["line"]
            src, base = where.get(f, (f, 1))
            found.append((src, base + n, m["text"]))
    os.unlink(r.name)
    return found


def words(text: str) -> list[str]:
    try:
        return shlex.split(text, comments=True)
    except ValueError:
        return text.split()


def first_arg_flags() -> dict[str, set[str]]:
    """{script name: flags it reads only as $1}: the script tests "${1:-}" (or "$1") against --flag."""
    out: dict[str, set[str]] = {}
    for f in files("skills/*.sh", "bin/*.sh"):
        flags = set(re.findall(r'\[\s*"\$\{?1(?::-)?\}?"\s*=\s*(--[\w-]+)\s*\]', f.read_text()))
        if flags and not re.search(r'case\s+"?\$\{?1', f.read_text()):
            out[f.name] = flags
    return out


def bash32(f: Path, rel) -> list[str]:
    """The bash32 rule for one .sh file (see the docstring)."""
    lines = f.read_text(errors="replace").splitlines()
    if any("bash32: exempt" in h for h in lines[:15]):
        return []
    # comments and single-quoted text (sed, jq and awk programs) aren't shell
    out, code = [], [SINGLE_QUOTED.sub("''", re.sub(r"(^|\s)#.*", "", l)) if not l.startswith("#!") else "" for l in lines]
    for n, l in enumerate(code, 1):
        for rx, fix in BASH4:
            if re.search(rx, l):
                out.append(f"{rel(f)}:{n} bash32: {fix}: {lines[n - 1].strip()[:100]}")
    if re.search(r"^\s*set\s+(-[a-zA-Z]*u|-o\s+nounset)", "\n".join(code), re.MULTILINE):
        arrays = {a or b for a, b in EMPTYABLE.findall("\n".join(code))}
        for n, l in enumerate(code, 1):
            for a in arrays:
                if re.search(r'(?<!\+)"\$\{' + a + r'\[@\]\}"', l):
                    out.append(f"{rel(f)}:{n} bash32: {a} can be empty: ${{{a}[@]+\"${{{a}[@]}}\"}} (bash 3.2 + set -u): "
                               f"{lines[n - 1].strip()[:80]}")
    return out


def main() -> int:
    problems: list[str] = []
    rel = lambda p: str(p.relative_to(R)) if p.is_relative_to(R) else str(p)   # noqa: E731
    for f in files("skills/*/SKILL.md"):
        t = f.read_text()
        if len(t) > MAX_CHARS or t.count("\n") > MAX_LINES:
            problems.append(f"{rel(f)}:1 size: {len(t):,} chars, {t.count(chr(10))} lines (max {MAX_CHARS:,}, {MAX_LINES}): "
                            "move detail to references/")
    if "--sizes-only" in sys.argv:
        print("\n".join(problems))
        return 1 if problems else 0
    for f in files("skills/*.sh", "bin/*.sh", "skills/*.py", "bin/*.py"):
        head = f.read_text(errors="replace").splitlines()[:15]
        if head and head[0].startswith("#!") and not any(f.name in h for h in head):
            problems.append(f"{rel(f)}:1 header: no usage line naming {f.name} in the first 15 lines")
    for f in files("skills/*.sh", "bin/*.sh"):
        problems += bash32(f, rel)
    with tempfile.TemporaryDirectory() as tmp:
        where: dict[Path, tuple[Path, int]] = {}
        for k, md in enumerate(files("skills/*.md", "skills/**/*.md", "*.md")):
            d = Path(tmp) / str(k)
            d.mkdir()
            where.update(md_blocks(md, d))
        cmds = commands(files("skills/*.sh", "bin/*.sh") + list(where), where)
    first = first_arg_flags()
    for src, line, text in cmds:
        w = words(text)
        if not w:
            continue
        name = os.path.basename(w[0].strip('"').split("}")[-1].split("/")[-1])
        if name == "rm" and any(re.match(r"^-\w*r", a) for a in w[1:]):
            for a in w[1:]:
                if "$" in a and not re.search(r"\$\{\w+:\?", a) and not a.startswith("-"):
                    problems.append(f"{rel(src)}:{line} rm-guard: {text.strip()[:120]}")
                    break
        if name in first:
            later = [a for a in w[2:] if a in first[name]]
            if later:
                problems.append(f"{rel(src)}:{line} first-arg: {name} reads {later[0]} only as its first argument: "
                                f"{text.strip()[:120]}")
        if name == "timeout" and src.suffix == ".sh":
            problems.append(f"{rel(src)}:{line} bash32: timeout is not on macOS: {text.strip()[:100]}")
        if name == "yq" and "json" in " ".join(w[1:3]) and any(a in ("-oy", "-o=yaml", "-oyaml") or a == "y" for a in w):
            expr = next((a for a in w[1:] if a.startswith(".")), "")
            if PLAIN_PATH.match(expr):
                problems.append(f"{rel(src)}:{line} jq-one: one value is `jq -r '{expr}'`: {text.strip()[:100]}")
    print("\n".join(problems))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
