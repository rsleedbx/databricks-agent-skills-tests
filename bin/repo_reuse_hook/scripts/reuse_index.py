# Vendored copy of shared unit 'reuse_index': edit the master, then run `reuse-first vendor sync` (any other edit shows as drift).
"""Index of the functions in your repos, and the similarity rule that decides "this looks like that".

Standard library only: the hook runs it with `python3 -I`, outside any venv.

A row is one function: repo, path, name, line, lines, first doc line, and its body tokens. Python
functions come from `ast` (methods included); TypeScript/JavaScript `function f(` and
`const f = (...) =>`, and shell `f() {` from a brace-matching scan. Tests, vendored copies and
generated folders are left out, so a match points at the code a new function should reuse.
Other rows are for find only (all_rows() leaves them out of the similarity rule): kind sql (a SQL file and each
object it creates), yaml (a file and the named items of its top-level lists), doc (headings of references/ and docs/
pages and of SKILL.md bodies), const (a Python module's upper-case constants), skill (a SKILL.md's front matter).
Each repo entry also says which clone it is (group: the common git dir, shared by worktrees), its branch, whether
it's the main checkout, and its uncommitted paths, so find_grouped() shows a file once per clone.

Alike, for functions of at least 6 lines, from two local signals (no model call):
- tokens: body tokens weighted by rarity across the index (IDF-weighted Jaccard), so words every
  function has (self, return, json, print) don't make two functions look alike; and
- effects: what the code does: the calls it makes (last two dotted parts) and the command, URL,
  API path and SQL-object strings it names. Two functions doing one job in different words
  share these even when their tokens differ.
A pair is alike when weighted tokens reach MIN_SIM, or effects reach MIN_EFFECT with at least
MIN_SHARED_EFFECTS shared, or the names match and tokens reach MIN_SIM_SAME_NAME. Boilerplate
names (main, __init__, parse_args, ...) are never matched. Thresholds can be overridden in
repos.json ("thresholds").
"""

from __future__ import annotations

import ast
import datetime as dt
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

Row = dict[str, Any]
SKILL = Path(__file__).resolve().parents[1]
# Your data, apart from the skill's code: repos.json, shared/ (masters), reports/, seen-repos.json.
HOME = Path(os.environ.get("REUSE_FIRST_HOME", Path.home() / ".claude/reuse-first")).expanduser()
CONFIG = Path(os.environ.get("REUSE_FIRST_CONFIG", HOME / "repos.json"))
SEEN = HOME / "seen-repos.json"   # repos you edited in that repos.json doesn't list: indexed too
BIG = HOME / "big-repos.json"     # repos left out for size, so they're counted once, not per edit
MAX_REPO_FILES = 20000            # a bigger repo you edit in (a monorepo) isn't indexed: the hook stays fast
CACHE = Path(os.environ.get("REUSE_FIRST_CACHE", Path.home() / ".cache/reuse-first"))
INDEX = CACHE / "index.json"
CODE_EXT = {".py", ".sh", ".bash", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "dist", "build", "outputs", "__pycache__",
             ".next", "coverage", "target", ".mypy_cache", ".pytest_cache", ".ruff_cache", "site-packages"}
MIN_LINES = 3          # indexed; a function under SHORT_LINES needs SHORT_SIM to match
SHORT_LINES = 6
SHORT_SIM = 0.75       # weighted; or SHORT_RAW_SIM unweighted
SHORT_RAW_SIM = 0.9
MAX_CMD_DF = 8         # a command or API path in at most this many functions is distinctive
MAX_EFFECT_DF = 30     # an effect in more functions than this (F.col, json.loads) says nothing
MIN_SIM = 0.55
NEAR_SIM = 0.3          # a near miss, for the judge
NEAR_SHARED_EFFECTS = 2
MIN_SIM_SAME_NAME = 0.4
MIN_EFFECT = 0.6
MIN_SHARED_EFFECTS = 3
BOILERPLATE = {"main", "__init__", "__repr__", "__str__", "__eq__", "__hash__", "parse_args",
               "setUp", "tearDown", "run", "cli", "handler", "default"}
CALL = re.compile(r"([A-Za-z_][\w.]*)\s*\(")
STRING = re.compile(r"""["'`]([^"'`\n]{3,120})["'`]""")
EFFECT_STRING = re.compile(r"^(?:[a-z][\w-]*(?: [\w./:-]+)*|/[\w/.{}:-]+|https?://\S+|[\w-]+\.[\w-]+\.[\w-]+)$")
COMMON_CALLS = {"print", "len", "str", "int", "float", "list", "dict", "set", "tuple", "isinstance",
                "range", "enumerate", "sorted", "open", "join", "split", "strip", "get", "append",
                "format", "items", "keys", "values", "replace", "startswith", "endswith", "lower",
                "upper", "super", "map", "filter", "zip", "min", "max", "sum", "any", "all", "type",
                "return", "if", "for", "while", "not", "and", "or", "in", "elif", "except", "with",
                "assert", "yield", "await", "lambda", "catch", "switch", "typeof", "new"}
LIST_OF_STRINGS = re.compile(r"(?:\brun|Popen|check_output|check_call|\bcall|spawn|spawnSync|execFile|execFileSync|execa)\s*\(\s*\[\s*((?:[\"'][^\"'\n]{1,60}[\"']\s*,\s*){1,}[\"'][^\"'\n]{1,60}[\"'])")
TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\d+|[^\sA-Za-z0-9_]")
JS_FN = re.compile(r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s*\*?\s*([A-Za-z_$][\w$]*)\s*\(|"
                   r"^\s*(?:export\s+)?(?:const|let)\s+([A-Za-z_$][\w$]*)\s*(?::[^=]+)?=\s*(?:async\s+)?"
                   r"(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*(?::[^=]+)?=>", re.M)
SH_FN = re.compile(r"^\s*(?:function\s+)?([A-Za-z_][\w-]*)\s*\(\)\s*\{|^\s*function\s+([A-Za-z_][\w-]*)\s*\{", re.M)


def is_test(rel: str) -> bool:
    name = Path(rel).name
    parts = set(Path(rel).parts)
    return bool(parts & {"tests", "test", "__tests__", "pytests"}) or name.startswith("test_") or \
        name.endswith(("_test.py", ".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx", ".test.js"))


def is_code(path: Path) -> bool:
    if path.suffix in CODE_EXT:
        return True
    if path.suffix == "" and path.parent.name == "bin":
        try:
            return path.read_text(errors="replace")[:2] == "#!"
        except OSError:
            return False
    return False


def tokens(body: str) -> set[str]:
    return set(TOKEN.findall(body))


def effects(body: str) -> set[str]:
    """What the code does: calls it makes (last two dotted parts), strings naming commands, URLs,
    API paths or three-part names, and `cmd:` the external command a process call spells
    (run(["databricks", "current-user", "me", "-o", "json"]) -> cmd:databricks current-user me) or
    an API path (/api/2.0/sql/statements)."""
    out = set()
    for m in CALL.finditer(body):
        parts = m.group(1).split(".")
        tail = ".".join(parts[-2:]) if len(parts) > 1 else parts[0]
        if parts[-1] not in COMMON_CALLS and not parts[-1][:1].isupper():
            out.add("call:" + tail)
    for m in STRING.finditer(body):
        v = m.group(1).strip()
        if EFFECT_STRING.match(v) and not v.isdigit() and len(v.split()) <= 6:
            out.add("str:" + v)
            if re.match(r"^/(?:api|v\d+)(?:/|$)", v) or re.match(r"^/[\w.-]+/(?:api|v\d+)/", v):
                out.add("cmd:" + v)  # an API path
    for m in LIST_OF_STRINGS.finditer(body):
        words = [w.strip().strip("\"'") for w in m.group(1).split(",")]
        head = []
        for w in words:
            if w.startswith("-") or not re.fullmatch(r"[\w.:/-]+", w):
                break
            head.append(w)
        if len(head) >= 3:
            out.add("cmd:" + " ".join(head[:3]))
    return out


def weighted_jaccard(a: set[str] | list[str], b: set[str] | list[str], idf: dict[str, Any]) -> float:
    a, b = set(a), set(b)
    union = a | b
    if not union:
        return 0.0
    w = lambda xs: sum(idf.get(x, 1.0) for x in xs)  # noqa: E731
    return w(a & b) / w(union)


def idf_table(rows: list[Row]) -> dict[str, Any]:
    import math

    n = max(len(rows), 1)
    df: dict[str, int] = {}
    for r in rows:
        for t in set(r["tokens"]) | set(r.get("effects") or []):
            df[t] = df.get(t, 0) + 1
    out = {t: math.log(1 + n / c) for t, c in df.items()}
    out["\x00df"] = df  # type: ignore[assignment]
    return out


def jaccard(a: set[str] | list[str], b: set[str] | list[str]) -> float:
    a, b = set(a), set(b)
    return len(a & b) / len(a | b) if a or b else 0.0


def _brace_end(text: str, start: int, comments: str = "#") -> int:
    """Index just past the brace block that starts at the first '{' at or after start. Comments are skipped
    (comments="#": shell, where # starts one at the start of a word, not in $# or ${#x}; "//": JS and TS, with
    /* */), so an apostrophe in a comment (# the model's rows) doesn't open a quote that hides the closing brace.
    Shell uses _shell_block_end instead."""
    i = text.find("{", start)
    if i < 0:
        return len(text)
    depth = 0
    quote = ""
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == quote:
                quote = ""
        elif comments == "#" and ch == "#" and (i == 0 or text[i - 1] in " \t\n;"):
            nl = text.find("\n", i)
            i = len(text) if nl < 0 else nl
            continue
        elif comments == "//" and text.startswith("//", i):
            nl = text.find("\n", i)
            i = len(text) if nl < 0 else nl
            continue
        elif comments == "//" and text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = len(text) if end < 0 else end + 2
            continue
        elif ch in "'\"`":
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


SH_OPEN = re.compile(r"(?:^|(?<=[\s;&|(]))\{(?=\s)")                   # { as a word: a group or a body
SH_CLOSE = re.compile(r"(?:(?<=[\s;&])|^)\}(?=$|[\s;&|)<>])", re.MULTILINE)  # } as a word, after ; or a break
SH_COMMENT = re.compile(r"(?:^|(?<=[\s;]))#[^\n]*", re.MULTILINE)       # not $# or ${#x}


def _shell_blank(text: str) -> str:
    """text with comments and one-line single-quoted strings blanked (same length): an awk program on one line
    ('$1 == t { print $2 }') holds brace words that aren't the shell's. Measured on the tests repo's 145 shell
    scripts (2026-11-20), tracking quotes further (double quotes, single quotes across lines) placed more function
    ends wrong than it fixed: quoting starts over inside $( ), and awk programs span lines."""
    text = SH_COMMENT.sub(lambda m: " " * len(m.group(0)), text)
    return re.sub(r"'[^'\n]*'", lambda m: "'" + " " * (len(m.group(0)) - 2) + "'", text)


def _shell_block_end(text: str, start: int) -> int:
    """Index just past the shell function body whose { is at or after start. In shell { and } are reserved
    words, so only braces standing as words count (not ${var}, not {printf), outside comments and one-line
    single-quoted strings (_shell_blank)."""
    body = _shell_blank(text)
    i = body.find("{", start)
    if i < 0:
        return len(text)
    depth = 0
    for m in re.finditer(r"\{|\}", body[i:]):
        j = i + m.start()
        if body[j] == "{" and (j == i or SH_OPEN.match(body, j)):
            depth += 1
        elif body[j] == "}" and SH_CLOSE.match(body, j):
            depth -= 1
            if depth == 0:
                return j + 1
    return len(text)


def functions(text: str, path: str) -> list[Row]:
    """Every function in one file's text: name, line, lines, doc, body."""
    out: list[Row] = []
    suffix = Path(path).suffix
    if suffix == ".py" or (suffix == "" and text.startswith("#!") and "python" in text.split("\n", 1)[0]):
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return out
        lines = text.splitlines()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                end = getattr(node, "end_lineno", node.lineno) or node.lineno
                keep = list(range(node.lineno - 1, end))
                first = node.body[0] if node.body else None
                if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant) \
                        and isinstance(first.value.value, str):
                    # The docstring describes, it doesn't do: its examples aren't the function's effects.
                    drop = set(range(first.lineno - 1, (first.end_lineno or first.lineno)))
                    keep = [i for i in keep if i not in drop]
                body = "\n".join(lines[i] for i in keep)
                doc = (ast.get_docstring(node) or "").strip().split("\n")[0][:160]
                out.append({"name": node.name, "line": node.lineno, "lines": end - node.lineno + 1,
                            "doc": doc, "body": body})
        return out
    lang = ast_lang(path, text)
    if lang and AST_GREP:
        got = ast_functions(text, lang)
        if got is not None:
            return got
    return scan_functions(text, path)


def scan_functions(text: str, path: str) -> list[Row]:
    """Shell and JS/TS functions without ast-grep: a regex finds each start, a brace scan its end (the fallback;
    ast-grep's syntax tree is exact where this scan guesses)."""
    out: list[Row] = []
    suffix = Path(path).suffix
    pattern = SH_FN if suffix in (".sh", ".bash", "") else JS_FN
    for m in pattern.finditer(text):
        name = next(g for g in m.groups() if g)
        at = m.end() - 1 if text[m.end() - 1] == "{" else m.end()
        end = _shell_block_end(text, at) if suffix in (".sh", ".bash", "") else _brace_end(text, at, "//")
        body = text[m.start():end]
        line = text.count("\n", 0, m.start()) + 1
        out.append({"name": name, "line": line, "lines": body.count("\n") + 1, "doc": "", "body": body})
    return out


# ast-grep (tree-sitter), when installed: shell, JS and TS functions from a syntax tree, so a function ends where
# the language says, whatever its strings, comments and nested quoting hold. REUSE_FIRST_NO_AST_GREP=1 turns it off.
AST_GREP = None if os.environ.get("REUSE_FIRST_NO_AST_GREP") else shutil.which("ast-grep")
AST_LANGS = {".sh": "bash", ".bash": "bash", ".js": "javascript", ".jsx": "javascript", ".mjs": "javascript",
             ".cjs": "javascript", ".ts": "typescript", ".tsx": "tsx"}
JS_KINDS = ("{kind: function_declaration}, {kind: generator_function_declaration}, {kind: method_definition}, "
            "{kind: variable_declarator, has: {field: value, any: [{kind: arrow_function}, {kind: function_expression}]}}")
AST_NAME = re.compile(r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?(?:static\s+)?(?:get\s+|set\s+)?"
                      r"(?:function\s*\*?\s*)?(?:(?:const|let|var)\s+)?([A-Za-z_$][\w$-]*)")


def ast_lang(path: str, text: str) -> str | None:
    suffix = Path(path).suffix
    if suffix == "":
        first = text.split("\n", 1)[0]
        return "bash" if first.startswith("#!") and re.search(r"\b(ba|z|k)?sh\b", first) else None
    return AST_LANGS.get(suffix)


def ast_rule(lang: str) -> str:
    kinds = "{kind: function_definition}" if lang == "bash" else JS_KINDS
    return f"id: fn-{lang}\nlanguage: {lang}\nrule:\n  any: [{kinds}]"


def ast_rows(matches: list[dict]) -> list[Row]:
    out = []
    for m in matches:
        name = AST_NAME.match(m["text"])
        if not name:
            continue
        start, end = m["range"]["start"]["line"] + 1, m["range"]["end"]["line"] + 1
        out.append({"name": name.group(1), "line": start, "lines": end - start + 1, "doc": "", "body": m["text"]})
    return out


def ast_functions(text: str, lang: str) -> list[Row] | None:
    """One file's functions from ast-grep over its text (stdin), or None when ast-grep fails (the caller scans)."""
    try:
        res = subprocess.run([AST_GREP, "scan", "--inline-rules", ast_rule(lang), "--json=compact", "--stdin"],
                             input=text, capture_output=True, text=True, timeout=20, check=False)
        return ast_rows(json.loads(res.stdout or "[]")) if res.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def ast_functions_batch(root: Path, rels: list[str]) -> dict[str, list[Row]]:
    """{path: functions} for a repo's shell, JS and TS files in one ast-grep run per language (an index build),
    instead of one per file. A file it doesn't report has no functions; a failed run reports nothing (scan)."""
    by_lang: dict[str, list[str]] = {}
    for rel in rels:
        lang = AST_LANGS.get(Path(rel).suffix)
        if lang:
            by_lang.setdefault(lang, []).append(rel)
    out: dict[str, list[Row]] = {}
    for lang, files in by_lang.items():
        got: dict[str, list[dict]] = {f: [] for f in files}
        try:
            for i in range(0, len(files), 500):   # the command line stays short
                res = subprocess.run([AST_GREP, "scan", "--inline-rules", ast_rule(lang), "--json=compact",
                                      *files[i:i + 500]], cwd=root, capture_output=True, text=True, timeout=300,
                                     check=False)
                if res.returncode not in (0, 1):
                    raise ValueError(res.stderr[:200])
                for m in json.loads(res.stdout or "[]"):
                    got.setdefault(m["file"], []).append(m)
        except (OSError, ValueError, subprocess.TimeoutExpired):
            continue
        out.update({f: ast_rows(ms) for f, ms in got.items()})
    return out


def load_config() -> dict[str, Any]:
    if CONFIG.exists():
        return json.loads(CONFIG.read_text())
    return {"repos": {}}


def repo_root(path: Path) -> Path | None:
    res = subprocess.run(["git", "-C", str(path if path.is_dir() else path.parent), "rev-parse",
                          "--show-toplevel"], capture_output=True, text=True)
    return Path(res.stdout.strip()) if res.returncode == 0 and res.stdout.strip() else None


def vendored_dests(root: Path) -> set[str]:
    f = root / ".vendored.json"
    if not f.exists():
        return set()
    try:
        return {c["dest"] for c in json.loads(f.read_text()).get("copies", [])}
    except (ValueError, KeyError):
        return set()


def ignore_globs(root: Path) -> list[str]:
    """Paths that are known copies, not duplication (generated per-agent copies, build output),
    from repos.json "ignore": {"<repo name>": ["plugins/**"]} by the repo folder's name."""
    cfg = load_config()
    names = {n for n, p in cfg.get("repos", {}).items() if Path(p).expanduser() == root} | {root.name}
    out: list[str] = []
    for n in names:
        out += (cfg.get("ignore") or {}).get(n, [])
    return out


def repo_files(root: Path) -> list[Path]:
    res = subprocess.run(["git", "-C", str(root), "ls-files", "-co", "--exclude-standard"],
                         capture_output=True, text=True)
    paths = res.stdout.split("\n") if res.returncode == 0 else []
    import fnmatch

    globs = ignore_globs(root)
    out = []
    for rel in paths:
        if not rel or any(part in SKIP_DIRS for part in Path(rel).parts):
            continue
        if any(fnmatch.fnmatch(rel, g) for g in globs):
            continue
        p = root / rel
        if p.is_file() and (is_code(p) or is_skill(p) or is_artifact(p)):
            out.append(p)
    return out


FRONT = re.compile(r"\A---\s*\n(.*?)\n---\s*(?:\n|\Z)", re.S)


def skill_of(text: str) -> tuple[str, str] | None:
    """(name, description) from a SKILL.md's front matter (block scalars folded), or None."""
    m = FRONT.match(text)
    if not m:
        return None
    fields: dict[str, str] = {}
    key = None
    for line in m.group(1).splitlines():
        km = re.match(r"^([A-Za-z_-]+):\s*(.*)$", line)
        if km:
            key, val = km.group(1), km.group(2).strip()
            fields[key] = "" if val in (">", ">-", "|", "|-", ">+") else val.strip("\"'")
        elif key and line.startswith((" ", "\t")):
            fields[key] = (fields[key] + " " + line.strip()).strip()
    name = fields.get("name", "")
    return (name, fields.get("description", "")) if name else None


def is_skill(path: Path) -> bool:
    return path.name == "SKILL.md"


def skill_row(repo: str, root: str, rel: str, text: str) -> Row | None:
    got = skill_of(text)
    if not got:
        return None
    name, desc = got
    return {"repo": repo, "root": root, "path": rel, "name": name, "line": 1, "lines": text.count("\n") + 1,
            "doc": desc[:300], "tokens": sorted(w.lower() for w in tokens(desc) if len(w) > 2), "effects": [],
            "kind": "skill"}


def row_of(repo: str, root: str, rel: str, f: Row) -> Row:
    return {"repo": repo, "root": root, "path": rel, "name": f["name"], "line": f["line"],
            "lines": f["lines"], "doc": f["doc"],
            "tokens": sorted(tokens(f["body"]) - {f["name"]}), "effects": sorted(effects(f["body"]))}


# Artifacts: what isn't a function but is reused as one (a SQL view, a YAML block, a reference page). Indexed for
# find only (kinds sql, yaml, doc): all_rows() leaves them out, so they're never compared as duplicate code.
DOC_DIRS = {"references", "docs"}
SQL_CREATE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMP(?:ORARY)?\s+)?"
    r"(MATERIALIZED\s+VIEW|STREAMING\s+TABLE|VIEW|TABLE|FUNCTION|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.${}`\"]+)",
    re.IGNORECASE | re.MULTILINE)
SQL_COMMENT = re.compile(r"\bCOMMENT\s+'((?:[^']|'')*)'", re.IGNORECASE)
YAML_KEY = re.compile(r"^( *)(?:- )?([A-Za-z_][\w-]*):(?:\s|$)")
YAML_ITEM_NAME = re.compile(r"^( *)- name:\s*['\"]?([^'\"#\n]+?)['\"]?\s*(?:#.*)?$")
YAML_DOC_FIELDS = ("description", "comment", "target", "role", "kind", "summary")
MD_HEADING = re.compile(r"^(#{1,4})\s+(.+?)\s*#*\s*$")


def is_artifact(path: Path) -> bool:
    if path.suffix == ".sql" or path.suffix in (".yaml", ".yml"):
        return True
    return path.suffix == ".md" and not is_skill(path) and bool(set(path.parts) & DOC_DIRS)


def _words(text: str) -> list[str]:
    return sorted({w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", text)})[:400]


def _comment_block(lines: list[str], mark: str) -> tuple[str, int]:
    """The comment lines at the top of a file (after a shebang or blank lines), joined; and how many lines."""
    out, i = [], 0
    while i < len(lines) and (not lines[i].strip() or lines[i].startswith("#!")):
        i += 1
    while i < len(lines) and lines[i].lstrip().startswith(mark):
        out.append(lines[i].lstrip()[len(mark):].strip())
        i += 1
    return " ".join(x for x in out if x), i


def sql_artifacts(text: str, rel: str) -> list[Row]:
    """A SQL file: one row for the file (its header comments, `-- key: value` lines included), one per CREATE
    (the object's last name part, its comment lines above and its COMMENT '...')."""
    lines = text.splitlines()
    head, _ = _comment_block(lines, "--")
    out = [{"name": Path(rel).stem, "line": 1, "lines": len(lines), "doc": head[:300], "words": _words(text)}]
    for m in SQL_CREATE.finditer(text):
        name = re.sub(r"\$\{[^}]*\}\.?|[`\"]", "", m.group(2)).split(".")[-1]
        if not name:
            continue
        line = text.count("\n", 0, m.start()) + 1
        above, j = [], line - 2
        while j >= 0 and lines[j].lstrip().startswith("--"):
            above.insert(0, lines[j].lstrip()[2:].strip())
            j -= 1
        nxt = SQL_CREATE.search(text, m.end())
        body = text[m.start():nxt.start() if nxt else len(text)]
        cm = SQL_COMMENT.search(body)
        doc = " ".join(x for x in above + ([cm.group(1).replace("''", "'")] if cm else []) if x)
        out.append({"name": name, "line": line, "lines": body.count("\n") + 1,
                    "doc": f"{m.group(1).upper()}: {doc}"[:300], "words": _words(body)})
    return out


def yaml_artifacts(text: str, rel: str) -> list[Row]:
    """A YAML file: one row for the file (its leading comments; its keys as words), and one per named item of a
    top-level list (`signals: - name: lineitem_lateness`), documented by its description, comment, target or role."""
    lines = text.splitlines()
    head, _ = _comment_block(lines, "#")
    keys = {m.group(2) for ln in lines if (m := YAML_KEY.match(ln))}
    top = [m.group(2) for ln in lines if (m := YAML_KEY.match(ln)) and not m.group(1) and not ln.startswith("-")]
    out = [{"name": Path(rel).stem, "line": 1, "lines": len(lines), "doc": (head or "keys: " + ", ".join(top))[:300],
            "words": sorted({k.lower() for k in keys} | set(_words(head)))[:400]}]
    block = ""
    for i, ln in enumerate(lines):
        km = YAML_KEY.match(ln)
        if km and not km.group(1) and not ln.startswith("-"):
            block = km.group(2)
        im = YAML_ITEM_NAME.match(ln)
        if not (im and block and len(im.group(1)) <= 4):
            continue
        ind = len(im.group(1))
        end = i + 1
        while end < len(lines) and (not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip()) > ind):
            end += 1
        item = lines[i:end]
        fields = {m.group(2): ln2.split(":", 1)[1].strip().strip("'\"") for ln2 in item
                  if (m := YAML_KEY.match(ln2)) and len(m.group(1)) <= ind + 4}
        doc = "; ".join(f"{f}: {fields[f]}" for f in YAML_DOC_FIELDS if fields.get(f))
        out.append({"name": im.group(2).strip(), "line": i + 1, "lines": end - i,
                    "doc": f"{block}: {doc}"[:300], "words": _words("\n".join(item))})
    return out


def md_artifacts(text: str, rel: str) -> list[Row]:
    """A reference or docs page: one row per heading (#..####), documented by the first line of text below it."""
    lines, out = text.splitlines(), []
    for i, ln in enumerate(lines):
        m = MD_HEADING.match(ln)
        if not m:
            continue
        j = i + 1
        while j < len(lines) and not lines[j].strip():
            j += 1
        first = lines[j].strip() if j < len(lines) and not MD_HEADING.match(lines[j]) else ""
        end = j
        while end < len(lines) and not MD_HEADING.match(lines[end]):
            end += 1
        out.append({"name": m.group(2), "line": i + 1, "lines": end - i, "doc": first[:300],
                    "words": _words("\n".join(lines[i:end]))})
    return out


def artifact_rows(repo: str, root: str, rel: str, text: str) -> list[Row]:
    suffix = Path(rel).suffix
    kind = {".sql": "sql", ".yaml": "yaml", ".yml": "yaml", ".md": "doc"}[suffix]
    parse = {"sql": sql_artifacts, "yaml": yaml_artifacts, "doc": md_artifacts}[kind]
    return [{"repo": repo, "root": root, "path": rel, "name": a["name"], "line": a["line"], "lines": a["lines"],
             "doc": a["doc"], "tokens": a["words"], "effects": [], "kind": kind} for a in parse(text, rel)]


def py_constants(repo: str, root: str, rel: str, text: str) -> list[Row]:
    """A Python module's upper-case constants with a string or number value (ROUTE_TAG = "lfc_medallion_route"):
    names a caller should import, not spell again. Kind const, for find only."""
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return []
    out = []
    for node in tree.body:
        if not (isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, (str, int, float)) and not isinstance(node.value.value, bool)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for t in targets:
            if isinstance(t, ast.Name) and t.id.isupper() and len(t.id) > 2:
                val = str(node.value.value)
                out.append({"repo": repo, "root": root, "path": rel, "name": t.id, "line": node.lineno, "lines": 1,
                            "doc": f"= {val!r}"[:300], "tokens": _words(f"{t.id} {val}"), "effects": [],
                            "kind": "const"})
    return out


def file_rows(repo: str, root: Path, rel: str, text: str, pre: list[Row] | None = None) -> list[Row]:
    """Every row of one file: a skill (and its sections), its artifacts (SQL, YAML, a reference page), or its
    functions (and a Python module's constants)."""
    p = root / rel
    if is_skill(p):
        sr = skill_row(repo, str(root), rel, text)
        body = FRONT.sub("", text, count=1)
        sections = [{"repo": repo, "root": str(root), "path": rel, "name": a["name"],
                     "line": a["line"] + text.count("\n", 0, len(text) - len(body)), "lines": a["lines"],
                     "doc": a["doc"], "tokens": a["words"], "effects": [], "kind": "doc"}
                    for a in md_artifacts(body, rel)]
        return ([sr] if sr else []) + sections
    if is_artifact(p):
        return artifact_rows(repo, str(root), rel, text)
    fns = pre if pre is not None else functions(text, rel)   # pre: the repo's ast-grep batch had this file
    rows = [row_of(repo, str(root), rel, f) for f in fns if f["lines"] >= MIN_LINES]
    return rows + (py_constants(repo, str(root), rel, text) if p.suffix == ".py" else [])


def checkout_meta(root: Path) -> dict[str, Any]:
    """Which repo a checkout belongs to (worktrees of one clone share its common git dir), its branch, whether it's
    the main checkout, and its uncommitted paths (changed or untracked), so find shows one result per repo."""
    def git(*args: str) -> str:
        res = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, check=False)
        return res.stdout.strip() if res.returncode == 0 else ""
    common = git("rev-parse", "--path-format=absolute", "--git-common-dir")
    gitdir = git("rev-parse", "--path-format=absolute", "--git-dir")
    dirty = []
    for ln in git("status", "--porcelain", "-uall").splitlines():
        path = ln[3:].split(" -> ")[-1].strip('"')
        if path:
            dirty.append(path)
    return {"group": common or str(root), "branch": git("branch", "--show-current") or "(detached)",
            "main": bool(common) and common == gitdir, "dirty": sorted(dirty)}


def index_repo(name: str, root: Path) -> list[Row]:
    rows: list[Row] = []
    copies = vendored_dests(root)
    files = repo_files(root)
    pre = ast_functions_batch(root, [str(p.relative_to(root)) for p in files]) if AST_GREP else {}
    for p in files:
        rel = str(p.relative_to(root))
        if is_test(rel) or rel in copies:
            continue
        try:
            text = p.read_text(errors="replace")
        except OSError:
            continue
        if len(text) > 400_000:
            continue
        rows += file_rows(name, root, rel, text, pre.get(rel))
    return rows


def seen_repos() -> dict[str, str]:
    try:
        return json.loads(SEEN.read_text())
    except (OSError, ValueError):
        return {}


def repo_name(root: Path) -> str:
    """The name repos.json gives this checkout, else the one it was remembered under, else its folder."""
    for n, p in load_config().get("repos", {}).items():
        if Path(p).expanduser() == root:
            return n
    for n, p in seen_repos().items():
        if Path(p) == root:
            return n
    return root.name


def remember_repo(root: Path) -> str | None:
    """Index a repo you edit in from now on (not one with more than MAX_REPO_FILES files). Its name."""
    known = {Path(p).expanduser() for p in load_config().get("repos", {}).values()} | \
        {Path(p) for p in seen_repos().values()}
    if root in known:
        return repo_name(root)
    try:
        big = set(json.loads(BIG.read_text()))
    except (OSError, ValueError):
        big = set()
    if str(root) in big:
        return None
    res = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, text=True)
    if res.returncode != 0 or res.stdout.count("\n") > MAX_REPO_FILES:
        HOME.mkdir(parents=True, exist_ok=True)
        BIG.write_text(json.dumps(sorted(big | {str(root)})) + "\n")
        return None
    seen = seen_repos()
    taken = set(load_config().get("repos", {})) | set(seen)
    name = root.name if root.name not in taken else f"{root.parent.name}-{root.name}"
    seen[name] = str(root)
    HOME.mkdir(parents=True, exist_ok=True)
    SEEN.write_text(json.dumps(seen, indent=1, sort_keys=True) + "\n")
    return name


def build(extra_roots: list[Path] | None = None) -> dict[str, Any]:
    """Index every configured and remembered repo (plus extra_roots) and write the cache."""
    cfg = load_config()
    repos: dict[str, Any] = {}
    targets = {n: Path(p).expanduser() for n, p in cfg.get("repos", {}).items()}
    seen = seen_repos()
    gone = {n for n, p in seen.items() if not Path(p).is_dir()}   # a removed worktree or clone: forget it
    if gone:                                                      # (editing in it again remembers it)
        SEEN.write_text(json.dumps({n: p for n, p in seen.items() if n not in gone}, indent=1, sort_keys=True) + "\n")
    for n, p in seen.items():
        if n not in gone:
            targets.setdefault(n, Path(p))
    for r in extra_roots or []:
        targets.setdefault(r.name, r)
    for name, root in targets.items():
        if root.is_dir():
            repos[str(root)] = {"name": name, "indexed_at": now(), "rows": index_repo(name, root),
                                **checkout_meta(root)}
    data = {"built_at": now(), "repos": repos}
    CACHE.mkdir(parents=True, exist_ok=True)
    tmp = INDEX.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(INDEX)
    return data


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load() -> dict[str, Any] | None:
    try:
        return json.loads(INDEX.read_text())
    except (OSError, ValueError):
        return None


def age_hours(data: dict[str, Any]) -> float:
    built = dt.datetime.strptime(data["built_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - built).total_seconds() / 3600


def all_rows(data: dict[str, Any], kind: str = "function") -> list[Row]:
    """Function rows (the default), skill rows (kind="skill"), or both (kind="any")."""
    return [r for repo in data.get("repos", {}).values() for r in repo["rows"]
            if kind == "any" or r.get("kind", "function") == kind]


def update_file(data: dict[str, Any], root: Path, rel: str) -> None:
    """Re-index one file in the cached index (after a write: the file is now uncommitted)."""
    key = str(root)
    repo = data["repos"].setdefault(key, {"name": root.name, "indexed_at": now(), "rows": []})
    repo["rows"] = [r for r in repo["rows"] if r["path"] != rel]
    p = root / rel
    if p.is_file() and not is_test(rel) and rel not in vendored_dests(root) and \
            (is_skill(p) or is_artifact(p) or is_code(p)):
        repo["rows"] += file_rows(repo["name"], root, rel, p.read_text(errors="replace"))
        repo["dirty"] = sorted(set(repo.get("dirty") or []) | {rel})
    CACHE.mkdir(parents=True, exist_ok=True)
    INDEX.write_text(json.dumps(data))


def thresholds() -> dict[str, float]:
    t = {"min_sim": MIN_SIM, "min_sim_same_name": MIN_SIM_SAME_NAME, "min_effect": MIN_EFFECT,
         "min_shared_effects": MIN_SHARED_EFFECTS}
    t.update(load_config().get("thresholds") or {})
    return t


def pair_score(name: str, toks: set[str], effs: set[str], r: Row, idf: dict[str, Any],
               t: dict[str, float], lines: tuple[int, int] = (SHORT_LINES, SHORT_LINES)
               ) -> tuple[float, str] | None:
    """(score, why) when a function and an index row are alike, else None. lines = the two
    functions' line counts: a short one matches only when nearly identical."""
    if name in BOILERPLATE or r["name"] in BOILERPLATE:
        return None
    tok = weighted_jaccard(toks, r["tokens"], idf)
    df = idf.get("\x00df") or {}
    reffs = {x for x in r.get("effects") or [] if df.get(x, 0) <= MAX_EFFECT_DF}
    effs = {x for x in effs if df.get(x, 0) <= MAX_EFFECT_DF}
    shared = effs & reffs
    # A distinctive command or API path both run: a duplicate at any length (a short wrapper too).
    cmds = sorted(x[4:] for x in shared if x.startswith("cmd:") and df.get(x, 0) <= MAX_CMD_DF)
    if cmds:
        return max(tok, 0.7), "both run " + ", ".join(f"`{c}`" for c in cmds[:2])
    if min(lines[0], r.get("lines", SHORT_LINES)) < SHORT_LINES:
        raw = jaccard(toks, r["tokens"])
        return (max(tok, raw), "body, short") if tok >= SHORT_SIM or raw >= SHORT_RAW_SIM else None
    eff = len(shared) / len(effs | reffs) if effs | reffs else 0.0
    if tok >= t["min_sim"]:
        return tok, "body"
    if eff >= t["min_effect"] and len(shared) >= t["min_shared_effects"]:
        return eff, "does the same: " + ", ".join(sorted(x.split(":", 1)[1] for x in shared)[:4])
    if r["name"] == name and tok >= t["min_sim_same_name"]:
        return tok, "same name"
    return None


def similar(name: str, body: str, rows: list[Row], skip: tuple[str, str] | None = None,
            n: int = 3, idf: dict[str, Any] | None = None,
            ignore: set[tuple[str, str]] | None = None) -> list[tuple[float, Row, str]]:
    """Rows alike a function (name, body), best first. skip = (root, path) of the file itself;
    ignore = (name, "repo:path:name") pairs the user already approved as a different job."""
    idf = idf if idf is not None else idf_table(rows)
    t = thresholds()
    toks, effs = tokens(body) - {name}, effects(body)
    hits = []
    n_lines = body.count("\n") + 1
    for r in rows:
        if skip and (r["root"], r["path"]) == skip:
            continue
        if ignore and (name, f"{r['repo']}:{r['path']}:{r['name']}") in ignore:
            continue
        got = pair_score(name, toks, effs, r, idf, t, (n_lines, r.get("lines", SHORT_LINES)))
        if got:
            hits.append((got[0], r, got[1]))
    # Ties: the same name first, then the closest size (the twin, not a big function that also calls it).
    hits.sort(key=lambda h: (-round(h[0], 2), h[1]["name"] != name, abs(h[1].get("lines", 0) - n_lines)))
    return hits[:n]


def near_misses(name: str, body: str, rows: list[Row], skip: tuple[str, str] | None = None,
                n: int = 3, idf: dict[str, Any] | None = None, ignore: set[tuple[str, str]] | None = None,
                exclude: set[tuple[str, str, str]] | None = None) -> list[tuple[float, Row]]:
    """Rows the rule didn't flag but that might do the same job: tokens at NEAR_SIM or more, the same
    name, or NEAR_SHARED_EFFECTS shared distinctive effects. Best first; for a judge by meaning.
    exclude = (root, path, name) of rows already flagged."""
    idf = idf if idf is not None else idf_table(rows)
    t = thresholds()
    df = idf.get("\x00df") or {}
    toks = tokens(body) - {name}
    effs = {x for x in effects(body) if df.get(x, 0) <= MAX_EFFECT_DF}
    n_lines = body.count("\n") + 1
    out = []
    for r in rows:
        if name in BOILERPLATE or r["name"] in BOILERPLATE:
            continue
        if skip and (r["root"], r["path"]) == skip:
            continue
        if exclude and (r["root"], r["path"], r["name"]) in exclude:
            continue
        if ignore and (name, f"{r['repo']}:{r['path']}:{r['name']}") in ignore:
            continue
        tok = weighted_jaccard(toks, r["tokens"], idf)
        shared = effs & set(r.get("effects") or [])
        if tok >= NEAR_SIM or r["name"] == name or len(shared) >= NEAR_SHARED_EFFECTS:
            if pair_score(name, toks, effects(body), r, idf, t, (n_lines, r.get("lines", SHORT_LINES))):
                continue
            out.append((tok + 0.05 * len(shared) + 0.2 * (r["name"] == name), r))
    out.sort(key=lambda h: -h[0])
    return out[:n]


SKILL_NEAR = 0.05  # weighted description overlap for a candidate; the judge decides (at most 4 per skill)


def similar_skills(name: str, desc: str, skills: list[Row], skip: tuple[str, str] | None = None,
                   n: int = 4) -> list[tuple[float, Row]]:
    """Existing skills a new one might repeat: the same name, or descriptions with rare words in common."""
    idf = idf_table(skills)
    toks = {w.lower() for w in tokens(desc) if len(w) > 2}
    out = []
    for r in skills:
        if skip and (r["root"], r["path"]) == skip:
            continue
        sim = weighted_jaccard(toks, r["tokens"], idf)
        if r["name"] == name or sim >= SKILL_NEAR:
            out.append((sim + (0.5 if r["name"] == name else 0.0), r))
    out.sort(key=lambda h: -h[0])
    return out[:n]


FLOOR = 0.4        # find_grouped drops results under this share of the top score (show_all keeps them)
HERE_BOOST = 3     # results from the repo you're in rank ahead of equal ones elsewhere


def find(words: list[str], rows: list[Row], n: int = 15) -> list[tuple[int, Row]]:
    """Rank functions and skills by query words: 3 per word in the name, 2 in the doc line (a
    skill's description), 1 in the path, 1 in the code (an identifier or named string containing
    it, so `find chrome playwright` finds a body that calls connect_over_cdp on DevToolsActivePort).
    The bonus for every word found goes only to a row that has one in its name, doc or path: a long
    function whose code happens to hold every word (a 200-line shell loop) isn't a match by itself."""
    q = [w.lower() for w in words]
    out = []
    for r in rows:
        name, doc, path = r["name"].lower(), r["doc"].lower(), r["path"].lower()
        code = " ".join(r["tokens"]).lower() + " " + " ".join(r.get("effects") or []).lower()
        hit = {w: (w in name, w in doc, w in path, w in code) for w in q}
        score = sum(3 * a + 2 * b + c + d for a, b, c, d in hit.values())
        if score and all(any(h) for h in hit.values()) and any(a or b or c for a, b, c, _ in hit.values()):
            score += 2 * len(q)
        if score:
            out.append((score + (5 if name == "_".join(q) else 0), r))
    out.sort(key=lambda x: (-x[0], x[1]["repo"], x[1]["path"]))
    return out[:n]


def one_per_clone(data: dict[str, Any], rows: list[Row]) -> list[Row]:
    """rows without the worktree copies: a function in several checkouts of one clone (same path and name) is kept
    once, from the main checkout when it's indexed, so a report counts code, not checkouts. The kept row's `also`
    lists the other checkouts' branches."""
    meta = data.get("repos", {})

    def group(r: Row) -> str:
        return (meta.get(r["root"]) or {}).get("group") or r["root"]

    def rank(r: Row) -> tuple[bool, str]:
        return (not (meta.get(r["root"]) or {}).get("main"), r["root"])
    best: dict[tuple[str, str, str], Row] = {}
    also: dict[tuple[str, str, str], list[str]] = {}
    for r in rows:
        k = (group(r), r["path"], r["name"])
        branch = (meta.get(r["root"]) or {}).get("branch") or r["repo"]
        if k not in best or rank(r) < rank(best[k]):
            if k in best:
                also.setdefault(k, []).append((meta.get(best[k]["root"]) or {}).get("branch") or best[k]["repo"])
            best[k] = r
        else:
            also.setdefault(k, []).append(branch)
    return [{**r, "also": sorted(set(also[k]))} if k in also else r for k, r in best.items()]


def find_grouped(words: list[str], data: dict[str, Any], n: int = 15, repos: list[str] | None = None,
                 here: Path | None = None, show_all: bool = False) -> list[tuple[int, Row, str]]:
    """find() over every row, one result per repo: checkouts of one clone (its worktrees) share a group, and a file
    and name found in several is shown once, from the main checkout when it has it. The note says where else it is:
    "also on <branches>", "only on <branch>" (not in the main checkout), "uncommitted" (changed or untracked).
    repos: only these repos (a checkout's name finds its whole clone). here: the checkout you're in, whose clone's
    results get HERE_BOOST. Results under FLOOR of the top score are dropped unless show_all."""
    meta = {root: repo for root, repo in data.get("repos", {}).items()}
    names: dict[str, set[str]] = {}
    for root, repo in meta.items():
        names.setdefault(repo.get("group") or root, set()).add(repo["name"])
    here_group = meta.get(str(here), {}).get("group") or str(here) if here else None
    rows = []
    for root, repo in meta.items():
        group = repo.get("group") or root
        if repos and not (names[group] & set(repos)):
            continue
        for r in repo["rows"]:
            rows.append({**r, "_group": group})
    ranked = [(s + (HERE_BOOST if r["_group"] == here_group else 0), r) for s, r in find(words, rows, n=len(rows))]
    groups: dict[tuple[str, str, str], list[tuple[int, Row]]] = {}
    for score, r in ranked:
        groups.setdefault((r["_group"], r["path"], r["name"]), []).append((score, r))
    out = []
    for hits in groups.values():
        best = max(s for s, _ in hits)
        checkouts = {r["root"]: r for _, r in hits}
        main = [r for root, r in checkouts.items() if meta[root].get("main")]
        rep = main[0] if main else max(hits, key=lambda h: h[0])[1]
        others = sorted(meta[root].get("branch", meta[root]["name"]) for root in checkouts if root != rep["root"])
        dirty = rep["path"] in set(meta[rep["root"]].get("dirty") or [])
        notes = []
        if not main and any(meta[root].get("group") for root in checkouts):
            notes.append(f"only on {meta[rep['root']].get('branch', '?')}")
        elif others:
            notes.append(f"also on {', '.join(others)}")
        if dirty:
            notes.append("uncommitted")
        out.append((best, {k: v for k, v in rep.items() if k != "_group"}, "; ".join(notes)))
    out.sort(key=lambda x: (-x[0], x[1]["repo"], x[1]["path"]))
    if out and not show_all:
        out = [o for o in out if o[0] >= FLOOR * out[0][0]]
    return out[:n]


# The hook's side: the file after a Write/Edit/MultiEdit, the functions it adds, and the approval prompt.
def new_text(tool: str, inp: dict[str, Any], old: str) -> str | None:
    """A file's text after a Write, Edit or MultiEdit tool call (old: its text before)."""
    if tool == "Write":
        return inp.get("content")
    edits = inp.get("edits") if tool == "MultiEdit" else [inp]
    text = old
    for e in edits or []:
        a, b = e.get("old_string", ""), e.get("new_string", "")
        if a and a in text:
            text = text.replace(a, b) if e.get("replace_all") else text.replace(a, b, 1)
    return text


def added_functions(old: str, new: str, path: str) -> list[dict[str, Any]]:
    """Functions in the new text whose name the old text doesn't define."""
    before = {f["name"] for f in functions(old, path)}
    return [f for f in functions(new, path) if f["name"] not in before and f["lines"] >= MIN_LINES]


def ask(reason: str) -> None:
    """Ask the user to approve the tool call (a PreToolUse hook's answer on stdout), with the reason."""
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                             "permissionDecision": "ask",
                                             "permissionDecisionReason": reason}}))
