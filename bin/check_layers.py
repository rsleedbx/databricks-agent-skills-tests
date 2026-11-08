#!/usr/bin/env python3
"""bin/check_layers.py [--report] [--baseline] [--root DIR]: the layers in layers.yaml, and the one-way rule.

Finds every reference from a skill's files (or a bin script) to another skill or bin script: skills/<x>/..., ../<x>/...,
$SK/<x>, <x>/scripts/... written bare, and bin/<file>. Each reference is an edge; an edge is allowed when its target's
layer is the source's layer or one that the source's layer may use (layers.yaml). A disallowed edge fails the check
unless layers-baseline.txt has a line for it:  <from> -> <to>: <file>:<pattern>  (pattern: text on the same line).
A baseline line that matches no edge fails too, so the baseline only shrinks. A skill or bin file that isn't listed in
layers.yaml fails: add it to the layer it belongs to.

  --report    the edge table by layer pair, then each skill-level edge with its count and files
  --baseline  print the baseline lines for the current disallowed edges (to paste into layers-baseline.txt)
  --paths L1,L2  print the paths of those layers (skill folders and bin files that exist), for git filter-repo
  --root DIR  check another tree (bin/test_check_layers.py uses a fixture tree)

Exit 0 when clean, 1 with one line per problem. Stdlib only.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

R = Path(__file__).resolve().parent.parent
SCAN_DIRS = ("skills", ".claude/skills", "bin")
BASELINE = "layers-baseline.txt"
TOKEN_CHARS = re.compile(r"[\w.\-/$]")
BASELINE_LINE = re.compile(r"^(\S+) -> (\S+): ([^:\s]+):(.+?)\s*$")


# --- layers.yaml: the subset this file uses (nested mappings, block lists "- x", flow lists "[a, b]") ----------------

def _scalar(text: str):
    text = text.strip()
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        return [] if not inner else [x.strip() for x in inner.split(",")]
    return text


def parse_yaml(text: str) -> dict:
    lines = []
    for raw in text.splitlines():
        s = re.sub(r"(^|\s)#.*$", "", raw).rstrip()
        if s.strip():
            lines.append((len(s) - len(s.lstrip(" ")), s.strip()))

    def block(i: int, indent: int):
        if lines[i][1].startswith("- "):
            out = []
            while i < len(lines) and lines[i][0] == indent and lines[i][1].startswith("- "):
                out.append(_scalar(lines[i][1][2:]))
                i += 1
            return out, i
        out = {}
        while i < len(lines) and lines[i][0] == indent:
            key, _, rest = lines[i][1].partition(":")
            rest = rest.strip()
            i += 1
            if rest:
                out[key.strip()] = _scalar(rest)
            elif i < len(lines) and lines[i][0] > indent:
                out[key.strip()], i = block(i, lines[i][0])
            else:
                out[key.strip()] = {}
        return out, i

    if not lines:
        return {}
    value, _ = block(0, lines[0][0])
    return value


# --- the model -------------------------------------------------------------------------------------------------------

class Model:
    def __init__(self, layers: dict):
        self.layers = layers
        self.node_layer: dict[str, str] = {}        # "lfc-source-x" or "bin/name.sh" -> layer
        self.declared: dict[str, list[str]] = {}     # layer -> declared skill names (not pending)
        self.bin_names: set[str] = set()
        self.problems: list[str] = []
        for lname, spec in layers.items():
            if not isinstance(spec, dict):
                self.problems.append(f"layers.yaml: layer {lname} has no body")
                continue
            if "may_use" not in spec:
                self.problems.append(f"layers.yaml: layer {lname} needs may_use: [...]")
            self.declared[lname] = list(spec.get("skills", []) or [])
            for name in self.declared[lname]:
                self._claim(name, lname)
            for name in spec.get("pending", []) or []:       # known names not on main yet: may appear, classified when they do
                self._claim(name, lname)
            for name in spec.get("bin", []) or []:
                self.bin_names.add(name)
                self._claim("bin/" + name, lname)

    def _claim(self, node: str, lname: str) -> None:
        if node in self.node_layer and self.node_layer[node] != lname:
            self.problems.append(f"layers.yaml: {node} is listed in two layers")
        self.node_layer[node] = lname

    def may_use(self, lname: str) -> set[str]:
        use = (self.layers.get(lname) or {}).get("may_use", [])
        return set(self.layers) if use == ["all"] else set(use or [])

    def allowed(self, src: str, dst: str) -> bool:
        ls, ld = self.node_layer[src], self.node_layer[dst]
        return ls == ld or ld in self.may_use(ls)


def load_model(root: Path) -> Model:
    path = root / "layers.yaml"
    if not path.is_file():
        raise SystemExit(f"no {path}")
    return Model(parse_yaml(path.read_text()).get("layers") or {})


def check_inventory(model: Model, root: Path) -> None:
    """Every skill dir and every bin file is listed; every listed skill exists (pending names may be absent)."""
    skills_root = root / "skills"
    # tool caches and hidden folders (.ruff_cache, __pycache__) aren't skills
    on_disk = {p.name for p in skills_root.iterdir() if p.is_dir() and not p.name.startswith((".", "__"))} if skills_root.is_dir() else set()
    for name in sorted(on_disk):
        if name not in model.node_layer:
            model.problems.append(f"unknown skill {name}: add it to layers.yaml, to the layer it belongs to")
    for lname, names in model.declared.items():
        for name in names:
            in_repo = (root / "skills" / name).is_dir() or (root / ".claude" / "skills" / name).is_dir()
            if not in_repo:
                model.problems.append(f"layers.yaml lists {name} ({lname}) but there is no such skill: remove it")
    bin_dir = root / "bin"
    on_disk_bin = {p.name for p in bin_dir.iterdir() if p.is_file() and p.name != "__pycache__"} if bin_dir.is_dir() else set()
    for name in sorted(on_disk_bin - model.bin_names):
        model.problems.append(f"unknown bin script {name}: add it to layers.yaml under bin: of its layer")
    for name in sorted(model.bin_names - on_disk_bin):
        model.problems.append(f"layers.yaml lists bin/{name} but there is no such file: remove it")


# --- the edges -------------------------------------------------------------------------------------------------------

def _pattern(model: Model) -> re.Pattern:
    names = sorted((n for n in model.node_layer if not n.startswith("bin/")), key=len, reverse=True)
    alt = "|".join(re.escape(n) for n in names) or "(?!)"
    # <name>/ after a non-name character (bare, ../<name>/, $SK/<name>/), or skills/<name> and SK/<name> at any end
    return re.compile(
        rf"(?<![\w.\-])({alt})(?=/)|(?<=skills/)({alt})(?![\w\-])|(?<=SK/)({alt})(?![\w\-])|(?<=SK\}}/)({alt})(?![\w\-])")


def scan_files(root: Path):
    for d in SCAN_DIRS:
        base = root / d
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or "__pycache__" in p.parts:
                continue
            try:
                text = p.read_text()
            except (UnicodeDecodeError, OSError):
                continue
            if "\0" in text:
                continue
            yield p, text


def node_of(root: Path, path: Path) -> str | None:
    rel = path.relative_to(root).parts
    if rel[0] == "bin" and len(rel) == 2:
        return "bin/" + rel[1]
    if rel[0] == "skills" and len(rel) >= 2:
        return rel[1]
    if rel[:2] == (".claude", "skills") and len(rel) >= 3:
        return rel[2]
    return None


def find_edges(model: Model, root: Path):
    """Yields (from, to, relative file, line number, line text, token) for every reference to another node."""
    names_re = _pattern(model)
    bin_re = re.compile(r"(?<![\w.\-])bin/([\w.\-]+)")
    for path, text in scan_files(root):
        src = node_of(root, path)
        if src is None or src not in model.node_layer:
            continue
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(text.splitlines(), 1):
            for m in names_re.finditer(line):
                name = next(g for g in m.groups() if g)
                dst = name
                if dst != src and dst in model.node_layer:
                    yield src, dst, rel, lineno, line, _token(line, m.start())
            for m in bin_re.finditer(line):
                dst = "bin/" + m.group(1)
                if dst != src and dst in model.node_layer:
                    yield src, dst, rel, lineno, line, _token(line, m.start())


def _token(line: str, start: int) -> str:
    end = start
    while end < len(line) and TOKEN_CHARS.match(line[end]):
        end += 1
    return line[start:end]


def read_baseline(root: Path):
    path = root / BASELINE
    entries = []
    if path.is_file():
        for n, raw in enumerate(path.read_text().splitlines(), 1):
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            m = BASELINE_LINE.match(s)
            if not m:
                raise SystemExit(f"{path}:{n}: want '<from> -> <to>: <file>:<pattern>', got: {s}")
            entries.append(((m.group(1), m.group(2), m.group(3)), m.group(4)))
    return entries


def evaluate(model: Model, root: Path):
    """Returns (edges: list of dict, problems: list of str, stale: list of str, baseline_lines: list of str)."""
    edges = []
    for src, dst, rel, lineno, line, token in find_edges(model, root):
        allowed = model.allowed(src, dst)
        edges.append({"from": src, "to": dst, "file": rel, "line": lineno, "text": line, "token": token,
                      "allowed": allowed, "baselined": False})
    baseline = read_baseline(root)
    hits = Counter()
    for e in edges:
        if e["allowed"]:
            continue
        for key, pattern in baseline:
            if key == (e["from"], e["to"], e["file"]) and pattern in e["text"]:
                e["baselined"] = True
                hits[(key, pattern)] += 1
    problems = []
    for e in edges:
        if not e["allowed"] and not e["baselined"]:
            problems.append(
                f"disallowed edge {e['from']} ({model.node_layer[e['from']]}) -> {e['to']} ({model.node_layer[e['to']]}) "
                f"at {e['file']}:{e['line']}: {e['token']}. Remove the reference, or (for a known gap) add to "
                f"{BASELINE}: {e['from']} -> {e['to']}: {e['file']}:{e['token']}")
    stale = []
    for key, pattern in baseline:
        if hits[(key, pattern)] == 0:
            stale.append(f"stale baseline entry, matches no edge now: {key[0]} -> {key[1]}: {key[2]}:{pattern}: delete it")
    lines = sorted({f"{e['from']} -> {e['to']}: {e['file']}:{e['token']}" for e in edges if not e["allowed"]})
    return edges, problems, stale, lines


def report(model: Model, edges: list) -> str:
    out = []
    pair = defaultdict(lambda: [0, 0, 0])      # layer pair -> allowed, baselined, disallowed (edges, not references)
    for e in edges:
        key = (model.node_layer[e["from"]], model.node_layer[e["to"]])
        pair[key][0 if e["allowed"] else 1 if e["baselined"] else 2] += 1
    out.append(f"{'from layer':<12} {'to layer':<12} {'allowed':>8} {'baselined':>10} {'disallowed':>11}")
    for (a, b), (ok, base, bad) in sorted(pair.items(), key=lambda kv: (-kv[1][2], kv[0])):
        out.append(f"{a:<12} {b:<12} {ok:>8} {base:>10} {bad:>11}")
    out.append("")
    out.append("skill and bin edges (references), by from -> to:")
    nodes = defaultdict(lambda: [0, set(), False])
    for e in edges:
        k = (e["from"], e["to"])
        nodes[k][0] += 1
        nodes[k][1].add(e["file"])
        nodes[k][2] = nodes[k][2] or not e["allowed"]
    for (a, b), (n, files, bad) in sorted(nodes.items(), key=lambda kv: (not kv[1][2], kv[0])):
        flag = "" if not bad else "  (not allowed)"
        out.append(f"  {a} -> {b}: {n} refs in {len(files)} file(s){flag}")
    return "\n".join(out)


def layer_paths(model: Model, root: Path, layers: list[str]) -> list[str]:
    """The repo paths of the given layers' skills (skills/<x> or .claude/skills/<x>) and bin files, if they exist."""
    out = []
    for lname in layers:
        if lname not in model.layers:
            raise SystemExit(f"no layer {lname}: layers are {', '.join(model.layers)}")
        spec = model.layers[lname] or {}
        for name in (spec.get("skills") or []) + (spec.get("pending") or []):
            for rel in (f"skills/{name}", f".claude/skills/{name}"):
                if (root / rel).is_dir():
                    out.append(rel)
        for name in spec.get("bin") or []:
            if (root / "bin" / name).is_file():
                out.append(f"bin/{name}")
    return sorted(set(out))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Check the layers in layers.yaml and the one-way rule.")
    ap.add_argument("--report", action="store_true", help="print the edge table")
    ap.add_argument("--baseline", action="store_true", help="print baseline lines for the current disallowed edges")
    ap.add_argument("--paths", metavar="LAYERS", help="print the paths of these layers (comma-separated), one per line")
    ap.add_argument("--root", type=Path, default=R, help="the repo to check (default: this checkout)")
    args = ap.parse_args(argv)
    model = load_model(args.root)
    if args.paths:
        for path in layer_paths(model, args.root, args.paths.split(",")):
            print(path)
        return 0
    check_inventory(model, args.root)
    edges, problems, stale, lines = evaluate(model, args.root)
    if args.report:
        print(report(model, edges))
        print()
    if args.baseline:
        print("\n".join(lines))
        return 0
    bad = model.problems + problems + stale
    for p in bad:
        print(p)
    allowed = sum(e["allowed"] for e in edges)
    base = sum(e["baselined"] for e in edges)
    print(f"layers: {len(edges)} references: {allowed} allowed, {base} baselined, "
          f"{len(problems)} disallowed, {len(stale)} stale baseline entries")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
