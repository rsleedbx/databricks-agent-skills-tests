# Vendored copy of shared unit 'repo_reuse_hook': edit the master, then run `reuse-first vendor sync` (any other edit shows as drift).
"""Claude Code PreToolUse hook: ask before an edit adds a function like one this repo already has.

    .claude/settings.json, PreToolUse, matcher Write|Edit|MultiEdit:
    f="$CLAUDE_PROJECT_DIR/<this folder>/repo_reuse_hook.py"; [ ! -f "$f" ] || python3 -I "$f"

For a code file (Python, shell, JavaScript/TypeScript; tests left out) in the session's repo, it works out the
file's text after the edit, takes the functions the edit adds, and compares each with this repo's own functions
(reuse_index next to it: weighted body tokens, and the commands, calls and API paths both run). On a match it asks,
naming the existing function and why it matched; the user approves or not. Asked once per function per session.
Otherwise silent. Any error lets the edit through, so a bug here can't stop work.

Only this repo is compared: nothing is read from other repos and nothing is sent anywhere.
Silent when REUSE_FIRST_GLOBAL is set: a broader reuse hook of your own runs instead (one that also compares with
your other repos), so each edit is checked once.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reuse_index as ri  # the shared unit next to this file

GLOBAL_ENV = "REUSE_FIRST_GLOBAL"
MAX_FUNCTIONS = 4   # added functions compared per edit


def asked_file() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "repo-reuse-hook" / "asked.json"


def first_time(session: str, keys: list[str]) -> list[str]:
    """The keys not yet asked about in this session; records them (one session's keys are kept)."""
    f = asked_file()
    try:
        seen = json.loads(f.read_text())
    except (OSError, ValueError):
        seen = {}
    done = set(seen.get(session, [])) if isinstance(seen, dict) else set()
    new = [k for k in keys if k not in done]
    if new:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps({session: sorted(done | set(new))}))
    return new


def check(event: dict[str, Any]) -> str:
    """The reason to ask about this tool call, or '' to let it through silently."""
    tool, inp = event.get("tool_name", ""), event.get("tool_input") or {}
    if tool not in ("Write", "Edit", "MultiEdit") or not inp.get("file_path"):
        return ""
    path = Path(inp["file_path"])
    project = os.environ.get("CLAUDE_PROJECT_DIR") or event.get("cwd") or ""
    root = ri.repo_root(Path(project)) if project else None
    if not root or not path.is_absolute() or not path.resolve().is_relative_to(root.resolve()):
        return ""   # only the session's repo: this hook knows no other
    rel = str(path.resolve().relative_to(root.resolve()))
    if not ri.is_code(path) or ri.is_test(rel):
        return ""
    old = path.read_text(errors="replace") if path.is_file() else ""
    new = ri.new_text(tool, inp, old)
    added = ri.added_functions(old, new or "", rel)[:MAX_FUNCTIONS]
    if not added:
        return ""
    rows = [r for r in ri.index_repo(root.name, root) if r.get("kind", "function") == "function"]
    idf = ri.idf_table(rows)
    found = []
    for f in added:
        for score, r, why in ri.similar(f["name"], f["body"], rows, skip=(str(root), rel), idf=idf):
            line = f"`{f['name']}` is like `{r['name']}` in {r['path']}:{r['line']} ({why}, {score:.0%})"
            found.append((f"{rel}:{f['name']}", line))
    if not found:
        return ""
    fresh = set(first_time(str(event.get("session_id") or ""), sorted({k for k, _ in found})))
    lines = [line for k, line in found if k in fresh]
    if not lines:
        return ""
    return ("This edit adds code like code this repo already has:\n- " + "\n- ".join(lines) +
            "\nUse or extend the existing function instead of a second one, unless the job differs.")


def main() -> int:
    if os.environ.get(GLOBAL_ENV):
        return 0
    try:
        reason = check(json.load(sys.stdin))
    except Exception:  # noqa: BLE001  never block an edit on a hook bug
        return 0
    if reason:
        ri.ask(reason)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
