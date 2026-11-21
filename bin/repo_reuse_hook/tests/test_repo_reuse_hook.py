# Vendored copy of shared unit 'repo_reuse_hook': edit the master, then run `reuse-first vendor sync` (any other edit shows as drift).
"""repo_reuse_hook: asks before an edit adds a function like one the repo has (stdlib + pytest; needs git)."""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE / "scripts"), str(HERE.parent / "reuse_index" / "scripts")]   # a copy, or the masters

import repo_reuse_hook as h

EXISTING = '''import subprocess


def table_rows(profile, sql):
    """One SQL statement through the Databricks CLI, its rows."""
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", profile,
                          "--json", sql], capture_output=True, text=True, check=True).stdout
    rows = out.splitlines()
    return [r.split("\\t") for r in rows if r]
'''
COPY = '''import subprocess


def run_sql(profile, statement):
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", profile,
                          "--json", statement], capture_output=True, text=True, check=True).stdout
    lines = out.splitlines()
    return [x.split("\\t") for x in lines if x]
'''
OTHER = '''def greet(name):
    """Hello to a name, three ways."""
    a = f"hi {name}"
    b = a.upper()
    return [a, b, a + "!"]
'''


@pytest.fixture
def repo(tmp_path: Path, monkeypatch) -> Path:
    r = tmp_path / "r"
    (r / "lib").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    (r / "lib/sql.py").write_text(EXISTING)
    subprocess.run(["git", "-C", str(r), "add", "."], check=True)
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", str(r))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("REUSE_FIRST_HOME", str(tmp_path / "none"))   # no personal config: the defaults
    monkeypatch.delenv(h.GLOBAL_ENV, raising=False)
    return r


def run(event: dict, monkeypatch, capsys) -> str:
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(event)))
    assert h.main() == 0
    out = capsys.readouterr().out
    return json.loads(out)["hookSpecificOutput"]["permissionDecisionReason"] if out else ""


def write(path: Path, content: str, session: str = "s1") -> dict:
    return {"tool_name": "Write", "tool_input": {"file_path": str(path), "content": content}, "session_id": session}


def test_a_copy_of_a_repo_function_asks_once_per_session(repo: Path, monkeypatch, capsys) -> None:
    reason = run(write(repo / "tool.py", COPY), monkeypatch, capsys)
    assert "`run_sql` is like `table_rows` in lib/sql.py:4" in reason
    assert run(write(repo / "tool.py", COPY), monkeypatch, capsys) == ""             # asked already
    assert run(write(repo / "tool.py", COPY, "s2"), monkeypatch, capsys)             # a new session asks again


def test_silent_for_new_code_tests_other_repos_and_when_a_broader_hook_runs(repo: Path, tmp_path: Path,
                                                                             monkeypatch, capsys) -> None:
    assert run(write(repo / "greet.py", OTHER), monkeypatch, capsys) == ""
    assert run(write(repo / "tests/test_x.py", COPY), monkeypatch, capsys) == ""
    assert run(write(tmp_path / "elsewhere/tool.py", COPY), monkeypatch, capsys) == ""
    monkeypatch.setenv(h.GLOBAL_ENV, "1")
    assert run(write(repo / "tool.py", COPY), monkeypatch, capsys) == ""


def test_an_edit_counts_only_the_functions_it_adds(repo: Path, monkeypatch, capsys) -> None:
    (repo / "tool.py").write_text(COPY)
    edit = {"tool_name": "Edit", "session_id": "s1", "tool_input": {
        "file_path": str(repo / "tool.py"), "old_string": "lines = out", "new_string": "lines = out  # rows"}}
    assert run(edit, monkeypatch, capsys) == ""                                      # run_sql was there before


def test_a_bad_event_lets_the_edit_through(repo: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json"))
    assert h.main() == 0 and capsys.readouterr().out == ""
