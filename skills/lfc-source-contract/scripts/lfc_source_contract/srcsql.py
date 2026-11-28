"""srcsql: SQL on a source through its contract's SQL command (sql_cmd: words that read SQL on stdin, the database as
their first argument), from Python:

    from lfc_source_contract.srcsql import Runner
    src = Runner.from_contract("~/.config/lfc/sources/me_ss1.json")   # or Runner(os.environ["SRC_SQL"])
    src("SELECT 1;", "master")                                        # its output; SourceError when it fails

from_contract goes through source-env.sh, so the command is the one the shell harness would run (the contract's
sql_cmd, else the engine's default client) and the contract's other fields come with it (src.env).
"""
from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

SOURCE_ENV = Path(__file__).resolve().parents[1] / "source-env.sh"


class SourceError(RuntimeError):
    def __init__(self, rc: int, err: str):
        super().__init__(f"source SQL failed (exit {rc}): {err.strip()[-400:]}")
        self.rc, self.err = rc, err


class Runner:
    """Calls the source's SQL command. env: what source-env.sh exported (SRC_ENGINE, CONN_USER, CONNECTION, ...)."""

    def __init__(self, cmd: str, env: dict | None = None):
        if not cmd:
            raise ValueError("no source SQL command (SRC_SQL, or a contract's sql_cmd)")
        self.cmd, self.env = cmd, dict(env or {})
        self.words = shlex.split(cmd)

    @classmethod
    def from_contract(cls, path: str, source_env: str | None = None) -> Runner:
        r = subprocess.run([source_env or str(SOURCE_ENV), os.path.expanduser(path)], capture_output=True, text=True,
                           check=False)
        if r.returncode:
            raise ValueError(f"source-env.sh {path}: {(r.stderr or r.stdout).strip()[-300:]}")
        env = exports(r.stdout)
        return cls(env.get("SRC_SQL", ""), env)

    def run(self, sql: str, database: str = "", check: bool = True) -> subprocess.CompletedProcess:
        p = subprocess.run(self.words + ([database] if database else []), input=sql, text=True, capture_output=True,
                           env={**os.environ, **self.env}, check=False)
        if check and p.returncode:
            raise SourceError(p.returncode, p.stderr or p.stdout)
        return p

    def __call__(self, sql: str, database: str = "") -> str:
        return self.run(sql, database).stdout


def exports(text: str) -> dict:
    """{NAME: value} from `export NAME=value` lines (shell-quoted values), as source-env.sh prints them."""
    out = {}
    for line in text.splitlines():
        words = shlex.split(line)
        if words[:1] == ["export"]:
            for w in words[1:]:
                if "=" in w:
                    k, v = w.split("=", 1)
                    out[k] = v
    return out
