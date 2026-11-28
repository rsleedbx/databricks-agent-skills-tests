"""script_common: what the source skills' Python scripts share (lfc-source-azure-sql, -flexserver, -eventhubs and
-firewall): a stop with its message and exit code (Fail, exits), a required environment variable (need), a password
reference read the way secret-handling's pw.sh reads it (read_password, check_password), a command-line tool run without
a shell (tool: sqlcmd, psql, mysql, uv), the last lines of a tool's output (tail), and the yq-style lines a status prints.

Passwords are returned as text and go to a tool in an environment variable (SQLCMDPASSWORD, PGPASSWORD, MYSQL_PWD) or
a private file, never on a command line.
"""
from __future__ import annotations

import functools
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PW_PY = os.path.join(HERE, "..", "..", "secret-handling", "scripts", "pw.py")
KEYRING = "keyring:"


class Fail(Exception):
    """A stop: its text goes to stderr (when not empty), and the script exits with rc."""

    def __init__(self, rc: int = 1, text: str = ""):
        super().__init__(text)
        self.rc, self.text = rc, text


def exits(main):
    """Wraps main(argv) -> exit code: a Fail becomes its message on stderr and its code."""
    @functools.wraps(main)
    def run(argv):
        try:
            return main(argv)
        except Fail as e:
            if e.text:
                print(e.text, file=sys.stderr)
            return e.rc
    return run


def need(name: str, hint: str = "") -> str:
    """The environment variable's value. Exit 1 when it's empty (the scripts' ${NAME:?})."""
    v = os.environ.get(name, "")
    if not v:
        raise Fail(1, f"{name} is required" + (f": {hint}" if hint else "") + " (see the usage)")
    return v


def tool(argv: list, *, env: dict | None = None, stdin: str | None = None, stdout=subprocess.PIPE,
         merge_stderr: bool = False):
    """Runs a command-line tool (sqlcmd, psql, mysql, uv) with env added to the environment. No shell. Its stdout is
    returned as text unless stdout is None (shown) or subprocess.DEVNULL; its stderr is shown, or merged into stdout."""
    try:
        return subprocess.run(argv, env={**os.environ, **(env or {})}, input=stdin, stdout=stdout, text=True,
                              stderr=subprocess.STDOUT if merge_stderr else None, check=False)
    except FileNotFoundError:
        raise Fail(127, f"{argv[0]}: not on PATH (cli-setup's check-tools.sh local names it, with the install command)") from None


def read_password(ref: str) -> str:
    """The password a reference names: a private file, or keyring:<name> through pw.py (uv runs it, as pw.sh does).
    Its trailing newlines are dropped, as $(pw ...) dropped them."""
    if ref.startswith(KEYRING):
        p = tool(["uv", "run", "-q", "--script", PW_PY, "get", ref[len(KEYRING):]])
        if p.returncode:
            raise Fail(1)   # pw.py has said why
        return p.stdout.rstrip("\n")
    try:
        with open(ref) as f:
            return f.read().rstrip("\n")
    except OSError:
        raise Fail(2, f"can't read {ref}") from None


def check_password(ref: str) -> None:
    """pw_check: the reference can be read; the password isn't printed. Exit 2 when not."""
    if ref.startswith(KEYRING):
        if tool(["uv", "run", "-q", "--script", PW_PY, "get", ref[len(KEYRING):]], stdout=subprocess.DEVNULL).returncode:
            raise Fail(2)
    elif not os.access(ref, os.R_OK):
        raise Fail(2, f"can't read {ref}")


def tail(text: str, n: int) -> str:
    """The last n lines (tail -n)."""
    return "\n".join(text.splitlines()[-n:])


def scalar(v) -> str:
    """A value as the status lines print it (yq's output): null, true, false, or the text."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def print_fields(fields: dict) -> None:
    """One `key: value` line per field, in order."""
    for k, v in fields.items():
        print(f"{k}: {scalar(v)}")
