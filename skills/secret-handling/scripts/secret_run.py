#!/usr/bin/env python3
"""secret_run.py <file> <field|-> <ENV_NAME> -- <command> [args...]: run one command with a secret in an environment
variable, without printing it. <file> is JSON or YAML (field is a yq path, for example .dba.password) or a plain
password file (field -: the whole file, trailing newlines removed). The value goes only into ENV_NAME for that one
command; nothing is echoed, logged or written.
  Examples:
    python3 secret_run.py creds.json .dba.password SQLCMDPASSWORD -- sqlcmd -S host,1433 -U admin -Q "SELECT 1"
    python3 secret_run.py ~/.secrets/pg.pw - PGPASSWORD -- psql -h host -U admin -c 'SELECT 1'
"""
from __future__ import annotations

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import run


def read_secret(path: str, field: str) -> str:
    """The secret at field of a JSON/YAML file (yq reads it; the value is returned, never printed), or the whole file
    for field '-'. '' when there is no value. Trailing newlines are removed, as $(...) would."""
    if field == "-":
        with open(path) as fh:
            return fh.read().rstrip("\n")
    rc, out, _ = run(["yq", "-r", f'{field} // ""', path], 30)
    return out.rstrip("\n") if rc == 0 else ""


def main(argv: list[str]) -> int:
    if len(argv) < 5 or argv[3] != "--":
        print(__doc__.split("\n\n")[0] + "\n\nusage: secret_run.py <file> <field|-> <ENV_NAME> -- <command> [args...]",
              file=sys.stderr)
        return 2
    path, field, name, cmd = argv[0], argv[1], argv[2], argv[4:]
    if not os.access(path, os.R_OK):
        print(f"secret-run: can't read {path}", file=sys.stderr)
        return 2
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        print(f"secret-run: {name} isn't a variable name", file=sys.stderr)
        return 2
    value = read_secret(path, field)
    if not value:
        print(f"secret-run: {path} has no value at {field}", file=sys.stderr)
        return 2
    env = dict(os.environ, **{name: value})
    del value
    try:
        os.execvpe(cmd[0], cmd, env)
    except FileNotFoundError:
        print(f"secret-run: {cmd[0]}: not found", file=sys.stderr)
        return 127


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
