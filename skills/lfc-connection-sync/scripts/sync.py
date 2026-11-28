#!/usr/bin/env python3
"""sync.py <targets.tsv>: create or update one UC connection per row, in each row's workspace. Idempotent: rerun after a fix.
  targets.tsv (keep it private, umask 077), tab-separated, one row per connection per workspace, '#' comments allowed:
    profile  name  connection_type  host  port  user  password_file  [extra options as JSON, e.g. {"service_name": "FREE"}]
  Every connection gets trustServerCertificate=true (TLS on, the server certificate not verified) unless the extra options
  give userProvidedServerCertificate (a CA PEM, verified): a pipeline created after TLS enforcement fails a MySQL
  connection with neither (INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE), and with both.
  password_file is a private file or keyring:<name> (secret-handling scripts/pw.py).
  Prints one line per row: created | updated | FAILED <class>: <error>; after created/updated, keep_catalogs.py's lines for
  the connection and its foreign catalogs (RemoveAfter a month out). The password is read from its file into a request
  body that's dropped right away; it never appears on a command line or in the output.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for rel in ("../../lfc-rest/scripts", "../../secret-handling/scripts"):
    sys.path.insert(0, os.path.join(HERE, rel))
from lfc_rest import Rest
from pw import read_ref

KEEP_CATALOGS = os.path.join(HERE, "..", "..", "lfc-workspace-setup", "scripts", "keep_catalogs.py")
CONNECTIONS = "/api/2.1/unity-catalog/connections"


def failure_class(text: str) -> str:
    """The class of a connection error, from its message (what the operator acts on)."""
    t = text
    if "CREATE CONNECTION" in t or "CREATE_CONNECTION" in t:
        return "no_create_connection"
    if "does not have" in t or "PERMISSION_DENIED" in t:
        return "permission"
    if "limit" in t or "LIMIT" in t or "quota" in t:
        return "connection_limit"
    if "not supported" in t or "does not support" in t:
        return "unsupported_option"
    if any(x in t for x in ("invalid access token", "token", "401", "Unauthorized")):
        return "auth"
    return "other"


def options_for(row: dict, password: str) -> dict:
    """The connection's options: the row's host, port and user, the password, TLS on (see the header), and the extra
    options from the row, which win over these."""
    opts = {"host": row["host"], "port": row["port"], "user": row["user"], "password": password,
            "trustServerCertificate": "true"}
    opts.update(json.loads(row["extra"] or "{}"))
    if "userProvidedServerCertificate" in opts:
        opts.pop("trustServerCertificate", None)
    return opts


def parse_rows(path: str):
    """One dict per connection row; blank and '#' lines skipped. Empty columns stay empty (split on tab, not runs)."""
    names = ("profile", "name", "connection_type", "host", "port", "user", "password_file", "extra")
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            cols = (line.split("\t") + [""] * 8)[:8]
            yield dict(zip(names, cols))


def sync_row(api: Rest, row: dict) -> str:
    """created | updated | FAILED ...: one connection in one workspace; keep_catalogs' lines follow when it worked."""
    pw_ref = row["password_file"]
    password = read_ref(pw_ref)
    if password is None:
        return f"{row['profile']} {row['name']}: FAILED missing_password: {pw_ref}"
    body_options = options_for(row, password)
    del password
    path = f"{CONNECTIONS}/{row['name']}"
    try:
        api.get(path)
        exists = True
    except Exception:  # noqa: BLE001
        exists = False
    try:
        if exists:
            api.patch(path, {"options": body_options})
            op = "updated"
        else:
            api.post(CONNECTIONS, {"name": row["name"], "connection_type": row["connection_type"],
                                   "options": body_options})
            op = "created"
    except Exception as e:  # noqa: BLE001
        text = str(e)
        first = next((x for x in text.splitlines() if "error" in x.lower()), text.splitlines()[0] if text else "")
        first = first[:200]
        return f"{row['profile']} {row['name']}: FAILED {failure_class(first)}: {first}"
    out = [f"{row['profile']} {row['name']}: {op}"]
    res = subprocess.run([sys.executable, "-I", KEEP_CATALOGS, row["profile"], "--connection", row["name"]],
                         capture_output=True, text=True, check=False)
    out += ["  " + line for line in res.stdout.splitlines()]
    return "\n".join(out)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__.split("\n\n")[0] + "\n\nusage: sync.py <targets.tsv>", file=sys.stderr)
        return 2
    apis: dict[str, Rest] = {}
    for row in parse_rows(argv[0]):
        if row["profile"] not in apis:
            apis[row["profile"]] = Rest.connect(row["profile"])
        print(sync_row(apis[row["profile"]], row), flush=True)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    sys.exit(main(sys.argv[1:]))
