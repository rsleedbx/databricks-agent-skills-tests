#!/usr/bin/env python3
"""source_env.py <source.json>: validate a source contract (contract 1, lfc-source-contract) and print the exports the
tests use: eval "$(python3 -I source_env.py source.json)". Exit 2 with one line when the file breaks the contract (a
missing field, a bad engine or port, a contract version this doesn't read).

Replaces source-env.sh (kept as a two-line shim: saved contracts and other repos call that path).
Exports SRC_ENGINE, SRC_HOST, SRC_PORT, SRC_DB (the database, or the Oracle service), SRC_NAME, CONN_USER (the login),
SRC_CONTRACT (the file, absolute), and SRC_SQL, CONNECTION and SRC_FACTS when the contract gives them. SRC_SQL is the
contract's sql_cmd, else sql_client.py (the default client for the engine, which reads the password from SRC_CONTRACT
itself). Variables the contract doesn't give are left as they are. The password is never printed.
engine kafka (a Kafka broker, not a database): also SRC_TOPIC, SRC_SASL (default PLAIN) and SRC_PASSWORD_FILE (the
file holding the SASL password, absolute); no SRC_SQL.
Standard library only. Runs as a file (the contract's sql_cmd and SRC_SQL name files, not the package).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ENGINES = ["sqlserver", "postgres", "mysql", "oracle", "mariadb", "db2", "informix", "cockroachdb", "yugabytedb", "kafka"]
DB_ENGINES = ["db2", "informix", "cockroachdb", "yugabytedb"]
CLIENT_ENGINES = ["sqlserver", "postgres", "mysql", "mariadb", "oracle", "cockroachdb", "yugabytedb"]
OPTIONAL_STRINGS = ["database", "service", "password", "sql_cmd", "connection", "facts", "name", "topic",
                    "sasl_mechanism", "password_file"]
SQL_CLIENT = Path(__file__).resolve().parent / "sql_client.py"


class Refused(Exception):
    """The contract is refused: the message on stderr, exit 2."""


def usage() -> str:
    return ("usage: source_env.py <source.json>   (eval \"$(python3 -I source_env.py source.json)\")\n"
            "Validates a source contract (contract 1) and prints the exports the tests use.\n")


def _present(d: dict, key: str):
    """jq's `.key // null`: a missing, null or false value is absent."""
    v = d.get(key)
    return None if v is None or v is False else v


def _is_string(v) -> bool:
    return isinstance(v, str)


def _port_ok(v) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool) and v == int(v) and 1 <= v <= 65535)


def problems(d: dict) -> list:
    """Every problem with a parsed contract, in the order the checks are made; empty when the contract holds."""
    out = []
    engine = _present(d, "engine")
    engine = "" if engine is None else str(engine)
    if len(engine) == 0:
        out.append("missing engine")
    elif engine not in ENGINES:
        out.append(f"engine {engine} is not one of " + ", ".join(ENGINES))
    for key in ("host", "user"):
        v = d.get(key)
        if not (_is_string(v) and len(v) > 0):
            out.append(f"missing {key}")
    if not _port_ok(d.get("port")):
        out.append("missing port" if "port" not in d else "port must be a whole number from 1 to 65535")
    if engine == "oracle":
        if not (_is_string(d.get("service")) and d["service"]):
            out.append("missing service")
    elif engine in DB_ENGINES:
        if not (_is_string(d.get("database")) and d["database"]):
            out.append("missing database")
    elif engine == "kafka" and not (_is_string(d.get("topic")) and d["topic"]):
        out.append("missing topic")
    for key in OPTIONAL_STRINGS:
        if key in d and d[key] is not None and not _is_string(d[key]):
            out.append(f"{key} must be a string")
    return out


def _abs(p: str, base: Path) -> str:
    return p if p.startswith("/") else str(base / p)


def _q(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def exports(path: str, d: dict) -> tuple:
    """(lines to print on stdout, warnings for stderr) for a valid contract file at path."""
    abs_path = str(Path(os.path.abspath(path)))
    base = Path(abs_path).parent
    lines, warn = [], []

    def out(name, value):
        lines.append(f"export {name}={_q(str(value))}")

    engine = str(_present(d, "engine") or "")
    port = d.get("port")
    out("SRC_ENGINE", engine)
    out("SRC_HOST", d.get("host") or "")
    out("SRC_PORT", str(int(port)))
    out("SRC_DB", _present(d, "database") or _present(d, "service") or "")
    out("SRC_NAME", _present(d, "name") or "")
    out("CONN_USER", d.get("user") or "")
    out("SRC_CONTRACT", abs_path)
    conn = _present(d, "connection") or ""
    if conn:
        out("CONNECTION", conn)
    facts = _present(d, "facts") or ""
    if facts:
        out("SRC_FACTS", _abs(facts, base))
    has_pw = len(_present(d, "password") or "")
    cmd = _present(d, "sql_cmd") or ""

    if engine == "kafka":   # a broker: no SQL client
        out("SRC_TOPIC", _present(d, "topic") or "")
        out("SRC_SASL", _present(d, "sasl_mechanism") or "PLAIN")
        pwf = _present(d, "password_file") or ""
        if pwf:
            out("SRC_PASSWORD_FILE", _abs(pwf, base))
        return lines, warn

    has_client = engine in CLIENT_ENGINES
    if cmd:
        out("SRC_SQL", cmd)
    elif has_client and (has_pw != 0 or engine in ("cockroachdb", "yugabytedb")):
        out("SRC_SQL", str(SQL_CLIENT))   # insecure CockroachDB and YugabyteDB have no password
    elif has_client:
        warn.append(f"{path}: no sql_cmd and no password: SRC_SQL is not set (the default client needs the password)")
    else:
        warn.append(f"{path}: no default SQL client for {engine}: SRC_SQL is not set (give sql_cmd)")
    return lines, warn


def main(argv: list) -> int:
    if not argv:
        sys.stdout.write(usage())
        return 2
    path = argv[0]

    def bad(msg: str) -> int:
        print(f"source-env: {path}: {msg}", file=sys.stderr)
        return 2

    if not os.path.isdir(os.path.dirname(os.path.abspath(path))):
        print(f"source-env: no such folder for {path}", file=sys.stderr)
        return 2
    if not os.access(path, os.R_OK) or os.path.isdir(path):
        return bad("can't read it")
    try:
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
    except (OSError, ValueError):
        return bad("not a JSON object")
    if not isinstance(d, dict):
        return bad("not a JSON object")
    if "contract" not in d or d["contract"] is None:
        return bad('missing contract (the version: "contract": 1)')
    cv = d["contract"]
    if not (type(cv) is int and cv == 1):
        print(f"source-env: {path} is contract {json.dumps(cv)}; this reads contract 1 only: use a source-env.sh "
              f"that reads {json.dumps(cv)}, or have the producer write contract 1", file=sys.stderr)
        return 2
    found = problems(d)
    if found:
        return bad(", ".join(found))
    lines, warn = exports(path, d)
    for w in warn:
        print(f"source-env: {w}", file=sys.stderr)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
