"""slot: a slot database (lfc-source-cloud-vm's lfc-slot on a source VM) from the laptop, through ssh (your slot key and the VM's forced command, or the VM admin
login as another owner), and its source contract (lfc-source-contract, contract 1).

    s = Slot(host="203.0.113.10", name="dbz-ms", key="~/.ssh/lfc_slot_ed25519")
    conn = s.up("sqlserver")            # {"host", "port", "user", "password", ...}; starts it if parked. Never print it
    s.sql("SELECT 1;")                  # as the engine's admin
    s.container("whoami", admin_key="~/.ssh/<admin key>", owner="me")   # a shell in its container (Db2's ASN tools)

  python3 -I slot.py sql [database] [--env]       SQL from stdin in the slot database (SLOT_HOST, SLOT_NAME, SLOT_KEY,
                                                  or SLOT_OWNER + SLOT_ADMIN_KEY [+ SLOT_ADMIN_USER]); --env: its info.
                                                  SQLCMD_FLAGS with -y asks SQL Server for long values whole
  python3 -I slot.py source [--connection <name>] [--facts <file>] [--out <file>] < up.json
                                                  the contract for a database from `lfc-slot up` (its JSON, the last
                                                  line on stdin), same settings; its sql_cmd is `slot.py sql` for it
Standard library only, and runnable as a file (no package import), so a contract's sql_cmd can name it.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

WORD = re.compile(r"^[A-Za-z0-9_.-]+$")          # what the forced command lets through (slot-ssh.sh)
OWNER = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
WAITS = (5, 15, 30, 60, 120)                     # between tries when ssh itself fails (exit 255)
SLOT_KEY = "~/.ssh/lfc_slot_ed25519"
HERE = Path(__file__).resolve()


class Fail(Exception):
    """The command's end: the message on stderr, then exit rc (2: a usage or setting refused before ssh)."""

    def __init__(self, msg: str | None, rc: int = 1):
        super().__init__(msg or "")
        self.msg, self.rc = msg, rc


class Slot:
    """One slot database. owner + admin_key: another owner's database, through the VM admin login as that owner."""

    def __init__(self, host: str, name: str, key: str = SLOT_KEY, user: str = "slot", owner: str | None = None,
                 admin_user: str = "azureuser", admin_key: str | None = None):
        if not re.match(r"^[A-Za-z0-9.:-]+$", host or ""):
            raise Fail(f"SLOT_HOST is an address: {host}", 2)
        if not WORD.match(name or ""):
            raise Fail(f"a database name is letters, digits, _ . -: {name}", 2)
        if owner is not None and not OWNER.match(owner):
            raise Fail(f"SLOT_OWNER is an owner name (a-z, 0-9, -): {owner}", 2)
        self.host, self.name, self.key, self.user = host, name, os.path.expanduser(key), user
        self.owner, self.admin_user = owner, admin_user
        self.admin_key = os.path.expanduser(admin_key) if admin_key else None

    def ssh(self, key: str, user: str) -> list[str]:
        return ["ssh", "-i", key, "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", f"{user}@{self.host}"]

    def words(self) -> list[str]:
        """ssh and the words before lfc-slot's command: the forced command, or sudo lfc-slot as the owner."""
        if self.owner and self.admin_key:
            return self.ssh(self.admin_key, self.admin_user) + ["sudo", "/usr/local/sbin/lfc-slot", "--owner", self.owner]
        return self.ssh(self.key, self.user)

    def run(self, words: list[str], stdin: str | None = None, capture: bool = True,
            waits=WAITS) -> subprocess.CompletedProcess:
        """lfc-slot's words; ssh's own failures (exit 255, not a refused key) tried again after each wait."""
        for w in words:
            if not WORD.match(w):
                raise Fail(f"a word is letters, digits, _ . -: {w}", 2)
        for wait in (*waits, None):
            p = subprocess.run(self.words() + words, input=stdin, text=True, check=False,
                               stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture else None)
            if p.returncode != 255 or "Permission denied" in (p.stderr or "") or wait is None:
                return p
            print(f"ssh to {self.host} failed; retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
        return p

    def _out(self, words: list[str], stdin: str | None = None) -> str:
        p = self.run(words, stdin)
        if p.returncode:
            raise RuntimeError(f"slot {words[0]} {self.name}: {p.stderr.strip()[-400:]}")
        return p.stdout

    def up(self, engine: str, over_limit: bool = False, src: str = "") -> dict:
        """Start (or find running) the database; its login, with the password. Never print the result whole."""
        words = ["up", engine, self.name] + (["--from", src] if src else ["--empty"]) + \
            (["--over-limit"] if over_limit else [])
        info = json.loads(self._out(words).strip().splitlines()[-1])
        info.setdefault("owner", self.owner)
        return info

    def sql(self, text: str, database: str | None = None, wide: bool = False) -> str:
        """text on the engine's admin client in the database's container; its output."""
        return self._out(["sql", self.name] + ([database] if database else []) + (["--wide"] if wide else []), text)

    def info(self) -> str:
        return self._out(["info", self.name])

    def container_name(self, owner: str) -> str:
        return f"lfc-slot-{owner}-{self.name}"

    def container(self, cmd: str, admin_key: str, owner: str, detached: bool = False, root: bool = True) -> str:
        """cmd in the database's container through the VM admin login (podman exec), for setup beyond SQL; detached:
        left running (a capture agent); root False: as the container's own user (Informix: dbaccess on its PATH)."""
        c = self.container_name(owner)
        remote = (f"sudo podman exec {'-d ' if detached else ''}{'-u 0 ' if root else ''}{shlex.quote(c)} "
                  f"bash -lc {shlex.quote(cmd)}")
        p = subprocess.run(self.ssh(os.path.expanduser(admin_key), self.admin_user) + [remote], text=True,
                           capture_output=True, check=False)
        if p.returncode:
            raise RuntimeError(f"container {c}: {cmd[:80]}: {(p.stderr or p.stdout).strip()[-600:]}")
        return p.stdout

    def setup(self, steps: list, admin_key: str | None = None, owner: str | None = None, log=print) -> None:
        """An engine's setup steps: ("sql", text[, database]), ("shell" | "shell_detached" | "shell_default", text) in
        the container, ("sleep", seconds)."""
        for kind, text, *where in steps:
            log(f"setup {self.name}: {kind} {text.splitlines()[0][:100] if text else ''}")
            if kind == "sql":
                out = self.sql(text, where[0] if where and where[0] else None)
                if out.strip():
                    log("  " + out.strip().replace("\n", "\n  ")[-800:])
            elif kind in ("shell", "shell_detached", "shell_default"):
                if not admin_key or not owner:
                    raise ValueError(f"{self.name}: a shell step needs the VM admin login (admin_key) and the owner")
                out = self.container(text, admin_key, owner, detached=kind == "shell_detached",
                                     root=kind != "shell_default")
                if out.strip():
                    log("  " + out.strip()[-800:])
            elif kind == "sleep":
                time.sleep(int(text))
            else:
                raise ValueError(f"unknown setup step {kind}")


def from_env(name: str | None = None) -> Slot:
    """The Slot the SLOT_* settings name (the contract's sql_cmd sets them)."""
    e = os.environ
    if not e.get("SLOT_HOST") or not (name or e.get("SLOT_NAME")):
        raise Fail("set SLOT_HOST and SLOT_NAME", 2)
    owner = e.get("SLOT_OWNER") or None
    if owner and not e.get("SLOT_ADMIN_KEY"):
        raise Fail("SLOT_OWNER needs SLOT_ADMIN_KEY (the VM admin login)", 2)
    return Slot(e["SLOT_HOST"], name or e["SLOT_NAME"], key=e.get("SLOT_KEY") or SLOT_KEY, owner=owner,
                admin_user=e.get("SLOT_ADMIN_USER") or "azureuser", admin_key=e.get("SLOT_ADMIN_KEY"))


def sql_cmd(s: Slot) -> str:
    """The contract's sql_cmd for s: `slot.py sql` with its settings, as words (none may hold a space)."""
    env = [f"SLOT_HOST={s.host}", f"SLOT_NAME={s.name}"]
    if os.environ.get("SLOT_KEY"):
        env.append(f"SLOT_KEY={s.key}")
    if s.owner:
        env += [f"SLOT_OWNER={s.owner}", f"SLOT_ADMIN_KEY={s.admin_key}", f"SLOT_ADMIN_USER={s.admin_user}"]
    words = ["env", *env, "python3", "-I", str(HERE), "sql"]
    for w in words:
        if re.search(r"\s", w):
            raise Fail(f"{w.split('=')[0]} has a space: the contract's sql_cmd is words", 2)
    return " ".join(words)


def contract(up: dict, s: Slot, connection: str = "", facts: str = "") -> dict:
    """The source contract for the database `lfc-slot up` answered: the password only without a connection."""
    c = {"contract": 1, "name": up["name"], "engine": up["engine"], "host": up["host"], "port": up["port"],
         "user": up["user"]}
    c.update({k: up[k] for k in ("database", "service") if up.get(k)})
    if connection:
        c["connection"] = connection
    elif up.get("password"):
        c["password"] = up["password"]
    c["sql_cmd"] = sql_cmd(s)
    if facts:
        c["facts"] = facts
    return c


def slot_sql(argv: list[str]) -> int:
    """stdin as SQL in the slot database ([database]); --env: its info. The SQL's exit status."""
    s = from_env()
    if "--env" in argv:
        return s.run(["info", s.name], capture=False).returncode
    # the database is the first word; a client's own flags after it (a harness's "$SRC_SQL $DB -F |") are ignored,
    # as slot-sql.sh did
    db = argv[:1] if argv and not argv[0].startswith("-") else []
    wide = "-y" in (os.environ.get("SQLCMD_FLAGS") or "")
    words = ["sql", s.name, *db, *(["--wide"] if wide else [])]
    return s.run(words, sys.stdin.read(), capture=False).returncode


def slot_source(argv: list[str]) -> int:
    conn = facts = out = ""
    a = list(argv)
    while a:
        o = a.pop(0)
        if o in ("--connection", "--facts", "--out") and a:
            v = a.pop(0)
            conn, facts, out = (v, facts, out) if o == "--connection" else (conn, v, out) if o == "--facts" else \
                (conn, facts, v)
        else:
            raise Fail(f"unknown argument {o}", 2)
    lines = [ln for ln in sys.stdin.read().splitlines() if ln.strip()]
    if not lines:
        raise Fail("no lfc-slot up JSON on stdin", 2)
    up = json.loads(lines[-1])
    if not OWNER.match(up.get("name") or ""):
        raise Fail("the up JSON has no database name", 2)
    if facts:
        if not os.access(facts, os.R_OK):
            raise Fail(f"no facts file {facts}", 2)
        facts = str(Path(facts).resolve())
    c = json.dumps(contract(up, from_env(up["name"]), conn, facts), separators=(",", ":"))
    if not out:
        if not conn:
            raise Fail("without --connection the contract holds the password: give --out <file>", 2)
        print(c)
        return 0
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(c + "\n")
    os.chmod(out, 0o600)
    print(out)
    return 0


def main(cmd: str, argv: list[str]) -> int:
    try:
        if cmd == "sql":
            return slot_sql(argv)
        if cmd == "source":
            return slot_source(argv)
        print(__doc__.strip(), file=sys.stderr)
        return 2
    except Fail as x:
        if x.msg:
            print(f"slot-{cmd}: {x.msg}", file=sys.stderr)
        return x.rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "", sys.argv[2:]))
